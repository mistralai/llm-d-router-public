#!/usr/bin/env python3
"""
EPP single-request trace analyzer.

Prints a per-endpoint breakdown for one x-request-id:
  - Token-load score for every endpoint that reached the scorer
  - GPU utilization (avg across GPUs) at request time
  - Which pod was selected, which were filtered out before scoring

Usage:
    python3 epp_request_trace.py --request-id 46993c86-a81f-4cd0-b7df-144bc0b31cbb
    python3 epp_request_trace.py --request-id <id> --since-hours 2

    # Run over multiple traces:
    for id in <id1> <id2> <id3>; do
        python3 epp_request_trace.py --request-id $id
    done

Environment:
    GRAFANA_URL                    Base URL of your Grafana instance
    GRAFANA_SERVICE_ACCOUNT_TOKEN  Grafana service account bearer token
"""

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone

import requests

LOKI_UID = "41c0108a-fc72-48e9-af7f-39185a9e4787"
PROMETHEUS_UID = "17540100-48ea-4637-bfe1-2ef50e76363f"
DEFAULT_APP = "glm-5-2-precise-epp"
DEFAULT_NAMESPACE = "vortex"
DEFAULT_POD_PATTERN = "glm-5-2-vllm-public.*"


def _loki_query_range(
    session: requests.Session,
    grafana_url: str,
    logql: str,
    start_ns: int,
    end_ns: int,
    limit: int = 10,
) -> list[dict]:
    url = f"{grafana_url}/api/datasources/proxy/uid/{LOKI_UID}/loki/api/v1/query_range"
    resp = session.get(
        url,
        params={
            "query": logql,
            "start": start_ns,
            "end": end_ns,
            "limit": limit,
            "direction": "backward",
        },
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json().get("data", {}).get("result", [])


def _prometheus_instant(
    session: requests.Session,
    grafana_url: str,
    promql: str,
    time_s: float,
) -> list[dict]:
    url = f"{grafana_url}/api/datasources/proxy/uid/{PROMETHEUS_UID}/api/v1/query"
    resp = session.get(url, params={"query": promql, "time": int(time_s)}, timeout=30)
    resp.raise_for_status()
    return resp.json().get("data", {}).get("result", [])


def _strip_rank(name: str) -> str:
    if "-rank-" in name:
        return name[: name.rfind("-rank-")]
    return name


def fetch_picker_entry(
    session: requests.Session,
    grafana_url: str,
    app: str,
    namespace: str,
    request_id: str,
    since_hours: float,
) -> tuple[int | None, dict | None]:
    """Return (timestamp_ns, parsed_log_entry) for the picker log of this request ID."""
    logql = (
        f'{{app="{app}", namespace="{namespace}"}}'
        f' | json | x_request_id="{request_id}"'
        f' | msg="Selecting endpoints from candidates sorted by max score"'
    )
    now = datetime.now(timezone.utc)
    end_ns = int(now.timestamp() * 1e9)
    start_ns = int((now - timedelta(hours=since_hours)).timestamp() * 1e9)

    streams = _loki_query_range(session, grafana_url, logql, start_ns, end_ns, limit=3)
    for stream in streams:
        for ts_str, line in stream.get("values", []):
            return int(ts_str), json.loads(line)
    return None, None


def print_trace(
    session: requests.Session,
    grafana_url: str,
    args: argparse.Namespace,
) -> None:
    print(f"  Looking up {args.request_id}...", file=sys.stderr)
    ts_ns, entry = fetch_picker_entry(
        session, grafana_url, args.app, args.namespace, args.request_id, args.since_hours
    )
    if entry is None:
        print(
            f"ERROR: picker log not found for {args.request_id} in last {args.since_hours}h",
            file=sys.stderr,
        )
        return

    ts_s = ts_ns / 1e9
    req_dt = datetime.fromtimestamp(ts_s, tz=timezone.utc)
    num_candidates = entry.get("num-of-candidates", "?")
    scored_raw = entry.get("scored-endpoints", [])

    # Build ordered candidate map: pod_name -> score. Index 0 is the winner.
    candidates: dict[str, float] = {}
    winner_name: str | None = None
    for i, se in enumerate(scored_raw):
        name = _strip_rank(se.get("Endpoint", {}).get("Name", ""))
        score = se.get("Score", float("nan"))
        if name:
            candidates[name] = score
            if i == 0:
                winner_name = name

    # GPU utilization at request time — one series per GPU (8 per pod)
    print("  Fetching GPU utilization...", file=sys.stderr)
    gpu_query = (
        f'DCGM_FI_DEV_GPU_UTIL{{'
        f'namespace="{args.namespace}", pod=~"{args.pod_pattern}"}}'
    )
    gpu_results = _prometheus_instant(session, grafana_url, gpu_query, ts_s)
    gpu_by_pod: dict[str, list[float]] = {}
    for r in gpu_results:
        pod = r["metric"].get("pod", "")
        val = r.get("value")
        if pod and val:
            gpu_by_pod.setdefault(pod, []).append(float(val[1]))
    # (min, avg, max) per pod across all GPUs
    gpu_stats: dict[str, tuple[float, float, float]] = {
        pod: (min(vals), sum(vals) / len(vals), max(vals))
        for pod, vals in gpu_by_pod.items()
    }

    # Build ordered pod list:
    #   1. winner first
    #   2. other candidates sorted by GPU util (avg) descending
    #   3. filtered-out pods sorted by GPU util (avg) descending
    all_pod_names = set(list(candidates) + list(gpu_stats))
    common_prefix = os.path.commonprefix(sorted(all_pod_names)) if all_pod_names else ""

    def _gpu_desc(pod: str) -> float:
        return -gpu_stats.get(pod, (0.0, 0.0, 0.0))[1]

    other_candidates = sorted(
        (p for p in candidates if p != winner_name), key=_gpu_desc
    )
    filtered_out = sorted(
        (p for p in gpu_stats if p not in candidates), key=_gpu_desc
    )
    all_pods = (
        ([winner_name] if winner_name else []) + other_candidates + filtered_out
    )

    # ── Header ──────────────────────────────────────────────────────────────
    sep = "=" * 74
    winner_suffix = winner_name[len(common_prefix) :] if winner_name else "?"
    winner_score = candidates.get(winner_name, float("nan")) if winner_name else float("nan")
    _winner_stats = gpu_stats.get(winner_name, (float("nan"), float("nan"), float("nan"))) if winner_name else (float("nan"), float("nan"), float("nan"))
    winner_gpu_mn, winner_gpu_av, winner_gpu_mx = _winner_stats

    print()
    print(sep)
    print(f"  Request  : {args.request_id}")
    print(f"  Time     : {req_dt.strftime('%Y-%m-%dT%H:%M:%SZ')}")
    print(
        f"  Winner   : ...{winner_suffix}"
        f"   score={winner_score:.7f}"
        f"   GPU={winner_gpu_mn:.0f}/{winner_gpu_av:.0f}/{winner_gpu_mx:.0f}%"
    )
    print(
        f"  Scored   : {len(candidates)} candidates"
        f"   ({num_candidates} passed to picker)"
        f"   ({len(gpu_stats) - len(candidates)} filtered out)"
    )
    print(f"  Prefix   : {common_prefix}")
    print(sep)

    # ── Table ────────────────────────────────────────────────────────────────
    suf_w = max((len(p[len(common_prefix) :]) for p in all_pods), default=10) + 1
    gpu_col_w = 13  # "100/100/100%" = 12 chars + 1 padding
    print(f"  {'Suffix':<{suf_w}}  {'Score':>10}  {'GPU min/avg/max':>{gpu_col_w}}  Status")
    print("  " + "-" * (suf_w + 10 + gpu_col_w + 22))

    prev_section = None
    for pod in all_pods:
        suffix = pod[len(common_prefix) :]
        score = candidates.get(pod)
        stats = gpu_stats.get(pod)

        if pod == winner_name:
            section = "selected"
        elif score is not None:
            section = "candidate"
        else:
            section = "filtered"

        if prev_section is not None and section != prev_section:
            print()

        score_s = f"{score:>10.7f}" if score is not None else f"{'(filtered)':>10}"
        if stats is not None:
            mn, av, mx = stats
            gpu_s = f"{mn:.0f}/{av:.0f}/{mx:.0f}%".rjust(gpu_col_w)
        else:
            gpu_s = "n/a".rjust(gpu_col_w)

        if section == "selected":
            status = "<-- SELECTED"
        elif section == "candidate":
            status = "candidate"
        else:
            status = "filtered out"

        print(f"  {suffix:<{suf_w}}  {score_s}  {gpu_s}  {status}")
        prev_section = section

    print()


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--request-id", required=True, help="x-request-id to analyze")
    parser.add_argument("--app", default=DEFAULT_APP, help="Loki app label")
    parser.add_argument("--namespace", default=DEFAULT_NAMESPACE)
    parser.add_argument(
        "--pod-pattern",
        default=DEFAULT_POD_PATTERN,
        help="Prometheus pod regex for GPU util (default: glm-5-2-vllm-public.*)",
    )
    parser.add_argument(
        "--since-hours",
        type=float,
        default=24.0,
        help="How far back to search Loki for the request (default: 24h)",
    )
    args = parser.parse_args()

    grafana_url = os.environ.get("GRAFANA_URL", "").rstrip("/")
    token = os.environ.get("GRAFANA_SERVICE_ACCOUNT_TOKEN", "")
    if not grafana_url or not token:
        print("ERROR: set GRAFANA_URL and GRAFANA_SERVICE_ACCOUNT_TOKEN", file=sys.stderr)
        sys.exit(1)

    session = requests.Session()
    session.headers["Authorization"] = f"Bearer {token}"

    print_trace(session, grafana_url, args)


if __name__ == "__main__":
    main()
