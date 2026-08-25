# Data Parallel Handlers

## Data Parallel Profile Handler

**Type:** `data-parallel-profile-handler`

> **Deprecated:** Use `single-profile-handler` with Istio >= 1.28.1 instead. See [single/](../single/).

Provides a profile handler for data-parallel inference routing, where a request is scheduled to one pod among multiple replicas serving the same model. Injects the `X-Data-Parallel-Endpoint` header pointing to the selected pod and rewrites the target port to `primaryPort`.

**Constraints:**
- Requires exactly one scheduling profile in the config.

**Parameters:**
- `primaryPort` (int, optional, default: `8000`): Primary service port (1–65535).

**Configuration Example:**
```yaml
plugins:
  - type: data-parallel-profile-handler
    name: dp-handler
    parameters:
      primaryPort: 8000
```

**Migration:** Replace with `single-profile-handler` (requires Istio >= 1.28.1):

**Before:**
```yaml
plugins:
  - type: data-parallel-profile-handler
    parameters:
      primaryPort: 8000
```

**After:**
```yaml
plugins:
  - type: single-profile-handler
```

## DP Rank Header Handler

**Type:** `dp-rank-header-handler`

Pins a request to the selected logical endpoint's rank by setting
`x-data-parallel-rank` after endpoint selection. This is for vLLM Internal and
Hybrid LB deployments where multiple local ranks share one serving endpoint.

When this handler is configured, EPP detects each pod's local rank count from
the `engine` labels on `vllm:cache_config_info`. This makes each `(pod,
rank)` independently schedulable while retaining the pod's shared serving
address. Pods with different rank counts can share one InferencePool.

`--endpoint-data-parallel-size`, or the equivalent Helm value
`router.modelServers.dataParallelSize`, provides a fallback when metrics do not
expose the rank count. The handler pins every request to the rank selected by
the normal scheduling pipeline.

When precise prefix cache routing is enabled, logical endpoint metadata also
selects each rank's KV-event socket. The producer's
`kvEventsConfig.podDiscoveryConfig.dataParallelSize` setting is only required
by the legacy one-endpoint-per-pod mode. Do not use this handler with External
LB; each rank is already a separate network endpoint there.

```yaml
# Helm values
router:
  modelServers:
    dataParallelSize: 8 # optional fallback

# EndpointPickerConfig
plugins:
  - type: precise-prefix-cache-producer
    parameters:
      kvEventsConfig:
        discoverPods: true
        podDiscoveryConfig:
          socketPort: 5557
  - type: dp-rank-header-handler
```

The handler exports `llm_d_epp_dp_rank_routing_total`. The `decision` label is
`endpoint` when EPP pins the selected logical endpoint's rank. `precise_kv` and
`vllm_internal` describe the legacy one-endpoint-per-pod fallback, depending on
whether precise cache state selected a rank. The `rank` label contains the
pinned rank or `none`.

```promql
sum(rate(llm_d_epp_dp_rank_routing_total{decision="endpoint"}[5m]))
```

---

## Related Documentation
- [SingleProfileHandler](../single/)
- [Disagg Profile Handler](../disagg/)
