"""Pre-flight diagnostics run before a rebuild."""

from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path

from mistral_release.gitcmd import git_out, is_ancestor


def parse_slug(url: str) -> str | None:
    """Returns the ``owner/repo`` slug from a git remote URL, or None."""
    match = re.search(r"[:/]([^/:]+/[^/:]+?)(?:\.git)?/?$", url)
    return match.group(1) if match else None


def upstream_slug(upstream_remote: str, cwd: Path) -> str | None:
    """Returns the ``owner/repo`` slug of the upstream remote, or None."""
    return parse_slug(git_out("remote", "get-url", upstream_remote, cwd=cwd))


def gh_merged_prs(slug: str, subject: str) -> list[dict[str, object]]:
    """Returns merged upstream PRs whose title contains ``subject`` (best effort).

    Raises RuntimeError if the ``gh`` call fails (e.g. not authenticated).
    """
    proc = subprocess.run(
        [
            "gh",
            "pr",
            "list",
            "-R",
            slug,
            "-S",
            f"{subject} in:title",
            "-s",
            "merged",
            "-L",
            "10",
            "--json",
            "number,title,url",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip())
    data = json.loads(proc.stdout or "[]")
    needle = subject.lower()
    return [pr for pr in data if needle in str(pr["title"]).lower()]


def preflight_checks(
    branches: list[str],
    base_ref: str,
    origin_remote: str,
    upstream_remote: str,
    use_gh: bool,
    cwd: Path,
) -> list[str]:
    """Runs setup-mistake diagnostics and returns a list of warning messages.

    Checks each branch is not stacked on another listed branch, and (via ``gh``,
    best effort) has not already been merged upstream. Nothing here aborts; it
    surfaces the likely cause before a later conflict report. A stale base is not
    flagged here (upstream moves fast and branches are not kept rebased); staleness
    is only reported as a hint when it actually causes a conflict.
    """
    warnings: list[str] = []
    refs = {b: f"{origin_remote}/{b}" for b in branches}

    for branch in branches:
        for other in branches:
            if other != branch and is_ancestor(refs[other], refs[branch], cwd=cwd):
                warnings.append(
                    f"{branch}: is stacked on top of listed branch '{other}'. Each feature "
                    f"branch should sit directly on {base_ref}; list a single merged branch "
                    "instead of stacking two."
                )

    if not use_gh:
        warnings.append("gh checks skipped (gh CLI not available or --no-gh).")
        return warnings

    slug = upstream_slug(upstream_remote, cwd)
    if slug is None:
        warnings.append(
            "gh merged-PR check skipped (could not parse upstream remote URL)."
        )
        return warnings

    for branch in branches:
        subjects = git_out(
            "log", "--no-merges", "--format=%s", f"{base_ref}..{refs[branch]}", cwd=cwd
        ).splitlines()
        for subject in dict.fromkeys(subjects):  # de-dup, keep order
            try:
                matches = gh_merged_prs(slug, subject)
            except RuntimeError as exc:
                warnings.append(f"gh merged-PR check skipped ({exc}).")
                return warnings
            for pr in matches:
                warnings.append(
                    f"{branch}: commit '{subject}' may already be merged upstream "
                    f"(PR #{pr['number']} {pr['url']}). Consider dropping it from the list."
                )

    return warnings
