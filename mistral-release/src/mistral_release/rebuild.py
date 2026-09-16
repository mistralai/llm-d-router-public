"""Replaying feature-branch commits onto a base ref to rebuild a branch."""

from __future__ import annotations

import subprocess
from dataclasses import dataclass, field
from pathlib import Path

from mistral_release.config import DEFAULT_CONFIG
from mistral_release.gitcmd import git, git_out


class ConflictError(RuntimeError):
    """Raised when applying a branch commit produces a merge conflict."""

    def __init__(
        self, branch: str, commit: str, subject: str, files: list[str]
    ) -> None:
        self.branch = branch
        self.commit = commit
        self.subject = subject
        self.files = files
        super().__init__(f"conflict applying {commit} from {branch}")


@dataclass
class RebuildResult:
    """Outcome of a rebuild: the new tip and per-branch bookkeeping."""

    base_sha: str
    new_sha: str
    applied: list[tuple[str, str, str]] = field(
        default_factory=list
    )  # branch, commit, subject
    skipped: list[tuple[str, str, str]] = field(
        default_factory=list
    )  # branch, commit, reason


def patch_id(commit: str, cwd: Path) -> str | None:
    """Returns the stable patch id of a commit, or None if its diff is empty."""
    show = subprocess.run(
        ["git", "show", commit], cwd=cwd, capture_output=True, text=True, check=True
    )
    result = subprocess.run(
        ["git", "patch-id", "--stable"],
        input=show.stdout,
        cwd=cwd,
        capture_output=True,
        text=True,
        check=True,
    )
    out = result.stdout.strip()
    return out.split()[0] if out else None


def rebuild(
    worktree: Path,
    base_ref: str,
    branches: list[str],
    origin_remote: str,
    config_text: str,
) -> RebuildResult:
    """Replays the listed branches onto ``base_ref`` inside ``worktree``.

    The worktree must already be checked out at ``base_ref`` (detached). Returns a
    RebuildResult describing what was applied. Commits already applied by an earlier
    branch are skipped by patch id, and commits already present upstream drop out as
    empty. Raises ConflictError on the first conflicting commit, leaving the
    cherry-pick aborted.

    After replaying, the exact ``config_text`` used is written to the branch, with a
    commit added only if it differs from what the replay produced, so the rebuilt
    branch always carries the list it was built from.
    """
    base_sha = git_out("rev-parse", base_ref, cwd=worktree)
    result = RebuildResult(base_sha=base_sha, new_sha=base_sha)
    seen_patch_ids: set[str] = set()

    for branch in branches:
        ref = f"{origin_remote}/{branch}"
        commits = git_out(
            "rev-list", "--reverse", "--no-merges", f"{base_ref}..{ref}", cwd=worktree
        ).split()
        for commit in commits:
            subject = git_out("log", "-1", "--format=%s", commit, cwd=worktree)
            pid = patch_id(commit, cwd=worktree)
            if pid is not None and pid in seen_patch_ids:
                result.skipped.append(
                    (branch, commit, "already applied by an earlier branch")
                )
                continue

            head_before = git_out("rev-parse", "HEAD", cwd=worktree)
            pick = git(
                "cherry-pick", "--empty=drop", "-x", commit, cwd=worktree, check=False
            )
            if pick.returncode != 0:
                files = git_out(
                    "diff", "--name-only", "--diff-filter=U", cwd=worktree
                ).splitlines()
                git("cherry-pick", "--abort", cwd=worktree, check=False)
                raise ConflictError(branch, commit, subject, files)

            head_after = git_out("rev-parse", "HEAD", cwd=worktree)
            if head_after == head_before:
                result.skipped.append(
                    (branch, commit, "already present upstream (empty)")
                )
            else:
                if pid is not None:
                    seen_patch_ids.add(pid)
                result.applied.append((branch, commit, subject))

    _pin_config(worktree, config_text)
    result.new_sha = git_out("rev-parse", "HEAD", cwd=worktree)
    return result


def _pin_config(worktree: Path, config_text: str) -> None:
    """Writes ``config_text`` into the branch, committing it only if it changed."""
    (worktree / DEFAULT_CONFIG).write_text(config_text)
    if git_out("status", "--porcelain", "--", DEFAULT_CONFIG, cwd=worktree):
        git("add", "--", DEFAULT_CONFIG, cwd=worktree)
        git(
            "commit",
            "-s",
            "-m",
            "Record the branch list used to rebuild this branch\n\n"
            f"Pins {DEFAULT_CONFIG} to the list that produced this branch.",
            cwd=worktree,
        )
