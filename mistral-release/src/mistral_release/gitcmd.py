"""Thin wrappers around the ``git`` command used across the package."""

from __future__ import annotations

import subprocess
from pathlib import Path


class GitError(RuntimeError):
    """Raised when a git invocation exits with a non-zero status."""


def git(
    *args: str, cwd: Path | None = None, check: bool = True
) -> subprocess.CompletedProcess[str]:
    """Runs a git command and returns the completed process.

    Raises GitError when ``check`` is set and the command fails.
    """
    proc = subprocess.run(
        ["git", *args],
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
    )
    if check and proc.returncode != 0:
        raise GitError(
            f"git {' '.join(args)} failed ({proc.returncode}):\n{proc.stderr.strip()}"
        )
    return proc


def git_out(*args: str, cwd: Path | None = None) -> str:
    """Returns the stripped stdout of a successful git command."""
    return git(*args, cwd=cwd).stdout.strip()


def is_ancestor(ancestor: str, descendant: str, cwd: Path | None = None) -> bool:
    """Returns whether ``ancestor`` is an ancestor of ``descendant``."""
    return (
        git(
            "merge-base", "--is-ancestor", ancestor, descendant, cwd=cwd, check=False
        ).returncode
        == 0
    )


def worktree_for_branch(branch: str, cwd: Path) -> Path | None:
    """Returns the path of the worktree that has ``branch`` checked out, or None."""
    path: str | None = None
    for line in git_out("worktree", "list", "--porcelain", cwd=cwd).splitlines():
        if line.startswith("worktree "):
            path = line[len("worktree ") :]
        elif line == f"branch refs/heads/{branch}" and path is not None:
            return Path(path)
    return None
