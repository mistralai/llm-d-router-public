"""Pushing rebuilt branches and syncing local branches/worktrees."""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path

from mistral_release.gitcmd import git, git_out, worktree_for_branch


@dataclass
class PushSpec:
    """A branch update to push: ``branch`` to ``new_sha``, force-updated or not."""

    branch: str
    new_sha: str
    force: bool


def push_branches(
    origin_remote: str,
    specs: list[PushSpec],
    do_push: bool,
    cwd: Path,
) -> None:
    """Reports and (when ``do_push``) pushes all ``specs`` in one atomic push.

    Branches already at their target are reported and skipped. A forced spec uses
    ``--force-with-lease`` against its current remote value; a non-forced one is a
    plain update that must fast-forward. With ``--atomic`` every ref updates or none
    do, so a rejected branch leaves the others unchanged too. Raises GitError if the
    push is rejected.
    """
    refspecs: list[str] = []
    lease_args: list[str] = []
    for spec in specs:
        remote_ref = f"{origin_remote}/{spec.branch}"
        expected = git(
            "rev-parse", "--verify", "--quiet", remote_ref, cwd=cwd, check=False
        )
        expected_sha = expected.stdout.strip() or None
        if expected_sha == spec.new_sha:
            print(f"  {remote_ref}: already up to date ({spec.new_sha[:12]})")
            continue
        current = expected_sha[:12] if expected_sha else "(new branch)"
        verb = "pushing" if do_push else "would push"
        mode = "force" if spec.force else "fast-forward"
        print(f"  {remote_ref}: {verb} {current} -> {spec.new_sha[:12]} ({mode})")
        refspecs.append(f"{spec.new_sha}:refs/heads/{spec.branch}")
        if spec.force and expected_sha:
            lease_args.append(f"--force-with-lease={spec.branch}:{expected_sha}")

    if not do_push or not refspecs:
        return
    git("push", "--atomic", origin_remote, *refspecs, *lease_args, cwd=cwd)


def update_local_branch(branch: str, new_sha: str, cwd: Path) -> None:
    """Moves the local ``branch`` (and its worktree, if any) to ``new_sha``.

    Does nothing if the branch does not exist locally or already points at
    ``new_sha``. A branch checked out in a clean worktree is updated with a hard
    reset (untracked files are kept); a worktree with uncommitted tracked changes is
    left alone with a warning, so no local work is discarded.
    """
    head = git(
        "rev-parse", "--verify", "--quiet", f"refs/heads/{branch}", cwd=cwd, check=False
    )
    if head.returncode != 0:
        return
    current = head.stdout.strip()
    if current == new_sha:
        print(f"  local {branch}: already at {new_sha[:12]}")
        return

    worktree = worktree_for_branch(branch, cwd)
    if worktree is None:
        git("branch", "-f", branch, new_sha, cwd=cwd)
        print(f"  local {branch}: {current[:12]} -> {new_sha[:12]}")
        return

    if git_out("status", "--porcelain", "--untracked-files=no", cwd=worktree):
        print(
            f"  local {branch}: worktree {worktree} has uncommitted changes; left as is.\n"
            f"    update it manually with: git -C {worktree} reset --hard {new_sha[:12]}",
            file=sys.stderr,
        )
        return

    git("reset", "--hard", new_sha, cwd=worktree)
    print(
        f"  local {branch}: worktree {worktree} reset {current[:12]} -> {new_sha[:12]}"
    )


def branches_branch_diverged(
    repo_root: Path, origin_remote: str, branches_branch: str
) -> bool:
    """Returns whether the checked-out branches branch differs from its origin copy.

    True when the local tip differs from origin, the origin branch is missing, or
    the working tree has uncommitted changes. Used to require an explicit target for
    personal-test rebuilds so the canonical branch is never rebuilt from local edits.
    """
    local = git_out("rev-parse", "HEAD", cwd=repo_root)
    origin_ref = f"{origin_remote}/{branches_branch}"
    remote = git(
        "rev-parse", "--verify", "--quiet", origin_ref, cwd=repo_root, check=False
    )
    if remote.returncode != 0 or remote.stdout.strip() != local:
        return True
    return bool(git("status", "--porcelain", cwd=repo_root).stdout.strip())
