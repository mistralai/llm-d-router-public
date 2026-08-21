"""Tests for atomic pushing and local branch/worktree syncing."""

from __future__ import annotations

import pytest

from mistral_release.gitcmd import GitError, worktree_for_branch
from mistral_release.remote import (
    PushSpec,
    branches_branch_diverged,
    push_branches,
    update_local_branch,
)


def remote_sha(box, remote, branch):
    out = box.git("ls-remote", remote, f"refs/heads/{branch}")
    return out.split("\t")[0] if out else ""


def test_atomic_push_success(sandbox):
    u4 = sandbox.commit("core.txt", "core2\n", "U4")  # advances work main, not origin
    mm = sandbox.rev("upstream/main")
    push_branches(
        "origin",
        [PushSpec("main", u4, force=False), PushSpec("mistral-main", mm, force=True)],
        do_push=True,
        cwd=sandbox.work,
    )
    assert remote_sha(sandbox, "origin", "main") == u4
    assert remote_sha(sandbox, "origin", "mistral-main") == mm


def test_atomic_push_rejects_all_on_non_ff(sandbox):
    u3 = sandbox.rev("upstream/main")
    u1 = sandbox.rev("upstream/main~2")
    with pytest.raises(GitError):
        push_branches(
            "origin",
            [
                PushSpec("main", u1, force=False),
                PushSpec("mistral-main", u3, force=True),
            ],
            do_push=True,
            cwd=sandbox.work,
        )
    # main unchanged and mistral-main never created: the whole push rolled back.
    assert remote_sha(sandbox, "origin", "main") == u3
    assert remote_sha(sandbox, "origin", "mistral-main") == ""


def test_dry_run_pushes_nothing(sandbox):
    u1 = sandbox.rev("upstream/main~2")
    push_branches(
        "origin",
        [PushSpec("mistral-main", u1, force=True)],
        do_push=False,
        cwd=sandbox.work,
    )
    assert remote_sha(sandbox, "origin", "mistral-main") == ""


def test_update_local_resets_clean_worktree(sandbox):
    u1 = sandbox.rev("upstream/main~2")
    u3 = sandbox.rev("upstream/main")
    sandbox.git("branch", "zz", u1)
    wt = sandbox.root / "zzwt"
    sandbox.git("worktree", "add", str(wt), "zz")

    update_local_branch("zz", u3, sandbox.work)
    assert sandbox.rev("HEAD", cwd=wt) == u3
    assert sandbox.rev("zz") == u3


def test_update_local_skips_dirty_worktree(sandbox):
    u1 = sandbox.rev("upstream/main~2")
    u3 = sandbox.rev("upstream/main")
    sandbox.git("branch", "zz", u3)
    wt = sandbox.root / "zzwt"
    sandbox.git("worktree", "add", str(wt), "zz")
    (wt / "README.md").write_text("dirty\n")

    update_local_branch("zz", u1, sandbox.work)
    assert sandbox.rev("zz") == u3  # left untouched


def test_update_local_repoints_branch_without_worktree(sandbox):
    u1 = sandbox.rev("upstream/main~2")
    u3 = sandbox.rev("upstream/main")
    sandbox.git("branch", "zz2", u1)
    update_local_branch("zz2", u3, sandbox.work)
    assert sandbox.rev("zz2") == u3


def test_update_local_missing_branch_is_noop(sandbox):
    u3 = sandbox.rev("upstream/main")
    update_local_branch("does-not-exist", u3, sandbox.work)  # must not raise


def test_worktree_for_branch(sandbox):
    u3 = sandbox.rev("upstream/main")
    sandbox.git("branch", "zz", u3)
    wt = sandbox.root / "zzwt"
    sandbox.git("worktree", "add", str(wt), "zz")
    found = worktree_for_branch("zz", sandbox.work)
    assert found is not None and found.resolve() == wt.resolve()
    assert worktree_for_branch("main", sandbox.work).resolve() == sandbox.work.resolve()


def test_branches_branch_diverged(sandbox):
    sandbox.feature(
        "mistral-branches", "upstream/main", {".mistral_branches.txt": "x\n"}, "cfg"
    )
    sandbox.git("checkout", "-q", "mistral-branches")

    # in sync and clean
    assert branches_branch_diverged(sandbox.work, "origin", "mistral-branches") is False

    # local commit ahead of origin
    sandbox.commit("note.txt", "ahead\n", "local ahead")
    assert branches_branch_diverged(sandbox.work, "origin", "mistral-branches") is True

    # back in sync, but dirty working tree
    sandbox.git("checkout", "-q", "-B", "mistral-branches", "origin/mistral-branches")
    (sandbox.work / ".mistral_branches.txt").write_text("y\n")
    assert branches_branch_diverged(sandbox.work, "origin", "mistral-branches") is True
