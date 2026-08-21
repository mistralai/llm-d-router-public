"""Tests for the cherry-pick replay that rebuilds a branch."""

from __future__ import annotations

import pytest
from mistral_release.rebuild import ConflictError, rebuild


def run_rebuild(box, branches, config_text="list\n"):
    base = box.rev("upstream/main")
    wt = box.root / "rebuild-wt"
    box.git("worktree", "add", "--detach", str(wt), base)
    try:
        return rebuild(wt, "upstream/main", branches, "origin", config_text)
    finally:
        box.git("worktree", "remove", "--force", str(wt))


def _extra_commit(box, branch, files, message):
    """Adds another commit to an existing origin branch and re-pushes it."""
    box.git("checkout", "-q", branch)
    for name, content in files.items():
        (box.work / name).write_text(content)
    box.git("add", "-A")
    box.git("commit", "-q", "-m", message)
    box.git("push", "-q", "-f", "origin", f"{branch}:refs/heads/{branch}")
    box.git("checkout", "-q", "main")
    box.git("fetch", "-q", "origin")


def test_clean_replay(sandbox):
    sandbox.feature("feat/clean", "upstream/main", {"clean.txt": "c\n"}, "add clean")
    result = run_rebuild(sandbox, ["feat/clean"])
    assert len(result.applied) == 1
    assert result.skipped == []
    assert sandbox.git("show", f"{result.new_sha}:clean.txt") == "c"


def test_patch_id_dedup_across_branches(sandbox):
    sandbox.feature("feat/one", "upstream/main", {"shared.txt": "s\n"}, "add shared")
    sandbox.feature(
        "feat/two", "upstream/main", {"shared.txt": "s\n"}, "add shared dup"
    )
    _extra_commit(sandbox, "feat/two", {"two.txt": "t\n"}, "add two")

    result = run_rebuild(sandbox, ["feat/one", "feat/two"])
    applied_subjects = [s for _, _, s in result.applied]
    skipped_reasons = [r for _, _, r in result.skipped]
    assert "add shared" in applied_subjects
    assert "add two" in applied_subjects
    assert "add shared dup" not in applied_subjects
    assert any("already applied by an earlier branch" in r for r in skipped_reasons)


def test_commit_already_upstream_is_dropped(sandbox):
    # core.txt with the same content is already upstream (commit U2).
    u1 = sandbox.rev("upstream/main~2")
    sandbox.feature("feat/redundant", u1, {"core.txt": "core\n"}, "re-add core")
    result = run_rebuild(sandbox, ["feat/redundant"])
    assert result.applied == []
    assert any("already present upstream" in r for _, _, r in result.skipped)


def test_conflict_raises(sandbox):
    u1 = sandbox.rev("upstream/main~2")
    sandbox.feature("feat/conflict", u1, {"README.md": "conflict\n"}, "clash readme")
    with pytest.raises(ConflictError) as excinfo:
        run_rebuild(sandbox, ["feat/conflict"])
    assert excinfo.value.branch == "feat/conflict"
    assert "README.md" in excinfo.value.files


def test_config_is_pinned_into_result(sandbox):
    sandbox.feature("feat/clean", "upstream/main", {"clean.txt": "c\n"}, "add clean")
    result = run_rebuild(sandbox, ["feat/clean"], config_text="feat/clean\n")
    assert (
        sandbox.git("show", f"{result.new_sha}:.mistral_branches.txt") == "feat/clean"
    )
    assert sandbox.git("log", "-1", "--format=%s", result.new_sha).startswith(
        "Record the branch list"
    )
