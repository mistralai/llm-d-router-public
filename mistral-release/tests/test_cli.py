"""Tests for base selection in the command-line workflow."""

from __future__ import annotations

from pathlib import Path

import pytest
from conftest import Sandbox
from mistral_release.cli import main


def _prepare_rebuild(
    sandbox: Sandbox, *, conflict_with_latest: bool
) -> tuple[str, str, Path]:
    pinned_base = sandbox.rev("upstream/main")
    sandbox.git("push", "-q", "origin", f"{pinned_base}:refs/heads/upstream-main")
    feature_files = (
        {"README.md": "feature\n"}
        if conflict_with_latest
        else {"feature.txt": "feature\n"}
    )
    sandbox.feature("feat/base", pinned_base, feature_files, "add feature")
    latest_base = sandbox.commit("README.md", "upstream v4\n", "U4: advance")
    sandbox.git("push", "-q", "upstream", "main:refs/heads/main")
    sandbox.git("fetch", "-q", "upstream")
    sandbox.git("fetch", "-q", "origin")
    config = sandbox.root / "branches.txt"
    config.write_text("feat/base\n")
    return pinned_base, latest_base, config


@pytest.mark.parametrize(
    "mode_args",
    [
        ("--no-update-main",),
        ("--triggered-by", "feat/base"),
        ("--triggered-by", "mistral-branches"),
    ],
)
def test_no_update_modes_use_pinned_base(
    sandbox: Sandbox,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    mode_args: tuple[str, ...],
) -> None:
    pinned_base, _, config = _prepare_rebuild(sandbox, conflict_with_latest=True)
    sandbox.git("remote", "set-url", "upstream", str(sandbox.root / "missing.git"))
    monkeypatch.chdir(sandbox.work)

    result = main(["--config", str(config), "--no-gh", *mode_args])

    assert result == 0
    output = capsys.readouterr().out
    assert "Fetching origin ..." in output
    assert "Fetching upstream/main ..." not in output
    assert f"Base:   origin/upstream-main = {pinned_base[:12]}" in output


def test_update_main_uses_latest_upstream_base(
    sandbox: Sandbox,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _, latest_base, config = _prepare_rebuild(sandbox, conflict_with_latest=False)
    monkeypatch.chdir(sandbox.work)

    result = main(["--config", str(config), "--no-gh", "--update-main"])

    assert result == 0
    output = capsys.readouterr().out
    assert "Fetching upstream/main ..." in output
    assert f"Base:   upstream/main = {latest_base[:12]}" in output
