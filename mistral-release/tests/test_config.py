"""Tests for branch-list parsing and source resolution."""

from __future__ import annotations

from mistral_release.config import config_source, load_config, parse_config_text


def test_parse_strips_comments_and_whitespace():
    text = (
        "# header comment\n"
        "\n"
        "  feat/a   # trailing comment\n"
        "feat/b\n"
        "   # indented full-line comment\n"
        "\tfeat/c\t\n"
    )
    assert parse_config_text(text) == ["feat/a", "feat/b", "feat/c"]


def test_parse_empty_is_empty():
    assert parse_config_text("# only comments\n\n   \n") == []


def test_config_source_explicit_file(tmp_path):
    cfg = tmp_path / "list.txt"
    cfg.write_text("feat/a\n")
    kind, value, desc = config_source(
        tmp_path, cfg, "origin/mistral-branches", "mistral-branches"
    )
    assert kind == "file"
    assert value == str(cfg)
    assert "--config file" in desc


def test_config_source_working_tree_on_branches_branch(sandbox):
    sandbox.feature(
        "mistral-branches",
        "upstream/main",
        {".mistral_branches.txt": "feat/a\n"},
        "add list",
    )
    sandbox.git("checkout", "-q", "mistral-branches")

    kind, value, desc = config_source(
        sandbox.work, None, "origin/mistral-branches", "mistral-branches"
    )
    assert kind == "file"
    assert value == str(sandbox.work / ".mistral_branches.txt")
    assert "local working tree" in desc


def test_config_source_ref_when_not_on_branch(sandbox):
    sandbox.feature(
        "mistral-branches",
        "upstream/main",
        {".mistral_branches.txt": "feat/a\n"},
        "add list",
    )
    # work is left on main
    kind, value, desc = config_source(
        sandbox.work, None, "origin/mistral-branches", "mistral-branches"
    )
    assert kind == "ref"
    assert value == "origin/mistral-branches"
    assert "committed on the remote" in desc


def test_load_config_from_ref(sandbox):
    sandbox.feature(
        "mistral-branches",
        "upstream/main",
        {".mistral_branches.txt": "# c\nfeat/a\nfeat/b\n"},
        "add list",
    )
    branches, text = load_config("ref", "origin/mistral-branches", sandbox.work)
    assert branches == ["feat/a", "feat/b"]
    assert text == "# c\nfeat/a\nfeat/b\n"
