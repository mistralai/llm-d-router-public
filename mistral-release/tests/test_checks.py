"""Tests for slug parsing and the stacked-branch pre-flight diagnostic."""

from __future__ import annotations

import pytest
from mistral_release.checks import parse_slug, preflight_checks


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("git@github.com:llm-d/llm-d-router.git", "llm-d/llm-d-router"),
        ("https://github.com/llm-d/llm-d-router.git", "llm-d/llm-d-router"),
        ("https://github.com/llm-d/llm-d-router", "llm-d/llm-d-router"),
        ("ssh://git@github.com/owner/repo.git", "owner/repo"),
        ("/tmp/foo/bar.git", "foo/bar"),
    ],
)
def test_parse_slug(url, expected):
    assert parse_slug(url) == expected


def test_preflight_flags_stacked_branch(sandbox):
    sandbox.feature("feat/base", "upstream/main", {"a.txt": "a\n"}, "add a")
    sandbox.feature("feat/stacked", "feat/base", {"b.txt": "b\n"}, "add b on base")

    warnings = preflight_checks(
        ["feat/base", "feat/stacked"],
        "upstream/main",
        "origin",
        "upstream",
        use_gh=False,
        cwd=sandbox.work,
    )
    assert any(
        "feat/stacked" in w and "stacked on top of listed branch 'feat/base'" in w
        for w in warnings
    )


def test_preflight_independent_branches_have_no_stacked_warning(sandbox):
    sandbox.feature("feat/a", "upstream/main", {"a.txt": "a\n"}, "add a")
    sandbox.feature("feat/b", "upstream/main", {"b.txt": "b\n"}, "add b")

    warnings = preflight_checks(
        ["feat/a", "feat/b"],
        "upstream/main",
        "origin",
        "upstream",
        use_gh=False,
        cwd=sandbox.work,
    )
    assert not any("stacked on top" in w for w in warnings)
    # only the gh-skipped notice is expected
    assert warnings == ["gh checks skipped (gh CLI not available or --no-gh)."]
