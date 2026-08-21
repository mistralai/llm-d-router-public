"""Shared fixtures: an isolated git sandbox with upstream + origin remotes."""

from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass
from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def _git_env(monkeypatch):
    """Isolates git from user/system config and pins a deterministic identity."""
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", os.devnull)
    monkeypatch.setenv("GIT_CONFIG_SYSTEM", os.devnull)
    monkeypatch.setenv("GIT_AUTHOR_NAME", "Test")
    monkeypatch.setenv("GIT_AUTHOR_EMAIL", "test@example.com")
    monkeypatch.setenv("GIT_COMMITTER_NAME", "Test")
    monkeypatch.setenv("GIT_COMMITTER_EMAIL", "test@example.com")


@dataclass
class Sandbox:
    """A work clone wired to bare ``upstream`` and ``origin`` remotes.

    ``main`` starts with three commits (README v1 -> v3 and an added ``core.txt``);
    ``upstream/main`` and ``origin/main`` both point at that tip.
    """

    root: Path
    work: Path

    def git(self, *args: str, cwd: Path | None = None) -> str:
        proc = subprocess.run(
            ["git", *args],
            cwd=cwd or self.work,
            capture_output=True,
            text=True,
            check=True,
        )
        return proc.stdout.strip()

    def rev(self, ref: str, cwd: Path | None = None) -> str:
        return self.git("rev-parse", ref, cwd=cwd)

    def write(self, name: str, content: str) -> None:
        (self.work / name).write_text(content)

    def commit(self, name: str, content: str, message: str) -> str:
        self.write(name, content)
        self.git("add", "--", name)
        self.git("commit", "-m", message)
        return self.rev("HEAD")

    def feature(self, name: str, base: str, files: dict[str, str], message: str) -> str:
        """Creates ``name`` off ``base`` with ``files``, pushes it to origin.

        Returns the branch tip sha. Leaves the work tree back on ``main`` with the
        remote-tracking refs refreshed.
        """
        self.git("checkout", "-q", "-B", name, base)
        for filename, content in files.items():
            (self.work / filename).write_text(content)
        self.git("add", "-A")
        self.git("commit", "-q", "-m", message)
        sha = self.rev("HEAD")
        self.git("push", "-q", "origin", f"{name}:refs/heads/{name}")
        self.git("checkout", "-q", "main")
        self.git("fetch", "-q", "origin")
        return sha


@pytest.fixture
def sandbox(tmp_path: Path) -> Sandbox:
    root = tmp_path
    upstream = root / "upstream.git"
    origin = root / "origin.git"
    work = root / "work"

    subprocess.run(
        ["git", "init", "-q", "--bare", "-b", "main", str(upstream)], check=True
    )
    subprocess.run(
        ["git", "init", "-q", "--bare", "-b", "main", str(origin)], check=True
    )
    subprocess.run(["git", "init", "-q", "-b", "main", str(work)], check=True)

    box = Sandbox(root=root, work=work)
    box.git("remote", "add", "upstream", str(upstream))
    box.git("remote", "add", "origin", str(origin))

    box.commit("README.md", "v1\n", "U1: initial")
    box.commit("core.txt", "core\n", "U2: add core")
    box.commit("README.md", "v3\n", "U3: bump readme")

    box.git("push", "-q", "upstream", "main:refs/heads/main")
    box.git("push", "-q", "origin", "main:refs/heads/main")
    box.git("fetch", "-q", "upstream")
    box.git("fetch", "-q", "origin")
    return box
