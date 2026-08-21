"""Reading and locating the ``.mistral_branches.txt`` branch list."""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

from mistral_release.gitcmd import GitError, git, git_out

DEFAULT_CONFIG = ".mistral_branches.txt"


def reserved_conflicts(branches: list[str], reserved: Iterable[str]) -> list[str]:
    """Returns the sorted reserved names that wrongly appear in the branch list.

    ``main`` and the rebuilt target are produced from the list, never a source of
    commits, so listing them is always a mistake. An empty result means the list is
    clean.
    """
    reserved_set = set(reserved)
    return sorted({b for b in branches if b in reserved_set})


def is_triggering_branch(
    triggered_by: str, main_branch: str, branches: list[str]
) -> bool:
    """Returns whether a push to ``triggered_by`` should rebuild the target.

    A push matters only when it lands on the main branch (a new upstream base) or on
    one of the feature branches that feed the rebuild. Any other push is a no-op.
    """
    return triggered_by == main_branch or triggered_by in branches


def branches_branch_guard_applies(
    current_branch: str,
    branches_branch: str,
    target_branch: str | None,
    triggered_by: str | None,
) -> bool:
    """Returns whether the divergent-branches-branch guard should be evaluated.

    The guard stops an interactive run from pushing a non-canonical build off local
    edits to the branches branch. It applies only when run on that branch with no
    explicit target. An automated push run (``triggered_by`` set) is the canonical
    path and is exempt, since CI legitimately runs on the branches branch and may
    have an incidentally dirty tree (e.g. a re-resolved lockfile).
    """
    return (
        triggered_by is None
        and current_branch == branches_branch
        and target_branch is None
    )


def parse_config_text(text: str) -> list[str]:
    """Returns the ordered branch list from config file contents.

    Blank lines and comments are dropped. ``#`` starts a comment either on its own
    line or after a branch name; surrounding whitespace is ignored.
    """
    branches: list[str] = []
    for raw in text.splitlines():
        name = raw.split("#", 1)[0].strip()
        if name:
            branches.append(name)
    return branches


def config_source(
    repo_root: Path,
    explicit: Path | None,
    config_ref: str,
    branches_branch: str,
) -> tuple[str, str, str]:
    """Returns ``(kind, value, description)`` for where the branch list is read.

    ``kind`` is ``"file"`` (``value`` is a path) or ``"ref"`` (``value`` is a git
    ref). Resolution order: an explicit ``--config`` file; the working-tree file
    when the current branch is ``branches_branch`` (so uncommitted edits are used);
    then ``config_ref`` for any other checkout. The description states plainly
    whether local edits are in play, so nothing is silently missing.
    """
    if explicit is not None:
        return "file", str(explicit), f"{explicit} (--config file)"

    current = git_out("rev-parse", "--abbrev-ref", "HEAD", cwd=repo_root)
    local = repo_root / DEFAULT_CONFIG
    if current == branches_branch and local.exists():
        description = (
            f"{local}\n         (local working tree of '{branches_branch}', "
            "including any uncommitted edits)"
        )
        return "file", str(local), description

    description = (
        f"{config_ref}:{DEFAULT_CONFIG}\n         (committed on the remote; local "
        f"edits are used only when run on the '{branches_branch}' branch)"
    )
    return "ref", config_ref, description


def load_config(kind: str, value: str, repo_root: Path) -> tuple[list[str], str]:
    """Returns the ordered branch list and the raw file text for a config source."""
    if kind == "file":
        text = Path(value).read_text()
    else:
        proc = git("show", f"{value}:{DEFAULT_CONFIG}", cwd=repo_root, check=False)
        if proc.returncode != 0:
            raise GitError(
                f"could not read {DEFAULT_CONFIG} from '{value}':\n{proc.stderr.strip()}"
            )
        text = proc.stdout
    return parse_config_text(text), text
