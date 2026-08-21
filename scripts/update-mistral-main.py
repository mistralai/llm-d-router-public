#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Rebuild the ``mistral-main`` branch from scratch on top of ``upstream/main``.

``mistral-main`` is reconstructed as ``upstream/main`` plus every mistral-specific
commit contributed by the branches listed in ``.mistral_branches.txt`` (see that
file for the format). Branches are applied in list order; commits already present
upstream or already applied by an earlier branch are skipped by patch id. The
``main`` branch is mirrored to the latest ``upstream/main`` at the same time.

The rebuild happens in a throwaway detached worktree, so the current checkout is
never touched and the script runs from any worktree. Nothing is pushed unless
``--push`` is given.

Before rebuilding, each listed branch is checked for the common setup mistakes
(stacked on another listed branch; already merged upstream) and a clear diagnostic
is printed. A cherry-pick conflict is a hard error: the offending branch and commit
are reported and must be fixed on that branch, never by editing the rebuilt branch
by hand; when that branch is behind upstream the message says to update it first.

For a personal test, run from the ``mistral-branches`` worktree with your branch
added to ``.mistral_branches.txt`` (uncommitted is fine) and pass a throwaway
``--target-branch``; ``main`` is left untouched in that mode.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

DEFAULT_CONFIG = ".mistral_branches.txt"
DEFAULT_UPSTREAM_REMOTE = "upstream"
DEFAULT_UPSTREAM_BRANCH = "main"
DEFAULT_ORIGIN_REMOTE = "origin"
DEFAULT_TARGET_BRANCH = "mistral-main"
DEFAULT_MAIN_BRANCH = "main"
DEFAULT_BRANCHES_BRANCH = "mistral-branches"


class GitError(RuntimeError):
    """Raised when a git invocation exits with a non-zero status."""


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


def upstream_slug(upstream_remote: str) -> str | None:
    """Returns the ``owner/repo`` slug of the upstream remote, or None."""
    url = git_out("remote", "get-url", upstream_remote)
    match = re.search(r"[:/]([^/:]+/[^/:]+?)(?:\.git)?$", url)
    return match.group(1) if match else None


def gh_merged_prs(slug: str, subject: str) -> list[dict[str, object]]:
    """Returns merged upstream PRs whose title contains ``subject`` (best effort).

    Raises RuntimeError if the ``gh`` call fails (e.g. not authenticated).
    """
    proc = subprocess.run(
        [
            "gh",
            "pr",
            "list",
            "-R",
            slug,
            "-S",
            f"{subject} in:title",
            "-s",
            "merged",
            "-L",
            "10",
            "--json",
            "number,title,url",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip())
    data = json.loads(proc.stdout or "[]")
    needle = subject.lower()
    return [pr for pr in data if needle in str(pr["title"]).lower()]


def preflight_checks(
    branches: list[str],
    base_ref: str,
    origin_remote: str,
    upstream_remote: str,
    use_gh: bool,
) -> list[str]:
    """Runs setup-mistake diagnostics and returns a list of warning messages.

    Checks each branch is not stacked on another listed branch, and (via ``gh``,
    best effort) has not already been merged upstream. Nothing here aborts; it
    surfaces the likely cause before a later conflict report. A stale base is not
    flagged here (upstream moves fast and branches are not kept rebased); staleness
    is only reported as a hint when it actually causes a conflict.
    """
    warnings: list[str] = []
    refs = {b: f"{origin_remote}/{b}" for b in branches}

    for branch in branches:
        for other in branches:
            if other != branch and is_ancestor(refs[other], refs[branch]):
                warnings.append(
                    f"{branch}: is stacked on top of listed branch '{other}'. Each feature "
                    f"branch should sit directly on {base_ref}; list a single merged branch "
                    "instead of stacking two."
                )

    if not use_gh:
        warnings.append("gh checks skipped (gh CLI not available or --no-gh).")
        return warnings

    slug = upstream_slug(upstream_remote)
    if slug is None:
        warnings.append(
            "gh merged-PR check skipped (could not parse upstream remote URL)."
        )
        return warnings

    for branch in branches:
        subjects = git_out(
            "log", "--no-merges", "--format=%s", f"{base_ref}..{refs[branch]}"
        ).splitlines()
        for subject in dict.fromkeys(subjects):  # de-dup, keep order
            try:
                matches = gh_merged_prs(slug, subject)
            except RuntimeError as exc:
                warnings.append(f"gh merged-PR check skipped ({exc}).")
                return warnings
            for pr in matches:
                warnings.append(
                    f"{branch}: commit '{subject}' may already be merged upstream "
                    f"(PR #{pr['number']} {pr['url']}). Consider dropping it from the list."
                )

    return warnings


@dataclass
class RebuildResult:
    """Outcome of a rebuild: the new tip and per-branch bookkeeping."""

    base_sha: str
    new_sha: str
    applied: list[tuple[str, str, str]] = field(
        default_factory=list
    )  # (branch, commit, subject)
    skipped: list[tuple[str, str, str]] = field(
        default_factory=list
    )  # (branch, commit, reason)


def rebuild(
    worktree: Path,
    base_ref: str,
    branches: list[str],
    origin_remote: str,
    config_text: str,
) -> RebuildResult:
    """Replays the listed branches onto ``base_ref`` inside ``worktree``.

    The worktree must already be checked out at ``base_ref`` (detached). Returns a
    RebuildResult describing what was applied. Raises ConflictError on the first
    conflicting commit, leaving the cherry-pick aborted.

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

    (worktree / DEFAULT_CONFIG).write_text(config_text)
    if git_out("status", "--porcelain", "--", DEFAULT_CONFIG, cwd=worktree):
        git("add", "--", DEFAULT_CONFIG, cwd=worktree)
        git(
            "commit",
            "-s",
            "-m",
            f"Record the branch list used to rebuild this branch\n\n"
            f"Pins {DEFAULT_CONFIG} to the list that produced this branch.",
            cwd=worktree,
        )

    result.new_sha = git_out("rev-parse", "HEAD", cwd=worktree)
    return result


def push_branch(
    origin_remote: str,
    target: str,
    new_sha: str,
    do_push: bool,
    cwd: Path,
    force: bool,
) -> None:
    """Reports and (when ``do_push``) pushes ``new_sha`` to ``origin/target``.

    A branch that does not yet exist on the remote is created. An existing one is
    updated with ``--force-with-lease`` when ``force`` is set; otherwise a plain
    push is attempted, which fails (raising GitError) unless it fast-forwards.
    """
    remote_ref = f"{origin_remote}/{target}"
    expected = git("rev-parse", "--verify", "--quiet", remote_ref, cwd=cwd, check=False)
    expected_sha = expected.stdout.strip() or None

    if expected_sha == new_sha:
        print(f"  {remote_ref}: already up to date ({new_sha[:12]})")
        return

    current = expected_sha[:12] if expected_sha else "(new branch)"
    verb = "pushing" if do_push else "would push"
    mode = "force" if force else "fast-forward"
    print(f"  {remote_ref}: {verb} {current} -> {new_sha[:12]} ({mode})")
    if not do_push:
        return

    refspec = f"{new_sha}:refs/heads/{target}"
    push_args = ["push", origin_remote, refspec]
    if force and expected_sha:
        push_args.append(f"--force-with-lease={target}:{expected_sha}")
    git(*push_args, cwd=cwd)


def worktree_for_branch(branch: str, cwd: Path) -> Path | None:
    """Returns the path of the worktree that has ``branch`` checked out, or None."""
    path: str | None = None
    for line in git_out("worktree", "list", "--porcelain", cwd=cwd).splitlines():
        if line.startswith("worktree "):
            path = line[len("worktree ") :]
        elif line == f"branch refs/heads/{branch}" and path is not None:
            return Path(path)
    return None


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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--config", type=Path, default=None, help="explicit branch-list file"
    )
    parser.add_argument(
        "--config-ref",
        default=None,
        help="ref to read the branch list from when not on the branches branch (default: <origin>/<branches-branch>)",
    )
    parser.add_argument(
        "--branches-branch",
        default=DEFAULT_BRANCHES_BRANCH,
        help="branch that owns the script and branch list",
    )
    parser.add_argument("--upstream-remote", default=DEFAULT_UPSTREAM_REMOTE)
    parser.add_argument("--upstream-branch", default=DEFAULT_UPSTREAM_BRANCH)
    parser.add_argument("--origin-remote", default=DEFAULT_ORIGIN_REMOTE)
    parser.add_argument(
        "--target-branch",
        default=None,
        help="branch to rebuild (default: mistral-main; use a throwaway name for a "
        "personal test; required when on the branches branch with local changes)",
    )
    parser.add_argument(
        "--main-branch",
        default=DEFAULT_MAIN_BRANCH,
        help="branch mirrored to upstream/main",
    )
    parser.add_argument(
        "--update-main",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="mirror upstream/main into origin/<main-branch> (default: only for the canonical target)",
    )
    parser.add_argument(
        "--force-push-main",
        action="store_true",
        help="force-push <main-branch> (default: fast-forward only; the target branch is always force-pushed)",
    )
    parser.add_argument(
        "--no-fetch",
        action="store_true",
        help="skip fetching remotes (use local refs as-is)",
    )
    parser.add_argument(
        "--no-gh", action="store_true", help="skip the gh CLI merged-PR check"
    )
    parser.add_argument(
        "--strict", action="store_true", help="treat pre-flight warnings as errors"
    )
    parser.add_argument(
        "--push",
        action="store_true",
        help="force-push results to origin (default: dry-run)",
    )
    parser.add_argument(
        "--no-update-local",
        action="store_true",
        help="do not move local branches/worktrees to match what was pushed",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    repo_root = Path(git_out("rev-parse", "--show-toplevel"))
    config_ref = args.config_ref or f"{args.origin_remote}/{args.branches_branch}"
    current_branch = git_out("rev-parse", "--abbrev-ref", "HEAD", cwd=repo_root)

    config_kind, config_value, config_desc = config_source(
        repo_root, args.config, config_ref, args.branches_branch
    )
    print(f"Branch list source: {config_desc}\n")

    for remote in (args.upstream_remote, args.origin_remote):
        if remote not in git_out("remote").splitlines():
            print(
                f"error: remote '{remote}' is not configured. Add it, e.g.:\n"
                f"  git remote add {args.upstream_remote} "
                "https://github.com/llm-d/llm-d-router.git",
                file=sys.stderr,
            )
            return 2

    if not args.no_fetch:
        print(
            f"Fetching {args.upstream_remote}/{args.upstream_branch} and {args.origin_remote} ..."
        )
        git("fetch", args.upstream_remote, args.upstream_branch)
        git("fetch", "--prune", args.origin_remote)

    if (
        current_branch == args.branches_branch
        and args.target_branch is None
        and branches_branch_diverged(
            repo_root, args.origin_remote, args.branches_branch
        )
    ):
        if args.push:
            print(
                f"error: on '{args.branches_branch}' with local state that differs from "
                f"{args.origin_remote}/{args.branches_branch}.\n"
                "Pass an explicit --target-branch to push a personal test build, or "
                "commit and push these edits to the branch before pushing the canonical "
                "target.",
                file=sys.stderr,
            )
            return 2
        print(
            f"Notice: building from the local (uncommitted) state of "
            f"'{args.branches_branch}'.\n"
            "        Allowed here only because this is a dry-run, so you can check a "
            "change before\n"
            "        pushing it. A --push run would require an explicit --target-branch "
            "or a synced branch.\n"
        )

    target_branch = args.target_branch or DEFAULT_TARGET_BRANCH
    update_main = args.update_main
    if update_main is None:
        update_main = target_branch == DEFAULT_TARGET_BRANCH

    try:
        branches, config_text = load_config(config_kind, config_value, repo_root)
    except (GitError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    if not branches:
        print("error: no branches listed in the config source.", file=sys.stderr)
        return 2

    base_ref = f"{args.upstream_remote}/{args.upstream_branch}"
    base_sha = git_out("rev-parse", base_ref)

    missing = [
        b
        for b in branches
        if git(
            "rev-parse",
            "--verify",
            "--quiet",
            f"{args.origin_remote}/{b}^{{commit}}",
            check=False,
        ).returncode
        != 0
    ]
    if missing:
        print(
            f"error: these branches are not on '{args.origin_remote}': {', '.join(missing)}",
            file=sys.stderr,
        )
        return 2

    print(f"Base:   {base_ref} = {base_sha[:12]}")
    print(f"Target: {target_branch}   (update {args.main_branch}: {update_main})")
    print(f"Branches ({len(branches)}): {', '.join(branches)}")

    use_gh = not args.no_gh and shutil.which("gh") is not None
    warnings = preflight_checks(
        branches, base_ref, args.origin_remote, args.upstream_remote, use_gh
    )
    if warnings:
        print("\nPre-flight diagnostics:")
        for message in warnings:
            print(f"  warning: {message}")
        if args.strict:
            print(
                "\nerror: pre-flight warnings present and --strict set.",
                file=sys.stderr,
            )
            return 3

    git("worktree", "prune")
    tmp = Path(tempfile.mkdtemp(prefix="mistral-main-rebuild-"))
    git("worktree", "add", "--detach", str(tmp), base_sha)
    try:
        try:
            result = rebuild(tmp, base_ref, branches, args.origin_remote, config_text)
        except ConflictError as exc:
            print(file=sys.stderr)
            print(
                f"error: conflict applying commit {exc.commit[:12]} ({exc.subject})",
                file=sys.stderr,
            )
            print(f"       from branch '{exc.branch}'", file=sys.stderr)
            if exc.files:
                print(
                    f"       conflicted files: {', '.join(exc.files)}", file=sys.stderr
                )
            stale = not is_ancestor(base_ref, f"{args.origin_remote}/{exc.branch}")
            if stale:
                print(
                    f"\n'{exc.branch}' is behind {base_ref}. First update it and retry, which\n"
                    "often resolves the conflict on its own:\n"
                    f"    git fetch {args.upstream_remote} {args.upstream_branch}\n"
                    f"    git rebase {base_ref} {exc.branch}\n"
                    f"    git push --force-with-lease {args.origin_remote} {exc.branch}",
                    file=sys.stderr,
                )
            print(
                f"\nOtherwise fix it on branch '{exc.branch}': resolve the conflict there, or, if\n"
                "it clashes with another listed branch, replace both with a single branch that\n"
                "merges them with the conflict resolved. The rebuilt branch is never edited by hand.",
                file=sys.stderr,
            )
            return 1

        print()
        for branch, commit, subject in result.applied:
            print(f"  applied  {commit[:12]}  [{branch}] {subject}")
        for branch, commit, reason in result.skipped:
            print(f"  skipped  {commit[:12]}  [{branch}] ({reason})")

        print(
            f"\nRebuilt {target_branch}: {result.new_sha[:12]} "
            f"= {base_ref} + {len(result.applied)} commit(s)"
        )

        print(f"\n{'Pushing' if args.push else 'Dry-run (pass --push to apply)'}:")
        if update_main:
            try:
                push_branch(
                    args.origin_remote,
                    args.main_branch,
                    base_sha,
                    args.push,
                    repo_root,
                    force=args.force_push_main,
                )
            except GitError as exc:
                print(
                    f"error: pushing {args.main_branch} failed:\n{exc}", file=sys.stderr
                )
                if not args.force_push_main:
                    print(
                        f"{args.main_branch} does not fast-forward from its remote; re-run "
                        "with --force-push-main to overwrite it.",
                        file=sys.stderr,
                    )
                return 1
        push_branch(
            args.origin_remote,
            target_branch,
            result.new_sha,
            args.push,
            repo_root,
            force=True,
        )

        if args.push and not args.no_update_local:
            print("\nUpdating local branches/worktrees:")
            if update_main:
                update_local_branch(args.main_branch, base_sha, repo_root)
            update_local_branch(target_branch, result.new_sha, repo_root)
        return 0
    finally:
        git("worktree", "remove", "--force", str(tmp), check=False)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except GitError as exc:
        print(f"error: {exc}", file=sys.stderr)
        sys.exit(1)
