"""Command-line entry point that wires the pieces together.

Rebuild the ``mistral-main`` branch from scratch on top of ``upstream/main``:
``mistral-main`` is reconstructed as ``upstream/main`` plus every mistral-specific
commit contributed by the branches listed in ``.mistral_branches.txt``. Branches
are applied in list order; commits already present upstream or applied by an
earlier branch are skipped. ``main`` is mirrored to the latest ``upstream/main`` at
the same time, and both branches are pushed together atomically.

The rebuild happens in a throwaway detached worktree, so the current checkout is
never touched. Nothing is pushed unless ``--push`` is given. On ``--push`` local
branches (and their worktrees) are moved to match what was pushed.
"""

from __future__ import annotations

import argparse
import shutil
import sys
import tempfile
from pathlib import Path

from mistral_release.checks import preflight_checks
from mistral_release.config import (
    branches_branch_guard_applies,
    config_source,
    is_triggering_branch,
    load_config,
    reserved_conflicts,
)
from mistral_release.gitcmd import GitError, git, git_out, is_ancestor
from mistral_release.rebuild import ConflictError, rebuild
from mistral_release.remote import (
    PushSpec,
    branches_branch_diverged,
    push_branches,
    update_local_branch,
)

DEFAULT_UPSTREAM_REMOTE = "upstream"
DEFAULT_UPSTREAM_BRANCH = "main"
DEFAULT_ORIGIN_REMOTE = "origin"
DEFAULT_TARGET_BRANCH = "mistral-main"
DEFAULT_MAIN_BRANCH = "main"
DEFAULT_BRANCHES_BRANCH = "mistral-branches"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="update-mistral-main",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--config", type=Path, default=None, help="explicit branch-list file"
    )
    parser.add_argument(
        "--config-ref",
        default=None,
        help="ref to read the branch list from when not on the branches branch "
        "(default: <origin>/<branches-branch>)",
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
        help="mirror upstream/main into origin/<main-branch> "
        "(default: only for the canonical target)",
    )
    parser.add_argument(
        "--force-push-main",
        action="store_true",
        help="force-push <main-branch> (default: fast-forward only; the target "
        "branch is always force-pushed)",
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
        "--triggered-by",
        default=None,
        help="branch whose push triggered this run (CI push events). The rebuild "
        "runs only if it is the main branch or a listed feature branch; otherwise "
        "the run exits cleanly. In this mode the main branch is never updated.",
    )
    parser.add_argument(
        "--push",
        action="store_true",
        help="push results to origin (default: dry-run)",
    )
    parser.add_argument(
        "--no-update-local",
        action="store_true",
        help="do not move local branches/worktrees to match what was pushed",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Parses arguments, runs the rebuild, and returns a process exit code."""
    args = build_parser().parse_args(argv)
    try:
        return _run(args)
    except GitError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


def _run(args: argparse.Namespace) -> int:
    repo_root = Path(git_out("rev-parse", "--show-toplevel"))
    config_ref = args.config_ref or f"{args.origin_remote}/{args.branches_branch}"
    current_branch = git_out("rev-parse", "--abbrev-ref", "HEAD", cwd=repo_root)

    config_kind, config_value, config_desc = config_source(
        repo_root, args.config, config_ref, args.branches_branch
    )
    print(f"Branch list source: {config_desc}\n")

    for remote in (args.upstream_remote, args.origin_remote):
        if remote not in git_out("remote", cwd=repo_root).splitlines():
            print(
                f"error: remote '{remote}' is not configured. Add it, e.g.:\n"
                f"  git remote add {args.upstream_remote} "
                "https://github.com/llm-d/llm-d-router.git",
                file=sys.stderr,
            )
            return 2

    if not args.no_fetch:
        print(
            f"Fetching {args.upstream_remote}/{args.upstream_branch} "
            f"and {args.origin_remote} ..."
        )
        git("fetch", args.upstream_remote, args.upstream_branch, cwd=repo_root)
        git("fetch", "--prune", args.origin_remote, cwd=repo_root)

    if branches_branch_guard_applies(
        current_branch, args.branches_branch, args.target_branch, args.triggered_by
    ) and branches_branch_diverged(repo_root, args.origin_remote, args.branches_branch):
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

    reserved = reserved_conflicts(branches, {args.main_branch, DEFAULT_TARGET_BRANCH})
    if reserved:
        print(
            f"error: {', '.join(reserved)} must not be listed in the branch config: "
            "they are built from the other branches, not a source of commits.",
            file=sys.stderr,
        )
        return 2

    if args.triggered_by is not None:
        # A push-triggered run mirrors CI: it only ever rebuilds the target and never
        # touches the main branch, whatever --update-main would otherwise select.
        update_main = False
        if not is_triggering_branch(args.triggered_by, args.main_branch, branches):
            print(
                f"Push to '{args.triggered_by}' does not affect {target_branch}: it is "
                f"neither '{args.main_branch}' nor a listed branch. Nothing to do."
            )
            return 0

    base_ref = f"{args.upstream_remote}/{args.upstream_branch}"
    base_sha = git_out("rev-parse", base_ref, cwd=repo_root)

    missing = [
        b
        for b in branches
        if git(
            "rev-parse",
            "--verify",
            "--quiet",
            f"{args.origin_remote}/{b}^{{commit}}",
            cwd=repo_root,
            check=False,
        ).returncode
        != 0
    ]
    if missing:
        print(
            f"error: these branches are not on '{args.origin_remote}': "
            f"{', '.join(missing)}",
            file=sys.stderr,
        )
        return 2

    print(f"Base:   {base_ref} = {base_sha[:12]}")
    print(f"Target: {target_branch}   (update {args.main_branch}: {update_main})")
    print(f"Branches ({len(branches)}): {', '.join(branches)}")

    use_gh = not args.no_gh and shutil.which("gh") is not None
    warnings = preflight_checks(
        branches, base_ref, args.origin_remote, args.upstream_remote, use_gh, repo_root
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

    git("worktree", "prune", cwd=repo_root)
    tmp = Path(tempfile.mkdtemp(prefix="mistral-main-rebuild-"))
    git("worktree", "add", "--detach", str(tmp), base_sha, cwd=repo_root)
    try:
        try:
            result = rebuild(tmp, base_ref, branches, args.origin_remote, config_text)
        except ConflictError as exc:
            _report_conflict(args, base_ref, exc, repo_root)
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

        specs: list[PushSpec] = []
        if update_main:
            specs.append(PushSpec(args.main_branch, base_sha, args.force_push_main))
        specs.append(PushSpec(target_branch, result.new_sha, force=True))

        print(f"\n{'Pushing' if args.push else 'Dry-run (pass --push to apply)'}:")
        try:
            push_branches(args.origin_remote, specs, args.push, repo_root)
        except GitError as exc:
            print(
                f"error: push failed (no branch was updated):\n{exc}", file=sys.stderr
            )
            if update_main and not args.force_push_main:
                print(
                    f"If {args.main_branch} does not fast-forward from its remote, re-run "
                    "with --force-push-main.",
                    file=sys.stderr,
                )
            return 1

        if args.push and not args.no_update_local:
            print("\nUpdating local branches/worktrees:")
            if update_main:
                update_local_branch(args.main_branch, base_sha, repo_root)
            update_local_branch(target_branch, result.new_sha, repo_root)
        return 0
    finally:
        git("worktree", "remove", "--force", str(tmp), cwd=repo_root, check=False)


def _report_conflict(
    args: argparse.Namespace, base_ref: str, exc: ConflictError, cwd: Path
) -> None:
    """Prints a conflict diagnostic, with an update-first hint for a stale branch."""
    print(file=sys.stderr)
    print(
        f"error: conflict applying commit {exc.commit[:12]} ({exc.subject})",
        file=sys.stderr,
    )
    print(f"       from branch '{exc.branch}'", file=sys.stderr)
    if exc.files:
        print(f"       conflicted files: {', '.join(exc.files)}", file=sys.stderr)
    if not is_ancestor(base_ref, f"{args.origin_remote}/{exc.branch}", cwd=cwd):
        print(
            f"\n'{exc.branch}' is behind {base_ref}. First update it and retry, which\n"
            "often resolves the conflict on its own:\n"
            f"    git fetch {args.upstream_remote} {args.upstream_branch}\n"
            f"    git rebase {base_ref} {exc.branch}\n"
            f"    git push --force-with-lease {args.origin_remote} {exc.branch}",
            file=sys.stderr,
        )
    print(
        f"\nOtherwise fix it on branch '{exc.branch}': resolve the conflict there, or, "
        "if\nit clashes with another listed branch, replace both with a single branch "
        "that\nmerges them with the conflict resolved. The rebuilt branch is never "
        "edited by hand.",
        file=sys.stderr,
    )


if __name__ == "__main__":
    sys.exit(main())
