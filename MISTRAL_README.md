# Mistral fork workflow

This fork of `llm-d/llm-d-router` keeps our not-yet-upstreamed changes as a set of
small feature branches that are replayed on top of upstream to produce a single
consumable branch. The replay is done by the `mistral-release` tool (in
`mistral-release/`), driven by `.mistral_branches.txt`.

## Branches

- **`main`**: an exact mirror of `upstream/main` (`llm-d/llm-d-router`). Updated by
  the tool. Do not commit here.
- **`mistral-main`**: `main` plus every mistral-specific commit from the branches
  listed in `.mistral_branches.txt`. Rebuilt from scratch on every update. This is
  the branch to deploy and run in dev and prod.
- **`mistral-branches`**: holds the tooling only, the `mistral-release/` package,
  `.mistral_branches.txt`, the CI workflow and this file. Edit the branch list
  here.
- **feature branches**: small, self-contained change each, based directly on `main`.

Do not develop on top of `mistral-main`. It is force-rebuilt and its history is
rewritten on every update, so any commit made on it is lost. Put your change on a
feature branch and add it to the list instead.

## Add a change

Base the branch on the latest `main`, keep it focused on one change:

```sh
git fetch upstream main
git switch -c myuser/my-fix upstream/main
# ... edit, commit ...
git push -u origin myuser/my-fix
```

If the change belongs upstream, open a PR against `llm-d/llm-d-router` and keep the
link. Then, on `mistral-branches`, add the branch to `.mistral_branches.txt` with a
comment linking that PR:

```
myuser/my-fix    # <what it does> (upstream: https://github.com/llm-d/llm-d-router/pull/NNNN)
```

Once the upstream PR merges, drop the branch from the list: the tool detects it
as already present upstream and skips it anyway, and the `gh` pre-flight check
flags it so you know it can go.

## Rebuild mistral-main

Run this from the `mistral-branches` worktree (that branch owns the tool and the
list). It works from any worktree of the repo, but running it elsewhere reads the
list from `origin/mistral-branches` rather than your local edits.

The first line of output states exactly which file or ref the list was read from,
so a missing branch is never a surprise. The rebuilt branch always carries that
exact `.mistral_branches.txt`, so any build documents the list that produced it.

Dry-run (default, touches nothing):

```sh
uv run --project mistral-release update-mistral-main
```

Apply (fast-forwards `main` to `upstream/main` and force-pushes `mistral-main`):

```sh
uv run --project mistral-release update-mistral-main --push
```

`main` is only fast-forwarded; if it has diverged from `upstream/main` the push
fails until you pass `--force-push-main`. `mistral-main` (or any `--target-branch`)
is always force-pushed.

On `--push`, local branches are moved to match what was pushed: a branch checked
out in a worktree is hard-reset when that worktree is clean (untracked files are
kept), or left untouched with a warning if it has uncommitted tracked changes; a
local branch with no worktree is just repointed. So a worktree sitting on `main` or
`mistral-main` ends up at the rebuilt commit rather than drifting behind. Pass
`--no-update-local` to skip this and only update the remote.

## Automatic rebuilds (CI)

The **Build mistral-main branch** GitHub Action rebuilds `mistral-main` for you.

- **On every push**: the action runs, but the tool exits cleanly and does nothing
  unless the pushed branch is `main` or one of the branches listed in
  `.mistral_branches.txt`. When it is, `mistral-main` is rebuilt and force-pushed.
  A push-triggered run never touches `main`. So pushing a feature branch that is in
  the list (or updating `mistral-branches`) is enough to refresh `mistral-main`.
- **Manual dispatch**: use it to force a rebuild, to run a dry-run (uncheck *push*),
  or to also mirror `main` to the latest `upstream/main` (check *update main*).

The tool's own pushes do not start another run: with the default `GITHUB_TOKEN`,
GitHub does not re-trigger workflows from pushes it makes, and a push to
`mistral-main` is a no-op anyway since it is not a listed branch. So a manual
dispatch that updates `main` does not loop back into another build.

Before replaying, it checks each listed branch and prints a diagnostic when a
branch is stacked on another listed branch or already appears in an upstream merged
PR. A stale base (branch behind the latest `main`) is not flagged on its own, since
upstream moves fast and branches are not kept rebased; it is only mentioned as the
first thing to try if it actually causes a conflict. A cherry-pick conflict stops
the run and names the branch and commit to fix.

## Personal test build

To try your own set of branches without touching `main` or `mistral-main`, run from
the `mistral-branches` worktree, add your branch to `.mistral_branches.txt`
(uncommitted is fine), and pick a throwaway target branch:

```sh
uv run --project mistral-release update-mistral-main --target-branch myuser/try --push
```

To just check whether an edit builds cleanly before pushing it, a plain dry-run on
`mistral-branches` with the edit uncommitted is enough:

```sh
# edit .mistral_branches.txt, then:
uv run --project mistral-release update-mistral-main
```

The dry-run uses your uncommitted list and prints a notice that it is doing so. A
`--push` run, by contrast, requires an explicit `--target-branch` whenever you run
on `mistral-branches` and its state differs from `origin/mistral-branches`
(uncommitted edits or unpushed commits), so a local experiment can never overwrite
the canonical `mistral-main` by accident.

`main` is left untouched when the target is not `mistral-main`, and the target
branch is created if it does not exist (only on `--push`; a dry-run creates
nothing). Clean up when done:

```sh
git push origin --delete myuser/try
```

## Keeping branches clean and resolving conflicts

Every branch must sit directly on `main` and apply independently of the others.

When upstream moves and a branch no longer applies, rebase that branch onto the
new `main` and resolve the conflict there, then push it back:

```sh
git fetch upstream main
git rebase upstream/main myuser/my-fix
# ... resolve, then ...
git push --force-with-lease origin myuser/my-fix
```

When two of our own branches conflict with each other, they cannot both apply on a
clean `main`. Do not stack one on the other (the pre-flight check flags that, and
ordering becomes fragile). Instead express the forward fix as a single integration
branch that merges both and resolves the conflict once:

```sh
git fetch upstream main
git switch -c myuser/a-plus-b upstream/main
git merge --no-ff origin/myuser/a origin/myuser/b   # resolve the conflict here
git push -u origin myuser/a-plus-b
```

Then list `myuser/a-plus-b` and remove `myuser/a` and `myuser/b`. This keeps each list entry
independent and the replay deterministic. To avoid re-resolving the same conflict
on future rebuilds, enable rerere once so git records and replays your resolution:

```sh
git config --global rerere.enabled true
```

## Developing the tool

The rebuild logic lives in the `mistral-release/` uv package (`src/mistral_release/`,
split into `gitcmd`, `config`, `rebuild`, `checks`, `remote`, `cli`). Run the tests
against isolated throwaway git repos with:

```sh
uv run --project mistral-release pytest
```
