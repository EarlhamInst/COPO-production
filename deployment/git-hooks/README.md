# Git hooks

Tracked git hooks for this repo. They are **not** active until each clone points
git at this directory (git can't enable hooks automatically, for security).

## One-time setup (per clone)

```bash
git config core.hooksPath deployment/git-hooks
```

That's it — every hook in this folder is now live for your clone. Undo with
`git config --unset core.hooksPath`.

## Hooks

### `post-checkout`
After a **branch switch**, if the schema or profile-type definitions differ
between the branch you left and the one you entered, it re-runs
`setup_schemas` then `setup_profile_types` inside the running `copo_web`
container — re-syncing Mongo + Postgres to the checked-out code.

COPO seeds these definitions into the database once and then serves them from
there, so switching between branches that differ (e.g. `main` <-> `edp`) would
otherwise leave the database describing the old branch (import errors, profiles
that won't render). The hook keeps the database in step with the code.

- Runs only on real branch switches, only when the relevant files changed.
- Skip a single checkout with `COPO_SKIP_SETUP=1 git checkout <branch>`.
- Override the target container with `COPO_WEB_CONTAINER=<name>`.
- If the stack isn't running, it prints the commands to run manually instead.
