# Git hooks

Tracked git hooks for this repo. They are **not** active until each clone points
git at this directory (git can't enable hooks automatically, for security).

## One-time setup (per clone)

```bash
git config core.hooksPath deployment/git-hooks
```

Every hook in the folder is now live for that clone. Undo it with
`git config --unset core.hooksPath`.

## Hooks

### `post-checkout`
After a **branch switch**, it re-syncs the running `copo_web` container to the
checked-out code:

- If anything under `requirements/` differs between the branch the developer
  left and the one that was switched to, it runs 
  `pip install -r requirements/dev.txt` in the container.

- If migrations, schemas or the setup commands differ, the commands 
  listed in `setup_commands` are executed in the order that they are 
  listed to update both MongoDB and PostgreSQL databases

  Commands that are re-run:
  
  ```bash
  python manage.py migrate --noinput
  python manage.py relax_unmodelled_columns
  python manage.py setup_schemas
  python manage.py setup_profile_types
  python manage.py setup_groups
  ```

COPO feeds these definitions into the database once and then serves them from
there, so switching between branches that differ
(e.g. `main` branch <-> `edp` branch) would otherwise leave the database
describing the old branch (import errors, profiles that won't render).
The hook keeps the database in step with the code.

The `migrate` command cannot remove a column that another branch's
migration added because that migration does not exist on the branch
that was switched to so, `relax_unmodelled_columns` makes such columns
optional so that inserts work again.

The branch that added the column will always set it so that
switching back is unaffected. The command only runs when `DEBUG=True` and
`--dry-run` lists the columns without changing them.

- Runs only on real branch switches and only when relevant files have been changed.
- Skip a single checkout with `COPO_SKIP_SETUP=1 git checkout <branch>`.
- Override the target container with `COPO_WEB_CONTAINER=<name>`.
- If the stack isn't running, it prints the commands to run manually instead.


**Synopsis**

Packages installed using this approach only live in the container, 
not the local Docker image, so they are lost when the container is recreated 
(via `docker compose down`/`up`). The local Docker compose files circumvent this 
by running the same `pip install` every time `copo_web` service starts.

This enables the developer to only really need to rebuild the local Docker image 
for system-level changes (like apt packages or a new Python version).

Note: `pip install` does not remove packages that a branch has dropped. They 
stay installed until the container is recreated.
