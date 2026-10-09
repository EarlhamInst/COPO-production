# Running the Playwright browser test suite

## What this is

`test/playwright/` is COPO's browser test suite: it drives a real Chromium
browser against a running COPO instance and checks that pages actually work,
the way a user would experience them. This is different from `test/unit/`,
which tests Python code directly with no browser involved.

The tests don't run inside your normal dev environment (`copo_web`). They run
in a separate, purpose-built container (`copo_playwright`) that has Python,
Playwright, and an actual browser installed — `copo_web` doesn't have any of
that, and isn't meant to.

## Prerequisites

**Step 1: Start the local project stack**

  The container, `copo_web`, should be serving at `http://localhost:8000` 
  in your browser. If it isn't, start it using VS Code's `Start all` compound 
  from `launch.json`.

**Step 2: Export environment variables**

  Export `COPO_PROJECT_SETUP_DIR` and `COPO_LOCAL_COMPOSE_FILE_NAME` in
  the terminal session (or permanently in your shell configuration file,
  `~/.zshenv` (zsh, the macOS default) or `~/.bashrc` (bash) then, reload
  with `source ~/.zshenv` or `source ~/.bashrc` (or open a new terminal)).
  They should point to the path of your local project stack's setup directory and
  the file name of its local Docker compose file (which is located inside the same directory).

  `~/.zshenv` is preferred over `~/.zshrc` because `zsh` only reads `~/.zshrc` for
  interactive shells so, the script will only work when typed into a terminal but
  fails with "not set" when launched by anything else like an IDE task or a
  background job etc..

  There are no defaults in the `run_playwright_tests.sh` script because each 
  developer's stack is located somewhere different. The script will stop and 
  display an error message if either is not set or doesn't exist.

  Example of setting the environment variables:
  ```
  export COPO_PROJECT_SETUP_DIR="$HOME/Desktop/project_setup"
  export COPO_LOCAL_COMPOSE_FILE_NAME="compose.yaml"
  ```

**Step 3: Build latest Playwright Docker image (optional)**

  You do not usually need to build the `playwright:v1.62.1` image because the
  Docker compose service already declares a `build:` section which builds the tag on your 
  local machine if it doesn't exist. This takes a few minutes to be built; it mainly involves 
  pulling the base image. To build it by hand — say, to rebuild after changing
  `requirements/dev.txt`:

  ```
  docker build . -f deployment/web/Dockerfile_playwright -t playwright:v1.62.1
  ```

  The tag tracks the base image version in `Dockerfile_playwright`, so it
  changes whenever that's bumped. That's deliberate: a bump asks for a tag
  nobody has yet, and the `build:` section turns what would be a confusing
  "image not found" into one automatic rebuild.

  `Dockerfile_playwright` bakes the repo's source in at build time
  (`COPY . /copo`), but `docker-compose.playwright.yaml` then bind-mounts
  your actual working tree over the top of that at container-start time —
  same pattern as `copo_web`. So in practice, edits to test files,
  `conftest.py`, or app code show up immediately on the next run; you only
  need to rebuild when *dependencies* change (`requirements/dev.txt`), since
  those are installed at build time and aren't part of the mount.

  Which tree gets mounted comes from `COPO_REPO_ROOT`, which
  `run_playwright_tests.sh` sets to the checkout it was invoked from. The
  compose file has no fallback on purpose — mounting a wrong-but-plausible
  path silently runs the suite against a different tree, and the results
  still look convincing. Invoking `docker compose` by hand therefore needs
  `COPO_REPO_ROOT` exported, or it stops with a message saying so.

### Running from a git worktree

The runner mounts whichever checkout you invoke it from, so running the suite
inside a worktree tests that worktree's **test** files. `copo_web` is a
different matter because its bind mount is declared in the local stack's own 
compose file. The environment variable, `$COPO_LOCAL_COMPOSE_FILE_NAME`, is 
set outside the repository and will always point to a fixed checkout.

So the app under test is *not* your worktree's app code. The runner detects
the mismatch and warns:

```bash
WARNING: tests run from   .../.claude/worktrees/my-branch
         copo_web serves  <path-to-project-directory-root>/COPO-production
         Changes to test files take effect; changes to app code do NOT.
```

Test-only changes (fixtures, assertions, helpers) are fine. To exercise
worktree **app code**, repoint `copo_web` at it and recreate that service —
which also restarts the Celery workers the submission tests depend on, so
bring them back up via VS Code's `Start all` afterwards.

## Running the tests

> Do not run the tests from inside the `copo_web` container's shell via 
> `docker compose exec` or via VS Code `Dev Containers` extension; always run 
> them from the root of the project's repository on your local machine.

```bash
# Navigate to the root of the project repository
cd </path/to/project/repository>/COPO-production

# Run entire Playwright test suite
test/playwright/scripts/run_playwright_tests.sh
```

Run from the repo root (on your Mac — not from inside the `copo_web`
container's shell; the script calls `docker compose`, which needs to talk to
Docker itself). With no arguments, this runs the whole `test/playwright/`
suite. Any other arguments you pass go straight through to `pytest`, e.g. to
run one specific file:

```bash
test/playwright/scripts/run_playwright_tests.sh test/playwright/e2e/test_case_login.py
```

Day to day, that's all you need — a normal pytest run tells you pass/fail
via its summary output and exit code, same as any test suite. Tracing is
**off** by default (it adds real overhead — see below), so a bare run like
the ones above never produces a trace file.

To record a trace, add `-t` anywhere in the arguments (it expands to
`--tracing=on -v` and is stripped before the rest is passed to pytest):

```bash
test/playwright/scripts/run_playwright_tests.sh -t test/playwright/e2e/test_case_login.py
```

### Trace size and overhead

One data point: a trivial test (one page load, one assertion, ~2s) produced
a 1.6MB trace with full screenshots + DOM snapshots + sources. A realistic
multi-step journey test will likely run several MB. Full tracing also slows
each action down somewhat (roughly 20-30%, per Playwright's own guidance) —
negligible for a single short test, more noticeable across a whole suite run
repeatedly. This is why tracing is opt-in rather than always-on.

## Tests marked `external` (real ENA/Zenodo calls)

Some tests make real network calls to third-party services — ENA's dev
sandbox (`wwwdev.ebi.ac.uk`, safe, EBI's own intended test environment) and
production Zenodo (`zenodo.org` — there is no Zenodo sandbox anywhere in
this repo). These are marked `@pytest.mark.external` and are **excluded by
default**: a bare `run_playwright_tests.sh` run always passes
`-m "not external"`.

To opt in and run them too, pass `-e` as the first argument:

```bash
test/playwright/scripts/run_playwright_tests.sh -e
```

`-e` can be combined with a specific file/test path, same as any other
argument. Expect these to be slower than the rest of the suite (they poll
for real, asynchronous ENA/Zenodo responses rather than asserting against
local, synchronous state) and to leave real side effects behind (an ENA
sandbox submission, a Zenodo draft deposition — never a published one).

## Watching a test live

While a test is running, you can watch the actual browser instead of
waiting for it to finish. Connect any VNC client to `localhost:5901` — on
macOS, Safari or Finder's `Cmd+K` understands `vnc://localhost:5901`
directly, no extra software needed. This only works while a run is actually
in progress; the container (and its VNC server) is torn down the moment the
test finishes.

## Viewing a trace after the fact

`-t` (or `--tracing=on` directly) records a full scrubbable timeline
(screenshots, DOM snapshots, network requests, console logs) for each test,
written to `test/playwright/test_results/<test-name>/trace.zip` (gitignored;
`run_playwright_tests.sh` sets `--output=test/playwright/test_results`
explicitly, since pytest-playwright's own default of `test-results` relative
to cwd would otherwise land at the repo root).

**pytest-playwright wipes the entire output directory at the start of every
run**, traced or not — so a trace from a previous run disappears the moment
you start the *next* run, not after it. If you want to keep a specific
trace, copy it out before running again.

Open a trace with:

```bash
python -m playwright show-trace test/playwright/test_results/<test-name>/trace.zip
```

(Requires `pip install playwright` on your Mac if you don't already have
it — this is separate from the Playwright install inside the container.)

Use this when a test fails and the pytest output alone doesn't explain why,
or the first time you write a new test and want to confirm it's actually
doing what you intended.

## Test-only login

Browser tests should log in as a local test user via username/password, not
by driving the real ORCID OAuth flow — ORCID is a third-party service with
no test credentials, and automating a real login through it is inherently
fragile (this is why the pre-existing suites under `test/testcases/` are
mostly broken). `create_test_user` (run automatically as part of
`setup_all`) creates a local user that logs in via allauth's plain
username/password form at `/accounts/login/`, which works with zero
application code changes.

## GitHub Actions

`.github/workflows/playwright.yml` is currently disabled (`workflow_dispatch`
only — it won't fire on PRs or pushes). It never actually ran the suite: its
`run: test playwright run` step isn't a real command (bash parses `test` as
its own built-in operator, not an invocation), and the commented-out
fallback below it wouldn't have worked either — it starts `manage.py
runserver` with no Postgres/Redis/Mongo/MinIO behind it, which the app needs
just to boot. Making this real means standing up those services (Mongo
needs a replica set + keyfile, per `$COPO_PROJECT_SETUP_DIR/$COPO_LOCAL_COMPOSE_FILE_NAME`) as GitHub
Actions services — still outstanding work. Until then, run the suite locally
as described above.
