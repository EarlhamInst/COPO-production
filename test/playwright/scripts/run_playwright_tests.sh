#!/bin/bash
# Runs the COPO Playwright suite against your local COPO project stack.
#
# Required environment variables (the script stops if either of them is not set):
#   COPO_PROJECT_SETUP_DIR   - the directory holding your local project deployment setup,
#                              i.e. "Step 1" of the "Local Setup Instructions" at
#                              https://copo-docs.readthedocs.io/en/latest/advanced/project-setup/project-local-setup-index.html
#   COPO_LOCAL_COMPOSE_FILE_NAME  - the name of the local project stack's Docker compose file 
#                                   that is located inside the COPO_PROJECT_SETUP_DIR
#
# Set them in the terminal (or once in ~/.zshenv (zsh) or ~/.bashrc (bash)), 
# then, reload the shell with `source ~/.zshenv` or `source ~/.bashrc`.
#
# Example:
#   export COPO_PROJECT_SETUP_DIR="$HOME/Desktop/project_setup"
#   export COPO_LOCAL_COMPOSE_FILE_NAME="compose.yaml"
#
# Usage:
#   test/playwright/scripts/run_playwright_tests.sh                     # whole suite
#   test/playwright/scripts/run_playwright_tests.sh -t test/playwright/e2e/test_case_login.py
#   test/playwright/scripts/run_playwright_tests.sh test/playwright/e2e/test_case_login.py --tracing=on -v
#   test/playwright/scripts/run_playwright_tests.sh -e test/playwright/e2e/test_case_submission_journey.py::test_full_submission_and_publish_journey
#
# -t records a trace (expands to --tracing=on -v). -e includes tests marked
# "external" (real calls to ENA's dev sandbox / production Zenodo) — these
# are excluded by default (-m "not external") since they're slow and hit
# real third-party services. Both flags are stripped before the rest of the
# arguments are passed straight through to pytest. With no other arguments,
# the whole test/playwright/ suite runs (a bare `pytest` would run test/unit
# instead, per pytest.ini's testpaths). Traces land in
# test-results/<test-name>/trace.zip — see docs/testing/PLAYWRIGHT.md.
set -euo pipefail

# Each developer's local project stack is located somewhere different. 
# As a result,  a default path is not set to avoid running the script 
# against a "wrong" stack.
#
# ${VAR:-} keeps `set -u` from aborting before the message below is printed.
PROJECT_SETUP_DIR="${COPO_PROJECT_SETUP_DIR:-}"
LOCAL_COMPOSE_FILE_NAME="${COPO_LOCAL_COMPOSE_FILE_NAME:-}"
MISSING_VARS=0

# Check whether the required environment variables are set
if [ -z "$PROJECT_SETUP_DIR" ]; then
  printf "COPO_PROJECT_SETUP_DIR is not set: export it to your local stack directory. \n"
  printf "e.g. export COPO_PROJECT_SETUP_DIR=\"$HOME/Desktop/project_setup\"\n" >&2
  MISSING_VARS=1
fi

if [ -z "$LOCAL_COMPOSE_FILE_NAME" ]; then
  printf "\nCOPO_LOCAL_COMPOSE_FILE_NAME is not set: export it to your local stack's compose file. \n"
  printf "e.g. export COPO_LOCAL_COMPOSE_FILE_NAME=\"compose.yaml\"\n\n" >&2
  MISSING_VARS=1
fi

if [ "$MISSING_VARS" -eq 1 ]; then
  echo "See the header of $0 for an example." >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLAYWRIGHT_COMPOSE="$SCRIPT_DIR/../docker-compose.playwright.yaml"

# Mount whichever checkout this script was invoked from, rather than a path
# baked into the compose file — so running the suite from a git worktree
# actually tests that worktree. docker-compose.playwright.yaml reads this.
COPO_REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
export COPO_REPO_ROOT

if [ ! -d "$PROJECT_SETUP_DIR" ]; then
  echo "Expected local stack directory not found: $PROJECT_SETUP_DIR" >&2
  echo "Check if COPO_PROJECT_SETUP_DIR is set in ~/.zshenv (zsh) or ~/.bashrc (bash)." >&2
  exit 1
fi

# Docker compose resolves a relative `-f` against the directory it runs in,
# which is PROJECT_SETUP_DIR (see the `cd` below), so it is checked in the
# same manner.
case "$LOCAL_COMPOSE_FILE_NAME" in
  /*) LOCAL_COMPOSE_PATH="$LOCAL_COMPOSE_FILE_NAME" ;;
  *) LOCAL_COMPOSE_PATH="$PROJECT_SETUP_DIR/$LOCAL_COMPOSE_FILE_NAME" ;;
esac

if [ ! -f "$LOCAL_COMPOSE_PATH" ]; then
  echo "Expected local project stack compose file not found: $LOCAL_COMPOSE_PATH" >&2
  echo "Check if COPO_LOCAL_COMPOSE_FILE_NAME is set in ~/.zshenv (zsh) or ~/.bashrc (bash)." >&2
  exit 1
fi

TRACE=0
INCLUDE_EXTERNAL=0
REMAINING_ARGS=()
for arg in "$@"; do
  if [ "$arg" = "-t" ]; then
    TRACE=1
  elif [ "$arg" = "-e" ]; then
    INCLUDE_EXTERNAL=1
  else
    REMAINING_ARGS+=("$arg")
  fi
done

# macOS ships bash 3.2 (frozen pre-GPLv3), where "${arr[@]}" on an empty
# array throws "unbound variable" under `set -u`, unlike bash 4.4+. Guard
# with a length check instead of expanding REMAINING_ARGS directly.
PYTEST_ARGS=()
if [ ${#REMAINING_ARGS[@]} -gt 0 ]; then
  PYTEST_ARGS=("${REMAINING_ARGS[@]}")
fi
if [ ${#PYTEST_ARGS[@]} -eq 0 ]; then
  PYTEST_ARGS=("test/playwright")
fi
if [ "$TRACE" -eq 1 ]; then
  PYTEST_ARGS+=("--tracing=on" "-v")
fi
if [ "$INCLUDE_EXTERNAL" -eq 0 ]; then
  PYTEST_ARGS+=("-m" "not external")
fi

# pytest-playwright's default --output ("test-results", relative to cwd) would
# land at the repo root. Keep it under test/playwright/ instead, alongside the
# suite itself. Not set globally in pytest.ini: test/unit runs in an
# environment where the pytest-playwright plugin isn't installed, and passing
# --output there would fail with "unrecognized arguments".
PYTEST_ARGS+=("--output=test/playwright/test_results")

cd "$PROJECT_SETUP_DIR"

# copo_web's own bind mount is declared in the local stack's compose file
# (LOCAL_COMPOSE_FILE_NAME), which lives outside this repo and names one
# fixed checkout. So when the suite runs from a worktree, the *test* files
# come from COPO_REPO_ROOT but the application under test still does not.
# That's harmless for test-only changes and quietly misleading for app-code
# ones, so say so rather than fail: repointing copo_web means recreating it,
# which also takes down the Celery workers the submission tests depend on.
WEB_CONTAINER="$(docker compose --env-file .env -f "$LOCAL_COMPOSE_FILE_NAME" \
  -f "$PLAYWRIGHT_COMPOSE" ps -q copo_web 2>/dev/null || true)"
if [ -n "$WEB_CONTAINER" ]; then
  WEB_MOUNT="$(docker inspect --format \
    '{{range .Mounts}}{{if eq .Destination "/copo"}}{{.Source}}{{end}}{{end}}' \
    "$WEB_CONTAINER" 2>/dev/null || true)"
  if [ -n "$WEB_MOUNT" ] && [ "$WEB_MOUNT" != "$COPO_REPO_ROOT" ]; then
    echo "WARNING: tests run from   $COPO_REPO_ROOT" >&2
    echo "         copo_web serves  $WEB_MOUNT" >&2
    echo "         Changes to test files take effect; changes to app code do NOT." >&2
    echo >&2
  fi
fi

docker compose --env-file .env -f "$LOCAL_COMPOSE_FILE_NAME" -f "$PLAYWRIGHT_COMPOSE" \
  run --rm --service-ports copo_playwright pytest "${PYTEST_ARGS[@]}"
