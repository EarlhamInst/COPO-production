#!/bin/bash
# Runs the COPO Playwright suite against the local stack in project_setup/.
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

PROJECT_SETUP_DIR="$HOME/Desktop/project_setup"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLAYWRIGHT_COMPOSE="$SCRIPT_DIR/../docker-compose.playwright.yaml"

if [ ! -d "$PROJECT_SETUP_DIR" ]; then
  echo "Expected local stack directory not found: $PROJECT_SETUP_DIR" >&2
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
docker compose --env-file .env -f compose.yaml -f "$PLAYWRIGHT_COMPOSE" \
  run --rm --service-ports copo_playwright pytest "${PYTEST_ARGS[@]}"
