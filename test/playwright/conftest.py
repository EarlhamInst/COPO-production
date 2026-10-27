import os
import subprocess

import pytest
import requests

from test.playwright.config.settings import BASE_URL, TEST_USER_PASSWORD, TEST_USER_USERNAME
from test.playwright.e2e.utils.helpers import run_django_shell

# pytest-playwright provides `browser` and `context` fixtures out of the box.
# We override `context` here so every test's browser context starts already
# logged in, instead of each test re-driving the login form itself.
STORAGE_STATE_PATH = "test/playwright/.auth_state.json"


@pytest.fixture(scope="session")
def browser_context_args(browser_context_args):
    return {**browser_context_args, "base_url": BASE_URL}


@pytest.fixture(scope="session", autouse=True)
def _use_data_test_attribute(playwright):
    # COPO's templates mark test hooks with data-test="...", not Playwright's
    # own default of data-testid — point get_by_test_id() at the right one.
    playwright.selectors.set_test_id_attribute("data-test")


@pytest.fixture(scope="session")
def storage_state(browser):
    """
    Log in once for the whole test session and persist the resulting cookies
    to disk, so per-test browser contexts can be seeded from this instead of
    each test having to drive the login form and onboarding modals itself.
    """
    context = browser.new_context(base_url=BASE_URL)
    page = context.new_page()

    page.goto("/accounts/login/")
    page.fill("#id_login", TEST_USER_USERNAME)
    page.fill("#id_password", TEST_USER_PASSWORD)
    page.click("button[type=submit]")
    page.wait_for_load_state("networkidle")

    accept_cookies = page.locator("#acceptCookies")
    if accept_cookies.is_visible():
        accept_cookies.click()

    gdpr_submit = page.locator("#submit")
    if gdpr_submit.is_visible():
        page.fill("#emaddres", TEST_USER_USERNAME)
        page.check("#gdpr_check")
        gdpr_submit.click()
        page.wait_for_load_state("networkidle")

    context.storage_state(path=STORAGE_STATE_PATH)
    context.close()
    return STORAGE_STATE_PATH


@pytest.fixture
def context(new_context, storage_state):
    # Route through pytest-playwright's own `new_context` factory (rather than
    # calling browser.new_context() ourselves) so its artifacts recorder still
    # sees this context — that's what actually drives --tracing/--screenshot/
    # --video, not the CLI flags alone. Its own fixture teardown closes the
    # context and saves the trace; nothing further to do here.
    return new_context(storage_state=storage_state)


@pytest.fixture(scope="session")
def ena_dev_sandbox():
    # Sample manifest validation looks every taxon up on ENA_URL_HOSTNAME
    # (ENA's dev sandbox locally), and sample submission goes there too. When
    # that host is down -- seen 2026-10-20, every /ena/taxonomy and
    # /ena/browser path returned an nginx 404 -- a dependent test can only
    # time out on a disabled "Finish" button, which reads like a COPO bug.
    # Skip with the real reason instead.
    host = os.environ.get("ENA_URL_HOSTNAME") or "www.ebi.ac.uk"
    url = f"https://{host}/ena/taxonomy/rest/tax-id/9606"
    try:
        status = requests.get(url, timeout=30).status_code
    except requests.RequestException as error:
        pytest.skip(f"ENA unreachable ({url}): {error}")
    if status != 200:
        pytest.skip(f"ENA taxonomy lookup is down ({url} returned HTTP {status})")
    return host


@pytest.fixture(scope="session")
def sapio_test_instance():
    # Unlike ENA above, a missing or unreachable Sapio is a setup problem in
    # the local stack rather than a third-party outage, so fail rather than
    # skip.
    if not os.environ.get("SAPIO_API_URL"):
        pytest.fail(
            "SAPIO_API_URL is not set in the Playwright container. Run the suite via "
            "test/playwright/scripts/run_playwright_tests.sh with copo_web running."
        )
    try:
        run_django_shell(
            "from src.apps.ei_edp.utils.lims.sapio.datamanager import Sapio\n"
            "Sapio().dataRecordManager.query_data_records("
            "data_type_name='Project', data_field_name='C_ProjectIdentifier', value_list=['PLAYWRIGHT_PROBE'])\n"
        )
    except subprocess.CalledProcessError as error:
        pytest.fail(f"Could not query Sapio at {os.environ['SAPIO_API_URL']}: {error.stderr[-2000:]}")
    return os.environ["SAPIO_API_URL"]
