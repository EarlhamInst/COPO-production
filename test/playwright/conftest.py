import pytest

from test.playwright.config.settings import BASE_URL, TEST_USER_PASSWORD, TEST_USER_USERNAME

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
