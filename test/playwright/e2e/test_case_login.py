import re
import subprocess
import sys

from playwright.sync_api import Page, expect

from test.playwright.config.test_ids import ProfilePage
from test.playwright.config.settings import (
    PROFILE_PAGE_URL,
    TEST_USER_PASSWORD,
    TEST_USER_USERNAME,
)
from test.playwright.e2e.helpers import dismiss_onboarding_modals, fill_and_submit_login_form


def test_login(page: Page):
    # `page`'s context is seeded from the storage_state fixture in conftest.py,
    # which logs in once via the test user's username/password (not ORCID —
    # see docs/testing/PLAYWRIGHT.md for why). So landing straight on the
    # profile page, with no redirect to the login form, is the actual thing
    # under test here: that the saved session is valid.
    page.goto(PROFILE_PAGE_URL)

    assert ProfilePage.PROFILE_URL_PART in page.url


def test_login_rejects_bad_credentials(new_context):
    # Deliberately not using the `page`/`context` fixtures here — those are
    # pre-authenticated via storage_state. This test drives the real login
    # form itself, so it needs a fresh, anonymous context.
    context = new_context()
    page = context.new_page()

    fill_and_submit_login_form(page, TEST_USER_USERNAME, "definitely-the-wrong-password")

    expect(page.locator(".errorlist.nonfield")).to_contain_text(
        "The username and/or password you specified are not correct."
    )

    context.close()


def test_login_shows_gdpr_modal_then_logout(new_context):
    # One continuous journey (login -> GDPR modal -> logout), not three
    # separate tests: logout only makes sense as a continuation of a real
    # session, so splitting it out would mean redoing this same login from
    # scratch just to reach the point this test is already at.
    #
    # The GDPR "add email address" modal only appears when the logged-in
    # user's email is blank (see broker_da.py's user_has_email check) —
    # create_test_user sets one by default, which is why the storage_state
    # fixture used by test_login never sees it. Clear it here so this test
    # actually exercises that step; dismiss_onboarding_modals sets it again
    # via the modal itself.
    subprocess.run(
        [sys.executable, "manage.py", "create_test_user", "--email=", "--password", TEST_USER_PASSWORD],
        cwd="/copo",
        check=True,
    )

    context = new_context()
    page = context.new_page()

    fill_and_submit_login_form(page, TEST_USER_USERNAME, TEST_USER_PASSWORD)
    dismiss_onboarding_modals(page)

    assert ProfilePage.PROFILE_URL_PART in page.url

    page.goto("/accounts/logout/")
    page.get_by_role("button", name="Sign Out").click()

    expect(page).to_have_url(re.compile(r".*/auth/login"))

    context.close()
