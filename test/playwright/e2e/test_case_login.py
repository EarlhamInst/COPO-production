from playwright.sync_api import Page

from test.playwright.config.test_ids import ProfilePage
from test.playwright.config.settings import PROFILE_PAGE_URL


def test_login(page: Page):
    # `page`'s context is seeded from the storage_state fixture in conftest.py,
    # which logs in once via the test user's username/password (not ORCID —
    # see docs/testing/PLAYWRIGHT.md for why). So landing straight on the
    # profile page, with no redirect to the login form, is the actual thing
    # under test here: that the saved session is valid.
    page.goto(PROFILE_PAGE_URL)

    assert ProfilePage.PROFILE_URL_PART in page.url
