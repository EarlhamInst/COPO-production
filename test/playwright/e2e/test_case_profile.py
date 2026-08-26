from playwright.sync_api import Page

from test.playwright.e2e.helpers import create_biodata_profile, delete_profile

TITLE = "Playwright Biodata Profile"
DESCRIPTION = "A description that is at least twenty characters long."


def test_create_and_delete_biodata_profile(page: Page):
    create_biodata_profile(page, TITLE, DESCRIPTION)
    delete_profile(page, TITLE)
