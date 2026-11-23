from playwright.sync_api import Page

from test.playwright.e2e.utils.helpers import (
    create_biodata_profile,
    delete_profile,
)

# Both profile titles and descriptions must be at least 20 characters long
TITLE = 'Playwright {profile_type} Profile'
DESCRIPTION = 'A description that is at least twenty characters long.'


def test_create_and_delete_biodata_profile(page: Page):
    create_biodata_profile(page, TITLE.format(profile_type='Biodata'), DESCRIPTION)
    delete_profile(page, TITLE.format(profile_type='Biodata'))
