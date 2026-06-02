import pytest
import re

from playwright.sync_api import expect, Page

from test.playwright.config.test_ids import LoginPage, ProfilePage
from test.playwright.config.settings import PROFILE_PAGE_URL


def test_login(page: Page):
    page.goto(PROFILE_PAGE_URL)
    
    expect(page).to_have_url(re.compile(r'.*/copo/auth/login/\?next=/copo/'))
    page.get_by_test_id(LoginPage.ORCID_LOGIN_BUTTON).click()
    page.wait_for_url("**/copo")
    print(page.url)

    assert ProfilePage.PROFILE_URL_PART in page.url
