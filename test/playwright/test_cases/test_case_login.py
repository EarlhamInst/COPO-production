import os

from test.playwright.test_ids import LoginPage, ProfilePage


def test_login(page):
    web_url = os.getenv(
        "BASE_URL", "http://localhost:8000"
    )
    page.goto(web_url)
    page.get_by_test_id(LoginPage.ORCID_LOGIN_BUTTON).click()
    page.wait_for_url("**/copo")
    print(page.url)

    assert ProfilePage.PROFILE_URL_PART in page.url
