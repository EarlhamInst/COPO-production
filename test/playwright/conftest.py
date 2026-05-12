# import os
# import pytest
# from playwright.sync_api import sync_playwright


# @pytest.fixture(scope="session")
# def browser():

#     with sync_playwright() as p:
#         headless = os.getenv("HEADLESS", "true").lower() == "true"

#         browser = p.chromium.launch(headless=headless)

#         yield browser

#         browser.close()
