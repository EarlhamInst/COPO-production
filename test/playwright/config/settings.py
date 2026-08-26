import os

BASE_URL = os.environ.get("BASE_URL", "http://localhost:8000")
PROFILE_PAGE_URL = f'{BASE_URL}/copo'
LOGIN_URL = f'{BASE_URL}/accounts/login/'
DJANGO_SUPERUSER_USERNAME = "admin"
DJANGO_SUPERUSER_PASSWORD = "admin"
DJANGO_SUPERUSER_EMAIL = "admin@test.com"
DEBUG = True

# Matches src/apps/copo_core/management/commands/create_test_user.py's
# defaults. The password is never hardcoded here — it must be created with
# the same value the stack was seeded with, via $COPO_TEST_USER_PASSWORD.
TEST_USER_USERNAME = "copo_browser_test_user"
TEST_USER_PASSWORD = os.environ.get("COPO_TEST_USER_PASSWORD", "")