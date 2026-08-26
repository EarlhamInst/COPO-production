"""Shared fixtures and guards for the unit test suite.

Both fixtures here are autouse: they apply to every unit test without being
asked for, because they enforce properties the whole suite depends on.
"""

import os

import django

# Configure Django before importing anything from common/ or src/. COPO modules
# read settings at import time (common.lookup.resolver needs BASE_DIR), so this
# has to run first, at conftest import, not inside a fixture.
#
# Done here rather than left to pytest.ini's DJANGO_SETTINGS_MODULE because
# that key is only read by the pytest-django plugin, which lives in dev.txt and
# so is absent from the deployed image. Doing it ourselves keeps the unit suite
# runnable anywhere, with no dependency beyond pytest itself.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'src.main_config.settings.test')
django.setup()

from unittest import mock  # noqa: E402

import pytest  # noqa: E402

from common.validators import helpers as validator_helpers  # noqa: E402


@pytest.fixture(autouse=True)
def no_network():
    """Fail loudly if a unit test tries to reach the network.

    Unit tests must be self-contained. Without this, a test that forgot to
    patch its HTTP client would still pass on a developer machine with an
    internet connection, then fail in CI or hang on a slow ENA/OLS endpoint.
    Patching at the Session level catches requests.get/post and anything built
    on a Session.
    """

    def refuse(*args, **kwargs):
        raise AssertionError(
            'Unit tests must not make network calls. Patch the HTTP client for '
            'this test, or move the test to an integration suite.'
        )

    with mock.patch('requests.sessions.Session.request', refuse):
        yield


@pytest.fixture(autouse=True)
def silent_validator_logger():
    """Stop validator logging from writing logs/<date>.log into the repo.

    common.validators.helpers instantiates a Logger at module level and calls
    it on its error paths, which creates and appends to a dated log file in
    BASE_DIR. Harmless in the app, but a test suite should not litter the
    working tree.
    """
    with mock.patch.object(validator_helpers, 'l', mock.MagicMock()):
        yield
