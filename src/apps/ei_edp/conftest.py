"""Django bootstrap for the EDP app's tests.

Mirrors test/unit/conftest.py, which does not apply here: a conftest only covers
its own directory tree. Django has to be configured before this app's modules
are imported, because they read settings at import time.

Kept as a conftest rather than pytest.ini's DJANGO_SETTINGS_MODULE because that
key is only read by the pytest-django plugin, which is a dev-only dependency and
is absent from the deployed image.
"""

import os

import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'src.main_config.settings.test')
django.setup()
