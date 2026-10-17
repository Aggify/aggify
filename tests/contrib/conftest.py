"""One Django configuration for every test module in this package (settings are process-global)."""

import django
from django.conf import settings

from tests.conftest import MONGO_URI

if not settings.configured:
    settings.configure(
        SECRET_KEY="test",
        USE_TZ=True,
        DATABASES={"default": {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"}},
        INSTALLED_APPS=["django.contrib.contenttypes", "aggify.contrib.django"],
        ROOT_URLCONF="tests.contrib.django_urls",
        ALLOWED_HOSTS=["*"],
        AGGIFY={"django-test": {"URI": MONGO_URI, "NAME": "aggify_django_tests", "OPTIONS": {"maxPoolSize": 7}}},
        DEFAULT_AUTO_FIELD="django.db.models.AutoField",
    )
    django.setup()
