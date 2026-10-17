"""Django integration.

``INSTALLED_APPS = [..., "aggify.contrib.django"]`` and configure the connection::

    AGGIFY = {
        "default": {"URI": "mongodb://localhost:27017", "NAME": "shop", "OPTIONS": {"maxPoolSize": 50}},
    }

Documents behave like Django models where it matters: ``Model.objects``, ``Q``/``F``, ``DoesNotExist``,
pagination, signals, ``get_object_or_404`` / ``aget_object_or_404`` and ``document_form()``.
"""

default_app_config = "aggify.contrib.django.apps.AggifyConfig"
