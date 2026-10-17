"""Read ``settings.AGGIFY`` (or ``MONGODB_URI`` / ``MONGODB_NAME``) and register the connections."""

from __future__ import annotations

from typing import Any, Dict

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured

import aggify


def setup_from_settings() -> Dict[str, Any]:
    config = getattr(settings, "AGGIFY", None)
    if config is None:
        uri = getattr(settings, "MONGODB_URI", None)
        if not uri:
            return {}
        config = {"default": {"URI": uri, "NAME": getattr(settings, "MONGODB_NAME", None)}}
    if not isinstance(config, dict):
        raise ImproperlyConfigured("settings.AGGIFY must be a dict of {alias: {URI, NAME, OPTIONS}}.")
    out = {}
    for alias, params in config.items():
        if "URI" not in params:
            raise ImproperlyConfigured(f"settings.AGGIFY[{alias!r}] needs a 'URI'.")
        out[alias] = aggify.connect(params["URI"], params.get("NAME"), alias=alias, **params.get("OPTIONS", {}))
    extra = getattr(settings, "AGGIFY_SETTINGS", None)
    if extra:
        aggify.configure(**extra)
    return out
