"""Global (process wide) settings.

Settings are plain attributes so they can be changed in tests or at start-up::

    import aggify
    aggify.configure(default_max_time_ms=5_000, allow_javascript=False)

Every default is the *safe* one.
"""

from __future__ import annotations

from dataclasses import dataclass, fields
from typing import Any, Optional


@dataclass
class Settings:
    #: Server-side JS ($where, $function, $accumulator) is disabled unless this is True.
    allow_javascript: bool = False
    #: Applied to every find/aggregate/count when set (maxTimeMS). ``None`` = server default.
    default_max_time_ms: Optional[int] = None
    #: Upper bound for the length of user supplied ``regex``/``iregex`` patterns.
    max_regex_length: int = 256
    #: Hard cap on ``limit`` (None = unlimited). Handy for public APIs.
    max_limit: Optional[int] = None
    #: Maximum length of an ``__in`` / ``__nin`` / ``__all`` list.
    max_in_list: int = 100_000
    #: Reject unknown keyword arguments when instantiating a model (mass-assignment guard).
    strict_models: bool = True
    #: Raise on unknown field names in filters instead of passing them through.
    strict_fields: bool = True
    #: Maximum nesting depth accepted when validating user supplied documents.
    max_value_depth: int = 64


settings = Settings()


def configure(**kwargs: Any) -> Settings:
    names = {f.name for f in fields(Settings)}
    for key, value in kwargs.items():
        if key not in names:
            raise TypeError(f"Unknown Aggify setting {key!r}")
        setattr(settings, key, value)
    return settings
