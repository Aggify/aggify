"""Input hardening.

The classic MongoDB injection is *operator injection*: a service passes a value
taken from a request straight into a query and the client sends
``{"$ne": ""}`` (or ``{"$regex": ".*"}``) instead of a string.  Aggify never
trusts a value to be "just data":

* every value is converted/validated by its :class:`~aggify.fields.Field`;
* values for fields Aggify knows nothing about are scanned for ``$`` keys;
* regex lookups escape their input (no ReDoS / regex injection);
* server side JavaScript is blocked unless explicitly enabled;
* field names are validated before they are used as a path.

Use :class:`Raw` as an explicit escape hatch.
"""

from __future__ import annotations

import re
from typing import Any, Iterable
from urllib.parse import urlsplit, urlunsplit

from aggify.conf import settings
from aggify.exceptions import SecurityError, UnsafeQueryError

JS_OPERATORS = frozenset({"$where", "$function", "$accumulator"})
WRITE_STAGES = frozenset({"$out", "$merge"})

_FIELD_RE = re.compile(r"^[^\x00$.][^\x00]*$")


class Raw:
    """Mark a value as trusted: it is passed to MongoDB untouched.

    >>> Product.objects.filter(meta=Raw({"$exists": True}))   # doctest: +SKIP
    """

    __slots__ = ("value",)

    def __init__(self, value: Any) -> None:
        self.value = value

    def __repr__(self) -> str:
        return f"Raw({self.value!r})"


def escape_regex(value: str) -> str:
    """Escape ``value`` so it matches literally inside a MongoDB (PCRE) regex."""
    return re.escape(value)


def validate_field_name(name: str, *, allow_dots: bool = False) -> str:
    """Return ``name`` or raise if it is unsafe to use as a document key."""
    if not isinstance(name, str) or not name:
        raise UnsafeQueryError(f"Invalid field name: {name!r}")
    parts = name.split(".") if allow_dots else [name]
    for part in parts:
        if not part or part.startswith("$") or "\x00" in part or (not allow_dots and "." in part):
            raise UnsafeQueryError(f"Unsafe field name: {name!r}")
    return name


def validate_dict_key(key: Any) -> str:
    """Keys of user stored dictionaries must not look like operators or paths."""
    if not isinstance(key, str) or not key or key.startswith("$") or "." in key or "\x00" in key:
        raise UnsafeQueryError(f"Unsafe dictionary key: {key!r}")
    return key


def assert_safe_value(value: Any, _depth: int = 0) -> Any:
    """Recursively refuse mappings that carry ``$`` operators.

    Used for values that no field definition vouched for.  Returns ``value``.
    """
    if isinstance(value, Raw):
        return value.value
    if _depth > settings.max_value_depth:
        raise UnsafeQueryError("Value is nested too deeply.")
    if isinstance(value, dict):
        for k, v in value.items():
            if isinstance(k, str) and k.startswith("$"):
                raise UnsafeQueryError(
                    f"Operator {k!r} found in a user supplied value. "
                    "Wrap the value in aggify.Raw(...) if this is intentional."
                )
            assert_safe_value(v, _depth + 1)
    elif isinstance(value, (list, tuple, set, frozenset)):
        for v in value:
            assert_safe_value(v, _depth + 1)
    return value


def scan_stages(obj: Any, *, allow_javascript: bool | None = None, _depth: int = 0) -> None:
    """Walk a pipeline / filter document and reject server side JS."""
    allowed = settings.allow_javascript if allow_javascript is None else allow_javascript
    if _depth > 200:
        raise UnsafeQueryError("Pipeline is nested too deeply.")
    if isinstance(obj, dict):
        for key, value in obj.items():
            if key in JS_OPERATORS and not allowed:
                raise SecurityError(
                    f"{key} executes JavaScript on the server and is disabled. "
                    "Enable it with aggify.configure(allow_javascript=True)."
                )
            scan_stages(value, allow_javascript=allow_javascript, _depth=_depth + 1)
    elif isinstance(obj, (list, tuple)):
        for item in obj:
            scan_stages(item, allow_javascript=allow_javascript, _depth=_depth + 1)


def mask_uri(uri: str) -> str:
    """Hide the password of a MongoDB URI so it can be logged safely."""
    try:
        parts = urlsplit(uri)
    except ValueError:  # pragma: no cover - very malformed uri
        return "<invalid uri>"
    if parts.password is None and "@" not in parts.netloc:
        return uri
    host = parts.netloc.rsplit("@", 1)[-1]
    user = parts.username
    netloc = f"{user}:***@{host}" if user else host
    return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))


def ensure_iterable(value: Any, what: str = "value") -> list:
    if isinstance(value, (str, bytes, dict)) or not isinstance(value, Iterable):
        raise UnsafeQueryError(f"{what} must be a list/tuple/set, got {type(value).__name__}")
    items = list(value)
    if len(items) > settings.max_in_list:
        raise UnsafeQueryError(f"{what} has {len(items)} items (limit {settings.max_in_list}).")
    return items
