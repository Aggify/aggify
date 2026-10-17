"""Field types.

Conversion pipeline
-------------------
``to_python``   user/input value  -> canonical Python value (lenient: ``"5"`` -> ``5``)
``validate``    canonical value   -> raises :class:`ValidationError` on constraint breach
``to_mongo``    canonical value   -> BSON compatible value (strict about types)
``from_mongo``  BSON value        -> canonical Python value

``prepare_value`` (``to_mongo(to_python(v))``) is what filters use, which is why
``filter(name={"$ne": ""})`` is rejected: a ``StringField`` can only ever
produce a ``str``.
"""

from __future__ import annotations

import copy
import datetime as dt
import decimal
import enum
import itertools
import re
import uuid
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
    overload,
)

import bson
from bson import Binary, Decimal128, ObjectId

from aggify import registry
from aggify.conf import settings
from aggify.exceptions import ValidationError
from aggify.lookups import LOOKUPS, get_lookup, prepare_kind
from aggify.security import Raw, assert_safe_value, validate_dict_key

T = TypeVar("T")


class _NotProvided:
    def __repr__(self) -> str:
        return "NOT_PROVIDED"

    def __bool__(self) -> bool:
        return False


NOT_PROVIDED: Any = _NotProvided()

MAX_INT64 = 2**63 - 1
MIN_INT64 = -(2**63)


class Field(Generic[T]):
    """Base class of all fields (also a descriptor on documents)."""

    _counter = itertools.count()
    python_type: Any = object
    default_error_messages = {
        "required": "This field is required.",
        "invalid": "Enter a valid value.",
        "choices": "Value {value!r} is not a valid choice.",
    }

    def __init__(
        self,
        *,
        db_field: Optional[str] = None,
        default: Any = NOT_PROVIDED,
        required: bool = False,
        unique: bool = False,
        index: bool = False,
        primary_key: bool = False,
        choices: Optional[Union[Sequence[Any], Type[enum.Enum]]] = None,
        validators: Iterable[Callable[[Any], Any]] = (),
        help_text: str = "",
        verbose_name: Optional[str] = None,
        editable: bool = True,
        exclude_from_output: bool = False,
    ) -> None:
        self.db_field: str = db_field
        self.default = default
        self.required = required
        self.unique = unique
        self.index = index
        self.primary_key = primary_key
        self.choices = choices
        self.validators = list(validators)
        self.help_text = help_text
        self.verbose_name = verbose_name
        self.editable = editable
        self.exclude_from_output = exclude_from_output
        self.name: str = ""
        self.model: Any = None
        self._creation_order = next(Field._counter)

    # ------------------------------------------------------------------ #
    # descriptor protocol
    # ------------------------------------------------------------------ #
    def contribute_to_class(self, model: Any, name: str) -> None:
        self.name = name
        self.model = model
        if self.primary_key:
            self.db_field = "_id"
        elif self.db_field is None:
            self.db_field = name
        setattr(model, name, self)

    @overload
    def __get__(self, instance: None, owner: Any) -> Field[T]: ...
    @overload
    def __get__(self, instance: Any, owner: Any) -> T: ...

    def __get__(self, instance: Any, owner: Any = None) -> Any:
        if instance is None:
            return self
        return instance._data.get(self.name)

    def __set__(self, instance: Any, value: Any) -> None:
        # lenient conversion on assignment (``doc.age = "41"`` -> 41); strict checks run in clean()/to_mongo()
        instance._data[self.name] = None if value is None else self.to_python(value)

    # ------------------------------------------------------------------ #
    # defaults
    # ------------------------------------------------------------------ #
    def has_default(self) -> bool:
        return self.default is not NOT_PROVIDED

    def get_default(self) -> Any:
        if self.default is NOT_PROVIDED:
            return None
        return self.default() if callable(self.default) else self.default

    # ------------------------------------------------------------------ #
    # conversions
    # ------------------------------------------------------------------ #
    def to_python(self, value: Any) -> Any:
        return value

    def from_mongo(self, value: Any) -> Any:
        return self.to_python(value)

    def to_mongo(self, value: Any) -> Any:
        return value

    def pre_save(self, instance: Any, adding: bool) -> None:
        """Hook run by ``Document.save`` (auto_now & friends)."""

    # ------------------------------------------------------------------ #
    # validation
    # ------------------------------------------------------------------ #
    def fail(self, key: str, **fmt: Any) -> ValidationError:
        messages = {}
        for klass in reversed(type(self).__mro__):
            messages.update(getattr(klass, "default_error_messages", {}))
        return ValidationError(messages[key].format(**fmt), field=self.name or None)

    def validate(self, value: Any) -> None:
        """Constraint validation of an already canonical, non-None value."""
        if self.choices is not None:
            allowed = self.choice_values()
            if value not in allowed:
                raise self.fail("choices", value=value)
        for validator in self.validators:
            try:
                validator(value)
            except ValidationError:
                raise
            except (ValueError, TypeError) as exc:
                raise ValidationError(str(exc), field=self.name or None) from None

    def choice_values(self) -> List[Any]:
        ch = self.choices
        if ch is None:
            return []
        if isinstance(ch, type) and issubclass(ch, enum.Enum):
            return [m.value for m in ch] + list(ch)
        return [c[0] if isinstance(c, (tuple, list)) and len(c) == 2 else c for c in ch]

    def clean(self, value: Any) -> Any:
        """Full cycle used by ``Document.full_clean``: convert + validate."""
        if value is None:
            if self.required:
                raise self.fail("required")
            return None
        value = self.to_python(value)
        self.to_mongo(value)  # strict type check
        self.validate(value)
        return value

    # ------------------------------------------------------------------ #
    # lookups
    # ------------------------------------------------------------------ #
    def prepare_value(self, value: Any) -> Any:
        """Value as it must appear in a query (strict type, no constraints)."""
        if isinstance(value, Raw):
            return value.value
        if value is None:
            return None
        return self.to_mongo(self.to_python(value))

    def lookup_field(self, lookup: str, value: Any) -> Field:
        """Field used to prepare ``value`` for ``lookup`` (lists delegate to their item field)."""
        return self

    def prepare_lookup(self, lookup: str, value: Any) -> Any:
        if isinstance(value, Raw):
            return value.value
        lk = get_lookup(lookup, self.name)
        target = self.lookup_field(lookup, value)
        if lk.name == "exact" or lk.name == "ne":
            return target._prepare_exact(value)
        return prepare_kind(lk.kind, lk.name, value, target.prepare_value)

    def _prepare_exact(self, value: Any) -> Any:
        return self.prepare_value(value)

    # ------------------------------------------------------------------ #
    # introspection
    # ------------------------------------------------------------------ #
    def deconstruct_kwargs(self) -> Dict[str, Any]:
        return {"db_field": self.db_field, "required": self.required, "default": self.default}

    def clone(self) -> Field:
        import copy

        new = copy.copy(self)
        new.validators = list(self.validators)
        return new

    def __repr__(self) -> str:
        label = f"{type(self).__name__}"
        return f"<{label}: {self.name}>" if self.name else f"<{label}>"


# --------------------------------------------------------------------------- #
# scalars
# --------------------------------------------------------------------------- #
class AnyField(Field[Any]):
    """Accepts any BSON compatible value (operator-looking keys are refused)."""

    def from_mongo(self, value: Any) -> Any:
        # containers are copied so the change-tracking snapshot never aliases user-visible values
        return copy.deepcopy(value) if isinstance(value, (dict, list)) else value

    def to_mongo(self, value: Any) -> Any:
        return assert_safe_value(value)

    def prepare_value(self, value: Any) -> Any:
        if isinstance(value, Raw):
            return value.value
        return assert_safe_value(value)


class ObjectIdField(Field[ObjectId]):
    python_type = ObjectId
    default_error_messages = {"invalid": "{value!r} is not a valid ObjectId."}

    def to_python(self, value: Any) -> Any:
        if isinstance(value, str) and len(value) == 24:
            try:
                return ObjectId(value)
            except bson.errors.InvalidId:
                return value
        if isinstance(value, bytes) and len(value) == 12:
            return ObjectId(value)
        return value

    def to_mongo(self, value: Any) -> Any:
        if not isinstance(value, ObjectId):
            raise ValidationError(self.default_error_messages["invalid"].format(value=value), field=self.name or None)
        return value


class StringField(Field[str]):
    python_type = str
    default_error_messages = {
        "invalid": "Expected a string.",
        "min_length": "Ensure this value has at least {limit} characters.",
        "max_length": "Ensure this value has at most {limit} characters.",
        "regex": "Value does not match the required pattern.",
    }

    def __init__(
        self,
        *,
        min_length: Optional[int] = None,
        max_length: Optional[int] = None,
        regex: Optional[Union[str, re.Pattern[str]]] = None,
        strip: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.min_length, self.max_length, self.strip = min_length, max_length, strip
        self.regex = re.compile(regex) if isinstance(regex, str) else regex

    def to_python(self, value: Any) -> Any:
        if isinstance(value, bytes):
            try:
                value = value.decode()
            except UnicodeDecodeError:
                return value
        if isinstance(value, str) and self.strip:
            value = value.strip()
        return value

    def to_mongo(self, value: Any) -> Any:
        if not isinstance(value, str):
            raise self.fail("invalid")
        if "\x00" in value:
            raise ValidationError("NUL characters are not allowed.", field=self.name or None)
        return value

    def validate(self, value: Any) -> None:
        if self.min_length is not None and len(value) < self.min_length:
            raise self.fail("min_length", limit=self.min_length)
        if self.max_length is not None and len(value) > self.max_length:
            raise self.fail("max_length", limit=self.max_length)
        if self.regex is not None and not self.regex.match(value):
            raise self.fail("regex")
        super().validate(value)


_EMAIL_RE = re.compile(
    r"^[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?)+$"
)


class EmailField(StringField):
    default_error_messages = {"email": "Enter a valid email address."}

    def __init__(self, **kwargs: Any) -> None:
        kwargs.setdefault("max_length", 254)
        super().__init__(**kwargs)

    def to_python(self, value: Any) -> Any:
        value = super().to_python(value)
        return value.strip().lower() if isinstance(value, str) else value

    def validate(self, value: Any) -> None:
        if not _EMAIL_RE.match(value):
            raise self.fail("email")
        super().validate(value)


class URLField(StringField):
    default_error_messages = {"url": "Enter a valid URL."}

    def __init__(self, *, schemes: Sequence[str] = ("http", "https"), **kwargs: Any) -> None:
        kwargs.setdefault("max_length", 2048)
        super().__init__(**kwargs)
        self.schemes = tuple(schemes)

    def validate(self, value: Any) -> None:
        from urllib.parse import urlsplit

        try:
            parts = urlsplit(value)
        except ValueError:
            raise self.fail("url") from None
        if parts.scheme not in self.schemes or not parts.netloc:
            raise self.fail("url")
        super().validate(value)


_SLUG_RE = re.compile(r"^[-\w]+\Z")


class SlugField(StringField):
    def __init__(self, **kwargs: Any) -> None:
        kwargs.setdefault("max_length", 50)
        kwargs.setdefault("regex", _SLUG_RE)
        super().__init__(**kwargs)


class IntField(Field[int]):
    python_type = int
    default_error_messages = {
        "invalid": "Expected an integer.",
        "min_value": "Ensure this value is >= {limit}.",
        "max_value": "Ensure this value is <= {limit}.",
        "range": "Integer out of the 64-bit range.",
    }

    def __init__(self, *, min_value: Optional[int] = None, max_value: Optional[int] = None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.min_value, self.max_value = min_value, max_value

    def to_python(self, value: Any) -> Any:
        if isinstance(value, bool):
            return value
        if isinstance(value, int):
            return value
        if isinstance(value, float) and value.is_integer():
            return int(value)
        if isinstance(value, str):
            try:
                return int(value.strip())
            except ValueError:
                return value
        if isinstance(value, bson.Int64):
            return int(value)
        return value

    def to_mongo(self, value: Any) -> Any:
        if isinstance(value, bool) or not isinstance(value, int):
            raise self.fail("invalid")
        if not MIN_INT64 <= value <= MAX_INT64:
            raise self.fail("range")
        return value

    def validate(self, value: Any) -> None:
        if self.min_value is not None and value < self.min_value:
            raise self.fail("min_value", limit=self.min_value)
        if self.max_value is not None and value > self.max_value:
            raise self.fail("max_value", limit=self.max_value)
        super().validate(value)


LongField = IntField


class FloatField(Field[float]):
    python_type = float
    default_error_messages = {
        "invalid": "Expected a number.",
        "min_value": "Ensure this value is >= {limit}.",
        "max_value": "Ensure this value is <= {limit}.",
    }

    def __init__(self, *, min_value: Optional[float] = None, max_value: Optional[float] = None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.min_value, self.max_value = min_value, max_value

    def to_python(self, value: Any) -> Any:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            try:
                return float(value.strip())
            except ValueError:
                return value
        if isinstance(value, decimal.Decimal):
            return float(value)
        return value

    def to_mongo(self, value: Any) -> Any:
        if isinstance(value, bool) or not isinstance(value, float):
            raise self.fail("invalid")
        return value

    def validate(self, value: Any) -> None:
        if self.min_value is not None and value < self.min_value:
            raise self.fail("min_value", limit=self.min_value)
        if self.max_value is not None and value > self.max_value:
            raise self.fail("max_value", limit=self.max_value)
        super().validate(value)


class DecimalField(Field[decimal.Decimal]):
    """Exact decimals, stored as BSON Decimal128."""

    python_type = decimal.Decimal
    default_error_messages = {
        "invalid": "Expected a decimal number.",
        "max_digits": "Ensure there are no more than {limit} digits in total.",
        "decimal_places": "Ensure there are no more than {limit} decimal places.",
        "min_value": "Ensure this value is >= {limit}.",
        "max_value": "Ensure this value is <= {limit}.",
    }

    def __init__(
        self,
        *,
        max_digits: Optional[int] = None,
        decimal_places: Optional[int] = None,
        min_value: Optional[Any] = None,
        max_value: Optional[Any] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.max_digits, self.decimal_places = max_digits, decimal_places
        self.min_value = None if min_value is None else decimal.Decimal(str(min_value))
        self.max_value = None if max_value is None else decimal.Decimal(str(max_value))

    def to_python(self, value: Any) -> Any:
        if isinstance(value, decimal.Decimal):
            return value
        if isinstance(value, Decimal128):
            return value.to_decimal()
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, str)):
            try:
                return decimal.Decimal(value.strip() if isinstance(value, str) else value)
            except decimal.InvalidOperation:
                return value
        if isinstance(value, float):
            return decimal.Decimal(str(value))
        return value

    def from_mongo(self, value: Any) -> Any:
        return self.to_python(value)

    def to_mongo(self, value: Any) -> Any:
        if not isinstance(value, decimal.Decimal) or not value.is_finite():
            raise self.fail("invalid")
        return Decimal128(value)

    def validate(self, value: Any) -> None:
        _, digits, exp = value.as_tuple()
        places = max(-int(exp), 0)
        total = max(len(digits), places)
        if self.max_digits is not None and total > self.max_digits:
            raise self.fail("max_digits", limit=self.max_digits)
        if self.decimal_places is not None and places > self.decimal_places:
            raise self.fail("decimal_places", limit=self.decimal_places)
        if self.min_value is not None and value < self.min_value:
            raise self.fail("min_value", limit=self.min_value)
        if self.max_value is not None and value > self.max_value:
            raise self.fail("max_value", limit=self.max_value)
        super().validate(value)


class BooleanField(Field[bool]):
    python_type = bool
    default_error_messages = {"invalid": "Expected a boolean."}

    def to_python(self, value: Any) -> Any:
        if isinstance(value, str):
            low = value.strip().lower()
            if low in ("true", "1", "yes", "on"):
                return True
            if low in ("false", "0", "no", "off"):
                return False
        if isinstance(value, int) and not isinstance(value, bool) and value in (0, 1):
            return bool(value)
        return value

    def to_mongo(self, value: Any) -> Any:
        if not isinstance(value, bool):
            raise self.fail("invalid")
        return value


UTC = dt.timezone.utc


def _ms(value: dt.datetime) -> dt.datetime:
    """BSON stores milliseconds: truncate so in-memory values equal what the database returns."""
    return value.replace(microsecond=value.microsecond // 1000 * 1000)


def _parse_dt(value: str) -> Optional[dt.datetime]:
    text = value.strip()
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        return dt.datetime.fromisoformat(text)
    except ValueError:
        return None


class DateTimeField(Field[dt.datetime]):
    """Timezone aware datetimes, always normalised to UTC.

    Naive datetimes are *assumed to be UTC*.
    """

    python_type = dt.datetime
    default_error_messages = {"invalid": "Expected a datetime."}

    def __init__(self, *, auto_now: bool = False, auto_now_add: bool = False, **kwargs: Any) -> None:
        if auto_now or auto_now_add:
            kwargs.setdefault("editable", False)
        super().__init__(**kwargs)
        self.auto_now, self.auto_now_add = auto_now, auto_now_add

    def pre_save(self, instance: Any, adding: bool) -> None:
        if self.auto_now or (self.auto_now_add and adding):
            instance._data[self.name] = _ms(dt.datetime.now(UTC))

    def to_python(self, value: Any) -> Any:
        if isinstance(value, dt.datetime):
            return _ms(value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC))
        if isinstance(value, dt.date):
            return dt.datetime(value.year, value.month, value.day, tzinfo=UTC)
        if isinstance(value, str):
            parsed = _parse_dt(value)
            return self.to_python(parsed) if parsed else value
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            try:
                return _ms(dt.datetime.fromtimestamp(value, UTC))
            except (OverflowError, OSError, ValueError):
                return value
        return value

    def to_mongo(self, value: Any) -> Any:
        if not isinstance(value, dt.datetime):
            raise self.fail("invalid")
        return value


class DateField(Field[dt.date]):
    """Calendar dates (stored as midnight UTC datetimes - BSON has no date type)."""

    python_type = dt.date
    default_error_messages = {"invalid": "Expected a date."}

    def to_python(self, value: Any) -> Any:
        if isinstance(value, dt.datetime):
            return value.astimezone(UTC).date() if value.tzinfo else value.date()
        if isinstance(value, dt.date):
            return value
        if isinstance(value, str):
            try:
                return dt.date.fromisoformat(value.strip()[:10])
            except ValueError:
                return value
        return value

    def to_mongo(self, value: Any) -> Any:
        if isinstance(value, dt.datetime) or not isinstance(value, dt.date):
            raise self.fail("invalid")
        return dt.datetime(value.year, value.month, value.day, tzinfo=UTC)


class UUIDField(Field[uuid.UUID]):
    python_type = uuid.UUID
    default_error_messages = {"invalid": "Expected a UUID."}

    def to_python(self, value: Any) -> Any:
        if isinstance(value, uuid.UUID):
            return value
        if isinstance(value, str):
            try:
                return uuid.UUID(value)
            except ValueError:
                return value
        if isinstance(value, Binary) and value.subtype in (3, 4):
            return value.as_uuid()
        return value

    def to_mongo(self, value: Any) -> Any:
        if not isinstance(value, uuid.UUID):
            raise self.fail("invalid")
        return value


class BinaryField(Field[bytes]):
    python_type = bytes
    default_error_messages = {"invalid": "Expected bytes.", "max_bytes": "Too large (max {limit} bytes)."}

    def __init__(self, *, max_bytes: Optional[int] = None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.max_bytes = max_bytes

    def to_python(self, value: Any) -> Any:
        if isinstance(value, Binary):
            return bytes(value)
        if isinstance(value, bytearray):
            return bytes(value)
        return value

    def to_mongo(self, value: Any) -> Any:
        if not isinstance(value, bytes):
            raise self.fail("invalid")
        return Binary(value)

    def validate(self, value: Any) -> None:
        if self.max_bytes is not None and len(value) > self.max_bytes:
            raise self.fail("max_bytes", limit=self.max_bytes)
        super().validate(value)


class EnumField(Field[enum.Enum]):
    """Stores ``Enum.value``; accepts members or raw values."""

    default_error_messages = {"invalid": "Not a valid {enum} value."}

    def __init__(self, enum_class: Type[enum.Enum], **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.enum_class = enum_class

    @property
    def python_type(self) -> Any:
        return self.enum_class

    def to_python(self, value: Any) -> Any:
        if isinstance(value, self.enum_class):
            return value
        try:
            return self.enum_class(value)
        except (ValueError, TypeError):
            return value

    def to_mongo(self, value: Any) -> Any:
        if not isinstance(value, self.enum_class):
            raise self.fail("invalid", enum=self.enum_class.__name__)
        return value.value


# --------------------------------------------------------------------------- #
# containers
# --------------------------------------------------------------------------- #
class ListField(Field[List[Any]]):
    python_type = list

    default_error_messages = {
        "invalid": "Expected a list.",
        "min_length": "Ensure there are at least {limit} items.",
        "max_length": "Ensure there are at most {limit} items.",
    }

    def __init__(
        self,
        field: Optional[Field] = None,
        *,
        min_length: Optional[int] = None,
        max_length: Optional[int] = None,
        **kwargs: Any,
    ) -> None:
        kwargs.setdefault("default", list)
        super().__init__(**kwargs)
        self.field: Field = field if field is not None else AnyField()
        self.min_length, self.max_length = min_length, max_length

    def contribute_to_class(self, model: Any, name: str) -> None:
        super().contribute_to_class(model, name)
        self.field.name = name
        self.field.model = model
        if self.field.db_field is None:
            self.field.db_field = name

    def to_python(self, value: Any) -> Any:
        if isinstance(value, (tuple, set, frozenset)):
            value = list(value)
        if isinstance(value, list):
            return [None if v is None else self.field.to_python(v) for v in value]
        return value

    def from_mongo(self, value: Any) -> Any:
        if isinstance(value, list):
            return [None if v is None else self.field.from_mongo(v) for v in value]
        return value

    def to_mongo(self, value: Any) -> Any:
        if not isinstance(value, list):
            raise self.fail("invalid")
        if len(value) > settings.max_in_list * 10:
            raise ValidationError("List is too long.", field=self.name or None)
        return [None if v is None else self.field.to_mongo(v) for v in value]

    def validate(self, value: Any) -> None:
        if self.min_length is not None and len(value) < self.min_length:
            raise self.fail("min_length", limit=self.min_length)
        if self.max_length is not None and len(value) > self.max_length:
            raise self.fail("max_length", limit=self.max_length)
        errors: Dict[str, List[str]] = {}
        for i, item in enumerate(value):
            if item is None:
                continue
            try:
                self.field.validate(item)
            except ValidationError as exc:
                for k, msgs in (exc.errors or {"": [exc.message]}).items():
                    errors.setdefault(f"{self.name}[{i}]" + (f".{k.split('.', 1)[-1]}" if "." in k else ""), []).extend(
                        msgs
                    )
        if errors:
            raise ValidationError(errors=errors)
        super().validate(value)

    def lookup_field(self, lookup: str, value: Any) -> Field:
        if lookup in ("exact", "ne") and isinstance(value, (list, tuple)):
            return self  # whole array comparison
        if lookup in ("in", "nin", "all") or lookup in LOOKUPS:
            return self.field
        return self

    def _prepare_exact(self, value: Any) -> Any:
        if isinstance(value, (list, tuple)):
            return self.to_mongo(self.to_python(list(value)))
        return self.field.prepare_value(value)

    def prepare_value(self, value: Any) -> Any:
        if isinstance(value, Raw):
            return value.value
        if isinstance(value, (list, tuple)):
            return self.to_mongo(self.to_python(list(value)))
        return self.field.prepare_value(value)


class DictField(Field[Dict[str, Any]]):
    python_type = dict
    default_error_messages = {"invalid": "Expected a mapping."}

    def __init__(self, field: Optional[Field] = None, **kwargs: Any) -> None:
        kwargs.setdefault("default", dict)
        super().__init__(**kwargs)
        self.field: Field = field if field is not None else AnyField()

    def to_python(self, value: Any) -> Any:
        if isinstance(value, dict):
            return {k: (None if v is None else self.field.to_python(v)) for k, v in value.items()}
        return value

    def from_mongo(self, value: Any) -> Any:
        if isinstance(value, dict):
            return {k: (None if v is None else self.field.from_mongo(v)) for k, v in value.items()}
        return value

    def to_mongo(self, value: Any) -> Any:
        if not isinstance(value, dict):
            raise self.fail("invalid")
        out = {}
        for k, v in value.items():
            validate_dict_key(k)
            out[k] = None if v is None else self.field.to_mongo(v)
        return out

    def validate(self, value: Any) -> None:
        for v in value.values():
            if v is not None:
                self.field.validate(v)
        super().validate(value)

    def lookup_field(self, lookup: str, value: Any) -> Field:
        return self.field if lookup not in ("exact", "ne") or not isinstance(value, dict) else self

    def _prepare_exact(self, value: Any) -> Any:
        if isinstance(value, dict):
            return self.to_mongo(self.to_python(value))
        return self.field.prepare_value(value)

    def prepare_value(self, value: Any) -> Any:
        if isinstance(value, Raw):
            return value.value
        if isinstance(value, dict):
            return self.to_mongo(self.to_python(value))
        return self.field.prepare_value(value)


class EmbeddedField(Field[Any]):
    """A document nested inside another (``EmbeddedDocument`` subclass)."""

    def __init__(self, document_type: Union[str, type], **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._document_type = document_type

    @property
    def document_type(self) -> Any:
        if isinstance(self._document_type, str):
            self._document_type = registry.resolve(self._document_type, relative_to=self.model)
        return self._document_type

    @property
    def python_type(self) -> Any:
        return self.document_type

    def to_python(self, value: Any) -> Any:
        cls = self.document_type
        if isinstance(value, dict):
            return cls(**value)
        return value

    def from_mongo(self, value: Any) -> Any:
        if isinstance(value, dict):
            return self.document_type.from_mongo(value)
        return value

    def to_mongo(self, value: Any) -> Any:
        if not isinstance(value, self.document_type):
            raise self.fail("invalid")
        return value.to_mongo()

    def validate(self, value: Any) -> None:
        try:
            value.full_clean()
        except ValidationError as exc:
            raise exc.prefixed(self.name) from None
        super().validate(value)

    def prepare_value(self, value: Any) -> Any:
        if isinstance(value, Raw):
            return value.value
        if isinstance(value, dict):
            return self.document_type(**value).to_mongo()
        return self.to_mongo(self.to_python(value))

    default_error_messages = {"invalid": "Expected an embedded document."}


EmbeddedDocumentField = EmbeddedField


class ReferenceField(Field[Any]):
    """A reference to another document, stored as its primary key.

    ``post.owner`` returns the related object (cached by ``select_related`` /
    assignment, fetched on demand in sync code). ``post.owner_id`` is always the raw id.
    """

    default_error_messages = {"invalid": "Expected a {model} instance or its primary key."}

    def __init__(self, document_type: Union[str, type], **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._document_type = document_type

    @property
    def document_type(self) -> Any:
        if isinstance(self._document_type, str):
            self._document_type = registry.resolve(self._document_type, relative_to=self.model)
        return self._document_type

    @property
    def python_type(self) -> Any:
        return self.document_type

    @property
    def pk_field(self) -> Field:
        return self.document_type._meta.pk

    def contribute_to_class(self, model: Any, name: str) -> None:
        super().contribute_to_class(model, name)
        # `owner_id` style accessor (read/write the raw id without fetching)
        accessor = f"{name}_id"
        if not hasattr(model, accessor):
            setattr(model, accessor, _ReferenceIdAccessor(self))

    def __get__(self, instance: Any, owner: Any = None) -> Any:
        if instance is None:
            return self
        value = instance._data.get(self.name)
        if value is None or hasattr(value, "_meta"):
            return value
        return instance._resolve_reference_sync(self, value)

    def to_python(self, value: Any) -> Any:
        if hasattr(value, "_meta"):
            return value
        return self.pk_field.to_python(value)

    def from_mongo(self, value: Any) -> Any:
        return self.pk_field.from_mongo(value)

    def to_mongo(self, value: Any) -> Any:
        if hasattr(value, "_meta"):
            if not isinstance(value, self.document_type):
                raise self.fail("invalid", model=self.document_type.__name__)
            pk = value.pk
            if pk is None:
                raise ValidationError("Save the referenced document first.", field=self.name or None)
            return self.pk_field.to_mongo(pk)
        try:
            return self.pk_field.to_mongo(value)
        except ValidationError:
            raise self.fail("invalid", model=self.document_type.__name__) from None

    def validate(self, value: Any) -> None:
        super().validate(value)


class _ReferenceIdAccessor:
    def __init__(self, field: ReferenceField) -> None:
        self.field = field

    def __get__(self, instance: Any, owner: Any = None) -> Any:
        if instance is None:
            return self
        value = instance._data.get(self.field.name)
        return value.pk if hasattr(value, "_meta") else value

    def __set__(self, instance: Any, value: Any) -> None:
        instance._data[self.field.name] = value


class GeoPointField(Field[Tuple[float, float]]):
    """GeoJSON ``Point``; the Python value is ``(longitude, latitude)``."""

    default_error_messages = {"invalid": "Expected (longitude, latitude).", "range": "Coordinates out of range."}

    def to_python(self, value: Any) -> Any:
        if isinstance(value, dict) and value.get("type") == "Point":
            value = value.get("coordinates")
        if isinstance(value, (list, tuple)) and len(value) == 2:
            try:
                return (float(value[0]), float(value[1]))
            except (TypeError, ValueError):
                return value
        return value

    def to_mongo(self, value: Any) -> Any:
        if not (isinstance(value, tuple) and len(value) == 2 and all(isinstance(v, float) for v in value)):
            raise self.fail("invalid")
        lng, lat = value
        if not (-180 <= lng <= 180 and -90 <= lat <= 90):
            raise self.fail("range")
        return {"type": "Point", "coordinates": [lng, lat]}

    def prepare_value(self, value: Any) -> Any:
        if isinstance(value, Raw):
            return value.value
        return self.to_mongo(self.to_python(value))


__all__ = [n for n in dir() if n.endswith("Field")] + ["Field", "NOT_PROVIDED"]
