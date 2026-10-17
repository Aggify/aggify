"""Field lookups (``price__gte=10``, ``name__icontains="al"`` ...).

A lookup knows how to render itself in the two MongoDB dialects:

* *query* form   - ``{"price": {"$gte": 10}}``  (used by ``find`` and ``$match``)
* *expression* form - ``{"$gte": ["$price", 10]}`` (used by ``$expr``, ``$cond``, ``$lookup`` pipelines)

Values arrive here *already prepared* (validated and converted by the field), so
a lookup never has to worry about injection - except for the regex family which
escapes its input.
"""

from __future__ import annotations

import re
from typing import Any, Callable, Dict, Optional

from aggify.conf import settings
from aggify.exceptions import InvalidLookup, UnsafeQueryError, ValidationError
from aggify.security import escape_regex

# value "kinds" understood by Field.prepare_lookup
SCALAR, LIST, BOOL, INT, RANGE, STRING, REGEX, QUERY, TYPE, MOD, NEAR = (
    "scalar",
    "list",
    "bool",
    "int",
    "range",
    "string",
    "regex",
    "query",
    "type",
    "mod",
    "near",
)

BSON_TYPES = frozenset(
    {
        "double",
        "string",
        "object",
        "array",
        "binData",
        "undefined",
        "objectId",
        "bool",
        "date",
        "null",
        "regex",
        "dbPointer",
        "javascript",
        "symbol",
        "javascriptWithScope",
        "int",
        "timestamp",
        "long",
        "decimal",
        "minKey",
        "maxKey",
        "number",
    }
)


class Lookup:
    name = ""
    kind = SCALAR
    operator: Optional[str] = None  # "$gt" ...

    def to_query(self, path: str, value: Any) -> Dict[str, Any]:
        return {path: {self.operator: value}}

    def to_expr(self, path_expr: Any, value: Any) -> Dict[str, Any]:
        if self.operator is None:
            raise InvalidLookup(self.name, str(path_expr))
        return {self.operator: [path_expr, value]}

    #: can the lookup take an expression (F(...)) as value?
    supports_expression = True
    #: can the lookup be rendered in aggregation-expression form (to_expr)?
    has_expr = True


class Exact(Lookup):
    name, operator = "exact", "$eq"

    def to_query(self, path, value):
        return {path: value}


class Simple(Lookup):
    def __init__(self, name: str, operator: str, kind: str = SCALAR) -> None:
        self.name, self.operator, self.kind = name, operator, kind


class In(Lookup):
    kind = LIST

    def __init__(self, name: str, operator: str) -> None:
        self.name, self.operator = name, operator

    def to_expr(self, path_expr, value):
        expr = {"$in": [path_expr, value]}
        return expr if self.name == "in" else {"$not": [expr]}


class IsNull(Lookup):
    name, kind = "isnull", BOOL

    def to_query(self, path, value):
        return {path: None} if value else {path: {"$ne": None}}

    def to_expr(self, path_expr, value):
        # missing < null in BSON order, so $lte null is true for missing *and* null
        return {"$lte": [path_expr, None]} if value else {"$gt": [path_expr, None]}

    supports_expression = False


class Exists(Lookup):
    name, kind = "exists", BOOL
    supports_expression = False
    has_expr = False

    def to_query(self, path, value):
        return {path: {"$exists": bool(value)}}


class Range(Lookup):
    name, kind = "range", RANGE

    def to_query(self, path, value):
        low, high = value
        return {path: {"$gte": low, "$lte": high}}

    def to_expr(self, path_expr, value):
        low, high = value
        return {"$and": [{"$gte": [path_expr, low]}, {"$lte": [path_expr, high]}]}


class Size(Lookup):
    name, kind = "size", INT

    def to_query(self, path, value):
        return {path: {"$size": value}}

    def to_expr(self, path_expr, value):
        return {"$eq": [{"$size": path_expr}, value]}


class All(Lookup):
    name, kind = "all", LIST
    supports_expression = False
    has_expr = False

    def to_query(self, path, value):
        return {path: {"$all": value}}


class ElemMatch(Lookup):
    name, kind = "elem_match", QUERY
    supports_expression = False
    has_expr = False

    def to_query(self, path, value):
        return {path: {"$elemMatch": value}}


class Type(Lookup):
    name, kind = "type", TYPE
    supports_expression = False
    has_expr = False

    def to_query(self, path, value):
        return {path: {"$type": value}}


class Mod(Lookup):
    name, kind = "mod", MOD
    supports_expression = False
    has_expr = False

    def to_query(self, path, value):
        return {path: {"$mod": list(value)}}


class Near(Lookup):
    """``location__near=(lng, lat)`` or ``(lng, lat, max_distance_metres)`` (``find`` only)."""

    name, kind = "near", NEAR
    supports_expression = False
    has_expr = False

    def to_query(self, path, value):
        lng, lat, *rest = value
        geo = {"$geometry": {"type": "Point", "coordinates": [lng, lat]}}
        if rest:
            geo["$maxDistance"] = rest[0]
        return {path: {"$near": geo}}


class StringLookup(Lookup):
    """contains / startswith / ... implemented with an *escaped* regex."""

    kind = STRING
    supports_expression = True

    def __init__(self, name: str, template: str, ignore_case: bool = False) -> None:
        self.name, self.template, self.ignore_case = name, template, ignore_case

    def pattern(self, value: str) -> str:
        return self.template.format(value=escape_regex(value))

    def to_query(self, path, value):
        spec: Dict[str, Any] = {"$regex": self.pattern(value)}
        if self.ignore_case:
            spec["$options"] = "i"
        return {path: spec}

    def to_expr(self, path_expr, value):
        spec: Dict[str, Any] = {"input": path_expr, "regex": self.pattern(value)}
        if self.ignore_case:
            spec["options"] = "i"
        return {"$regexMatch": spec}


class RegexLookup(StringLookup):
    kind = REGEX

    def pattern(self, value: str) -> str:
        return value


LOOKUPS: Dict[str, Lookup] = {}


def register(lookup: Lookup) -> Lookup:
    LOOKUPS[lookup.name] = lookup
    return lookup


for _l in (
    Exact(),
    Simple("ne", "$ne"),
    Simple("gt", "$gt"),
    Simple("gte", "$gte"),
    Simple("lt", "$lt"),
    Simple("lte", "$lte"),
    In("in", "$in"),
    In("nin", "$nin"),
    IsNull(),
    Exists(),
    Range(),
    Size(),
    All(),
    ElemMatch(),
    Type(),
    Mod(),
    Near(),
    StringLookup("contains", "{value}"),
    StringLookup("icontains", "{value}", True),
    StringLookup("startswith", "^{value}"),
    StringLookup("istartswith", "^{value}", True),
    StringLookup("endswith", "{value}$"),
    StringLookup("iendswith", "{value}$", True),
    StringLookup("iexact", "^{value}$", True),
    RegexLookup("regex", "{value}"),
    RegexLookup("iregex", "{value}", True),
):
    register(_l)

COMPARISON_LOOKUPS = ("gt", "gte", "lt", "lte")


def get_lookup(name: str, field_name: str = "") -> Lookup:
    try:
        return LOOKUPS[name]
    except KeyError:
        raise InvalidLookup(name, field_name) from None


def is_lookup(name: str) -> bool:
    return name in LOOKUPS


# --------------------------------------------------------------------------- #
# value preparation (kind -> validated value)
# --------------------------------------------------------------------------- #
def prepare_kind(kind: str, lookup_name: str, value: Any, prepare_one: Callable[[Any], Any]) -> Any:
    """Validate/convert ``value`` according to the lookup ``kind``.

    ``prepare_one`` converts a single scalar through the owning field.
    """
    from aggify.security import ensure_iterable

    if kind == SCALAR:
        return prepare_one(value)
    if kind == LIST:
        return [prepare_one(v) for v in ensure_iterable(value, f"__{lookup_name} value")]
    if kind == BOOL:
        if not isinstance(value, bool):
            raise ValidationError(f"__{lookup_name} expects a bool.")
        return value
    if kind == INT:
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValidationError(f"__{lookup_name} expects a non-negative int.")
        return value
    if kind == RANGE:
        items = ensure_iterable(value, "__range value")
        if len(items) != 2:
            raise ValidationError("__range expects exactly two values (low, high).")
        return [prepare_one(v) for v in items]
    if kind == STRING:
        if not isinstance(value, str):
            raise ValidationError(f"__{lookup_name} expects a str, got {type(value).__name__}.")
        return value
    if kind == REGEX:
        if isinstance(value, re.Pattern):
            value = value.pattern
        if not isinstance(value, str):
            raise ValidationError(f"__{lookup_name} expects a str pattern.")
        if len(value) > settings.max_regex_length:
            raise UnsafeQueryError(f"Regex is longer than {settings.max_regex_length} characters (possible ReDoS).")
        try:
            re.compile(value)
        except re.error as exc:
            raise ValidationError(f"Invalid regex: {exc}") from None
        return value
    if kind == TYPE:
        values = value if isinstance(value, (list, tuple)) else [value]
        for v in values:
            if not (isinstance(v, int) and not isinstance(v, bool)) and v not in BSON_TYPES:
                raise ValidationError(f"Unknown BSON type {v!r}.")
        return value
    if kind == MOD:
        items = ensure_iterable(value, "__mod value")
        if len(items) != 2 or not all(isinstance(i, (int, float)) for i in items):
            raise ValidationError("__mod expects (divisor, remainder).")
        return items
    if kind == NEAR:
        items = ensure_iterable(value, "__near value")
        if not 2 <= len(items) <= 3 or not all(isinstance(i, (int, float)) for i in items):
            raise ValidationError("__near expects (lng, lat[, max_distance]).")
        return items
    return value  # QUERY handled by the resolver
