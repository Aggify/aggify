"""Expressions, ``Q`` objects and the function library.

Everything here is *data*: building an expression does not touch a model.  It
is compiled (``resolve``) against a :class:`~aggify.schema.Context`, which maps
Python field names to database names and validates them.

    F("price") * F("qty")              -> {"$multiply": ["$price", "$qty"]}
    Q(age__gte=18) | Q(vip=True)        -> {"$or": [{"age": {"$gte": 18}}, {"vip": True}]}
    Sum("amount", filter=Q(paid=True))  -> {"$sum": {"$cond": [...]}}
"""

from __future__ import annotations

import datetime as dt
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Type

from aggify.exceptions import (
    InvalidArgument,
    InvalidLookup,
    InvalidOperator,
)
from aggify.fields import (
    BooleanField,
    DateTimeField,
    DictField,
    Field,
    FloatField,
    IntField,
    ListField,
    StringField,
)
from aggify.lookups import get_lookup
from aggify.schema import Context, Resolved, Scope, dynamic_field, resolve_path
from aggify.security import Raw, scan_stages

# --------------------------------------------------------------------------- #
# base
# --------------------------------------------------------------------------- #

_CMP = {"gt": "$gt", "gte": "$gte", "lt": "$lt", "lte": "$lte", "eq": "$eq", "ne": "$ne"}


class Expression:
    """Something that can be rendered as a MongoDB aggregation expression."""

    is_aggregate = False

    def resolve(self, ctx: Context) -> Any:  # pragma: no cover - abstract
        raise NotImplementedError

    def output_field(self, ctx: Context) -> Optional[Field]:
        return None

    # arithmetic -------------------------------------------------------
    def _combine(self, op: str, other: Any, reverse: bool = False) -> CombinedExpression:
        other = wrap(other, literal=True)
        return CombinedExpression(other, op, self) if reverse else CombinedExpression(self, op, other)

    def __add__(self, o: Any):
        return self._combine("$add", o)

    def __radd__(self, o: Any):
        return self._combine("$add", o, True)

    def __sub__(self, o: Any):
        return self._combine("$subtract", o)

    def __rsub__(self, o: Any):
        return self._combine("$subtract", o, True)

    def __mul__(self, o: Any):
        return self._combine("$multiply", o)

    def __rmul__(self, o: Any):
        return self._combine("$multiply", o, True)

    def __truediv__(self, o: Any):
        return self._combine("$divide", o)

    def __rtruediv__(self, o: Any):
        return self._combine("$divide", o, True)

    def __mod__(self, o: Any):
        return self._combine("$mod", o)

    def __rmod__(self, o: Any):
        return self._combine("$mod", o, True)

    def __pow__(self, o: Any):
        return self._combine("$pow", o)

    def __rpow__(self, o: Any):
        return self._combine("$pow", o, True)

    def __neg__(self):
        return CombinedExpression(Value(-1), "$multiply", self)

    def __abs__(self):
        return Func("$abs", self)

    # comparison (``==`` is deliberately NOT overloaded - use .eq()) -----
    def _cmp(self, op: str, other: Any) -> Comparison:
        return Comparison(op, self, wrap(other, literal=True))

    def __gt__(self, o: Any):
        return self._cmp("$gt", o)

    def __ge__(self, o: Any):
        return self._cmp("$gte", o)

    def __lt__(self, o: Any):
        return self._cmp("$lt", o)

    def __le__(self, o: Any):
        return self._cmp("$lte", o)

    def eq(self, o: Any):
        return self._cmp("$eq", o)

    def ne(self, o: Any):
        return self._cmp("$ne", o)

    def isin(self, values: Iterable[Any]):
        return Func("$in", self, Value(list(values)))

    # boolean ----------------------------------------------------------
    def __and__(self, other: Any):
        if isinstance(other, Q):
            return NotImplemented
        return BooleanExpression("$and", [self, wrap(other, literal=True)])

    def __or__(self, other: Any):
        if isinstance(other, Q):
            return NotImplemented
        return BooleanExpression("$or", [self, wrap(other, literal=True)])

    def __invert__(self):
        return BooleanExpression("$not", [self])

    # ordering ---------------------------------------------------------
    def asc(self) -> OrderBy:
        return OrderBy(self, False)

    def desc(self) -> OrderBy:
        return OrderBy(self, True)

    # misc -------------------------------------------------------------
    __hash__ = object.__hash__


def wrap(value: Any, *, literal: bool = False) -> Expression:
    """Coerce Python values into expressions.

    ``literal=False``: ``str`` means *field name*  (Func arguments, like Django)
    ``literal=True`` : ``str`` is a constant       (right hand side of operators)
    """
    if isinstance(value, Expression):
        return value
    if isinstance(value, Q):
        return QExpression(value)
    if isinstance(value, Raw):
        return RawExpression(value.value)
    if isinstance(value, str) and not literal:
        return F(value)
    return Value(value)


class RawExpression(Expression):
    """Trusted pre-built expression (explicit ``Raw`` escape hatch)."""

    def __init__(self, value: Any) -> None:
        scan_stages(value)
        self.value = value

    def resolve(self, ctx: Context) -> Any:
        return self.value


class Value(Expression):
    """A constant. Strings starting with ``$`` and dicts are wrapped in ``$literal``."""

    def __init__(self, value: Any, output_field: Optional[Field] = None) -> None:
        if isinstance(value, Raw):
            value = value.value
        self.value = value
        self._output = output_field

    def resolve(self, ctx: Optional[Context] = None) -> Any:
        v = self.value
        if isinstance(v, str) and v.startswith("$"):
            return {"$literal": v}
        if isinstance(v, dict):
            return {"$literal": v}
        if isinstance(v, (list, tuple)):
            items = [Value(i).resolve(ctx) for i in v]
            if any(isinstance(i, dict) for i in items):
                return {"$literal": list(v)}
            return items
        return v

    def output_field(self, ctx: Context) -> Optional[Field]:
        if self._output is not None:
            return self._output
        v = self.value
        if isinstance(v, bool):
            return BooleanField()
        if isinstance(v, int):
            return IntField()
        if isinstance(v, float):
            return FloatField()
        if isinstance(v, str):
            return StringField()
        if isinstance(v, dt.datetime):
            return DateTimeField()
        return None

    def __repr__(self) -> str:
        return f"Value({self.value!r})"


class F(Expression):
    """Reference to a field: ``F("price")``, ``F("owner__name")``, ``F("items")``.

    Paths follow the model schema (python names, ``__`` separator) and are
    validated when compiled.  Use :class:`Var` for ``$$variables``.
    """

    def __init__(self, name: str) -> None:
        if not isinstance(name, str) or not name:
            raise InvalidArgument("F() needs a field name.")
        self.name = name

    def resolve(self, ctx: Context) -> str:
        r = resolve_path(ctx.scope, self.name, with_lookup=False, allow_joins=ctx.allow_joins)
        ctx.add_joins(r.joins)
        return "$" + r.db_path

    def output_field(self, ctx: Context) -> Optional[Field]:
        try:
            return resolve_path(ctx.scope, self.name, with_lookup=False, allow_joins=ctx.allow_joins).field
        except Exception:
            return None

    # --- aggregate shortcuts kept from Aggify 0.x -------------------------
    def first(self):
        return First(self)

    def last(self):
        return Last(self)

    def min(self):
        return Min(self)

    def max(self):
        return Max(self)

    def sum(self):
        return Sum(self)

    def avg(self):
        return Avg(self)

    def __repr__(self) -> str:
        return f"F({self.name!r})"


class Var(Expression):
    """An aggregation variable (``$$name``): let-variables, ``$map`` aliases, ``$$ROOT`` ...

    ``Var("item.qty")`` -> ``"$$item.qty"``.
    """

    def __init__(self, name: str) -> None:
        if not name or name.startswith("$"):
            raise InvalidArgument("Var() takes the bare variable name, e.g. Var('ROOT').")
        self.name = name

    def resolve(self, ctx: Context) -> str:
        return "$$" + self.name


ROOT, CURRENT, NOW, REMOVE = Var("ROOT"), Var("CURRENT"), Var("NOW"), Var("REMOVE")
DESCEND, PRUNE, KEEP = Var("DESCEND"), Var("PRUNE"), Var("KEEP")


class CombinedExpression(Expression):
    ASSOCIATIVE = {"$add", "$multiply"}

    def __init__(self, lhs: Expression, op: str, rhs: Expression) -> None:
        self.lhs, self.op, self.rhs = lhs, op, rhs

    def resolve(self, ctx: Context) -> Any:
        left = self.lhs.resolve(ctx)
        right = self.rhs.resolve(ctx)
        if (
            self.op in self.ASSOCIATIVE
            and isinstance(left, dict)
            and list(left) == [self.op]
            and isinstance(self.lhs, CombinedExpression)
        ):
            return {self.op: [*left[self.op], right]}
        return {self.op: [left, right]}

    def output_field(self, ctx: Context) -> Optional[Field]:
        for side in (self.lhs, self.rhs):
            f = side.output_field(ctx)
            if isinstance(f, FloatField):
                return FloatField()
        if self.op == "$divide":
            return FloatField()
        return self.lhs.output_field(ctx) or self.rhs.output_field(ctx)


class Comparison(Expression):
    def __init__(self, op: str, lhs: Expression, rhs: Expression) -> None:
        self.op, self.lhs, self.rhs = op, lhs, rhs

    def resolve(self, ctx: Context) -> Any:
        return {self.op: [self.lhs.resolve(ctx), self.rhs.resolve(ctx)]}

    def output_field(self, ctx: Context) -> Field:
        return BooleanField()


class BooleanExpression(Expression):
    def __init__(self, op: str, parts: List[Expression]) -> None:
        self.op, self.parts = op, parts

    def resolve(self, ctx: Context) -> Any:
        resolved = [p.resolve(ctx) for p in self.parts]
        if self.op in ("$and", "$or"):
            flat: List[Any] = []
            for p, r in zip(self.parts, resolved):
                if isinstance(p, BooleanExpression) and p.op == self.op:
                    flat.extend(r[self.op])
                else:
                    flat.append(r)
            return {self.op: flat}
        return {self.op: resolved}

    def output_field(self, ctx: Context) -> Field:
        return BooleanField()


class OrderBy:
    def __init__(self, expression: Expression, descending: bool = False) -> None:
        self.expression, self.descending = expression, descending


# --------------------------------------------------------------------------- #
# Func
# --------------------------------------------------------------------------- #
class Func(Expression):
    """Generic ``{"$op": [args...]}`` expression.  Subclass or use directly::

    Func("$toUpper", "name")
    """

    function: str = ""
    #: render a single argument without the surrounding list
    unary_plain = True
    output: Optional[Type[Field]] = None

    def __init__(self, *args: Any, output_field: Optional[Field] = None, **options: Any) -> None:
        if args and isinstance(args[0], str) and args[0].startswith("$") and not self.function:
            self.function, args = args[0], args[1:]
        if not self.function:
            raise InvalidArgument("Func needs an operator name.")
        self.args = [wrap(a) for a in args]
        self._output = output_field
        self.options = options

    def resolve(self, ctx: Context) -> Any:
        resolved = [a.resolve(ctx) for a in self.args]
        if len(resolved) == 1 and self.unary_plain:
            return {self.function: resolved[0]}
        return {self.function: resolved}

    def output_field(self, ctx: Context) -> Optional[Field]:
        if self._output is not None:
            return self._output
        return self.output() if self.output else None

    def __repr__(self) -> str:
        return f"{type(self).__name__}({', '.join(map(repr, self.args))})"


class NamedFunc(Func):
    """Operators whose argument is a document: ``{"$op": {"input": .., ...}}``."""

    arg_names: Tuple[str, ...] = ()

    def __init__(self, *args: Any, output_field: Optional[Field] = None, **options: Any) -> None:
        names = self.arg_names
        if len(args) > len(names):
            raise InvalidArgument(f"{type(self).__name__} takes at most {len(names)} positional arguments.")
        self._output = output_field
        self.args = []
        self.named: Dict[str, Expression] = {}
        for name, value in zip(names, args):
            self.named[name] = wrap(value)
        for name, value in options.items():
            self.named[name] = wrap(value, literal=name in self.literal_options)
        self.options = {}

    literal_options: Tuple[str, ...] = ()

    def resolve(self, ctx: Context) -> Any:
        return {self.function: {k: v.resolve(ctx) for k, v in self.named.items() if v is not None}}


def _simple(name: str, op: str, output: Optional[Type[Field]] = None, doc: str = "") -> Type[Func]:
    return type(
        name, (Func,), {"function": op, "output": output, "__doc__": doc or f"``{op}``", "__module__": __name__}
    )


# math
Abs = _simple("Abs", "$abs")
Ceil = _simple("Ceil", "$ceil")
Floor = _simple("Floor", "$floor")
Round = _simple("Round", "$round")
Trunc = _simple("Trunc", "$trunc")
Sqrt = _simple("Sqrt", "$sqrt", FloatField)
Pow = _simple("Pow", "$pow")
Mod = _simple("Mod", "$mod")
Exp = _simple("Exp", "$exp", FloatField)
Ln = _simple("Ln", "$ln", FloatField)
Log10 = _simple("Log10", "$log10", FloatField)
# string
Upper = _simple("Upper", "$toUpper", StringField)
Lower = _simple("Lower", "$toLower", StringField)
Concat = _simple("Concat", "$concat", StringField)
Length = _simple("Length", "$strLenCP", IntField)
Substr = _simple("Substr", "$substrCP", StringField)
Split = _simple("Split", "$split", ListField)
IndexOfStr = _simple("IndexOfStr", "$indexOfCP", IntField)
# arrays / sets
Size = _simple("Size", "$size", IntField)
ArrayElemAt = _simple("ArrayElemAt", "$arrayElemAt")
ConcatArrays = _simple("ConcatArrays", "$concatArrays", ListField)
SetUnion = _simple("SetUnion", "$setUnion", ListField)
SetIntersection = _simple("SetIntersection", "$setIntersection", ListField)
SetDifference = _simple("SetDifference", "$setDifference", ListField)
IsArray = _simple("IsArray", "$isArray", BooleanField)
ReverseArray = _simple("ReverseArray", "$reverseArray", ListField)
Slice = _simple("Slice", "$slice", ListField)
IndexOfArray = _simple("IndexOfArray", "$indexOfArray", IntField)
ArrayToObject = _simple("ArrayToObject", "$arrayToObject", DictField)
ObjectToArray = _simple("ObjectToArray", "$objectToArray", ListField)
RangeOf = _simple("RangeOf", "$range", ListField)
# logic / types
Coalesce = _simple("Coalesce", "$ifNull")
Not = _simple("Not", "$not", BooleanField)
ToInt = _simple("ToInt", "$toInt", IntField)
ToLong = _simple("ToLong", "$toLong", IntField)
ToDouble = _simple("ToDouble", "$toDouble", FloatField)
ToString = _simple("ToString", "$toString", StringField)
ToBool = _simple("ToBool", "$toBool", BooleanField)
ToDate = _simple("ToDate", "$toDate", DateTimeField)
ToObjectId = _simple("ToObjectId", "$toObjectId")
ToDecimal = _simple("ToDecimal", "$toDecimal")
TypeOf = _simple("TypeOf", "$type", StringField)
IsNumber = _simple("IsNumber", "$isNumber", BooleanField)
MergeObjectsExpr = _simple("MergeObjectsExpr", "$mergeObjects", DictField)
Literal = _simple("Literal", "$literal")
GetField = _simple("GetField", "$getField")


class Rand(Func):  # noqa: F811 - takes no argument
    function = "$rand"
    output = FloatField

    def resolve(self, ctx: Context) -> Any:
        return {"$rand": {}}


class DatePart(Func):
    """``Year("created_at")``, ``Month("created_at", timezone="Asia/Tehran")``."""

    output = IntField

    def __init__(self, date: Any, timezone: Optional[str] = None) -> None:
        super().__init__(date)
        self.timezone = timezone

    def resolve(self, ctx: Context) -> Any:
        date = self.args[0].resolve(ctx)
        if self.timezone:
            return {self.function: {"date": date, "timezone": self.timezone}}
        return {self.function: date}


def _date_part(name: str, op: str) -> Type[DatePart]:
    return type(name, (DatePart,), {"function": op, "__module__": __name__})


Year = _date_part("Year", "$year")
Month = _date_part("Month", "$month")
DayOfMonth = _date_part("DayOfMonth", "$dayOfMonth")
DayOfWeek = _date_part("DayOfWeek", "$dayOfWeek")
DayOfYear = _date_part("DayOfYear", "$dayOfYear")
Hour = _date_part("Hour", "$hour")
Minute = _date_part("Minute", "$minute")
Second = _date_part("Second", "$second")
Week = _date_part("Week", "$week")
IsoWeek = _date_part("IsoWeek", "$isoWeek")


class DateToString(NamedFunc):
    function = "$dateToString"
    arg_names = ("date", "format")
    literal_options = ("format", "timezone", "onNull")
    output = StringField

    def __init__(self, date: Any, format: str = "%Y-%m-%d", timezone: Optional[str] = None, **kw: Any) -> None:
        opts: Dict[str, Any] = {"format": format}
        if timezone:
            opts["timezone"] = timezone
        opts.update(kw)
        super().__init__(date, **opts)


class DateTrunc(NamedFunc):
    function = "$dateTrunc"
    arg_names = ("date",)
    literal_options = ("unit", "binSize", "timezone", "startOfWeek")
    output = DateTimeField

    def __init__(
        self,
        date: Any,
        unit: str = "day",
        bin_size: Optional[int] = None,
        timezone: Optional[str] = None,
        start_of_week: Optional[str] = None,
    ) -> None:
        opts: Dict[str, Any] = {"unit": unit}
        if bin_size:
            opts["binSize"] = bin_size
        if timezone:
            opts["timezone"] = timezone
        if start_of_week:
            opts["startOfWeek"] = start_of_week
        super().__init__(date, **opts)


class DateAdd(NamedFunc):
    function = "$dateAdd"
    arg_names = ("startDate",)
    literal_options = ("unit", "timezone")
    output = DateTimeField

    def __init__(self, start: Any, unit: str, amount: Any, timezone: Optional[str] = None) -> None:
        opts: Dict[str, Any] = {"unit": unit, "amount": wrap(amount, literal=True)}
        if timezone:
            opts["timezone"] = timezone
        super().__init__(start, **opts)


class DateDiff(NamedFunc):
    function = "$dateDiff"
    arg_names = ("startDate", "endDate")
    literal_options = ("unit", "timezone")
    output = IntField

    def __init__(self, start: Any, end: Any, unit: str = "day", timezone: Optional[str] = None) -> None:
        opts: Dict[str, Any] = {"unit": unit}
        if timezone:
            opts["timezone"] = timezone
        super().__init__(start, end, **opts)


class Trim(NamedFunc):
    function = "$trim"
    arg_names = ("input",)
    literal_options = ("chars",)
    output = StringField


class ReplaceAll(NamedFunc):
    function = "$replaceAll"
    arg_names = ("input", "find", "replacement")
    literal_options = ("find", "replacement")
    output = StringField

    def __init__(self, input: Any, find: str, replacement: str) -> None:  # noqa: A002
        super().__init__(input)
        self.named["find"] = Value(find)
        self.named["replacement"] = Value(replacement)


class RegexMatch(NamedFunc):
    function = "$regexMatch"
    arg_names = ("input",)
    output = BooleanField

    def __init__(self, input: Any, regex: str, options: str = "") -> None:  # noqa: A002
        from aggify.lookups import prepare_kind

        prepare_kind("regex", "regex", regex, str)
        super().__init__(input)
        self.named["regex"] = Value(regex)
        if options:
            self.named["options"] = Value(options)


class Filter(NamedFunc):
    """``Filter("items", Var("item.qty") > 2)``: keep array elements matching a condition."""

    function = "$filter"
    arg_names = ("input",)
    output = ListField

    def __init__(self, input: Any, cond: Any, as_: str = "item", limit: Optional[int] = None) -> None:  # noqa: A002
        super().__init__(input)
        self.named["as"] = Value(as_)
        self.named["cond"] = wrap(cond, literal=True)
        if limit is not None:
            self.named["limit"] = Value(limit)


class MapArray(NamedFunc):
    """``MapArray("items", Var("item.price") * 2)`` -> ``$map``."""

    function = "$map"
    arg_names = ("input",)
    output = ListField

    def __init__(self, input: Any, expression: Any, as_: str = "item") -> None:  # noqa: A002
        super().__init__(input)
        self.named["as"] = Value(as_)
        self.named["in"] = wrap(expression, literal=True)


class Reduce(NamedFunc):
    """``Reduce("nums", 0, Var("value") + Var("this"))`` -> ``$reduce``."""

    function = "$reduce"
    arg_names = ("input",)

    def __init__(self, input: Any, initial: Any, expression: Any) -> None:  # noqa: A002
        super().__init__(input)
        self.named["initialValue"] = wrap(initial, literal=True)
        self.named["in"] = wrap(expression, literal=True)


class Let(NamedFunc):
    function = "$let"

    def __init__(self, vars: Dict[str, Any], expression: Any) -> None:  # noqa: A002
        self._output = None
        self.args = []
        self.options = {}
        self.named = {}
        self._vars = {k: wrap(v) for k, v in vars.items()}
        self._in = wrap(expression, literal=True)

    def resolve(self, ctx: Context) -> Any:
        return {"$let": {"vars": {k: v.resolve(ctx) for k, v in self._vars.items()}, "in": self._in.resolve(ctx)}}


# --------------------------------------------------------------------------- #
# conditionals
# --------------------------------------------------------------------------- #
class QExpression(Expression):
    """A ``Q`` used where an expression is expected (When/Case/Filter)."""

    def __init__(self, q: Q) -> None:
        self.q = q

    def resolve(self, ctx: Context) -> Any:
        return self.q.resolve_expr(ctx)

    def output_field(self, ctx: Context) -> Field:
        return BooleanField()


_LEGACY_OPS = {">": "$gt", ">=": "$gte", "<": "$lt", "<=": "$lte", "==": "$eq", "!=": "$ne"}


class Cond(Expression):
    """``$cond``.

    >>> Cond(Q(age__gte=18), "adult", "minor")
    >>> Cond(23, ">", 20, "hi", "bye")            # 0.x signature, still supported
    """

    def __init__(self, *args: Any, then: Any = None, otherwise: Any = None, **kwargs: Any) -> None:
        if len(args) == 5:  # legacy: value1, op, value2, then, else
            v1, op, v2, then, otherwise = args
            if op not in _LEGACY_OPS:
                raise InvalidOperator(op)
            self.condition: Expression = Comparison(_LEGACY_OPS[op], wrap(v1, literal=True), wrap(v2, literal=True))
        elif len(args) == 3:
            cond, then, otherwise = args
            self.condition = wrap(cond, literal=True)
        elif len(args) == 1 and "else_" not in kwargs:
            self.condition = wrap(args[0], literal=True)
        else:
            raise InvalidArgument("Cond(condition, then, otherwise)")
        self.then = wrap(then, literal=True)
        self.otherwise = wrap(otherwise, literal=True)

    def resolve(self, ctx: Context) -> Any:
        return {
            "$cond": {
                "if": self.condition.resolve(ctx),
                "then": self.then.resolve(ctx),
                "else": self.otherwise.resolve(ctx),
            }
        }

    def __iter__(self):  # ``dict(Cond(...))`` as in Aggify 0.x
        yield "$cond", self.resolve(Context(Scope({}, open=True)))["$cond"]

    def output_field(self, ctx: Context) -> Optional[Field]:
        return self.then.output_field(ctx) or self.otherwise.output_field(ctx)


class When:
    def __init__(self, condition: Any = None, then: Any = None, **lookups: Any) -> None:
        if lookups:
            condition = Q(**lookups) if condition is None else Q(condition) & Q(**lookups)
        if condition is None:
            raise InvalidArgument("When() needs a condition.")
        self.condition = wrap(condition, literal=True)
        self.then = wrap(then, literal=True)


class Case(Expression):
    """``$switch``:  Case(When(Q(score__gte=90), "A"), When(Q(score__gte=80), "B"), default="C")"""

    def __init__(self, *whens: When, default: Any = None) -> None:
        if not whens:
            raise InvalidArgument("Case() needs at least one When().")
        self.whens, self.default = whens, wrap(default, literal=True)

    def resolve(self, ctx: Context) -> Any:
        return {
            "$switch": {
                "branches": [{"case": w.condition.resolve(ctx), "then": w.then.resolve(ctx)} for w in self.whens],
                "default": self.default.resolve(ctx),
            }
        }

    def output_field(self, ctx: Context) -> Optional[Field]:
        return self.whens[0].then.output_field(ctx)


# --------------------------------------------------------------------------- #
# aggregates
# --------------------------------------------------------------------------- #
class Aggregate(Func):
    """Accumulators for ``$group`` (also valid as plain array operators)."""

    is_aggregate = True
    #: what to feed the accumulator when the optional ``filter`` is false
    neutral: Any = Var("REMOVE")

    def __init__(
        self,
        *args: Any,
        filter: Optional[Any] = None,
        window: Optional[Dict[str, Any]] = None,  # noqa: A002
        output_field: Optional[Field] = None,
    ) -> None:
        super().__init__(*args, output_field=output_field)
        self.filter = None if filter is None else wrap(filter, literal=True)
        self.window = window

    def resolve(self, ctx: Context) -> Any:
        resolved = [a.resolve(ctx) for a in self.args]
        if self.filter is not None:
            if len(resolved) != 1:
                raise InvalidArgument("filter= needs exactly one argument.")
            resolved = [
                {
                    "$cond": [
                        self.filter.resolve(ctx),
                        resolved[0],
                        self.neutral.resolve(ctx) if isinstance(self.neutral, Expression) else self.neutral,
                    ]
                }
            ]
        body: Any = resolved[0] if len(resolved) == 1 else resolved
        out = {self.function: body}
        if self.window is not None:
            return {**out, "window": self.window}
        return out

    def output_field(self, ctx: Context) -> Optional[Field]:
        if self._output is not None:
            return self._output
        if self.output is not None:
            return self.output()
        if self.args:
            return self.args[0].output_field(ctx)
        return None


class Sum(Aggregate):
    function, neutral = "$sum", 0

    def output_field(self, ctx):
        f = super().output_field(ctx)
        return f if f is not None else FloatField()


class Avg(Aggregate):
    function, output = "$avg", FloatField


class Min(Aggregate):
    function = "$min"


class Max(Aggregate):
    function = "$max"


class First(Aggregate):
    function = "$first"


class Last(Aggregate):
    function = "$last"


class Push(Aggregate):
    function, output, neutral = "$push", ListField, Var("REMOVE")


class AddToSet(Aggregate):
    function, output = "$addToSet", ListField


class StdDevPop(Aggregate):
    function, output = "$stdDevPop", FloatField


class StdDevSamp(Aggregate):
    function, output = "$stdDevSamp", FloatField


class MergeObjects(Aggregate):
    function, output = "$mergeObjects", DictField


class Median(Aggregate):
    """``$median`` (MongoDB 7.0+)."""

    def __init__(self, field: Any, p: float = 0.5, **kw: Any) -> None:
        super().__init__(field, **kw)
        self.p = p

    function, output = "$median", FloatField

    def resolve(self, ctx):
        return {"$median": {"input": self.args[0].resolve(ctx), "method": "approximate"}}


class Count(Aggregate):
    """``Count()`` counts documents, ``Count("field")`` counts documents where the field is set."""

    function, output = "$sum", IntField

    def __init__(
        self,
        field: Any = None,
        *,
        filter: Optional[Any] = None,  # noqa: A002
        window: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(*([] if field is None else [field]), filter=filter, window=window)

    def resolve(self, ctx: Context) -> Any:
        cond: List[Any] = []
        if self.args:
            cond.append({"$ne": [{"$ifNull": [self.args[0].resolve(ctx), None]}, None]})
        if self.filter is not None:
            cond.append(self.filter.resolve(ctx))
        if not cond:
            body: Any = 1
        else:
            body = {"$cond": [cond[0] if len(cond) == 1 else {"$and": cond}, 1, 0]}
        out = {"$sum": body}
        if self.window is not None:
            out["window"] = self.window
        return out


class _Ranking(Func):
    is_aggregate = True
    output = IntField

    def __init__(self, window: Optional[Dict[str, Any]] = None) -> None:
        super().__init__()
        self.window = window

    def resolve(self, ctx: Context) -> Any:
        return {self.function: {}}


class Rank(_Ranking):
    function = "$rank"


class DenseRank(_Ranking):
    function = "$denseRank"


class DocumentNumber(_Ranking):
    function = "$documentNumber"


class Shift(Func):
    is_aggregate = True

    function = "$shift"

    def __init__(self, output: Any, by: int, default: Any = None) -> None:
        super().__init__()
        self._out, self._by, self._default = wrap(output), by, wrap(default, literal=True)

    def resolve(self, ctx: Context) -> Any:
        return {"$shift": {"output": self._out.resolve(ctx), "by": self._by, "default": self._default.resolve(ctx)}}


# --------------------------------------------------------------------------- #
# Q objects
# --------------------------------------------------------------------------- #
def merge_and(parts: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    """AND several query documents into the most compact equivalent document."""
    out: Dict[str, Any] = {}
    extra: List[Dict[str, Any]] = []
    for part in parts:
        for key, value in part.items():
            if key == "$and":
                extra.extend(value)
            elif key not in out:
                out[key] = value
            elif (
                isinstance(out[key], dict)
                and isinstance(value, dict)
                and all(isinstance(k, str) and k.startswith("$") for k in (*out[key], *value))
                and not (set(out[key]) & set(value))
                and not key.startswith("$")
                and "$regex" not in out[key]
                and "$regex" not in value
            ):
                out[key] = {**out[key], **value}
            else:
                extra.append({key: value})
    if extra:
        existing = out.pop("$and", [])
        out["$and"] = [*existing, *extra]
    return out


class Q:
    """Composable query condition (immutable).

    >>> Q(age__gte=18) & ~Q(banned=True) | Q(role="admin")
    """

    AND, OR = "AND", "OR"

    def __init__(self, *args: Any, _connector: str = "AND", _negated: bool = False, **kwargs: Any) -> None:
        children: List[Any] = []
        for a in args:
            if isinstance(a, (Q, Expression)):
                children.append(a)
            elif isinstance(a, dict):
                raise TypeError("Q() does not accept dicts; use keyword arguments (or Raw for trusted documents).")
            elif isinstance(a, Raw):
                children.append(a)
            else:
                raise TypeError(f"Unsupported Q() argument: {a!r}")
        children.extend(kwargs.items())
        self.children = children
        self.connector = _connector
        self.negated = _negated

    # composition ----------------------------------------------------------
    def _combine(self, other: Any, connector: str) -> Q:
        if isinstance(other, Expression):
            other = Q(other)
        if not isinstance(other, Q):
            return NotImplemented
        if not other.children:
            return self._copy()
        if not self.children:
            return other._copy()
        obj = Q(_connector=connector)
        for side in (self, other):
            if side.connector == connector and not side.negated:
                obj.children.extend(side.children)
            else:
                obj.children.append(side)
        return obj

    def _copy(self) -> Q:
        q = Q(_connector=self.connector, _negated=self.negated)
        q.children = list(self.children)
        return q

    def __and__(self, other: Any):
        return self._combine(other, Q.AND)

    def __or__(self, other: Any):
        return self._combine(other, Q.OR)

    def __rand__(self, other: Any):
        return Q(other)._combine(self, Q.AND) if isinstance(other, Expression) else NotImplemented

    def __ror__(self, other: Any):
        return Q(other)._combine(self, Q.OR) if isinstance(other, Expression) else NotImplemented

    def __invert__(self) -> Q:
        q = self._copy()
        q.negated = not q.negated
        return q

    def __bool__(self) -> bool:
        return bool(self.children)

    def __repr__(self) -> str:
        body = f" {self.connector} ".join(repr(c) for c in self.children)
        return f"{'~' if self.negated else ''}Q({body})"

    # compilation ----------------------------------------------------------
    def resolve(self, ctx: Context) -> Dict[str, Any]:
        """Query-language form for ``find``/``$match``."""
        parts = [self._leaf(c, ctx) if not isinstance(c, Q) else c.resolve(ctx) for c in self.children]
        parts = [p for p in parts if p]
        if not parts:
            doc: Dict[str, Any] = {}
        elif self.connector == Q.AND:
            doc = merge_and(parts)
        else:
            doc = parts[0] if len(parts) == 1 else {"$or": parts}
        if self.negated and doc:
            doc = {"$nor": [doc]}
        return doc

    def _leaf(self, child: Any, ctx: Context) -> Dict[str, Any]:
        if isinstance(child, Raw):
            scan_stages(child.value)
            return child.value
        if isinstance(child, Expression):
            return {"$expr": child.resolve(ctx)}
        key, value = child
        return compile_condition(key, value, ctx)

    def resolve_expr(self, ctx: Context) -> Dict[str, Any]:
        """Aggregation-expression form (for ``$cond``, ``$filter``, ``$lookup`` pipelines)."""
        parts: List[Any] = []
        for c in self.children:
            if isinstance(c, Q):
                parts.append(c.resolve_expr(ctx))
            elif isinstance(c, Expression):
                parts.append(c.resolve(ctx))
            elif isinstance(c, Raw):
                parts.append(c.value)
            else:
                parts.append(compile_condition(c[0], c[1], ctx, as_expr=True))
        if not parts:
            expr: Any = True
        elif len(parts) == 1:
            expr = parts[0]
        else:
            expr = {"$and" if self.connector == Q.AND else "$or": parts}
        return {"$not": [expr]} if self.negated else expr


def compile_condition(key: str, value: Any, ctx: Context, *, as_expr: bool = False) -> Dict[str, Any]:
    """Compile a single ``field__lookup=value`` pair."""
    r: Resolved = resolve_path(ctx.scope, key, allow_joins=ctx.allow_joins)
    ctx.add_joins(r.joins)
    lookup_name = r.lookup or "exact"
    lk = get_lookup(lookup_name, key)
    field = r.field or dynamic_field(key)
    path = r.db_path

    if lookup_name == "elem_match":
        return _elem_match(r, value, ctx)

    if isinstance(value, (Expression,)) and not isinstance(value, (Value,)):
        if not (lk.supports_expression and lk.has_expr):
            raise InvalidLookup(lookup_name, key)
        inner = lk.to_expr("$" + path, value.resolve(ctx))
        return inner if as_expr else {"$expr": inner}

    if isinstance(value, Value):
        value = value.value
    prepared = field.prepare_lookup(lookup_name, value)
    if as_expr:
        if not lk.has_expr:
            raise InvalidLookup(lookup_name, key)
        if isinstance(prepared, str) and prepared.startswith("$") and lk.kind in ("scalar", "list"):
            prepared = {"$literal": prepared}
        return lk.to_expr("$" + path, prepared)
    return lk.to_query(path, prepared)


def _elem_match(r: Resolved, value: Any, ctx: Context) -> Dict[str, Any]:
    if isinstance(value, dict):
        raise InvalidArgument("elem_match takes a Q object (or Raw).")
    if isinstance(value, Raw):
        scan_stages(value.value)
        return {r.db_path: {"$elemMatch": value.value}}
    if not isinstance(value, Q):
        raise InvalidArgument("elem_match takes a Q object.")
    if r.inner_scope is None:
        raise InvalidArgument(f"{r.py_path!r} is not a list of embedded documents.")
    sub = ctx.child(r.inner_scope, allow_joins=False)
    return {r.db_path: {"$elemMatch": value.resolve(sub)}}


# Backwards compatible alias for 0.x ``Match``-style usage is intentionally not provided.


__all__ = [
    "Abs",
    "AddToSet",
    "Aggregate",
    "ArrayElemAt",
    "ArrayToObject",
    "Avg",
    "CURRENT",
    "Case",
    "Ceil",
    "Coalesce",
    "Concat",
    "ConcatArrays",
    "Cond",
    "Count",
    "DESCEND",
    "DateAdd",
    "DateDiff",
    "DatePart",
    "DateToString",
    "DateTrunc",
    "DayOfMonth",
    "DayOfWeek",
    "DayOfYear",
    "DenseRank",
    "DocumentNumber",
    "Exp",
    "F",
    "Filter",
    "First",
    "Floor",
    "Func",
    "GetField",
    "Hour",
    "IndexOfArray",
    "IndexOfStr",
    "IsArray",
    "IsNumber",
    "IsoWeek",
    "KEEP",
    "Last",
    "Length",
    "Let",
    "Literal",
    "Ln",
    "Log10",
    "Lower",
    "MapArray",
    "Max",
    "Median",
    "MergeObjects",
    "MergeObjectsExpr",
    "Min",
    "Minute",
    "Mod",
    "Month",
    "NOW",
    "Not",
    "ObjectToArray",
    "PRUNE",
    "Pow",
    "Push",
    "REMOVE",
    "ROOT",
    "Rand",
    "RangeOf",
    "Rank",
    "Reduce",
    "RegexMatch",
    "ReplaceAll",
    "ReverseArray",
    "Round",
    "Second",
    "SetDifference",
    "SetIntersection",
    "SetUnion",
    "Shift",
    "Size",
    "Slice",
    "Split",
    "Sqrt",
    "StdDevPop",
    "StdDevSamp",
    "Substr",
    "Sum",
    "ToBool",
    "ToDate",
    "ToDecimal",
    "ToDouble",
    "ToInt",
    "ToLong",
    "ToObjectId",
    "ToString",
    "Trim",
    "Trunc",
    "TypeOf",
    "Upper",
    "Value",
    "Var",
    "Week",
    "Year",
    "wrap",
]
