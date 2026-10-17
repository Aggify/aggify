"""Translate ``update(inc__views=1, push__tags="x", title="y")`` into a MongoDB update document.

Operators (prefix + ``__`` + field path):

==============  ===========  ==========================================
prefix          mongo        notes
==============  ===========  ==========================================
``set__``       ``$set``     also the default for plain ``field=value``
``unset__``     ``$unset``   value ignored (``True``)
``inc__``       ``$inc``     numbers only
``mul__``       ``$mul``
``min__``       ``$min``
``max__``       ``$max``
``push__``      ``$push``    one element (``push_all__`` for many)
``add_to_set__````$addToSet````add_to_set_all__`` for many
``pull__``      ``$pull``    ``pull_all__`` for many
``pop__``       ``$pop``     ``1`` last / ``-1`` first
``rename__``    ``$rename``  value is the new field name
``current_date__````$currentDate``
==============  ===========  ==========================================

Assigning an expression (``views=F("views") + 1``) switches to an update
*pipeline* (MongoDB 4.2+), which is atomic as well.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple, Union

from aggify.exceptions import InvalidArgument, ValidationError
from aggify.expressions import Expression
from aggify.fields import AnyField, Field, ListField
from aggify.schema import Context, Scope, resolve_path
from aggify.security import Raw, assert_safe_value, validate_field_name

OPERATORS = {
    "set": "$set",
    "unset": "$unset",
    "inc": "$inc",
    "mul": "$mul",
    "min": "$min",
    "max": "$max",
    "push": "$push",
    "push_all": "$push",
    "add_to_set": "$addToSet",
    "add_to_set_all": "$addToSet",
    "pull": "$pull",
    "pull_all": "$pullAll",
    "pop": "$pop",
    "rename": "$rename",
    "current_date": "$currentDate",
}

_MANY = {"push_all", "add_to_set_all", "pull_all"}


def _parse(model: Any, changes: Dict[str, Any]) -> List[Tuple[str, str, str, Optional[Field], Any]]:
    """[(op, user_path, db_path, field, value)]"""
    scope = Scope.for_model(model)
    out = []
    for key, value in changes.items():
        op, path = "set", key
        head, sep, tail = key.partition("__")
        if sep and head in OPERATORS:
            op, path = head, tail
        r = resolve_path(scope, path, with_lookup=False, allow_joins=False)
        if r.field is not None and r.field.primary_key:
            raise InvalidArgument("The primary key cannot be updated.")
        out.append((op, path, r.db_path, r.field, value))
    return out


def build_update(model: Any, changes: Dict[str, Any]) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
    if not changes:
        raise InvalidArgument("update() needs at least one change.")
    parsed = _parse(model, changes)
    if any(isinstance(value, Expression) for *_, value in parsed):
        return _build_pipeline_update(model, parsed)
    ops: Dict[str, Dict[str, Any]] = {}
    for op, path, db_path, field, value in parsed:
        bucket = ops.setdefault(OPERATORS[op], {})
        if db_path in bucket:
            raise InvalidArgument(f"{path!r} is updated twice.")
        if op == "unset":
            bucket[db_path] = ""
        elif op == "set":
            bucket[db_path] = _prepare(field, value)
        elif op in ("inc", "mul"):
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValidationError(f"{op}__ expects a number.", field=path)
            bucket[db_path] = value
        elif op in ("min", "max"):
            bucket[db_path] = _prepare(field, value)
        elif op in ("push", "add_to_set"):
            bucket[db_path] = _element_field(field, path).prepare_value(value)
        elif op in _MANY:
            inner = _element_field(field, path)
            if isinstance(value, (str, bytes, dict)) or not hasattr(value, "__iter__"):
                raise ValidationError(f"{op}__ expects a list.", field=path)
            items = [inner.prepare_value(v) for v in value]
            bucket[db_path] = items if op == "pull_all" else {"$each": items}
        elif op == "pull":
            inner = _element_field(field, path)
            bucket[db_path] = value.value if isinstance(value, Raw) else inner.prepare_value(value)
        elif op == "pop":
            if value not in (1, -1) or isinstance(value, bool):
                raise ValidationError("pop__ expects 1 or -1.", field=path)
            bucket[db_path] = value
        elif op == "rename":
            bucket[db_path] = validate_field_name(str(value), allow_dots=True)
        elif op == "current_date":
            bucket[db_path] = True
    return ops


def _build_pipeline_update(
    model: Any, parsed: List[Tuple[str, str, str, Optional[Field], Any]]
) -> List[Dict[str, Any]]:
    """Update with expressions => aggregation-pipeline update (MongoDB 4.2+, atomic).

    Operators are rewritten into expressions: ``inc__x=1`` -> ``x = ifNull(x, 0) + 1`` and so on.
    """
    ctx = Context(Scope.for_model(model), allow_joins=False)
    set_: Dict[str, Any] = {}
    unset: List[str] = []
    for op, path, db_path, field, value in parsed:
        cur = {"$ifNull": ["$" + db_path, 0]}
        arr = {"$ifNull": ["$" + db_path, []]}
        if isinstance(value, Expression):
            if op != "set":
                raise InvalidArgument(f"Expressions can only be assigned with plain/set__, not {op}__.")
            set_[db_path] = value.resolve(ctx)
        elif op == "set":
            prepared = _prepare(field, value)
            set_[db_path] = {"$literal": prepared} if _needs_literal(prepared) else prepared
        elif op == "unset":
            unset.append(db_path)
        elif op in ("inc", "mul"):
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValidationError(f"{op}__ expects a number.", field=path)
            set_[db_path] = {"$add": [cur, value]} if op == "inc" else {"$multiply": [cur, value]}
        elif op in ("min", "max"):
            prepared = _prepare(field, value)
            set_[db_path] = {"$" + op: ["$" + db_path, prepared]}
        elif op in ("push", "add_to_set"):
            item = _element_field(field, path).prepare_value(value)
            fn = "$concatArrays" if op == "push" else "$setUnion"
            set_[db_path] = {fn: [arr, [{"$literal": item}]]}
        else:
            raise InvalidArgument(f"{op}__ cannot be combined with expression assignments.")
    stages: List[Dict[str, Any]] = []
    if set_:
        stages.append({"$set": set_})
    if unset:
        stages.append({"$unset": unset})
    return stages


def _needs_literal(value: Any) -> bool:
    return isinstance(value, (dict, list)) or (isinstance(value, str) and value.startswith("$"))


def _prepare(field: Any, value: Any) -> Any:
    if isinstance(value, Raw):
        return value.value
    if field is None:
        return assert_safe_value(value)
    if value is None:
        return None
    return field.prepare_value(value)


def _element_field(field: Any, path: str) -> Field:
    if isinstance(field, ListField):
        return field.field
    if field is None or isinstance(field, AnyField):
        return AnyField()
    raise ValidationError(f"{path!r} is not a list field.", field=path)
