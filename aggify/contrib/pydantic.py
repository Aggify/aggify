"""Pydantic v2 bridge: build request/response schemas straight from Aggify documents.

UserIn  = pydantic_model(User, name="UserIn", for_input=True)
UserOut = pydantic_model(User, name="UserOut", exclude=["password_hash"])

@app.post("/users", response_model=UserOut)
async def create(body: UserIn):
    return await User.objects.acreate(**body.model_dump())
"""

from __future__ import annotations

import datetime as dt
import decimal
import uuid
from typing import Annotated, Any, Dict, Iterable, List, Optional, Tuple, Type

from bson import ObjectId

try:
    import pydantic
    from pydantic import BaseModel, ConfigDict, create_model
    from pydantic import Field as PField
    from pydantic_core import core_schema
except ImportError as exc:  # pragma: no cover
    raise ImportError("aggify.contrib.pydantic needs pydantic>=2: pip install 'aggify[pydantic]'") from exc

from aggify import fields as f


class _ObjectIdAnnotation:
    @classmethod
    def __get_pydantic_core_schema__(cls, source: Any, handler: Any) -> Any:
        def validate(value: Any) -> ObjectId:
            if isinstance(value, ObjectId):
                return value
            if isinstance(value, str) and ObjectId.is_valid(value) and len(value) == 24:
                return ObjectId(value)
            raise ValueError("must be a 24 character hex ObjectId")

        return core_schema.no_info_plain_validator_function(
            validate,
            serialization=core_schema.plain_serializer_function_ser_schema(
                str, return_schema=core_schema.str_schema(), when_used="json"
            ),
        )

    @classmethod
    def __get_pydantic_json_schema__(cls, schema: Any, handler: Any) -> Dict[str, Any]:
        return {"type": "string", "pattern": "^[0-9a-fA-F]{24}$", "examples": ["64b7f0c2e1a4c2d9b8a3f001"]}


PyObjectId = Annotated[ObjectId, _ObjectIdAnnotation]

_cache: Dict[Tuple[Any, ...], Type[BaseModel]] = {}


def _annotation(field: f.Field, *, for_input: bool, stack: Tuple[Any, ...]) -> Tuple[Any, Dict[str, Any]]:
    """(python annotation, pydantic Field kwargs) for a model field."""
    kw: Dict[str, Any] = {}
    if isinstance(field, f.EmbeddedField):
        return pydantic_model(field.document_type, for_input=for_input, _stack=stack), kw
    if isinstance(field, f.ReferenceField):
        return PyObjectId, kw
    if isinstance(field, f.ListField):
        inner, _ = _annotation(field.field, for_input=for_input, stack=stack)
        if field.min_length is not None:
            kw["min_length"] = field.min_length
        if field.max_length is not None:
            kw["max_length"] = field.max_length
        return List[inner], kw  # type: ignore[valid-type]
    if isinstance(field, f.DictField):
        inner, _ = _annotation(field.field, for_input=for_input, stack=stack)
        return Dict[str, inner], kw  # type: ignore[valid-type]
    if isinstance(field, f.StringField):
        if field.min_length is not None:
            kw["min_length"] = field.min_length
        if field.max_length is not None:
            kw["max_length"] = field.max_length
        if field.regex is not None:
            kw["pattern"] = field.regex.pattern
        return str, kw
    if isinstance(field, f.IntField):
        if field.min_value is not None:
            kw["ge"] = field.min_value
        if field.max_value is not None:
            kw["le"] = field.max_value
        return int, kw
    if isinstance(field, f.FloatField):
        if field.min_value is not None:
            kw["ge"] = field.min_value
        if field.max_value is not None:
            kw["le"] = field.max_value
        return float, kw
    if isinstance(field, f.DecimalField):
        if field.max_digits is not None:
            kw["max_digits"] = field.max_digits
        if field.decimal_places is not None:
            kw["decimal_places"] = field.decimal_places
        return decimal.Decimal, kw
    simple = {
        f.BooleanField: bool,
        f.DateTimeField: dt.datetime,
        f.DateField: dt.date,
        f.UUIDField: uuid.UUID,
        f.BinaryField: bytes,
        f.ObjectIdField: PyObjectId,
        f.GeoPointField: Tuple[float, float],
    }
    for klass, annotation in simple.items():
        if isinstance(field, klass):
            return annotation, kw
    if isinstance(field, f.EnumField):
        return field.enum_class, kw
    return Any, kw


def pydantic_model(
    model: Any,
    *,
    name: Optional[str] = None,
    include: Optional[Iterable[str]] = None,
    exclude: Optional[Iterable[str]] = None,
    for_input: bool = False,
    all_optional: bool = False,
    _stack: Tuple[Any, ...] = (),
) -> Type[BaseModel]:
    """Create (and cache) a Pydantic model describing ``model``.

    ``for_input=True`` drops the primary key and non-editable fields and forbids unknown keys.
    ``all_optional=True`` is handy for PATCH bodies.
    """
    inc = None if include is None else frozenset(include)
    exc = frozenset(exclude or ())
    key = (model, name, inc, exc, for_input, all_optional)
    if key in _cache:
        return _cache[key]
    if model in _stack:
        raise ValueError(f"Recursive embedded documents are not supported ({model.__name__}).")
    stack = (*_stack, model)

    definitions: Dict[str, Any] = {}
    for fname, fld in model._meta.fields.items():
        if (inc is not None and fname not in inc) or fname in exc or fld.exclude_from_output and not for_input:
            continue
        if for_input and (fld.primary_key or not fld.editable):
            continue
        ann, kw = _annotation(fld, for_input=for_input, stack=stack)
        if for_input:
            required = fld.required and not fld.has_default() and not all_optional
        else:
            required = fld.primary_key
        if required:
            definitions[fname] = (ann, PField(..., description=fld.help_text or None, **kw))
        else:
            default = None
            if fld.has_default() and for_input and not all_optional and not callable(fld.default):
                default = fld.default
            definitions[fname] = (Optional[ann], PField(default, description=fld.help_text or None, **kw))
    config = ConfigDict(extra="forbid" if for_input else "ignore", from_attributes=True, arbitrary_types_allowed=True)
    created = create_model(name or f"{model.__name__}{'In' if for_input else 'Out'}", __config__=config, **definitions)
    _cache[key] = created
    return created


def to_pydantic(instance: Any, schema: Optional[Type[BaseModel]] = None) -> BaseModel:
    schema = schema or pydantic_model(type(instance))
    return schema.model_validate(instance.to_dict())


def from_pydantic(model: Any, obj: BaseModel, **extra: Any) -> Any:
    data = obj.model_dump(exclude_unset=True)
    data.update(extra)
    return model.from_dict(data)
