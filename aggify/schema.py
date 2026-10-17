"""Field-path resolution.

``Scope`` describes *which fields exist at this point of a query/pipeline*.
Starting from a model it is evolved by stages (``$project`` narrows it,
``$group`` replaces it, ``$addFields`` extends it ...), which is what lets
Aggify keep validating names and converting values after complex stages
- something the 0.x series did by monkey-patching copies of mongoengine classes.

``resolve_path`` turns ``"owner__profile__city__icontains"`` into a database
path, the final field, the lookup and the joins ($lookup) the path needs.
"""

from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field as dc_field
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from aggify.conf import settings
from aggify.exceptions import InvalidField, InvalidLookup, UnsafeQueryError
from aggify.fields import AnyField, DictField, EmbeddedField, Field, ListField, ReferenceField
from aggify.lookups import LOOKUPS

JOIN_PREFIX = "_j_"


class Scope:
    """Immutable view of the fields visible at some point of a query."""

    __slots__ = ("fields", "model", "open")

    def __init__(self, fields: Mapping[str, Field], model: Any = None, *, open: bool = False) -> None:
        self.fields: Dict[str, Field] = dict(fields)
        self.model = model
        #: open scopes accept unknown names (after $project of expressions, raw stages ...)
        self.open = open

    # constructors -------------------------------------------------------
    @classmethod
    def for_model(cls, model: Any, *, open: bool = False) -> Scope:
        meta = model._meta
        return cls(meta.fields, model, open=open)

    def evolve(
        self,
        add: Optional[Mapping[str, Field]] = None,
        *,
        only: Optional[Iterable[str]] = None,
        drop: Iterable[str] = (),
        open: Optional[bool] = None,
        keep_model: bool = True,
    ) -> Scope:
        fields = dict(self.fields)
        if only is not None:
            keep = set(only)
            fields = {k: v for k, v in fields.items() if k in keep}
        for name in drop:
            fields.pop(name, None)
        if add:
            fields.update(add)
        return Scope(fields, self.model if keep_model else None, open=self.open if open is None else open)

    # lookups ------------------------------------------------------------
    def get(self, name: str) -> Field:
        if name == "pk" and self.model is not None:
            name = self.model._meta.pk.name
        found = self.fields.get(name)
        if found is not None:
            return found
        # also accept the database name (``_id``)
        for f in self.fields.values():
            if f.db_field == name and name != f.name and name.startswith("_"):
                return f
        if self.open or not settings.strict_fields:
            return _dynamic(name)
        raise InvalidField(name, self.model)

    def has(self, name: str) -> bool:
        return name in self.fields

    def __repr__(self) -> str:
        return f"Scope({sorted(self.fields)}{', open' if self.open else ''})"


def _dynamic(name: str) -> Field:
    f = AnyField()
    f.name = name
    f.db_field = name
    return f


def dynamic_field(name: str, like: Optional[Field] = None) -> Field:
    """A field entry for a computed column (copy of ``like`` when given)."""
    f = like.clone() if like is not None else AnyField()
    f.name = name
    f.db_field = name
    f.primary_key = False
    f.required = False
    f.unique = False
    f.index = False
    return f


@dataclass(frozen=True)
class Join:
    alias: str
    from_model: Any
    local_path: str
    many: bool = False  # list of references => no $unwind


@dataclass
class Resolved:
    db_path: str
    field: Optional[Field]
    lookup: Optional[str]
    py_path: str
    joins: List[Join] = dc_field(default_factory=list)
    #: scope of the document the path ends in (for elem_match on embedded lists)
    inner_scope: Optional[Scope] = None
    is_dict_key: bool = False


class Context:
    """Carries the scope and collects the joins required by expressions."""

    def __init__(self, scope: Scope, *, allow_joins: bool = True, joined: Iterable[str] = ()) -> None:
        self.scope = scope
        self.allow_joins = allow_joins
        self.joins: Dict[str, Join] = {}
        self.joined = set(joined)

    def add_joins(self, joins: Iterable[Join]) -> None:
        for j in joins:
            if not self.allow_joins:
                raise InvalidField(j.alias, None)
            if j.alias not in self.joined:
                self.joins.setdefault(j.alias, j)

    def child(self, scope: Scope, **kw: Any) -> Context:
        c = Context(scope, allow_joins=kw.get("allow_joins", self.allow_joins), joined=self.joined)
        c.joins = self.joins
        return c


def _unwrap(f: Field) -> Tuple[Field, bool]:
    many = False
    while isinstance(f, ListField):
        f = f.field
        many = True
    return f, many


def resolve_path(
    scope: Scope,
    path: str,
    *,
    with_lookup: bool = True,
    allow_joins: bool = True,
) -> Resolved:
    parts = path.split("__")
    if not path or any(p == "" for p in parts):
        raise InvalidField(path, scope.model)
    lookup: Optional[str] = None
    if with_lookup and len(parts) > 1 and parts[-1] in LOOKUPS:
        lookup = parts.pop()

    joined_name = "__".join(parts)
    if len(parts) > 1 and joined_name in scope.fields:
        parts = [joined_name]  # computed column from values("a__b") / annotate(): a literal name
    cur: Optional[Scope] = scope
    db_parts: List[str] = []
    py_parts: List[str] = []
    joins: List[Join] = []
    field: Optional[Field] = None
    inner_scope: Optional[Scope] = None
    in_dict = False
    dict_inner: Optional[Field] = None
    through_list = False

    i = 0
    while i < len(parts):
        part = parts[i]
        if in_dict:
            if part.startswith("$") or "\x00" in part:
                raise UnsafeQueryError(f"Unsafe key {part!r} in path {path!r}.")
            db_parts.append(part)
            py_parts.append(part)
            field = dict_inner
            if isinstance(dict_inner, EmbeddedField):
                cur = Scope.for_model(dict_inner.document_type)
                in_dict = False
            i += 1
            continue
        if cur is None:
            raise InvalidField("__".join(parts[: i + 1]), scope.model)

        f = cur.get(part)
        field = f
        db_parts.append(f.db_field or part)
        py_parts.append(part)
        base, many = _unwrap(f)
        through_list = through_list or many

        last = i == len(parts) - 1
        if isinstance(base, EmbeddedField):
            cur = Scope.for_model(base.document_type)
            inner_scope = cur
        elif isinstance(base, ReferenceField):
            if last:
                cur = None
            else:
                target = base.document_type
                rest = parts[i + 1 :]
                if rest in (["pk"], [target._meta.pk.name]):
                    # filtering on the referenced id needs no join
                    i = len(parts)
                    break
                if not allow_joins:
                    raise InvalidField(path, scope.model)
                alias = JOIN_PREFIX + "__".join(py_parts)
                joins.append(Join(alias, target, ".".join(db_parts), many=through_list))
                db_parts = [alias]
                cur = Scope.for_model(target)
                inner_scope = cur
        elif isinstance(base, DictField):
            in_dict = True
            dict_inner = base.field
            cur = None
        elif isinstance(base, AnyField) and cur.open:
            in_dict = True
            dict_inner = base
            cur = None
        else:
            cur = None
        i += 1

    if lookup is not None and lookup not in LOOKUPS:
        raise InvalidLookup(lookup, path)

    return Resolved(
        db_path=".".join(db_parts),
        field=field,
        lookup=lookup,
        py_path=path,
        joins=joins,
        inner_scope=inner_scope,
        is_dict_key=in_dict,
    )
