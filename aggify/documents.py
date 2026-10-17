"""``Document`` / ``EmbeddedDocument`` and the model metaclass.

A document is declared like a Django model::

    class Order(Document):
        number = StringField(required=True, unique=True)
        total = DecimalField(max_digits=12, decimal_places=2)
        owner = ReferenceField("User")
        created_at = DateTimeField(auto_now_add=True)

        class Meta:
            collection = "orders"
            ordering = ["-created_at"]
            indexes = [Index("owner", "-created_at")]

Every I/O method comes in two flavours: ``save()`` / ``asave()``, ``delete()`` /
``adelete()``, ``refresh_from_db()`` / ``arefresh_from_db()`` ...
"""

from __future__ import annotations

import asyncio
import copy
import datetime as dt
import re
from collections import OrderedDict
from typing import Any, ClassVar, Dict, Generic, Iterable, List, Optional, Set, Tuple, Type, TypeVar

from bson import ObjectId
from pymongo.errors import DuplicateKeyError

from aggify import connection, registry, signals
from aggify.conf import settings
from aggify.exceptions import (
    ConfigurationError,
    MultipleObjectsReturned,
    NotUniqueError,
    ObjectDoesNotExist,
    OptimisticLockError,
    SynchronousOnlyOperation,
    ValidationError,
)
from aggify.fields import (
    NOT_PROVIDED,
    Field,
    ObjectIdField,
    ReferenceField,
)
from aggify.indexes import collect_index_models

M = TypeVar("M", bound="Document")


def _snake(name: str) -> str:
    s = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s).lower()


class Options:
    """What ``Model._meta`` exposes (a subset of Django's API is mirrored on purpose)."""

    def __init__(self, model: type, meta: Any, abstract: bool, embedded: bool) -> None:
        self.model = model
        self.abstract = abstract
        self.embedded = embedded
        self.object_name = model.__name__
        self.model_name = self.object_name.lower()
        self.app_label: str = getattr(meta, "app_label", None) or _guess_app_label(model)
        self.label = f"{self.app_label}.{self.object_name}"
        self.verbose_name: str = getattr(meta, "verbose_name", None) or _snake(self.object_name).replace("_", " ")
        self.verbose_name_plural: str = getattr(meta, "verbose_name_plural", None) or self.verbose_name + "s"
        self.collection: str = (
            getattr(meta, "collection", None)
            or getattr(meta, "db_table", None)
            or (
                f"{self.app_label}_{_snake(self.object_name)}"
                if getattr(meta, "app_label", None)
                else _snake(self.object_name)
            )
        )
        self.db_alias: str = getattr(meta, "db_alias", connection.DEFAULT_ALIAS)
        self.ordering: List[str] = list(getattr(meta, "ordering", []) or [])
        self.indexes: List[Any] = list(getattr(meta, "indexes", []) or [])
        self.get_latest_by = getattr(meta, "get_latest_by", None)
        #: field name holding an integer version => optimistic locking on save()
        self.version_field: Optional[str] = getattr(meta, "version_field", None)
        self.validate_on_save: bool = getattr(meta, "validate_on_save", True)
        self.strict: bool = getattr(meta, "strict", settings.strict_models)
        self.fields: OrderedDict[str, Field] = OrderedDict()
        self.pk: Any = None
        self.managers: Dict[str, Any] = {}

    @property
    def concrete_fields(self) -> List[Field]:
        return list(self.fields.values())

    @property
    def db_fields(self) -> Dict[str, Field]:
        return {f.db_field: f for f in self.fields.values()}

    def get_field(self, name: str) -> Field:
        if name == "pk" and self.pk is not None:
            return self.pk
        try:
            return self.fields[name]
        except KeyError:
            from aggify.exceptions import InvalidField

            raise InvalidField(name, self.model) from None

    def __repr__(self) -> str:
        return f"<Options for {self.label}>"


def _guess_app_label(model: type) -> str:
    module = model.__module__
    parts = module.split(".")
    if parts[-1] in ("models", "documents") and len(parts) > 1:
        return parts[-2]
    return parts[0] if parts[0] != "__main__" else "main"


class ModelBase(type):
    def __new__(mcs, name: str, bases: Tuple[type, ...], attrs: Dict[str, Any], **kwargs: Any):
        parents = [b for b in bases if isinstance(b, ModelBase)]
        if not parents:  # BaseDocument itself
            return super().__new__(mcs, name, bases, attrs)

        meta = own_meta = attrs.pop("Meta", None)
        parents = [b for b in parents if getattr(b, "_meta", None) is not None] or parents
        if meta is None:  # inherit Meta of an abstract parent (like Django)
            for b in [p for p in parents if getattr(p, "_meta", None) is not None]:
                if b._meta.abstract and getattr(b, "_meta_source", None) is not None:
                    meta = b._meta_source
                    break
        abstract = bool(vars(own_meta).get("abstract", False)) if own_meta is not None else False
        embedded = bool(attrs.get("_is_embedded")) or any(getattr(b, "_is_embedded", False) for b in parents)

        declared: List[Tuple[str, Field]] = [(k, v) for k, v in list(attrs.items()) if isinstance(v, Field)]
        for k, _ in declared:
            attrs.pop(k)
        managers = [(k, v) for k, v in list(attrs.items()) if isinstance(v, BaseManager)]
        for k, _ in managers:
            attrs.pop(k)

        attrs["__qualname__"] = attrs.get("__qualname__", name)
        cls = super().__new__(mcs, name, bases, attrs)
        cls._is_embedded = embedded
        cls._meta_source = meta
        opts = Options(cls, meta, abstract, embedded)
        cls._meta = opts

        # inherited fields (from abstract or concrete parents)
        inherited: List[Tuple[str, Field]] = []
        for parent in [p for p in parents if getattr(p, "_meta", None) is not None]:
            for fname, fld in parent._meta.fields.items():
                if fname not in dict(inherited):
                    inherited.append((fname, fld.clone()))
        own = sorted(declared, key=lambda kv: kv[1]._creation_order)
        for fname, fld in [*inherited, *own]:
            if fname in opts.fields and fname not in dict(own):
                continue
            fld.contribute_to_class(cls, fname)
            opts.fields[fname] = fld

        # primary key
        if not embedded and not abstract:
            pks = [f for f in opts.fields.values() if f.primary_key]
            if not pks and "id" in opts.fields:
                f = opts.fields["id"]
                f.primary_key = True
                f.db_field = "_id"
                pks = [f]
            if len(pks) > 1:
                raise ConfigurationError(f"{name} has more than one primary key: {[f.name for f in pks]}")
            if not pks:
                pk = ObjectIdField(primary_key=True, default=ObjectId)
                pk.contribute_to_class(cls, "id")
                opts.fields = OrderedDict([("id", pk), *opts.fields.items()])
                pks = [pk]
            opts.pk = pks[0]
            if opts.pk.default is NOT_PROVIDED and isinstance(opts.pk, ObjectIdField):
                opts.pk.default = ObjectId
            if not opts.pk.has_default():
                opts.pk.required = True

        cls.DoesNotExist = type(
            "DoesNotExist",
            tuple(p.DoesNotExist for p in parents if hasattr(p, "DoesNotExist")) or (ObjectDoesNotExist,),
            {"__module__": cls.__module__},
        )
        cls.MultipleObjectsReturned = type(
            "MultipleObjectsReturned",
            tuple(p.MultipleObjectsReturned for p in parents if hasattr(p, "MultipleObjectsReturned"))
            or (MultipleObjectsReturned,),
            {"__module__": cls.__module__},
        )

        if not embedded and not abstract:
            for k, mgr in managers:
                mgr.contribute_to_class(cls, k)
            # inherit managers
            for parent in [p for p in parents if getattr(p, "_meta", None) is not None]:
                for k, mgr in getattr(parent._meta, "managers", {}).items():
                    if k not in opts.managers and not any(k == mk for mk, _ in managers):
                        mgr2 = copy.copy(mgr)
                        mgr2.contribute_to_class(cls, k)
            if "objects" not in opts.managers:
                Manager().contribute_to_class(cls, "objects")
        if not abstract:
            registry.register(cls)
        return cls


class BaseManager:
    pass


class DocumentState:
    __slots__ = ("adding", "raw", "loaded", "db_alias")

    def __init__(self) -> None:
        self.adding = True
        self.raw: Dict[str, Any] = {}
        self.loaded: Optional[Set[str]] = None  # None = every field was loaded
        self.db_alias: Optional[str] = None


class BaseDocument(metaclass=ModelBase):
    _meta: ClassVar[Options]
    _is_embedded: ClassVar[bool] = False
    DoesNotExist: ClassVar[Type[ObjectDoesNotExist]]
    MultipleObjectsReturned: ClassVar[Type[Exception]]

    def __init__(self, **values: Any) -> None:
        self._data: Dict[str, Any] = {}
        self._state = DocumentState()
        meta = self._meta
        names = set(meta.fields)
        ref_ids = {f"{n}_id": n for n, f in meta.fields.items() if isinstance(f, ReferenceField)}
        pending: Dict[str, Any] = {}
        for key, value in values.items():
            if key in names:
                pending[key] = value
            elif key in ref_ids:
                pending[ref_ids[key]] = value
            elif key == "pk" and meta.pk is not None:
                pending[meta.pk.name] = value
            elif meta.strict:
                raise TypeError(f"{type(self).__name__}() got an unexpected field {key!r}")
        for name, f in meta.fields.items():
            if name in pending:
                v = pending[name]
                self._data[name] = None if v is None else f.to_python(v)
            elif f.has_default():
                v = f.get_default()
                self._data[name] = None if v is None else f.to_python(v)
            else:
                self._data[name] = None
        signals.post_init.send(type(self), instance=self)

    # ------------------------------------------------------------------ #
    # (de)serialisation
    # ------------------------------------------------------------------ #
    @classmethod
    def from_mongo(cls, raw: Dict[str, Any], *, loaded: Optional[Iterable[str]] = None) -> Any:
        """Build an instance from a raw BSON document (no validation, no defaults for unloaded fields)."""
        obj = cls.__new__(cls)
        obj._data = {}
        obj._state = DocumentState()
        partial = loaded is not None
        loaded_set = set(loaded) if partial else None
        for name, f in cls._meta.fields.items():
            if partial and name not in loaded_set:  # type: ignore[operator]
                obj._data[name] = None
                continue
            if f.db_field in raw and raw[f.db_field] is not None:
                obj._data[name] = f.from_mongo(raw[f.db_field])
            elif f.has_default():
                v = f.get_default()
                obj._data[name] = None if v is None else f.to_python(v)
            else:
                obj._data[name] = None
        obj._state.adding = False
        obj._state.loaded = loaded_set
        if not cls._meta.embedded:
            obj._state.raw = raw  # no copy: every field's from_mongo builds fresh containers
        return obj

    def to_mongo(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for name, f in self._meta.fields.items():
            value = self._data.get(name)
            if value is None:
                continue
            out[f.db_field] = f.to_mongo(value)
        return out

    def to_dict(
        self,
        *,
        include: Optional[Iterable[str]] = None,
        exclude: Optional[Iterable[str]] = None,
        json_safe: bool = False,
        include_none: bool = True,
    ) -> Dict[str, Any]:
        """Python-name dict. ``json_safe=True`` yields only JSON/protobuf friendly primitives."""
        inc = set(include) if include is not None else None
        exc = set(exclude or ())
        out: Dict[str, Any] = {}
        for name, f in self._meta.fields.items():
            if (inc is not None and name not in inc) or name in exc or f.exclude_from_output:
                continue
            value = self._data.get(name)
            if isinstance(f, ReferenceField) and hasattr(value, "_meta"):
                value = value.pk
            if value is None and not include_none:
                continue
            out[name] = _to_primitive(value) if json_safe else _to_plain(value)
        return out

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Any:
        """Create an instance from a *Python-named* mapping, e.g. a parsed JSON body.

        Unknown keys raise ``TypeError`` (mass-assignment protection).
        """
        return cls(**data)

    # ------------------------------------------------------------------ #
    # validation
    # ------------------------------------------------------------------ #
    def full_clean(self, exclude: Iterable[str] = ()) -> None:
        """Convert and validate every field; raises one ``ValidationError`` with all problems."""
        errors: Dict[str, List[str]] = {}
        skip = set(exclude)
        for name, f in self._meta.fields.items():
            if name in skip:
                continue
            if self._state_loaded_skip(name):
                continue
            value = self._data.get(name)
            try:
                self._data[name] = f.clean(value)
            except ValidationError as exc:
                if exc.errors:
                    for k, msgs in exc.errors.items():
                        errors.setdefault(k or name, []).extend(msgs)
                else:
                    errors.setdefault(name, []).append(exc.message)
        if errors:
            raise ValidationError(errors=errors)
        self.clean()

    def clean(self) -> None:
        """Override for cross-field validation; raise ``ValidationError``."""

    def _state_loaded_skip(self, name: str) -> bool:
        st = getattr(self, "_state", None)
        return bool(st is not None and st.loaded is not None and name not in st.loaded)

    # ------------------------------------------------------------------ #
    # dunder
    # ------------------------------------------------------------------ #
    def __repr__(self) -> str:
        pk = getattr(self, "pk", None)
        return f"<{type(self).__name__}{'' if pk is None else f': {pk}'}>"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, BaseDocument) or type(self) is not type(other):
            return NotImplemented
        if self._meta.embedded:
            return self.to_mongo() == other.to_mongo()
        pk = self.pk
        return pk is not None and pk == other.pk

    def __hash__(self) -> int:
        if self._meta.embedded:
            raise TypeError("Embedded documents are unhashable.")
        if self.pk is None:
            raise TypeError("Unsaved documents are unhashable.")
        return hash((type(self), self.pk))

    def __setstate__(self, state: Dict[str, Any]) -> None:
        self.__dict__.update(state)


class EmbeddedDocument(BaseDocument):
    """A sub-document stored inside another document (no collection of its own)."""

    _is_embedded = True

    class Meta:
        abstract = False

    def __init__(self, **values: Any) -> None:
        super().__init__(**values)

    @property
    def pk(self) -> None:
        return None


def _to_plain(value: Any) -> Any:
    if isinstance(value, BaseDocument):
        return value.to_dict()
    if isinstance(value, list):
        return [_to_plain(v) for v in value]
    if isinstance(value, dict):
        return {k: _to_plain(v) for k, v in value.items()}
    return value


def _to_primitive(value: Any) -> Any:
    import decimal
    import enum
    import uuid

    from bson import Binary, Decimal128

    if isinstance(value, BaseDocument):
        return value.to_dict(json_safe=True)
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, dt.datetime):
        return value.isoformat().replace("+00:00", "Z")
    if isinstance(value, dt.date):
        return value.isoformat()
    if isinstance(value, (decimal.Decimal, Decimal128)):
        return str(value.to_decimal() if isinstance(value, Decimal128) else value)
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, enum.Enum):
        return _to_primitive(value.value)
    if isinstance(value, (bytes, Binary)):
        import base64

        return base64.b64encode(bytes(value)).decode()
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_to_primitive(v) for v in value]
    if isinstance(value, dict):
        return {k: _to_primitive(v) for k, v in value.items()}
    return value


# =========================================================================== #
# Document
# =========================================================================== #
class Document(BaseDocument):
    """A top-level document stored in its own collection."""

    class Meta:
        abstract = True

    objects: ClassVar[Any]

    @property
    def pk(self) -> Any:
        return self._data.get(self._meta.pk.name)

    @pk.setter
    def pk(self, value: Any) -> None:
        self._data[self._meta.pk.name] = self._meta.pk.to_python(value)

    # ------------------------------------------------------------------ #
    # change tracking
    # ------------------------------------------------------------------ #
    def _scope_fields(self) -> List[Field]:
        loaded = self._state.loaded
        return [f for f in self._meta.fields.values() if loaded is None or f.name in loaded]

    def get_changes(self) -> Tuple[Dict[str, Any], List[str]]:
        """(`$set` values, `$unset` keys) relative to the state loaded from / saved to the database."""
        raw = self._state.raw
        sets: Dict[str, Any] = {}
        unsets: List[str] = []
        for f in self._scope_fields():
            if f.primary_key:
                continue
            value = self._data.get(f.name)
            if value is None:
                if raw.get(f.db_field) is not None:
                    unsets.append(f.db_field)
                continue
            mongo_value = f.to_mongo(value)
            if f.db_field not in raw or raw[f.db_field] != mongo_value:
                sets[f.db_field] = mongo_value
        return sets, unsets

    @property
    def changed_fields(self) -> List[str]:
        sets, unsets = self.get_changes()
        by_db = self._meta.db_fields
        return [by_db[k].name for k in [*sets, *unsets]]

    @property
    def is_dirty(self) -> bool:
        return bool(self.changed_fields) or self._state.adding

    # ------------------------------------------------------------------ #
    # saving
    # ------------------------------------------------------------------ #
    def _before_save(self, validate: bool, update_fields: Optional[Iterable[str]]) -> bool:
        adding = self._state.adding
        for f in self._meta.fields.values():
            f.pre_save(self, adding)
        if validate and self._meta.validate_on_save:
            exclude = None
            if update_fields is not None and not adding:
                keep = set(update_fields)
                exclude = [n for n in self._meta.fields if n not in keep]
            self.full_clean(exclude=exclude or ())
        return adding

    def _plan_save(self, adding: bool, update_fields: Optional[Iterable[str]], force_insert: bool):
        meta = self._meta
        if adding or force_insert:
            doc = self.to_mongo()
            if meta.version_field:
                doc[meta.fields[meta.version_field].db_field] = 1
                self._data[meta.version_field] = 1
            return ("insert", doc, None)
        sets, unsets = self.get_changes()
        if update_fields is not None:
            keep = {meta.fields[n].db_field for n in update_fields}
            sets = {k: v for k, v in sets.items() if k in keep}
            unsets = [k for k in unsets if k in keep]
        if not sets and not unsets:
            return ("noop", None, None)
        flt: Dict[str, Any] = {"_id": meta.pk.to_mongo(self.pk)}
        update: Dict[str, Any] = {}
        if meta.version_field:
            vf = meta.fields[meta.version_field]
            current = self._data.get(vf.name) or 0
            flt[vf.db_field] = current
            sets.pop(vf.db_field, None)
            update["$inc"] = {vf.db_field: 1}
        if sets:
            update["$set"] = sets
        if unsets:
            update["$unset"] = {k: "" for k in unsets}
        return ("update", flt, update)

    def _after_save(self, adding: bool) -> None:
        self._state.adding = False
        self._state.raw = (
            copy.deepcopy(self.to_mongo())
            if self._state.loaded is None
            else {**self._state.raw, **copy.deepcopy(self.to_mongo())}
        )

    def _opts(self, alias: Optional[str] = None) -> Dict[str, Any]:
        return connection.session_kwargs(alias or self._meta.db_alias)

    def save(
        self, *, validate: bool = True, force_insert: bool = False, update_fields: Optional[Iterable[str]] = None
    ) -> Document:
        """Insert (new instance) or update only the changed fields (loaded instance)."""
        adding = self._before_save(validate, update_fields)
        signals.pre_save.send(type(self), instance=self, created=adding)
        kind, a, b = self._plan_save(adding, update_fields, force_insert)
        coll = connection.get_collection(type(self))
        kw = self._opts()
        try:
            if kind == "insert":
                coll.insert_one(a, **kw)
            elif kind == "update":
                res = coll.update_one(a, b, **kw)
                if res.matched_count == 0:
                    self._handle_missing_update(coll, kw)
                elif self._meta.version_field:
                    self._data[self._meta.version_field] = (self._data.get(self._meta.version_field) or 0) + 1
        except DuplicateKeyError as exc:
            raise NotUniqueError(_dup_message(self, exc)) from exc
        self._after_save(adding)
        signals.post_save.send(type(self), instance=self, created=adding)
        return self

    def _handle_missing_update(self, coll: Any, kw: Dict[str, Any]) -> None:
        if self._meta.version_field:
            raise OptimisticLockError(f"{type(self).__name__} {self.pk!r} was modified or deleted concurrently.")
        coll.insert_one(self.to_mongo(), **kw)  # deleted meanwhile: behave like Django (re-insert)

    async def asave(
        self, *, validate: bool = True, force_insert: bool = False, update_fields: Optional[Iterable[str]] = None
    ) -> Document:
        adding = self._before_save(validate, update_fields)
        await signals.pre_save.asend(type(self), instance=self, created=adding)
        kind, a, b = self._plan_save(adding, update_fields, force_insert)
        coll = connection.get_async_collection(type(self))
        kw = self._opts()
        try:
            if kind == "insert":
                await coll.insert_one(a, **kw)
            elif kind == "update":
                res = await coll.update_one(a, b, **kw)
                if res.matched_count == 0:
                    if self._meta.version_field:
                        raise OptimisticLockError(
                            f"{type(self).__name__} {self.pk!r} was modified or deleted concurrently."
                        )
                    await coll.insert_one(self.to_mongo(), **kw)
                elif self._meta.version_field:
                    self._data[self._meta.version_field] = (self._data.get(self._meta.version_field) or 0) + 1
        except DuplicateKeyError as exc:
            raise NotUniqueError(_dup_message(self, exc)) from exc
        self._after_save(adding)
        await signals.post_save.asend(type(self), instance=self, created=adding)
        return self

    # ------------------------------------------------------------------ #
    # delete / refresh
    # ------------------------------------------------------------------ #
    def delete(self) -> bool:
        self._check_pk("delete")
        signals.pre_delete.send(type(self), instance=self)
        res = connection.get_collection(type(self)).delete_one({"_id": self._meta.pk.to_mongo(self.pk)}, **self._opts())
        self._state.adding = True
        signals.post_delete.send(type(self), instance=self)
        return res.deleted_count > 0

    async def adelete(self) -> bool:
        self._check_pk("delete")
        await signals.pre_delete.asend(type(self), instance=self)
        res = await connection.get_async_collection(type(self)).delete_one(
            {"_id": self._meta.pk.to_mongo(self.pk)}, **self._opts()
        )
        self._state.adding = True
        await signals.post_delete.asend(type(self), instance=self)
        return res.deleted_count > 0

    def _check_pk(self, op: str) -> None:
        if self.pk is None:
            raise ValueError(f"Cannot {op} an instance without a primary key.")

    def _apply_raw(self, raw: Dict[str, Any], fields: Optional[Iterable[str]] = None) -> None:
        wanted = set(fields) if fields is not None else None
        for name, f in self._meta.fields.items():
            if wanted is not None and name not in wanted:
                continue
            if f.db_field in raw and raw[f.db_field] is not None:
                self._data[name] = f.from_mongo(raw[f.db_field])
            else:
                v = f.get_default() if f.has_default() else None
                self._data[name] = None if v is None else f.to_python(v)
        self._state.raw = copy.deepcopy(raw) if wanted is None else {**self._state.raw, **copy.deepcopy(raw)}
        self._state.adding = False

    def refresh_from_db(self, fields: Optional[Iterable[str]] = None) -> Document:
        self._check_pk("refresh")
        proj = None if fields is None else {self._meta.fields[n].db_field: 1 for n in fields}
        raw = connection.get_collection(type(self)).find_one(
            {"_id": self._meta.pk.to_mongo(self.pk)}, proj, **self._opts()
        )
        if raw is None:
            raise self.DoesNotExist(f"{type(self).__name__} {self.pk!r} no longer exists.")
        self._apply_raw(raw, fields)
        return self

    async def arefresh_from_db(self, fields: Optional[Iterable[str]] = None) -> Document:
        self._check_pk("refresh")
        proj = None if fields is None else {self._meta.fields[n].db_field: 1 for n in fields}
        raw = await connection.get_async_collection(type(self)).find_one(
            {"_id": self._meta.pk.to_mongo(self.pk)}, proj, **self._opts()
        )
        if raw is None:
            raise self.DoesNotExist(f"{type(self).__name__} {self.pk!r} no longer exists.")
        self._apply_raw(raw, fields)
        return self

    # ------------------------------------------------------------------ #
    # references
    # ------------------------------------------------------------------ #
    def _resolve_reference_sync(self, field: ReferenceField, pk: Any) -> Any:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            pass
        else:
            raise SynchronousOnlyOperation(
                f"Accessing {type(self).__name__}.{field.name} would block the event loop. "
                f"Use `await obj.afetch_related({field.name!r})`, select_related({field.name!r}) "
                f"or read `{field.name}_id`."
            )
        target = field.document_type.objects.filter(pk=pk).first()
        if target is not None:
            self._data[field.name] = target
        return target

    def fetch_related(self, *names: str) -> Document:
        for n in names:
            field = self._meta.get_field(n)
            current = self._data.get(n)
            if current is not None and not hasattr(current, "_meta"):
                self._data[n] = field.document_type.objects.filter(pk=current).first()
        return self

    async def afetch_related(self, *names: str) -> Document:
        for n in names:
            field = self._meta.get_field(n)
            current = self._data.get(n)
            if current is not None and not hasattr(current, "_meta"):
                self._data[n] = await field.document_type.objects.filter(pk=current).afirst()
        return self

    # ------------------------------------------------------------------ #
    # class level helpers
    # ------------------------------------------------------------------ #
    @classmethod
    def ensure_indexes(cls) -> List[str]:
        models = collect_index_models(cls)
        return connection.get_collection(cls).create_indexes(models) if models else []

    @classmethod
    async def aensure_indexes(cls) -> List[str]:
        models = collect_index_models(cls)
        return await connection.get_async_collection(cls).create_indexes(models) if models else []

    @classmethod
    def drop_collection(cls) -> None:
        connection.get_collection(cls).drop()

    @classmethod
    async def adrop_collection(cls) -> None:
        await connection.get_async_collection(cls).drop()


def _dup_message(doc: Document, exc: DuplicateKeyError) -> str:
    details = getattr(exc, "details", None) or {}
    key = details.get("keyValue")
    return f"Duplicate value for {type(doc).__name__}: {key}" if key else f"Duplicate key on {type(doc).__name__}"


# =========================================================================== #
# Manager
# =========================================================================== #
class Manager(BaseManager, Generic[M]):
    """``Model.objects`` - proxies every ``QuerySet`` method."""

    queryset_class: Optional[type] = None

    def __init__(self, queryset_class: Optional[type] = None) -> None:
        self.model: Any = None
        self.name: str = ""
        if queryset_class is not None:
            self.queryset_class = queryset_class

    def contribute_to_class(self, model: Any, name: str) -> None:
        self.model = model
        self.name = name
        model._meta.managers[name] = self
        setattr(model, name, _ManagerDescriptor(self))

    def get_queryset(self) -> Any:
        from aggify.queryset import QuerySet

        return (self.queryset_class or QuerySet)(self.model)

    @classmethod
    def from_queryset(cls, queryset_class: type, class_name: Optional[str] = None) -> Type[Manager]:
        return type(
            class_name or f"{cls.__name__}From{queryset_class.__name__}", (cls,), {"queryset_class": queryset_class}
        )

    def __getattr__(self, name: str) -> Any:
        if name.startswith("__") or name in ("model", "name"):
            raise AttributeError(name)
        return getattr(self.get_queryset(), name)

    def __copy__(self):
        new = type(self).__new__(type(self))
        new.__dict__.update(self.__dict__)
        return new


class _ManagerDescriptor:
    def __init__(self, manager: Manager) -> None:
        self.manager = manager

    def __get__(self, instance: Any, owner: Any = None) -> Manager:
        if instance is not None:
            raise AttributeError(f"Manager isn't accessible via {owner.__name__} instances")
        return self.manager


__all__ = ["Document", "EmbeddedDocument", "Manager", "Options", "BaseDocument"]
