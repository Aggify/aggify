"""``QuerySet``: the Django-style query API, with a sync and an async twin for every I/O method.

>>> adults = User.objects.filter(age__gte=18).exclude(banned=True).order_by("-created_at")
>>> page = await adults[20:40].alist()                      # async
>>> total = adults.count()                                  # sync
>>> by_country = (User.objects.values("country")
...               .annotate(n=Count(), avg_age=Avg("age")).order_by("-n"))

A queryset is lazy and immutable: every chaining method returns a new one.  It is compiled to a
plain ``find`` whenever possible and to an aggregation pipeline when it needs joins
(``owner__name="x"``), annotations, grouping or ``select_related``.
"""

from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterator,
    Dict,
    Generic,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)

from pymongo import ASCENDING, DESCENDING
from pymongo.errors import BulkWriteError

from aggify import connection
from aggify.conf import settings
from aggify.exceptions import (
    InvalidArgument,
    InvalidField,
    MongoIndexError,
    NotUniqueError,
    ObjectDoesNotExist,
)
from aggify.expressions import Aggregate, Count, F, OrderBy, Q, RawExpression, wrap
from aggify.fields import ListField, ReferenceField
from aggify.pipeline import Pipeline, convert_row
from aggify.schema import JOIN_PREFIX, Context, Join, Scope, dynamic_field, resolve_path
from aggify.security import Raw, scan_stages
from aggify.updates import build_update

if TYPE_CHECKING:
    from aggify.documents import Document

M = TypeVar("M", bound="Document")
_UNSET: Any = object()


class QuerySet(Generic[M]):
    def __init__(self, model: Type[M]) -> None:
        self.model = model
        self._where = Q()
        self._having = Q()
        self._grouped = False
        self._order: Optional[Tuple[Any, ...]] = None  # None -> Meta.ordering
        self._reversed = False
        self._start = 0
        self._stop: Optional[int] = None
        self._only: Optional[Tuple[str, ...]] = None
        self._defer: Tuple[str, ...] = ()
        self._values: Optional[Tuple[str, ...]] = None
        self._values_mode: str = "dict"  # dict | flat | tuple | named
        self._annotations: Dict[str, Any] = {}
        self._select_related: Tuple[str, ...] = ()
        self._prefetch_related: Tuple[str, ...] = ()
        self._none = False
        self._options: Dict[str, Any] = {}
        self._cache: Optional[List[Any]] = None

    # ------------------------------------------------------------------ #
    # chaining
    # ------------------------------------------------------------------ #
    def _clone(self) -> QuerySet[M]:
        c = type(self).__new__(type(self))
        c.__dict__.update(self.__dict__)
        c._annotations = dict(self._annotations)
        c._options = dict(self._options)
        c._cache = None
        return c

    @classmethod
    def as_manager(cls) -> Any:
        from aggify.documents import Manager

        return Manager.from_queryset(cls)()

    def all(self) -> QuerySet[M]:
        return self._clone()

    def none(self) -> QuerySet[M]:
        c = self._clone()
        c._none = True
        return c

    def _assert_not_sliced(self, what: str) -> None:
        if self._start or self._stop is not None:
            raise InvalidArgument(f"Cannot {what} a query once a slice has been taken.")

    def filter(self, *args: Any, **kwargs: Any) -> QuerySet[M]:
        self._assert_not_sliced("filter")
        c = self._clone()
        q = Q(*args, **kwargs)
        if c._grouped or c._annotations:  # may reference annotations: applied after them (HAVING)
            c._having = c._having & q
        else:
            c._where = c._where & q
        return c

    def exclude(self, *args: Any, **kwargs: Any) -> QuerySet[M]:
        q = Q(*args, **kwargs)
        return self.filter(~q) if q else self._clone()

    def order_by(self, *fields: Union[str, OrderBy, F]) -> QuerySet[M]:
        self._assert_not_sliced("reorder")
        c = self._clone()
        c._order = tuple(fields)
        c._reversed = False
        return c

    def reverse(self) -> QuerySet[M]:
        c = self._clone()
        c._reversed = not self._reversed
        return c

    def only(self, *fields: str) -> QuerySet[M]:
        c = self._clone()
        c._only = tuple(fields)
        return c

    def defer(self, *fields: str) -> QuerySet[M]:
        c = self._clone()
        c._defer = tuple(dict.fromkeys([*self._defer, *fields]))
        return c

    def values(self, *fields: str, **expressions: Any) -> QuerySet[Any]:
        """Rows as dicts.  Followed by ``annotate(<aggregate>)`` it groups, like Django."""
        c = self._clone()
        names = fields if (fields or expressions) else tuple(self.model._meta.fields)
        c._values = tuple(names) + tuple(expressions)
        c._values_mode = "dict"
        if expressions:
            c._annotations.update(expressions)
        return c

    def values_list(self, *fields: str, flat: bool = False, named: bool = False) -> QuerySet[Any]:
        if flat and len(fields) != 1:
            raise InvalidArgument("flat=True needs exactly one field.")
        c = self._clone()
        c._values = tuple(fields or self.model._meta.fields)
        c._values_mode = "flat" if flat else ("named" if named else "tuple")
        return c

    def annotate(self, *args: Any, **expressions: Any) -> QuerySet[M]:
        """Add computed attributes (``$addFields``) or, after ``values()``, aggregates (``$group``)."""
        c = self._clone()
        if args:
            raise InvalidArgument("annotate() takes keyword arguments: annotate(total=Sum('amount')).")
        for name in expressions:
            if name in self.model._meta.fields:
                raise InvalidArgument(f"The annotation {name!r} conflicts with a field on the model.")
        c._annotations.update(expressions)
        if c._values is not None and any(
            isinstance(wrap(e, literal=True), (Aggregate, RawExpression))
            and isinstance(wrap(e, literal=True), Aggregate)
            for e in expressions.values()
        ):
            c._grouped = True
        return c

    def search(self, text: str, *, language: Optional[str] = None, case_sensitive: bool = False) -> QuerySet[M]:
        """Full-text search (``$text``); needs a text index: ``Index("title", "body", type="text")``."""
        if not isinstance(text, str) or not text.strip():
            raise InvalidArgument("search() needs a non-empty string.")
        spec: Dict[str, Any] = {"$search": text, "$caseSensitive": bool(case_sensitive)}
        if language:
            spec["$language"] = str(language)
        return self.filter(Raw({"$text": spec}))

    def select_related(self, *names: str) -> QuerySet[M]:
        c = self._clone()
        c._select_related = tuple(dict.fromkeys([*self._select_related, *names])) if names else ()
        return c

    def prefetch_related(self, *names: str) -> QuerySet[M]:
        c = self._clone()
        c._prefetch_related = tuple(names)
        return c

    def hint(self, index: Any) -> QuerySet[M]:
        return self.options(hint=index)

    def options(self, **opts: Any) -> QuerySet[M]:
        """``max_time_ms``, ``hint``, ``collation``, ``comment``, ``batch_size``, ``allow_disk_use``."""
        c = self._clone()
        c._options.update(opts)
        return c

    # ------------------------------------------------------------------ #
    # slicing / indexing
    # ------------------------------------------------------------------ #
    def __getitem__(self, k: Union[int, slice]) -> Any:
        if isinstance(k, bool) or not isinstance(k, (int, slice)):
            raise TypeError("QuerySet indices must be integers or slices.")
        if isinstance(k, int):
            if k < 0:
                raise MongoIndexError("Negative indexing is not supported.")
            if self._cache is not None:
                return self._cache[k]
            rows = list(self._slice(k, k + 1))
            if not rows:
                raise IndexError("QuerySet index out of range")
            return rows[0]
        if k.step not in (None, 1):
            raise MongoIndexError("Slices with a step are not supported.")
        start = 0 if k.start is None else k.start
        if start < 0 or (k.stop is not None and k.stop < 0):
            raise MongoIndexError("Negative indexing is not supported.")
        return self._slice(start, k.stop)

    def _slice(self, start: int, stop: Optional[int]) -> QuerySet[M]:
        c = self._clone()
        base = self._start
        c._start = base + start
        if stop is not None:
            new_stop = base + stop
            c._stop = new_stop if self._stop is None else min(self._stop, new_stop)
        c._cache = None
        if c._stop is not None and c._stop <= c._start:
            return c.none()
        return c

    # ------------------------------------------------------------------ #
    # compilation
    # ------------------------------------------------------------------ #
    @property
    def ordered(self) -> bool:
        return bool(self._effective_order())

    def _effective_order(self) -> Tuple[Any, ...]:
        if self._order is None and self._grouped:
            order: Tuple[Any, ...] = ()  # like Django: Meta.ordering does not apply to grouped queries
        else:
            order = self._order if self._order is not None else tuple(self.model._meta.ordering)
        if self._reversed:
            order = tuple(_invert(o) for o in order)
        return order

    def _limit(self) -> Optional[int]:
        if self._stop is None:
            lim = None
        else:
            lim = self._stop - self._start
        if settings.max_limit is not None and lim is not None and lim > settings.max_limit:
            raise InvalidArgument(f"Slice of {lim} exceeds max_limit ({settings.max_limit}).")
        return lim

    def _scope(self) -> Scope:
        return Scope.for_model(self.model)

    def _needs_pipeline(self) -> bool:
        if self._annotations or self._grouped or self._select_related:
            return True
        if self._values is not None and any(
            ("__" in v) or v not in self.model._meta.fields and v != "pk" for v in self._values
        ):
            return True
        # joins required by filters / ordering?
        ctx = Context(self._scope())
        self._where.resolve(ctx)
        for o in self._effective_order():
            path = _order_path(o)
            r = resolve_path(ctx.scope, path, with_lookup=False)
            ctx.add_joins(r.joins)
        return bool(ctx.joins)

    def _find_spec(self) -> Dict[str, Any]:
        ctx = Context(self._scope(), allow_joins=False)
        flt = self._where.resolve(ctx)
        spec: Dict[str, Any] = {"filter": flt}
        order = self._effective_order()
        if order:
            sort = []
            for o in order:
                path = _order_path(o)
                r = resolve_path(ctx.scope, path, with_lookup=False, allow_joins=False)
                sort.append((r.db_path, DESCENDING if _is_desc(o) else ASCENDING))
            spec["sort"] = sort
        if self._start:
            spec["skip"] = self._start
        lim = self._limit()
        if lim is not None:
            spec["limit"] = lim
        proj = self._projection()
        if proj is not None:
            spec["projection"] = proj[0]
        return spec

    def _projection(self) -> Optional[Tuple[Dict[str, int], Optional[set]]]:
        """(`find` projection, set of loaded python field names)."""
        meta = self.model._meta
        names = list(meta.fields)
        if self._values is not None:
            wanted = [("pk" if v == "pk" else v) for v in self._values]
            wanted = [meta.pk.name if w == "pk" else w for w in wanted]
            proj = {meta.fields[w].db_field: 1 for w in wanted}
            if meta.pk.name not in wanted:
                proj["_id"] = 0
            return proj, set(wanted)
        if self._only is not None:
            keep = {meta.pk.name, *[meta.pk.name if n == "pk" else n for n in self._only]}
            for n in keep:
                if n not in meta.fields:
                    raise InvalidField(n, self.model)
            return {meta.fields[n].db_field: 1 for n in keep}, keep
        if self._defer:
            drop = {meta.pk.name if n == "pk" else n for n in self._defer}
            for n in drop:
                if n not in meta.fields:
                    raise InvalidField(n, self.model)
            drop.discard(meta.pk.name)
            return {meta.fields[n].db_field: 0 for n in drop}, set(names) - drop
        return None

    def to_pipeline(self) -> Pipeline:
        """The aggregation pipeline equivalent of this queryset (extend it with ``lookup`` & co.)."""
        meta = self.model._meta
        p = Pipeline(self.model)
        if self._where:
            p = p.filter(self._where)
        has_agg = self._values is not None and self._grouped
        scalar_ann = {
            k: v for k, v in self._annotations.items() if not (has_agg and isinstance(wrap(v, literal=True), Aggregate))
        }
        agg_ann = {k: v for k, v in self._annotations.items() if k not in scalar_ann}
        if scalar_ann:
            p = p.add_fields(**{k: wrap(v, literal=True) for k, v in scalar_ann.items()})
        if has_agg:
            plain = [v for v in (self._values or ()) if v not in agg_ann]
            p = p.group(plain or None, **agg_ann)
            # the group key lives in _id.<name>: project it back to the top level
            spec: Dict[str, Any] = {name: "$_id." + name for name in plain}
            spec.update({name: 1 for name in agg_ann})
            spec["_id"] = 0
            fields = {name: p.scope.fields[name] for name in agg_ann}
            for name in plain:
                try:
                    f = resolve_path(self._scope(), name, with_lookup=False).field
                except Exception:
                    f = None
                fields[name] = dynamic_field(name, f)
            p = p._push({"$project": spec}, scope=Scope(fields, None), reset_joins=True)
            if self._having:
                p = p.filter(self._having)
        elif self._having:
            p = p.filter(self._having)
        order = self._effective_order()
        if order:
            p = p.order_by(*order)
        if self._start:
            p = p.skip(self._start)
        lim = self._limit()
        if lim is not None:
            p = p.limit(lim)

        sel_aliases: List[str] = []
        if self._select_related:
            ctx = Context(p.scope, joined=p._joined)
            for j in self._select_joins():
                ctx.add_joins([j])
                sel_aliases.append(j.alias)
            p = p._with_joins(ctx)

        if self._values is not None and not has_agg:
            spec = {}
            fields = {}
            for name in self._values:
                if name in self._annotations:
                    expr = wrap(self._annotations[name], literal=True)
                    spec[name] = expr
                else:
                    spec[name] = F(name)
            if not any(n in ("pk", meta.pk.name) for n in self._values):
                spec["pk"] = 0
            p = p.project(**spec)
        elif not has_agg:
            proj = self._projection()
            if proj is not None and not self._values:
                spec2 = dict(proj[0])
                for a in sel_aliases:
                    if any(v == 1 for k, v in spec2.items() if k != "_id"):
                        spec2[a] = 1
                p = p._push({"$project": spec2}, reset_joins=False)
            leftovers = [a for a in p._joined if a not in sel_aliases] if proj is None or self._defer else []
            if leftovers and self._only is None:
                p = p._push({"$unset": sorted(leftovers)}, reset_joins=False)
        return p.options(
            **{
                k: v
                for k, v in self._options.items()
                if k in {"allow_disk_use", "max_time_ms", "hint", "collation", "comment", "batch_size"}
            }
        )

    def _select_joins(self) -> List[Join]:
        joins: List[Join] = []
        for name in self._select_related:
            model = self.model
            parts = name.split("__")
            alias_parts: List[str] = []
            for i, part in enumerate(parts):
                f = model._meta.fields.get(part)
                if not isinstance(f, ReferenceField):
                    raise InvalidField(name, self.model)
                alias_parts.append(part)
                alias = JOIN_PREFIX + "__".join(alias_parts)
                local_path = f.db_field if i == 0 else f"{JOIN_PREFIX}{'__'.join(alias_parts[:-1])}.{f.db_field}"
                joins.append(Join(alias, f.document_type, local_path))
                model = f.document_type
        return joins

    @property
    def query(self) -> Dict[str, Any]:
        """Debug view of what will be sent to MongoDB."""
        if self._none:
            return {"none": True}
        if self._needs_pipeline():
            return {"aggregate": self.to_pipeline().to_list()}
        return {"find": self._find_spec()}

    def explain(self, verbosity: str = "queryPlanner") -> Dict[str, Any]:
        if self._needs_pipeline():
            return self.to_pipeline().explain(verbosity)
        spec = self._find_spec()
        db = connection.get_database(self.model._meta.db_alias)
        cmd: Dict[str, Any] = {"find": self.model._meta.collection, "filter": spec["filter"]}
        if "sort" in spec:
            cmd["sort"] = dict(spec["sort"])
        return db.command({"explain": cmd, "verbosity": verbosity})

    async def aexplain(self, verbosity: str = "queryPlanner") -> Dict[str, Any]:
        if self._needs_pipeline():
            return await self.to_pipeline().aexplain(verbosity)
        spec = self._find_spec()
        db = connection.get_async_database(self.model._meta.db_alias)
        cmd: Dict[str, Any] = {"find": self.model._meta.collection, "filter": spec["filter"]}
        if "sort" in spec:
            cmd["sort"] = dict(spec["sort"])
        return await db.command({"explain": cmd, "verbosity": verbosity})

    # ------------------------------------------------------------------ #
    # execution helpers
    # ------------------------------------------------------------------ #
    def _find_kwargs(self, spec: Dict[str, Any]) -> Dict[str, Any]:
        kw: Dict[str, Any] = {}
        for k in ("sort", "skip", "limit", "projection"):
            if k in spec:
                kw[k] = spec[k]
        o = self._options
        if "max_time_ms" in o:
            kw["max_time_ms"] = o["max_time_ms"]
        elif settings.default_max_time_ms:
            kw["max_time_ms"] = settings.default_max_time_ms
        for k in ("hint", "collation", "comment", "batch_size"):
            if k in o:
                kw[k] = o[k]
        kw.update(connection.session_kwargs(self.model._meta.db_alias))
        return kw

    def _hydrate_row(self, row: Dict[str, Any], scope: Optional[Scope], loaded: Optional[set]) -> Any:
        if self._values is not None:
            data = convert_row(scope or self._scope(), row)
            if self._values_mode == "dict":
                return data
            vals = [data.get(v if v != "pk" else self.model._meta.pk.name, data.get(v)) for v in self._values]
            if self._values_mode == "flat":
                return vals[0]
            if self._values_mode == "named":
                from collections import namedtuple

                nt = namedtuple("Row", [v.replace("__", "_") for v in self._values])  # type: ignore[misc]
                return nt(*vals)
            return tuple(vals)
        obj = self.model.from_mongo(row, loaded=loaded)
        scope_ = scope
        for name in self._annotations:
            if name in row:
                f = scope_.fields.get(name) if scope_ is not None else None
                value = row[name]
                if f is not None and value is not None:
                    try:
                        value = f.from_mongo(value)
                    except Exception:  # noqa: S110 - keep the raw value if it cannot be converted
                        pass
                setattr(obj, name, value)
        for name in self._select_related:
            _attach_related(obj, self.model, name, row)
        return obj

    def _plan(self):
        """(mode, spec/pipeline, scope, loaded) for execution."""
        if self._needs_pipeline():
            p = self.to_pipeline()
            return "aggregate", p, p.scope, None
        spec = self._find_spec()
        proj = self._projection()
        return "find", spec, self._scope(), (proj[1] if proj else None)

    # ------------------------------------------------------------------ #
    # evaluation (sync)
    # ------------------------------------------------------------------ #
    def _fetch_all(self) -> List[Any]:
        if self._none:
            return []
        mode, plan, scope, loaded = self._plan()
        coll = connection.get_collection(self.model)
        if mode == "find":
            scan_stages(plan["filter"])
            rows = coll.find(plan["filter"], **self._find_kwargs(plan))
        else:
            rows = coll.aggregate(
                plan._final_stages(), **connection.session_kwargs(self.model._meta.db_alias), **plan._aggregate_kwargs()
            )
        result = [self._hydrate_row(r, scope, loaded) for r in rows]
        return self._prefetch_sync(result)

    def _prefetch_sync(self, objs: List[Any]) -> List[Any]:
        for name in self._prefetch_related:
            refs, field = self._prefetch_ids(objs, name)
            if not refs:
                continue
            related = {o.pk: o for o in field.document_type.objects.filter(pk__in=list(refs))}
            self._prefetch_assign(objs, name, related)
        return objs

    def _prefetch_ids(self, objs: List[Any], name: str) -> Tuple[set, ReferenceField]:
        if self._values is not None:
            raise InvalidArgument("prefetch_related() cannot be combined with values().")
        f = self.model._meta.fields.get(name)
        if not isinstance(f, ReferenceField):
            raise InvalidField(name, self.model)
        ids = set()
        for o in objs:
            v = o._data.get(name)
            if v is not None and not hasattr(v, "_meta"):
                ids.add(v)
        return ids, f

    @staticmethod
    def _prefetch_assign(objs: List[Any], name: str, related: Dict[Any, Any]) -> None:
        for o in objs:
            v = o._data.get(name)
            if v is not None and not hasattr(v, "_meta") and v in related:
                o._data[name] = related[v]

    def __iter__(self) -> Iterator[M]:
        if self._cache is None:
            self._cache = self._fetch_all()
        return iter(self._cache)

    def __len__(self) -> int:
        if self._cache is None:
            self._cache = self._fetch_all()
        return len(self._cache)

    def __bool__(self) -> bool:
        return self.exists() if self._cache is None else bool(self._cache)

    def __repr__(self) -> str:
        return f"<QuerySet {self.model.__name__} {self.query}>"

    def list(self) -> List[M]:
        return list(self)

    def iterator(self, chunk_size: int = 1000) -> Iterator[M]:
        """Stream results without caching them."""
        if self._none:
            return
        mode, plan, scope, loaded = self._plan()
        coll = connection.get_collection(self.model)
        if mode == "find":
            kw = self._find_kwargs(plan)
            kw.setdefault("batch_size", chunk_size)
            cursor = coll.find(plan["filter"], **kw)
        else:
            cursor = coll.aggregate(
                plan._final_stages(),
                batchSize=chunk_size,
                **connection.session_kwargs(self.model._meta.db_alias),
                **plan._aggregate_kwargs(),
            )
        try:
            for r in cursor:
                yield self._hydrate_row(r, scope, loaded)
        finally:
            cursor.close()

    def count(self) -> int:
        if self._none:
            return 0
        if self._cache is not None and not self._start and self._stop is None:
            return len(self._cache)
        if self._needs_pipeline():
            rows = self._count_pipeline().execute(raw=True)
            return rows[0]["n"] if rows else 0
        spec = self._find_spec()
        coll = connection.get_collection(self.model)
        kw: Dict[str, Any] = connection.session_kwargs(self.model._meta.db_alias)
        if "skip" in spec:
            kw["skip"] = spec["skip"]
        if "limit" in spec:
            kw["limit"] = spec["limit"]
        mt = self._options.get("max_time_ms", settings.default_max_time_ms)
        if mt:
            kw["maxTimeMS"] = mt
        return coll.count_documents(spec["filter"], **kw)

    def _count_pipeline(self) -> Pipeline:
        c = self._clone()
        c._values = None if not c._grouped else c._values
        c._only, c._defer, c._select_related = None, (), ()
        c._order = ()
        p = c.to_pipeline()
        return p._push({"$count": "n"})

    def exists(self) -> bool:
        if self._none:
            return False
        c = self._clone()
        c._order = ()
        c._start, c._stop = c._start, (c._start + 1)
        c._values, c._only, c._defer = None, ("pk",), ()
        return len(c._fetch_all()) > 0

    def get(self, *args: Any, **kwargs: Any) -> M:
        qs = self.filter(*args, **kwargs) if (args or kwargs) else self
        rows = list(qs._clone()._limit_to(2))
        if not rows:
            raise self.model.DoesNotExist(f"{self.model.__name__} matching query does not exist.")
        if len(rows) > 1:
            raise self.model.MultipleObjectsReturned(f"get() returned more than one {self.model.__name__}.")
        return rows[0]

    def _limit_to(self, n: int) -> QuerySet[M]:
        c = self._clone()
        c._stop = c._start + n if c._stop is None else min(c._stop, c._start + n)
        c._cache = None
        return c

    def _with_default_order(self) -> QuerySet[M]:
        if self.ordered or self._grouped or self._values is not None:
            return self
        return self.order_by("pk")

    def first(self) -> Optional[M]:
        qs = self._with_default_order()
        rows = list(qs._limit_to(1))
        return rows[0] if rows else None

    def last(self) -> Optional[M]:
        qs = self._with_default_order().reverse()
        rows = list(qs._limit_to(1))
        return rows[0] if rows else None

    def earliest(self, field: Optional[str] = None) -> M:
        return self._edge(field, False)

    def latest(self, field: Optional[str] = None) -> M:
        return self._edge(field, True)

    def _edge(self, field: Optional[str], latest: bool) -> M:
        field = field or self.model._meta.get_latest_by
        if not field:
            raise InvalidArgument("earliest()/latest() need a field or Meta.get_latest_by.")
        rows = list(self.order_by(("-" if latest else "") + field)._limit_to(1))
        if not rows:
            raise self.model.DoesNotExist(f"{self.model.__name__} matching query does not exist.")
        return rows[0]

    def in_bulk(self, ids: Iterable[Any], *, field_name: str = "pk") -> Dict[Any, M]:
        objs = list(self.filter(**{f"{field_name}__in": list(ids)}))
        return {_getattr_field(o, field_name): o for o in objs}

    def distinct(self, field: str) -> List[Any]:
        """Distinct values of ``field`` for the current filter."""
        f, path, flt, kw = self._distinct_args(field)
        values = connection.get_collection(self.model).distinct(path, flt, **kw)
        return [f.from_mongo(v) if f is not None and v is not None else v for v in values]

    def _distinct_args(self, field: str):
        ctx = Context(self._scope(), allow_joins=False)
        flt = self._where.resolve(ctx)
        r = resolve_path(ctx.scope, field, with_lookup=False, allow_joins=False)
        kw = connection.session_kwargs(self.model._meta.db_alias)
        mt = self._options.get("max_time_ms", settings.default_max_time_ms)
        if mt:
            kw["maxTimeMS"] = mt
        f = r.field.field if isinstance(r.field, ListField) else r.field
        return f, r.db_path, flt, kw

    def aggregate(self, **expressions: Any) -> Dict[str, Any]:
        """Aggregate over the whole queryset: ``aggregate(total=Sum("amount"), n=Count())``."""
        p = self._aggregate_pipeline(expressions)
        rows = p.execute()
        return self._aggregate_result(expressions, rows)

    def _aggregate_pipeline(self, expressions: Dict[str, Any]) -> Pipeline:
        if not expressions:
            raise InvalidArgument("aggregate() needs at least one expression.")
        c = self._clone()
        c._values, c._only, c._defer, c._select_related, c._order = None, None, (), (), ()
        c._grouped = False
        p = c.to_pipeline()
        return p.group(None, **expressions)

    @staticmethod
    def _aggregate_result(expressions: Dict[str, Any], rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        if rows:
            out = {k: v for k, v in rows[0].items() if k != "_id"}
            return out
        return {k: (0 if isinstance(wrap(v, literal=True), Count) else None) for k, v in expressions.items()}

    # ------------------------------------------------------------------ #
    # creation / writes (sync)
    # ------------------------------------------------------------------ #
    def create(self, **kwargs: Any) -> M:
        obj = self.model(**kwargs)
        obj.save(force_insert=True)
        return obj

    def get_or_create(self, defaults: Optional[Dict[str, Any]] = None, **kwargs: Any) -> Tuple[M, bool]:
        try:
            return self.get(**kwargs), False
        except ObjectDoesNotExist:
            params = {k: v for k, v in kwargs.items() if "__" not in k}
            params.update(defaults or {})
            try:
                return self.create(**params), True
            except NotUniqueError:
                return self.get(**kwargs), False

    def update_or_create(self, defaults: Optional[Dict[str, Any]] = None, **kwargs: Any) -> Tuple[M, bool]:
        obj, created = self.get_or_create(defaults=defaults, **kwargs)
        if not created and defaults:
            for k, v in defaults.items():
                setattr(obj, k, v)
            obj.save()
        return obj, created

    def bulk_create(
        self, objs: Iterable[M], *, batch_size: int = 1000, ordered: bool = True, validate: bool = True
    ) -> List[M]:
        objs = list(objs)
        batches = self._prepare_bulk(objs, batch_size, validate)
        coll = connection.get_collection(self.model)
        kw = connection.session_kwargs(self.model._meta.db_alias)
        try:
            for docs in batches:
                if docs:
                    coll.insert_many(docs, ordered=ordered, **kw)
        except BulkWriteError as exc:
            raise NotUniqueError(str(exc.details.get("writeErrors", [{}])[0].get("errmsg", exc))) from exc
        self._finish_bulk(objs)
        return objs

    def _prepare_bulk(self, objs: List[Any], batch_size: int, validate: bool) -> List[List[Dict[str, Any]]]:
        docs: List[Dict[str, Any]] = []
        for o in objs:
            o._before_save(validate, None)
            docs.append(o.to_mongo())
        return [docs[i : i + batch_size] for i in range(0, len(docs), batch_size)]

    @staticmethod
    def _finish_bulk(objs: List[Any]) -> None:
        for o in objs:
            o._after_save(True)

    def _write_filter(self) -> Dict[str, Any]:
        self._assert_not_sliced("update/delete")
        ctx = Context(self._scope(), allow_joins=False)
        flt = self._where.resolve(ctx)
        scan_stages(flt)
        return flt

    def update(self, *, upsert: bool = False, **changes: Any) -> int:
        """Atomic bulk update; returns the number of modified documents."""
        flt = self._write_filter()
        upd = build_update(self.model, changes)
        coll = connection.get_collection(self.model)
        res = coll.update_many(flt, upd, upsert=upsert, **connection.session_kwargs(self.model._meta.db_alias))
        return res.modified_count + (1 if res.upserted_id is not None else 0)

    def update_one(self, *, upsert: bool = False, **changes: Any) -> int:
        flt = self._write_filter()
        upd = build_update(self.model, changes)
        coll = connection.get_collection(self.model)
        res = coll.update_one(flt, upd, upsert=upsert, **connection.session_kwargs(self.model._meta.db_alias))
        return res.modified_count + (1 if res.upserted_id is not None else 0)

    def delete(self) -> int:
        """Bulk delete (no per-document signals, like Django's queryset delete without signals)."""
        flt = self._write_filter()
        res = connection.get_collection(self.model).delete_many(
            flt, **connection.session_kwargs(self.model._meta.db_alias)
        )
        return res.deleted_count

    # ------------------------------------------------------------------ #
    # async twins
    # ------------------------------------------------------------------ #
    async def _afetch_all(self) -> List[Any]:
        if self._none:
            return []
        mode, plan, scope, loaded = self._plan()
        coll = connection.get_async_collection(self.model)
        if mode == "find":
            scan_stages(plan["filter"])
            cursor = coll.find(plan["filter"], **self._find_kwargs(plan))
            rows = [r async for r in cursor]
        else:
            cursor = await coll.aggregate(
                plan._final_stages(), **connection.session_kwargs(self.model._meta.db_alias), **plan._aggregate_kwargs()
            )
            rows = [r async for r in cursor]
        result = [self._hydrate_row(r, scope, loaded) for r in rows]
        return await self._aprefetch(result)

    async def _aprefetch(self, objs: List[Any]) -> List[Any]:
        for name in self._prefetch_related:
            refs, field = self._prefetch_ids(objs, name)
            if not refs:
                continue
            related = {o.pk: o async for o in field.document_type.objects.filter(pk__in=list(refs))}
            self._prefetch_assign(objs, name, related)
        return objs

    async def alist(self) -> List[M]:
        if self._cache is None:
            self._cache = await self._afetch_all()
        return list(self._cache)

    def __aiter__(self) -> AsyncIterator[M]:
        async def gen() -> AsyncIterator[M]:
            for o in await self.alist():
                yield o

        return gen()

    async def aiterator(self, chunk_size: int = 1000) -> AsyncIterator[M]:
        """Stream results without caching them."""
        if self._none:
            return
        mode, plan, scope, loaded = self._plan()
        coll = connection.get_async_collection(self.model)
        if mode == "find":
            kw = self._find_kwargs(plan)
            kw.setdefault("batch_size", chunk_size)
            cursor = coll.find(plan["filter"], **kw)
        else:
            cursor = await coll.aggregate(
                plan._final_stages(),
                batchSize=chunk_size,
                **connection.session_kwargs(self.model._meta.db_alias),
                **plan._aggregate_kwargs(),
            )
        try:
            async for r in cursor:
                yield self._hydrate_row(r, scope, loaded)
        finally:
            await cursor.close()

    async def acount(self) -> int:
        if self._none:
            return 0
        if self._needs_pipeline():
            rows = await self._count_pipeline().aexecute(raw=True)
            return rows[0]["n"] if rows else 0
        spec = self._find_spec()
        kw: Dict[str, Any] = connection.session_kwargs(self.model._meta.db_alias)
        if "skip" in spec:
            kw["skip"] = spec["skip"]
        if "limit" in spec:
            kw["limit"] = spec["limit"]
        mt = self._options.get("max_time_ms", settings.default_max_time_ms)
        if mt:
            kw["maxTimeMS"] = mt
        return await connection.get_async_collection(self.model).count_documents(spec["filter"], **kw)

    async def aexists(self) -> bool:
        if self._none:
            return False
        c = self._clone()
        c._order = ()
        c._stop = c._start + 1
        c._values, c._only, c._defer = None, ("pk",), ()
        return len(await c._afetch_all()) > 0

    async def aget(self, *args: Any, **kwargs: Any) -> M:
        qs = self.filter(*args, **kwargs) if (args or kwargs) else self
        rows = await qs._limit_to(2).alist()
        if not rows:
            raise self.model.DoesNotExist(f"{self.model.__name__} matching query does not exist.")
        if len(rows) > 1:
            raise self.model.MultipleObjectsReturned(f"get() returned more than one {self.model.__name__}.")
        return rows[0]

    async def afirst(self) -> Optional[M]:
        qs = self._with_default_order()
        rows = await qs._limit_to(1).alist()
        return rows[0] if rows else None

    async def alast(self) -> Optional[M]:
        qs = self._with_default_order().reverse()
        rows = await qs._limit_to(1).alist()
        return rows[0] if rows else None

    async def aearliest(self, field: Optional[str] = None) -> M:
        return await self._aedge(field, False)

    async def alatest(self, field: Optional[str] = None) -> M:
        return await self._aedge(field, True)

    async def _aedge(self, field: Optional[str], latest: bool) -> M:
        field = field or self.model._meta.get_latest_by
        if not field:
            raise InvalidArgument("alatest()/aearliest() need a field or Meta.get_latest_by.")
        rows = await self.order_by(("-" if latest else "") + field)._limit_to(1).alist()
        if not rows:
            raise self.model.DoesNotExist(f"{self.model.__name__} matching query does not exist.")
        return rows[0]

    async def ain_bulk(self, ids: Iterable[Any], *, field_name: str = "pk") -> Dict[Any, M]:
        objs = await self.filter(**{f"{field_name}__in": list(ids)}).alist()
        return {_getattr_field(o, field_name): o for o in objs}

    async def adistinct(self, field: str) -> List[Any]:
        f, path, flt, kw = self._distinct_args(field)
        values = await connection.get_async_collection(self.model).distinct(path, flt, **kw)
        return [f.from_mongo(v) if f is not None and v is not None else v for v in values]

    async def aaggregate(self, **expressions: Any) -> Dict[str, Any]:
        rows = await self._aggregate_pipeline(expressions).aexecute()
        return self._aggregate_result(expressions, rows)

    async def acreate(self, **kwargs: Any) -> M:
        obj = self.model(**kwargs)
        await obj.asave(force_insert=True)
        return obj

    async def aget_or_create(self, defaults: Optional[Dict[str, Any]] = None, **kwargs: Any) -> Tuple[M, bool]:
        try:
            return await self.aget(**kwargs), False
        except ObjectDoesNotExist:
            params = {k: v for k, v in kwargs.items() if "__" not in k}
            params.update(defaults or {})
            try:
                return await self.acreate(**params), True
            except NotUniqueError:
                return await self.aget(**kwargs), False

    async def aupdate_or_create(self, defaults: Optional[Dict[str, Any]] = None, **kwargs: Any) -> Tuple[M, bool]:
        obj, created = await self.aget_or_create(defaults=defaults, **kwargs)
        if not created and defaults:
            for k, v in defaults.items():
                setattr(obj, k, v)
            await obj.asave()
        return obj, created

    async def abulk_create(
        self, objs: Iterable[M], *, batch_size: int = 1000, ordered: bool = True, validate: bool = True
    ) -> List[M]:
        objs = list(objs)
        batches = self._prepare_bulk(objs, batch_size, validate)
        coll = connection.get_async_collection(self.model)
        kw = connection.session_kwargs(self.model._meta.db_alias)
        try:
            for docs in batches:
                if docs:
                    await coll.insert_many(docs, ordered=ordered, **kw)
        except BulkWriteError as exc:
            raise NotUniqueError(str(exc.details.get("writeErrors", [{}])[0].get("errmsg", exc))) from exc
        self._finish_bulk(objs)
        return objs

    async def aupdate(self, *, upsert: bool = False, **changes: Any) -> int:
        flt = self._write_filter()
        upd = build_update(self.model, changes)
        res = await connection.get_async_collection(self.model).update_many(
            flt, upd, upsert=upsert, **connection.session_kwargs(self.model._meta.db_alias)
        )
        return res.modified_count + (1 if res.upserted_id is not None else 0)

    async def aupdate_one(self, *, upsert: bool = False, **changes: Any) -> int:
        flt = self._write_filter()
        upd = build_update(self.model, changes)
        res = await connection.get_async_collection(self.model).update_one(
            flt, upd, upsert=upsert, **connection.session_kwargs(self.model._meta.db_alias)
        )
        return res.modified_count + (1 if res.upserted_id is not None else 0)

    async def adelete(self) -> int:
        flt = self._write_filter()
        res = await connection.get_async_collection(self.model).delete_many(
            flt, **connection.session_kwargs(self.model._meta.db_alias)
        )
        return res.deleted_count


# --------------------------------------------------------------------------- #
def _order_path(o: Any) -> str:
    if isinstance(o, str):
        return o.lstrip("-+")
    if isinstance(o, OrderBy):
        e = o.expression
        if isinstance(e, F):
            return e.name
    if isinstance(o, F):
        return o.name
    raise InvalidArgument(f"Cannot order by {o!r}")


def _is_desc(o: Any) -> bool:
    if isinstance(o, str):
        return o.startswith("-")
    if isinstance(o, OrderBy):
        return o.descending
    return False


def _invert(o: Any) -> Any:
    if isinstance(o, str):
        return o[1:] if o.startswith("-") else "-" + o.lstrip("+")
    if isinstance(o, OrderBy):
        return OrderBy(o.expression, not o.descending)
    if isinstance(o, F):
        return OrderBy(o, True)
    return o


def _getattr_field(obj: Any, name: str) -> Any:
    return obj.pk if name == "pk" else getattr(obj, name)


def _attach_related(obj: Any, model: Any, name: str, row: Dict[str, Any]) -> None:
    parts = name.split("__")
    cur_obj, cur_model = obj, model
    for i, part in enumerate(parts):
        f = cur_model._meta.fields[part]
        alias = JOIN_PREFIX + "__".join(parts[: i + 1])
        raw = row.get(alias)
        if raw is None:
            return
        related = f.document_type.from_mongo(raw)
        cur_obj._data[part] = related
        cur_obj, cur_model = related, f.document_type
