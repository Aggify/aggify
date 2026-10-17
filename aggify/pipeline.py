"""The aggregation pipeline builder (``Aggify``).

Each method returns a **new** pipeline (the 0.x series mutated ``self``, which
made reuse of a partially built pipeline a source of bugs), validates field
names against the evolving :class:`~aggify.schema.Scope`, and can run itself
synchronously or asynchronously::

    top = (
        Aggify(Order)
        .filter(status="paid", created_at__gte=since)
        .group("customer", total=Sum("amount"), orders=Count())
        .order_by("-total")[:10]
    )
    rows = await top.aexecute()
"""

from __future__ import annotations

from typing import Any, AsyncIterator, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Tuple, Union

from aggify import connection
from aggify.conf import settings
from aggify.exceptions import (
    AggifyValueError,
    AlreadyExistsField,
    AnnotationError,
    InvalidArgument,
    InvalidEmbeddedField,
    InvalidProjection,
    MongoIndexError,
    OutStageError,
    PipelineError,
)
from aggify.expressions import (
    AddToSet,
    Aggregate,
    Avg,
    Cond,
    Count,
    Expression,
    F,
    First,
    Last,
    Max,
    MergeObjects,
    Min,
    OrderBy,
    Push,
    Q,
    RawExpression,
    StdDevPop,
    StdDevSamp,
    Sum,
    Var,
    merge_and,
    wrap,
)
from aggify.fields import (
    EmbeddedField,
    Field,
    FloatField,
    IntField,
    ListField,
)
from aggify.schema import JOIN_PREFIX, Context, Join, Scope, dynamic_field, resolve_path
from aggify.security import Raw, scan_stages, validate_field_name

_LEGACY_ACCUMULATORS = {
    "sum": Sum,
    "avg": Avg,
    "min": Min,
    "max": Max,
    "first": First,
    "last": Last,
    "push": Push,
    "addToSet": AddToSet,
    "stdDevPop": StdDevPop,
    "stdDevSamp": StdDevSamp,
    "mergeObjects": MergeObjects,
    "count": Count,
}

_TERMINAL = ("$out", "$merge")


def _stage_name(stage: Mapping[str, Any]) -> str:
    return next(iter(stage))


class Pipeline:
    def __init__(
        self,
        model: Any = None,
        *,
        scope: Optional[Scope] = None,
        stages: Optional[Iterable[Dict[str, Any]]] = None,
        joined: Iterable[str] = (),
        options: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.model = model
        if scope is None:
            scope = Scope.for_model(model) if model is not None else Scope({}, open=True)
        self.scope = scope
        self.stages: List[Dict[str, Any]] = list(stages or [])
        self._joined = frozenset(joined)
        self._options: Dict[str, Any] = dict(options or {})

    # ------------------------------------------------------------------ #
    # plumbing
    # ------------------------------------------------------------------ #
    def _clone(self, **changes: Any) -> Pipeline:
        new = type(self).__new__(type(self))
        new.__dict__.update(self.__dict__)
        new.stages = list(self.stages)
        new._options = dict(self._options)
        for k, v in changes.items():
            setattr(new, k, v)
        return new

    def _check_open(self, stage: str) -> None:
        if self.stages and _stage_name(self.stages[-1]) in _TERMINAL:
            raise OutStageError(stage)

    def _ctx(self, scope: Optional[Scope] = None, allow_joins: bool = True) -> Context:
        return Context(scope or self.scope, allow_joins=allow_joins, joined=self._joined)

    def _join_stages(self, joins: Mapping[str, Join]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for j in joins.values():
            out.append(
                {
                    "$lookup": {
                        "from": j.from_model._meta.collection,
                        "localField": j.local_path,
                        "foreignField": j.from_model._meta.pk.db_field,
                        "as": j.alias,
                    }
                }
            )
            if not j.many:
                out.append({"$unwind": {"path": "$" + j.alias, "preserveNullAndEmptyArrays": True}})
        return out

    def _push(
        self,
        stage: Dict[str, Any],
        *,
        ctx: Optional[Context] = None,
        scope: Optional[Scope] = None,
        reset_joins: bool = False,
    ) -> Pipeline:
        self._check_open(_stage_name(stage))
        new = self._clone()
        joined = set(self._joined)
        if ctx is not None and ctx.joins:
            new.stages.extend(self._join_stages(ctx.joins))
            joined.update(ctx.joins)
        new.stages.append(stage)
        new._joined = frozenset() if reset_joins else frozenset(joined)
        if scope is not None:
            new.scope = scope
        return new

    def _with_joins(self, ctx: Context) -> Pipeline:
        """Apply the joins collected in ``ctx`` as separate stages (before a following stage)."""
        if not ctx.joins:
            return self
        new = self._clone()
        new.stages.extend(self._join_stages(ctx.joins))
        new._joined = frozenset(set(self._joined) | set(ctx.joins))
        return new

    # ------------------------------------------------------------------ #
    # introspection / legacy protocol
    # ------------------------------------------------------------------ #
    def __iter__(self) -> Iterator[Dict[str, Any]]:
        return iter(self.stages)

    def __len__(self) -> int:
        return len(self.stages)

    def __bool__(self) -> bool:
        return True

    def to_list(self) -> List[Dict[str, Any]]:
        """Plain list of stages, ready for ``collection.aggregate``."""
        return [dict(s) for s in self.stages]

    @property
    def pipelines(self) -> List[Dict[str, Any]]:
        return self.stages

    def __repr__(self) -> str:
        name = getattr(self.model, "__name__", None)
        return f"<Aggify {name or ''} {len(self.stages)} stages: {[_stage_name(s) for s in self.stages]}>"

    # ------------------------------------------------------------------ #
    # $match
    # ------------------------------------------------------------------ #
    def filter(self, *args: Any, **kwargs: Any) -> Pipeline:
        """``$match``.  Accepts ``Q`` objects and ``field__lookup=value`` keywords."""
        q = Q(*args, **kwargs)
        if not q:
            return self
        ctx = self._ctx()
        doc = q.resolve(ctx)
        if not doc:
            return self._with_joins(ctx)
        base = self._with_joins(ctx)
        base._check_open("$match")
        # merge into a directly preceding $match
        if base.stages and _stage_name(base.stages[-1]) == "$match":
            new = base._clone()
            new.stages[-1] = {"$match": merge_and([base.stages[-1]["$match"], doc])}
            return new
        new = base._clone()
        new.stages.append({"$match": doc})
        return new

    match = filter

    def exclude(self, *args: Any, **kwargs: Any) -> Pipeline:
        q = Q(*args, **kwargs)
        return self.filter(~q) if q else self

    # ------------------------------------------------------------------ #
    # $sort / $skip / $limit
    # ------------------------------------------------------------------ #
    def order_by(self, *fields: Union[str, Expression]) -> Pipeline:
        """``$sort``: ``order_by("-created_at", "name")``. ``F("x").desc()`` also works."""
        if not fields:
            return self
        ctx = self._ctx()
        spec: Dict[str, int] = {}
        for item in fields:
            desc = False
            if isinstance(item, str):
                desc = item.startswith("-")
                path = item.lstrip("-+")
            elif isinstance(item, OrderBy):
                desc, item = item.descending, item.expression
                path = item.name if isinstance(item, F) else None
                if path is None:
                    raise InvalidArgument("Only F('field') can be ordered on; annotate() it first.")
            elif isinstance(item, F):
                path = item.name
            else:
                raise InvalidArgument(f"Cannot order by {item!r}")
            r = resolve_path(self.scope, path, with_lookup=False)
            ctx.add_joins(r.joins)
            spec[r.db_path] = -1 if desc else 1
        base = self._with_joins(ctx)
        if base.stages and _stage_name(base.stages[-1]) == "$sort":
            new = base._clone()
            new.stages[-1] = {"$sort": spec}
            return new
        return base._push({"$sort": spec})

    sort = order_by

    def skip(self, n: int) -> Pipeline:
        if isinstance(n, bool) or not isinstance(n, int) or n < 0:
            raise MongoIndexError("skip() takes a non-negative int.")
        return self._push({"$skip": n}) if n else self

    def limit(self, n: int) -> Pipeline:
        if isinstance(n, bool) or not isinstance(n, int) or n < 0:
            raise MongoIndexError("limit() takes a non-negative int.")
        if settings.max_limit is not None and n > settings.max_limit:
            raise InvalidArgument(f"limit {n} exceeds the configured max_limit ({settings.max_limit}).")
        if n == 0:
            raise InvalidArgument("limit(0) is not allowed (MongoDB treats it as 'no limit').")
        return self._push({"$limit": n})

    def __getitem__(self, index: Union[int, slice]) -> Pipeline:
        if isinstance(index, bool) or not isinstance(index, (int, slice)):
            raise AggifyValueError([int, slice], type(index))
        if isinstance(index, int):
            if index < 0:
                raise MongoIndexError()
            return self.skip(index).limit(1)
        if index.step not in (None, 1):
            raise MongoIndexError()
        start = 0 if index.start is None else index.start
        if start < 0 or (index.stop is not None and (index.stop < 0 or index.stop < start)):
            raise MongoIndexError()
        new = self.skip(start)
        if index.stop is not None:
            if index.stop - start == 0:
                raise InvalidArgument("Empty slice.")
            new = new.limit(index.stop - start)
        return new

    # ------------------------------------------------------------------ #
    # $project / $addFields / $set / $unset
    # ------------------------------------------------------------------ #
    def project(self, **spec: Any) -> Pipeline:
        """``$project``: ``project(name=1, secret=0)`` or computed ``project(full=Concat("a", Value(" "), "b"))``.

        Inclusion and exclusion cannot be mixed (``id`` / ``_id`` may be excluded in either mode).
        """
        if not spec:
            raise InvalidProjection("project() needs at least one field.")
        ctx = self._ctx()
        out: Dict[str, Any] = {}
        include: List[str] = []
        exclude: List[str] = []
        computed: Dict[str, Field] = {}
        pk_name = self.scope.model._meta.pk.name if self.scope.model is not None else "id"
        pk_flag: Optional[int] = None
        for key, value in spec.items():
            if isinstance(value, bool):
                value = int(value)
            if not isinstance(value, (Expression, Q, Raw)) and value in (0, 1):
                if key in (pk_name, "pk", "_id"):
                    pk_flag = value
                    out["_id"] = value
                    continue
                r = resolve_path(self.scope, key, with_lookup=False)
                ctx.add_joins(r.joins)
                out[r.db_path] = value
                (include if value == 1 else exclude).append(key.split("__")[0])
            else:
                expr = wrap(value, literal=True)
                validate_field_name(key)
                out[key] = expr.resolve(ctx)
                include.append(key)
                computed[key] = dynamic_field(key, expr.output_field(ctx))
        if include and exclude:
            raise InvalidProjection()
        if include:
            fields = {k: v for k, v in self.scope.fields.items() if k in include}
            fields.update(computed)
            if pk_flag != 0 and pk_name in self.scope.fields:
                fields = {pk_name: self.scope.fields[pk_name], **fields}
            new_scope = Scope(fields, self.scope.model, open=False)
        else:
            new_scope = self.scope.evolve(drop=[k for k in exclude] + ([pk_name] if pk_flag == 0 else []))
        return self._push({"$project": out}, ctx=ctx, scope=new_scope, reset_joins=True)

    def only(self, *names: str) -> Pipeline:
        return self.project(**{n: 1 for n in names})

    def defer(self, *names: str) -> Pipeline:
        return self.project(**{n: 0 for n in names})

    def add_fields(self, **fields: Any) -> Pipeline:
        """``$addFields``.  Strings are literals, ``F()`` references fields."""
        if not fields:
            return self
        ctx = self._ctx()
        stage: Dict[str, Any] = {}
        add: Dict[str, Field] = {}
        for name, value in fields.items():
            path = name.replace("__", ".")
            validate_field_name(path, allow_dots=True)
            expr = wrap(value, literal=True)
            stage[path] = expr.resolve(ctx)
            if "." not in path:
                add[name] = dynamic_field(name, expr.output_field(ctx))
        return self._push({"$addFields": stage}, ctx=ctx, scope=self.scope.evolve(add))

    set = add_fields

    def unset(self, *names: str) -> Pipeline:
        paths = []
        for n in names:
            r = resolve_path(self.scope, n, with_lookup=False, allow_joins=False)
            paths.append(r.db_path)
        return self._push({"$unset": paths}, scope=self.scope.evolve(drop=[n for n in names if "__" not in n]))

    # ------------------------------------------------------------------ #
    # $group + annotate
    # ------------------------------------------------------------------ #
    def group(self, by: Any = None, **accumulators: Any) -> Pipeline:
        """``$group``.

        ``by`` may be ``None`` (one bucket), a field name, a list of names,
        a ``{alias: expression}`` dict or an expression.  Keyword arguments are accumulators::

            .group(["status", "country"], n=Count(), revenue=Sum("amount"))
        """
        ctx = self._ctx()
        id_field: Optional[Field] = None
        if by is None:
            key: Any = None
        elif isinstance(by, str):
            e = F(by)
            key = e.resolve(ctx)
            id_field = e.output_field(ctx)
        elif isinstance(by, (list, tuple)):
            key = {}
            for name in by:
                e = F(name)
                key[name] = e.resolve(ctx)
        elif isinstance(by, dict):
            key = {k: wrap(v, literal=True).resolve(ctx) for k, v in by.items()}
        elif isinstance(by, Expression):
            key = by.resolve(ctx)
            id_field = by.output_field(ctx)
        else:
            raise InvalidArgument(f"Cannot group by {by!r}")
        stage: Dict[str, Any] = {"_id": key}
        fields: Dict[str, Field] = {}
        for name, expr in accumulators.items():
            validate_field_name(name)
            if name == "_id":
                raise InvalidArgument("'_id' is the group key.")
            e = wrap(expr, literal=True)
            if not isinstance(e, (Aggregate, RawExpression)):
                raise AnnotationError(f"{name}= must be an aggregate such as Sum(...), got {expr!r}")
            stage[name] = e.resolve(ctx)
            fields[name] = dynamic_field(name, e.output_field(ctx))
        id_scope_field = dynamic_field("_id", id_field)
        fields["_id"] = id_scope_field
        new = self._push({"$group": stage}, ctx=ctx, scope=Scope(fields, None, open=False), reset_joins=True)
        new._prior_scope = self.scope
        return new

    def annotate(self, *args: Any, **expressions: Any) -> Pipeline:
        """Add computed fields.

        * right after ``group()`` aggregates are merged into that ``$group``
        * otherwise it is ``$addFields``
        """
        # Aggify 0.x signature: annotate(annotate_name="x", accumulator="sum", f="field")
        if "annotate_name" in expressions or "accumulator" in expressions:
            name = expressions.pop("annotate_name", args[0] if args else None)
            acc = expressions.pop("accumulator", args[1] if len(args) > 1 else None)
            f = expressions.pop("f", args[2] if len(args) > 2 else None)
            if name is None or acc not in _LEGACY_ACCUMULATORS:
                raise AnnotationError(f"Invalid accumulator: {acc}")
            expressions[name] = _LEGACY_ACCUMULATORS[acc](f) if f is not None else _LEGACY_ACCUMULATORS[acc]()
        elif args:
            raise AnnotationError("annotate() takes keyword arguments: annotate(total=Sum('amount')).")
        if not expressions:
            return self
        last = _stage_name(self.stages[-1]) if self.stages else None
        if last == "$group" and all(isinstance(wrap(e, literal=True), Aggregate) for e in expressions.values()):
            self._check_open("$group")
            group_stage = dict(self.stages[-1]["$group"])
            ctx = self._ctx(self._prior_scope or self.scope, allow_joins=False)
            fields: Dict[str, Field] = {}
            for name, expr in expressions.items():
                validate_field_name(name)
                e = wrap(expr, literal=True)
                group_stage[name] = e.resolve(ctx)
                fields[name] = dynamic_field(name, e.output_field(ctx))
            new = self._clone()
            new.stages[-1] = {"$group": group_stage}
            new.scope = self.scope.evolve(fields)
            return new
        if last is None and any(isinstance(wrap(e, literal=True), Aggregate) for e in expressions.values()):
            raise AnnotationError("Annotations with aggregates apply to $group; call .group() first.")
        return self.add_fields(**expressions)

    #: scope in effect *before* the last ``$group`` (annotate() resolves accumulators against it)
    _prior_scope: Optional[Scope] = None

    # ------------------------------------------------------------------ #
    # arrays / joins
    # ------------------------------------------------------------------ #
    def unwind(self, path: str, preserve: bool = False, include_array_index: Optional[str] = None) -> Pipeline:
        """``$unwind``."""
        ctx = self._ctx()
        r = resolve_path(self.scope, path.lstrip("$"), with_lookup=False)
        ctx.add_joins(r.joins)
        dollar = "$" + r.db_path
        if not preserve and include_array_index is None:
            stage: Dict[str, Any] = {"$unwind": dollar}
        else:
            spec: Dict[str, Any] = {"path": dollar}
            if include_array_index:
                spec["includeArrayIndex"] = validate_field_name(include_array_index.lstrip("$"))
            if preserve:
                spec["preserveNullAndEmptyArrays"] = True
            stage = {"$unwind": spec}
        new_fields: Dict[str, Field] = {}
        top = path.split("__")[0]
        f = self.scope.fields.get(top)
        if "__" not in path and isinstance(f, ListField):
            inner = f.field.clone()
            inner.name, inner.db_field = f.name, f.db_field
            new_fields[top] = inner
        if include_array_index:
            new_fields[include_array_index.lstrip("$")] = dynamic_field(include_array_index.lstrip("$"), IntField())
        return self._push(stage, ctx=ctx, scope=self.scope.evolve(new_fields))

    def lookup(
        self,
        from_: Any,
        as_name: str,
        *,
        local_field: Optional[str] = None,
        foreign_field: Optional[str] = None,
        let: Optional[Mapping[str, Any]] = None,
        query: Optional[Q] = None,
        pipeline: Union[Pipeline, Sequence[Dict[str, Any]], None] = None,
        unwind: bool = False,
        preserve: bool = True,
    ) -> Pipeline:
        """``$lookup`` - both the equality form and the ``let``/``pipeline`` form.

        >>> Aggify(User).lookup(Order, "orders", local_field="id", foreign_field="user")
        >>> Aggify(User).lookup(Order, "paid", let={"uid": F("id")},
        ...                     query=Q(user=Var("uid"), status="paid"))
        """
        validate_field_name(as_name)
        if as_name in self.scope.fields:
            raise AlreadyExistsField(as_name)
        target_scope = Scope.for_model(from_)
        ctx = self._ctx()
        spec: Dict[str, Any] = {"from": from_._meta.collection}
        if let is None and pipeline is None and query is None:
            if not (local_field and foreign_field):
                raise InvalidArgument("lookup() needs local_field & foreign_field, or let/query/pipeline.")
            lr = resolve_path(self.scope, local_field, with_lookup=False)
            ctx.add_joins(lr.joins)
            fr = resolve_path(target_scope, foreign_field, with_lookup=False, allow_joins=False)
            spec.update(localField=lr.db_path, foreignField=fr.db_path)
        else:
            spec["let"] = {k: wrap(v).resolve(ctx) for k, v in (let or {}).items()}
            sub: List[Dict[str, Any]] = []
            if query is not None:
                sub_ctx = Context(target_scope, allow_joins=False)
                doc = query.resolve(sub_ctx)
                if doc:
                    sub.append({"$match": doc})
            if isinstance(pipeline, Pipeline):
                sub.extend(pipeline.to_list())
            elif pipeline is not None:
                scan_stages(list(pipeline))
                sub.extend(pipeline)
            spec["pipeline"] = sub
        spec["as"] = as_name
        joined = EmbeddedField(from_)
        field_obj: Field = ListField(joined)
        new_scope = self.scope.evolve({as_name: dynamic_field(as_name, field_obj)})
        new = self._push({"$lookup": spec}, ctx=ctx, scope=new_scope)
        if unwind:
            single = dynamic_field(as_name, joined)
            new = new.unwind(as_name, preserve=preserve)
            new.scope = new.scope.evolve({as_name: single})
        return new

    def graph_lookup(
        self,
        from_: Any,
        start_with: Any,
        connect_from: str,
        connect_to: str,
        as_name: str,
        *,
        max_depth: Optional[int] = None,
        depth_field: Optional[str] = None,
        restrict: Optional[Q] = None,
    ) -> Pipeline:
        ctx = self._ctx()
        target_scope = Scope.for_model(from_)
        spec: Dict[str, Any] = {
            "from": from_._meta.collection,
            "startWith": wrap(start_with).resolve(ctx),
            "connectFromField": resolve_path(target_scope, connect_from, with_lookup=False, allow_joins=False).db_path,
            "connectToField": resolve_path(target_scope, connect_to, with_lookup=False, allow_joins=False).db_path,
            "as": validate_field_name(as_name),
        }
        if max_depth is not None:
            spec["maxDepth"] = int(max_depth)
        if depth_field:
            spec["depthField"] = validate_field_name(depth_field)
        if restrict is not None:
            spec["restrictSearchWithMatch"] = restrict.resolve(Context(target_scope, allow_joins=False))
        scope = self.scope.evolve({as_name: dynamic_field(as_name, ListField(EmbeddedField(from_)))})
        return self._push({"$graphLookup": spec}, ctx=ctx, scope=scope)

    def union_with(self, other: Any, pipeline: Union[Pipeline, Sequence[Dict[str, Any]], None] = None) -> Pipeline:
        coll = other._meta.collection if hasattr(other, "_meta") else str(other)
        spec: Dict[str, Any] = {"coll": coll}
        if pipeline is not None:
            spec["pipeline"] = pipeline.to_list() if isinstance(pipeline, Pipeline) else list(pipeline)
            scan_stages(spec["pipeline"])
        return self._push({"$unionWith": spec})

    # ------------------------------------------------------------------ #
    # other stages
    # ------------------------------------------------------------------ #
    def facet(self, **facets: Union[Pipeline, Sequence[Dict[str, Any]]]) -> Pipeline:
        spec = {}
        for name, p in facets.items():
            validate_field_name(name)
            spec[name] = p.to_list() if isinstance(p, Pipeline) else list(p)
        scan_stages(spec)
        fields = {n: dynamic_field(n, ListField()) for n in spec}
        return self._push({"$facet": spec}, scope=Scope(fields, None), reset_joins=True)

    def bucket(self, by: Any, boundaries: Sequence[Any], default: Any = None, **output: Any) -> Pipeline:
        ctx = self._ctx()
        spec: Dict[str, Any] = {"groupBy": wrap(by).resolve(ctx), "boundaries": list(boundaries)}
        if default is not None:
            spec["default"] = default
        fields: Dict[str, Field] = {"_id": dynamic_field("_id")}
        if output:
            spec["output"] = {}
            for k, v in output.items():
                e = wrap(v, literal=True)
                spec["output"][k] = e.resolve(ctx)
                fields[k] = dynamic_field(k, e.output_field(ctx))
        return self._push({"$bucket": spec}, ctx=ctx, scope=Scope(fields, None), reset_joins=True)

    def bucket_auto(self, by: Any, buckets: int, granularity: Optional[str] = None, **output: Any) -> Pipeline:
        ctx = self._ctx()
        spec: Dict[str, Any] = {"groupBy": wrap(by).resolve(ctx), "buckets": int(buckets)}
        if granularity:
            spec["granularity"] = granularity
        fields: Dict[str, Field] = {"_id": dynamic_field("_id")}
        if output:
            spec["output"] = {}
            for k, v in output.items():
                e = wrap(v, literal=True)
                spec["output"][k] = e.resolve(ctx)
                fields[k] = dynamic_field(k, e.output_field(ctx))
        return self._push({"$bucketAuto": spec}, ctx=ctx, scope=Scope(fields, None), reset_joins=True)

    def count(self, name: str = "count") -> Pipeline:
        """``$count`` stage (named ``count`` as in Mongo; ``QuerySet.count()`` runs it)."""
        validate_field_name(name)
        return self._push(
            {"$count": name}, scope=Scope({name: dynamic_field(name, IntField())}, None), reset_joins=True
        )

    def sample(self, size: int) -> Pipeline:
        return self._push({"$sample": {"size": int(size)}})

    def sort_by_count(self, by: Any) -> Pipeline:
        ctx = self._ctx()
        scope = Scope({"_id": dynamic_field("_id"), "count": dynamic_field("count", IntField())}, None)
        return self._push({"$sortByCount": wrap(by).resolve(ctx)}, ctx=ctx, scope=scope, reset_joins=True)

    def _root_scope(self, expr: Any) -> Tuple[Any, Scope, Context]:
        ctx = self._ctx()
        scope = Scope({}, None, open=True)
        if isinstance(expr, str):
            r = resolve_path(self.scope, expr, with_lookup=False)
            ctx.add_joins(r.joins)
            f = r.field
            if isinstance(f, ListField):
                f = f.field
            scope = Scope.for_model(f.document_type)  # _replace() has verified it is embedded
            return "$" + r.db_path, scope, ctx
        return wrap(expr).resolve(ctx), scope, ctx

    def replace_root(
        self, new_root: Any = None, *, embedded_field: Optional[str] = None, merge: Optional[Mapping[str, Any]] = None
    ) -> Pipeline:
        """``$replaceRoot``.  ``replace_root("profile")`` / ``replace_root(F(...))`` /
        ``replace_root("profile", merge={"x": 0})`` (defaults merged under the new root)."""
        return self._replace("$replaceRoot", new_root if new_root is not None else embedded_field, merge)

    def replace_with(
        self, new_root: Any = None, *, embedded_field: Optional[str] = None, merge: Optional[Mapping[str, Any]] = None
    ) -> Pipeline:
        return self._replace("$replaceWith", new_root if new_root is not None else embedded_field, merge)

    def _replace(self, op: str, target: Any, merge: Optional[Mapping[str, Any]]) -> Pipeline:
        if target is None:
            raise InvalidArgument(f"{op} needs a field or expression.")
        if isinstance(target, str):
            r = resolve_path(self.scope, target, with_lookup=False)
            f = r.field
            if not isinstance(f, EmbeddedField) and not (
                isinstance(f, ListField) and isinstance(f.field, EmbeddedField)
            ):
                raise InvalidEmbeddedField(target)
        expr, scope, ctx = self._root_scope(target)
        new_scope = scope
        if merge:
            expr = {"$mergeObjects": [{k: wrap(v, literal=True).resolve(ctx) for k, v in merge.items()}, expr]}
            new_scope = scope.evolve(
                {k: dynamic_field(k, wrap(v, literal=True).output_field(ctx)) for k, v in merge.items()}
            )
        stage = {op: {"newRoot": expr}} if op == "$replaceRoot" else {op: expr}
        return self._push(stage, ctx=ctx, scope=new_scope, reset_joins=True)

    def redact(self, condition: Any, then: Any = "keep", otherwise: Any = "prune", **legacy: Any) -> Pipeline:
        """``$redact`` with ``$cond``: ``redact(Q(level__lte=2), "descend", "prune")``."""
        if legacy:  # 0.x: redact(value1=, condition=, value2=, then_value=, else_value=)
            raise InvalidArgument(
                "The 0.x redact(value1, condition, value2, ...) signature was replaced by redact(Q|expression, then, otherwise)."
            )
        names = {"DESCEND", "PRUNE", "KEEP"}

        def norm(v: Any) -> Var:
            n = str(v).replace("$", "").upper()
            if n not in names:
                raise InvalidArgument(
                    f"redact outcome must be one of {sorted(names)}.",
                )
            return Var(n)

        ctx = self._ctx()
        stage = {"$redact": Cond(condition, norm(then), norm(otherwise)).resolve(ctx)}
        return self._push(stage, ctx=ctx)

    def set_window_fields(
        self, *, partition_by: Any = None, sort_by: Optional[Mapping[str, int]] = None, **output: Any
    ) -> Pipeline:
        """``$setWindowFields``:

        >>> .set_window_fields(partition_by="user", sort_by={"created_at": 1},
        ...                    running=Sum("amount", window={"documents": ["unbounded", "current"]}),
        ...                    rank=Rank())
        """
        ctx = self._ctx()
        spec: Dict[str, Any] = {}
        if partition_by is not None:
            spec["partitionBy"] = wrap(partition_by).resolve(ctx)
        if sort_by:
            spec["sortBy"] = {resolve_path(self.scope, k, with_lookup=False).db_path: v for k, v in sort_by.items()}
        spec["output"] = {}
        fields: Dict[str, Field] = {}
        for name, expr in output.items():
            validate_field_name(name)
            e = wrap(expr, literal=True)
            body = e.resolve(ctx)
            window = getattr(e, "window", None)
            if window is not None and isinstance(body, dict) and "window" in body:
                body = dict(body)
                body.pop("window")
                body["window"] = window
            spec["output"][name] = body
            fields[name] = dynamic_field(name, e.output_field(ctx))
        return self._push({"$setWindowFields": spec}, ctx=ctx, scope=self.scope.evolve(fields))

    def geo_near(
        self,
        near: Tuple[float, float],
        distance_field: str = "distance",
        *,
        spherical: bool = True,
        max_distance: Optional[float] = None,
        key: Optional[str] = None,
        query: Optional[Q] = None,
    ) -> Pipeline:
        if self.stages:
            raise PipelineError("$geoNear must be the first stage.")
        spec: Dict[str, Any] = {
            "near": {"type": "Point", "coordinates": [float(near[0]), float(near[1])]},
            "distanceField": validate_field_name(distance_field),
            "spherical": spherical,
        }
        if max_distance is not None:
            spec["maxDistance"] = max_distance
        if key:
            spec["key"] = resolve_path(self.scope, key, with_lookup=False, allow_joins=False).db_path
        if query is not None:
            spec["query"] = query.resolve(Context(self.scope, allow_joins=False))
        return self._push(
            {"$geoNear": spec}, scope=self.scope.evolve({distance_field: dynamic_field(distance_field, FloatField())})
        )

    def out(self, coll: str, db: Optional[str] = None) -> Pipeline:
        """``$out`` - writes the result to a collection (replacing it). Must be the last stage."""
        validate_field_name(coll)
        stage = {"$out": coll} if db is None else {"$out": {"db": validate_field_name(db), "coll": coll}}
        return self._push(stage)

    def merge(
        self,
        into: Any,
        *,
        on: Union[str, Sequence[str]] = "_id",
        when_matched: str = "merge",
        when_not_matched: str = "insert",
    ) -> Pipeline:
        coll = into._meta.collection if hasattr(into, "_meta") else str(into)
        return self._push(
            {"$merge": {"into": coll, "on": on, "whenMatched": when_matched, "whenNotMatched": when_not_matched}}
        )

    def stage(self, name: str, spec: Any, *, allow_javascript: Optional[bool] = None) -> Pipeline:
        """Append any stage verbatim (Atlas ``$search``, ``$vectorSearch``, ``$densify`` ...).

        The stage is scanned for server-side JavaScript; the scope becomes *open*
        (unknown fields are accepted afterwards) because Aggify cannot know the output shape.
        """
        if not name.startswith("$"):
            name = "$" + name
        scan_stages(spec, allow_javascript=allow_javascript)
        return self._push({name: spec}, scope=self.scope.evolve(open=True), reset_joins=False)

    def raw(self, stage: Mapping[str, Any], *, allow_javascript: Optional[bool] = None) -> Pipeline:
        """Append a complete raw stage document (``{"$limit": 3}``)."""
        if not isinstance(stage, Mapping) or len(stage) != 1:
            raise InvalidArgument("A pipeline stage is a mapping with exactly one $stage key.")
        ((name, spec),) = stage.items()
        return self.stage(name, spec, allow_javascript=allow_javascript)

    # ------------------------------------------------------------------ #
    # execution
    # ------------------------------------------------------------------ #
    def options(self, **opts: Any) -> Pipeline:
        """Per-query options: ``allow_disk_use``, ``max_time_ms``, ``hint``, ``collation``, ``comment``, ``batch_size``."""
        allowed = {"allow_disk_use", "max_time_ms", "hint", "collation", "comment", "batch_size", "let"}
        bad = set(opts) - allowed
        if bad:
            raise InvalidArgument(f"Unknown aggregate option(s): {sorted(bad)}")
        new = self._clone()
        new._options.update(opts)
        return new

    #: option names -> the spelling of the ``aggregate`` command (pymongo forwards unknown kwargs verbatim)
    _COMMAND_OPTIONS = {"allow_disk_use": "allowDiskUse", "max_time_ms": "maxTimeMS", "batch_size": "batchSize"}

    def _aggregate_kwargs(self, **extra: Any) -> Dict[str, Any]:
        kw = dict(self._options)
        if "max_time_ms" not in kw and settings.default_max_time_ms:
            kw["max_time_ms"] = settings.default_max_time_ms
        kw.update(extra)
        return {self._COMMAND_OPTIONS.get(k, k): v for k, v in kw.items()}

    def _alias(self) -> str:
        return self.model._meta.db_alias if self.model is not None else connection.DEFAULT_ALIAS

    def _final_stages(self) -> List[Dict[str, Any]]:
        stages = self.to_list()
        scan_stages(stages)
        return stages

    def execute(self, *, raw: bool = False) -> List[Dict[str, Any]]:
        """Run on the synchronous driver and return a list of dicts (python field names)."""
        coll = connection.get_collection(self.model)
        cursor = coll.aggregate(
            self._final_stages(), **connection.session_kwargs(self._alias()), **self._aggregate_kwargs()
        )
        return [row if raw else convert_row(self.scope, row) for row in cursor]

    async def aexecute(self, *, raw: bool = False) -> List[Dict[str, Any]]:
        coll = connection.get_async_collection(self.model)
        cursor = await coll.aggregate(
            self._final_stages(), **connection.session_kwargs(self._alias()), **self._aggregate_kwargs()
        )
        return [row if raw else convert_row(self.scope, row) async for row in cursor]

    def iterate(self, *, raw: bool = False) -> Iterator[Dict[str, Any]]:
        coll = connection.get_collection(self.model)
        cursor = coll.aggregate(
            self._final_stages(), **connection.session_kwargs(self._alias()), **self._aggregate_kwargs()
        )
        try:
            for row in cursor:
                yield row if raw else convert_row(self.scope, row)
        finally:
            cursor.close()

    async def aiterate(self, *, raw: bool = False) -> AsyncIterator[Dict[str, Any]]:
        coll = connection.get_async_collection(self.model)
        cursor = await coll.aggregate(
            self._final_stages(), **connection.session_kwargs(self._alias()), **self._aggregate_kwargs()
        )
        try:
            async for row in cursor:
                yield row if raw else convert_row(self.scope, row)
        finally:
            await cursor.close()

    def explain(self, verbosity: str = "queryPlanner") -> Dict[str, Any]:
        db = connection.get_database(self._alias())
        return db.command(
            {
                "explain": {"aggregate": self.model._meta.collection, "pipeline": self._final_stages(), "cursor": {}},
                "verbosity": verbosity,
            }
        )

    async def aexplain(self, verbosity: str = "queryPlanner") -> Dict[str, Any]:
        db = connection.get_async_database(self._alias())
        return await db.command(
            {
                "explain": {"aggregate": self.model._meta.collection, "pipeline": self._final_stages(), "cursor": {}},
                "verbosity": verbosity,
            }
        )


Aggify = Pipeline


def convert_row(scope: Scope, row: Mapping[str, Any]) -> Dict[str, Any]:
    """Raw BSON row -> dict with Python field names and converted values."""
    by_db = {}
    for f in scope.fields.values():
        by_db.setdefault(f.db_field, f)
    out: Dict[str, Any] = {}
    for key, value in row.items():
        if key.startswith(JOIN_PREFIX):
            continue
        f = by_db.get(key)
        if f is None:
            out[key] = value
            continue
        out[f.name or key] = _convert(f, value)
    return out


def _convert(f: Field, value: Any) -> Any:
    """Convert one raw value; embedded documents keep exactly the keys that were present."""
    if value is None:
        return None
    try:
        if isinstance(f, ListField) and isinstance(value, list):
            return [_convert(f.field, v) for v in value]
        if isinstance(f, EmbeddedField) and isinstance(value, dict):
            return convert_row(Scope.for_model(f.document_type), value)
        return f.from_mongo(value)
    except Exception:
        return value
