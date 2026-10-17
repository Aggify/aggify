# Aggregation pipelines

The `Aggify` pipeline builder (alias `Pipeline`) is the layer under `QuerySet`. Use it when you need stages a
queryset does not expose (`$facet`, `$bucket`, `$setWindowFields`, `$graphLookup`, `$unionWith`, `$out`, ...) or when you
want full control of stage order.

```python
from aggify import Aggify, Sum, Count, F, Q

top = (
    Aggify(Order)
    .filter(status="paid", created_at__gte=since)
    .group("customer", total=Sum("amount"), orders=Count())
    .order_by("-total")[:10]
)
top.to_list()          # the plain list of stages
rows = await top.aexecute()      # or top.execute()
```

* **Immutable** - every method returns a new pipeline; keep a base and branch from it.
* **Schema aware** - Aggify tracks which fields exist after each stage (`project` narrows, `group` replaces,
  `add_fields`/`lookup` extend, ...), so typos raise `InvalidField` at build time and values are converted back to
  Python types when results are read.
* **Safe** - values go through field validation; stages passed with `raw()`/`stage()` are scanned for JavaScript.
* A pipeline can come from a queryset: `Order.objects.filter(...).to_pipeline()`.

## Stage reference

| method | stage | notes |
|---|---|---|
| `filter(*Q, **lookups)` / `match` / `exclude` | `$match` | joins are added automatically; adjacent matches merge |
| `order_by(...)` / `sort` | `$sort` | `"-field"`, `F("x").desc()` |
| `skip(n)`, `limit(n)`, `[a:b]` | `$skip`, `$limit` | |
| `project(**spec)`, `only()`, `defer()` | `$project` | `1`/`0` or expressions; mixing include/exclude raises |
| `add_fields(**expr)` / `set` | `$addFields` | strings are literals |
| `annotate(**expr)` | `$group` accumulators or `$addFields` | after `group()` aggregates are merged into it |
| `group(by, **accumulators)` | `$group` | `by`: `None`, `"field"`, `["a", "b"]`, `{name: expr}` |
| `unwind(path, preserve=, include_array_index=)` | `$unwind` | |
| `lookup(Model, as_name, ...)` | `$lookup` | equality form or `let`/`query`/`pipeline` form; `unwind=True` |
| `graph_lookup(...)` | `$graphLookup` | |
| `facet(**pipelines)` | `$facet` | values are pipelines or stage lists |
| `bucket(by, boundaries, default, **output)`, `bucket_auto(...)` | `$bucket`, `$bucketAuto` | |
| `count(name)`, `sample(n)`, `sort_by_count(expr)` | `$count`, `$sample`, `$sortByCount` | |
| `replace_root(field)` / `replace_with(...)` | `$replaceRoot` / `$replaceWith` | `merge={...}` supplies defaults |
| `redact(cond, then, otherwise)` | `$redact` | `"descend"`, `"prune"`, `"keep"` |
| `set_window_fields(partition_by=, sort_by=, **output)` | `$setWindowFields` | `Rank()`, `Sum(..., window=...)` |
| `union_with(Model, pipeline=)` | `$unionWith` | |
| `geo_near(...)` | `$geoNear` | must be first |
| `unset(*fields)` | `$unset` | |
| `out(coll, db=)`, `merge(into, ...)` | `$out`, `$merge` | nothing may follow them (`OutStageError`) |
| `stage("$name", spec)` / `raw({...})` | any | e.g. Atlas `$search`, `$vectorSearch`; scope becomes *open* |

## `$lookup` in its three shapes

```python
# 1. equality join
Aggify(Customer).lookup(Order, "orders", local_field="id", foreign_field="customer")

# 2. correlated sub-query: `let` variables are referenced with Var(...)
Aggify(Customer).lookup(
    Order, "paid_orders",
    let={"cid": F("id")},
    query=Q(customer=Var("cid"), status="paid"),
)

# 3. full sub-pipeline (sort, limit, project ... inside the lookup)
latest = Aggify(Order).filter(customer=Var("cid")).order_by("-created_at")[:1]
Aggify(Customer).lookup(Order, "latest", let={"cid": F("id")}, pipeline=latest)
```

`unwind=True` unwinds the joined array and types the field as a single embedded document, so
`.filter(latest__status="paid")` keeps working.

## Options and execution

```python
pipeline.options(allow_disk_use=True, max_time_ms=5000, hint="status_1", collation={"locale": "en"})

pipeline.execute()            # list of dicts (python field names, typed values)
pipeline.execute(raw=True)    # untouched BSON dicts
async for row in pipeline.aiterate(): ...
pipeline.explain() / await pipeline.aexplain()
```

Results are dicts, not model instances (aggregation output rarely has the model's shape). To get instances use a
`QuerySet`, or `Model.from_mongo(raw_row)`.
