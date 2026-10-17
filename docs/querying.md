# Querying

`Model.objects` returns a lazy, immutable `QuerySet`. Every chaining method returns a *new* queryset, so you can
safely share and extend them. Nothing touches the database until you iterate, slice to an item, or call a terminal
method (`get`, `first`, `count`, `exists`, `aggregate`, `update`, `delete`, ...). Each terminal method has an
`a`-prefixed async twin.

## Filtering

```python
Account.objects.filter(age__gte=18, country="DE")        # AND
Account.objects.exclude(banned=True)
Account.objects.filter(Q(vip=True) | Q(age__gt=60), ~Q(country="IR"))
Account.objects.filter(F("spent") > F("budget"))         # an expression as the condition
Account.objects.filter(age__gt=F("score") * 10)          # field vs expression
```

Multiple keyword arguments, chained `filter()` calls and `&` all mean AND. Conditions on the same field are merged
into a single operator document when possible and into `$and` when not, so nothing is ever silently overwritten.

### Lookups

| lookup | example | MongoDB |
|---|---|---|
| *(none)* / `exact` | `name="x"` | `{name: "x"}` |
| `ne` | `age__ne=3` | `$ne` |
| `gt` `gte` `lt` `lte` | `age__gte=18` | `$gt` ... |
| `in` / `nin` | `age__in=[1, 2]` | `$in` / `$nin` |
| `range` | `age__range=(18, 30)` | `$gte` + `$lte` |
| `isnull` | `deleted_at__isnull=True` | `{f: null}` (missing or null) |
| `exists` | `meta__exists=True` | `$exists` |
| `contains` / `icontains` | `name__icontains="al"` | escaped `$regex` |
| `startswith` / `istartswith` | `name__startswith="a"` | `^…` |
| `endswith` / `iendswith` | `name__endswith="z"` | `…$` |
| `iexact` | `name__iexact="ALI"` | `^…$` with `i` |
| `regex` / `iregex` | `name__regex=r"^a.+z$"` | `$regex` (explicit, length-capped) |
| `size` | `tags__size=2` | `$size` |
| `all` | `tags__all=["a", "b"]` | `$all` |
| `elem_match` | `items__elem_match=Q(qty__gte=5)` | `$elemMatch` |
| `type` | `x__type="string"` | `$type` |
| `mod` | `n__mod=(2, 0)` | `$mod` |
| `near` | `location__near=(lng, lat, 5000)` | `$near` (`find` only) |

* **Lists**: `tags="a"` matches arrays that contain `"a"`; `tags=["a", "b"]` matches that exact array; `tags__in`,
  `tags__contains`... apply to the elements.
* **Embedded documents / dicts**: `address__city="Tehran"`, `attrs__color="red"`, `attrs__size__gt=3`.
* **`pk`** is an alias for the primary key: `pk__in=[...]`.
* `contains`-style lookups **escape** their input: `name__contains=".*"` searches for the literal text `.*`.

### Across references (automatic joins)

```python
Post.objects.filter(owner__country="DE", owner__profile__verified=True)
Post.objects.order_by("-owner__created_at")
Post.objects.values("caption", "owner__name")
Order.objects.filter(items__product__category="toys")      # through a list of embedded docs
```

Aggify adds the `$lookup`/`$unwind` stages for you (aliased `_j_<path>`, never visible on your objects) and
re-uses a join when several parts of the query need it. Filtering on the reference's id (`owner=acc`,
`owner__id=...`) needs no join.

## Ordering and slicing

```python
Account.objects.order_by("-age", "name")
Account.objects.order_by("-age").reverse()
qs[10:20]            # skip 10, limit 10          qs[:5]   qs[5:]   qs[3]
qs.first()  qs.last()  qs.latest("created_at")  qs.earliest("created_at")
```

Negative indexes and steps are rejected (MongoDB cannot do them cheaply). `aggify.configure(max_limit=100)` caps
page sizes globally.

## Projections

```python
Account.objects.only("name", "age")       # partial instances (the primary key is always loaded)
Account.objects.defer("big_blob")
Account.objects.values("name", "address__city")                 # dicts
Account.objects.values_list("name", flat=True)                  # [..]
Account.objects.values_list("name", "age", named=True)          # namedtuples
```

## Aggregation, Django style

```python
from aggify import Count, Sum, Avg, Max, Q

# whole queryset -> one dict
Order.objects.aggregate(n=Count(), revenue=Sum("total"), biggest=Max("total"),
                        paid=Sum("total", filter=Q(status="paid")))

# GROUP BY: values() + annotate(<aggregate>)
Order.objects.values("customer__country").annotate(n=Count(), revenue=Sum("total")).order_by("-revenue")

# HAVING: filter() after annotate() works on the aggregates; before it, it is a WHERE
(Order.objects.filter(status="paid")                 # WHERE
      .values("customer").annotate(n=Count())
      .filter(n__gte=3))                              # HAVING

# row-level computed fields
Account.objects.annotate(full=Concat("first", Value(" "), "last"), tags_n=Size("tags")).filter(tags_n__gt=2)
```

!!! info "How grouping is decided"
    `annotate()` with an aggregate *after* `values()` groups by the `values()` fields (and expressions:
    `values(day=DateToString("created_at", "%Y-%m-%d"))`). Without `values()`, aggregates are evaluated per
    document as array operators (`Size("items")`, `Sum("items__qty")`). `Meta.ordering` is not applied to grouped
    queries, as in Django.

## Writing

```python
Account.objects.filter(age__lt=18).update(minor=True)             # update_many -> modified count
Account.objects.filter(pk=pk).update_one(inc__logins=1)
Account.objects.filter(email=e).update(upsert=True, name="New")
Account.objects.filter(...).delete()                              # delete_many (no per-doc signals)
Account.objects.bulk_create([...])
obj, created = Account.objects.get_or_create(email=e, defaults={"name": "x"})
obj, created = Account.objects.update_or_create(email=e, defaults={"name": "y"})
```

### Update operators

```python
Post.objects.filter(pk=pk).update(
    title="New",                    # $set
    inc__views=1,                   # $inc
    push__tags="news",              # $push            push_all__tags=[...]
    add_to_set__tags="hot",         # $addToSet        add_to_set_all__tags=[...]
    pull__tags="old",               # $pull            pull_all__tags=[...]
    pop__tags=-1,                   # $pop
    min__low=3, max__high=9, mul__score=2,
    unset__draft=True, rename__a="b", current_date__touched_at=True,
)
```

Values are validated by the field (`inc__views="x"` is rejected), paths are validated against the model, and
the primary key cannot be changed.

### Updates with expressions (atomic, server-side)

```python
Product.objects.update(price=Round(F("price") * Value(1.1), 2), inc__stock=-1)
```

Assigning an expression switches to a MongoDB *pipeline update*; `inc__`, `mul__`, `min__`, `max__`, `push__`,
`add_to_set__` and `unset__` are rewritten for you so they can be mixed in the same call.

## Other terminal methods

`get()` (raises `Model.DoesNotExist` / `Model.MultipleObjectsReturned`), `exists()`, `count()`,
`distinct("field")`, `in_bulk(ids)`, `iterator()` / `aiterator()` (streaming), `explain()` / `aexplain()`,
`search("text")` (full-text, needs a text index) and `query` (what will be executed).

## Dropping to a pipeline

```python
pipeline = Order.objects.filter(status="paid").to_pipeline()   # an Aggify Pipeline, see "Aggregation pipelines"
rows = await pipeline.lookup(...).facet(...).aexecute()
```
