# Aggify

**Aggify** is an async-first MongoDB ODM for Python with the API you already know from the Django ORM -
and it is not tied to Django: the same documents work in **FastAPI**, **gRPC (`grpc.aio`)**, Django (sync *and* async
views), scripts and workers.

```python
import aggify
from aggify import Document, StringField, DecimalField, ReferenceField, Q, F, Sum, Count

aggify.connect("mongodb://localhost:27017/shop")

class Customer(Document):
    name = StringField(required=True)
    country = StringField(max_length=2)

class Order(Document):
    customer = ReferenceField(Customer)
    status = StringField(choices=["new", "paid"], default="new")
    total = DecimalField()

# who are my best customers in Germany?  (join + group + sort + limit in one expression)
best = await (
    Order.objects.filter(status="paid", customer__country="DE")
    .values("customer__name")
    .annotate(revenue=Sum("total"), orders=Count())
    .order_by("-revenue")[:10]
    .alist()
)
```

That single expression compiles to a `$lookup` + `$unwind` + `$match` + `$group` + `$project` + `$sort` + `$limit` pipeline.
You never write it by hand, and you can always look at it: `qs.query`.

## Why Aggify

| | |
|---|---|
| **Async and sync** | Built on pymongo's native `AsyncMongoClient`. Every I/O method has an `a`-prefixed twin (`aget`, `alist`, `asave`, `acount`, ...). |
| **Django-style API** | `Model.objects.filter(...).exclude(...).order_by(...)[:10]`, `Q`, `F`, `annotate`, `aggregate`, `values`, `select_related`, `get_or_create`, signals, managers. |
| **Short complex queries** | Joins via `owner__name="x"`, grouping via `values().annotate()`, window functions, facets, `$lookup` pipelines, array operators - see the [cookbook](cookbook.md). |
| **Framework agnostic** | Optional extras for Django, FastAPI, Pydantic and gRPC. The core imports none of them. |
| **Secure by default** | Operator-injection proof (typed fields), escaped regexes, server-side JS disabled, validated field names, masked credentials. See [Security](security.md). |
| **Efficient writes** | `save()` sends only the changed fields (`$set`/`$unset`), optional optimistic locking, pipeline updates with expressions. |
| **Testable** | `capture_commands()` / `assert_num_queries()`, `assert_pipeline()`, differential property tests against MongoDB. |

## Install

```bash
pip install aggify                    # core: needs only pymongo >= 4.13
pip install "aggify[fastapi]"         # + pydantic & fastapi helpers
pip install "aggify[django]"          # + Django app, forms, shortcuts
pip install "aggify[grpc]"            # + grpc.aio helpers
pip install "aggify[all]"
```

Python 3.9 - 3.13, MongoDB 5.0+ (6.0+/7.0+ recommended; transactions need a replica set).

## Where next

* New here? Follow the [quickstart](quickstart.md).
* Coming from MongoEngine? Read [the migration guide](migration/mongoengine.md).
* Upgrading from Aggify 0.x? See [what changed](migration/aggify-0.md).
* Building an API? [FastAPI](integrations/fastapi.md), [gRPC](integrations/grpc.md), [Django](integrations/django.md).
