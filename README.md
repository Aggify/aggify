<p align="center">
  <img src="https://i.imgur.com/BBp9vUQ.png" alt="Aggify">
</p>
<p align="center">
    <em>Async-first MongoDB ODM with a Django-style API - for Django, FastAPI, gRPC and plain Python</em>
</p>

[![Package version](https://img.shields.io/pypi/v/aggify?color=%2334D058&label=pypi%20package)](https://pypi.org/project/aggify)
[![Downloads](https://img.shields.io/pypi/dm/aggify)](https://pypi.org/project/aggify)
[![Supported Python versions](https://img.shields.io/pypi/pyversions/aggify.svg?color=%2334D058)](https://pypi.org/project/aggify)
[![Coverage](https://img.shields.io/codecov/c/github/Aggify/aggify)](https://app.codecov.io/gh/Aggify/aggify)
[![License](https://img.shields.io/github/license/Aggify/aggify.svg)](https://github.com/Aggify/aggify/blob/main/LICENSE)
[![Telegram](https://img.shields.io/badge/-telegram-red?color=white&logo=telegram&logoColor=blue)](https://t.me/Aggify)

**Aggify 1.0** is a complete rewrite: no MongoEngine, native `async`/`await` on pymongo's `AsyncMongoClient`, an API
you already know from the Django ORM, and aggregation pipelines that fit on one line.

```python
import aggify
from aggify import Document, StringField, DecimalField, ReferenceField, Sum, Count

aggify.connect("mongodb://localhost:27017/shop")

class Customer(Document):
    name = StringField(required=True)
    country = StringField(max_length=2)

class Order(Document):
    customer = ReferenceField(Customer)
    status = StringField(choices=["new", "paid"], default="new")
    total = DecimalField()

best = await (
    Order.objects.filter(status="paid", customer__country="DE")      # $lookup + $unwind + $match
    .values("customer__name")
    .annotate(revenue=Sum("total"), orders=Count())                  # $group
    .filter(orders__gte=3)                                           # HAVING
    .order_by("-revenue")[:10]                                       # $sort + $limit
    .alist()
)
```

## Highlights

* **Sync *and* async** - every I/O method has an `a`-twin (`aget`, `alist`, `asave`, `acount`, `aupdate`, ...);
  one async client per event loop, so it is safe in FastAPI, `grpc.aio`, Django's `async_to_sync` and pytest-asyncio.
* **Django-style** - `Model.objects`, `Q`, `F`, `annotate`, `aggregate`, `values`, `select_related`, `get_or_create`,
  signals, managers, `DoesNotExist`, pagination-ready querysets.
* **Complex queries, short** - automatic joins (`owner__country="DE"`), `values().annotate()` grouping, conditional
  aggregates (`Sum("x", filter=Q(...))`), window functions, `$facet`, `$bucket`, `$graphLookup`, sub-pipeline
  `$lookup`, array operators, atomic expression updates. See the [cookbook](docs/cookbook.md).
* **Framework-agnostic integrations** - `pip install "aggify[fastapi|grpc|django|pydantic]"`. Pydantic schemas from
  documents, FastAPI lifespan/errors/pagination, gRPC status mapping, Django app/forms/shortcuts/middleware, and an
  `ObjectIdField` to link Django ORM models to Mongo documents.
* **Secure by default** - typed fields stop operator injection (`{"$ne": ""}`), regex lookups are escaped,
  server-side JavaScript is blocked, field names validated, strict constructors, resource limits, masked credentials.
* **Efficient** - `save()` sends only changed fields, optional optimistic locking, `only()/defer()/values()`,
  streaming iterators, bulk operations.
* **Well tested** - 470+ tests: unit, integration against a real replica set, differential property tests
  (random `Q` trees vs. a Python reference), FastAPI/gRPC/Django end-to-end tests, Python 3.9-3.13.

## Install

```bash
pip install aggify                       # core (pymongo >= 4.13)
pip install "aggify[fastapi]"            # + pydantic / fastapi helpers
pip install "aggify[django]" "aggify[grpc]" "aggify[all]"
```

## A tour

```python
# models: typed fields, embedded documents, references, indexes
class Account(Document):
    username = StringField(required=True, unique=True)
    tags = ListField(StringField())
    address = EmbeddedField(Address)
    created_at = DateTimeField(auto_now_add=True)
    class Meta:
        ordering = ["username"]
        indexes = [Index("tags")]

# CRUD - only changed fields are written
acc = await Account.objects.acreate(username="ali", tags=["vip"])
acc.address = Address(city="Tehran")
await acc.asave()                               # {"$set": {"address": {...}}}

# querying
Account.objects.filter(Q(tags="vip") | Q(address__city__istartswith="te"), ~Q(banned=True))
Account.objects.filter(age__gt=F("score") * 10).only("username")[:20]

# atomic updates, including server-side expressions
Account.objects.filter(pk=acc.pk).update(inc__logins=1, push__tags="beta")
Product.objects.update(price=Round(F("price") * Value(1.1), 2))

# transactions (replica set)
async with aggify.atransaction():
    await Order.objects.acreate(...)
    await Product.objects.filter(pk=pid).aupdate(inc__stock=-1)

# the compiled query is always inspectable
Account.objects.filter(address__city="Tehran").query
```

Need a stage a queryset does not have? The immutable pipeline builder is one call away:

```python
from aggify import Aggify
Aggify(Order).filter(status="paid").facet(
    by_country=Aggify(Order).group("customer__country", n=Count()),
    page=[{"$limit": 20}],
).execute()
```

## Documentation

Full documentation lives in [`docs/`](docs/index.md) (build with `make docs`):
[quickstart](docs/quickstart.md) - [models](docs/models.md) - [querying](docs/querying.md) -
[pipelines](docs/aggregation.md) - [cookbook](docs/cookbook.md) - [security](docs/security.md) -
[testing](docs/testing.md) - [Django](docs/integrations/django.md) - [FastAPI](docs/integrations/fastapi.md) -
[gRPC](docs/integrations/grpc.md) - [MongoEngine migration](docs/migration/mongoengine.md) -
[upgrading from 0.x](docs/migration/aggify-0.md) - [فارسی](docs/fa.md).

## Development

```bash
make install && make mongo && make test      # needs Docker for the throw-away MongoDB replica set
```

See [CONTRIBUTING.md](CONTRIBUTING.md). Security reports: [SECURITY.md](SECURITY.md).
