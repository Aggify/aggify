# Testing

## Your application's tests

```python
# conftest.py
import pytest, aggify
from aggify.testing import temporary_database

@pytest.fixture(scope="session", autouse=True)
def mongo():
    with temporary_database("mongodb://localhost:27017") as name:     # random db, dropped at the end
        yield name

@pytest.fixture(autouse=True)
def clean(mongo):
    for model in (User, Order):
        model.drop_collection()
```

Use `pytest-asyncio` (`asyncio_mode = "auto"`) for async tests. A throw-away MongoDB for CI:

```yaml
services:
  mongo:
    image: mongo:7
    ports: ["27017:27017"]
    options: >-
      --health-cmd "mongosh --quiet --eval 'db.runCommand({ping:1})'"
```

(add `--replSet rs0` + `rs.initiate()` if you test transactions).

### Count and inspect queries

```python
from aggify.testing import assert_num_queries, capture_commands, assert_pipeline, assert_query

def test_no_n_plus_one():
    with assert_num_queries(1):
        list(Post.objects.select_related("owner"))

def test_what_was_sent():
    with capture_commands() as log:
        Post.objects.filter(published=True).count()
    assert log.names == ["aggregate"] or log.names == ["count"]
    print(log.records[0].command)

def test_compiled_form_without_a_database():
    assert_query(Post.objects.filter(published=True), filter={"published": True})
    assert_pipeline(Post.objects.filter(owner__country="DE"), [
        {"$lookup": {...}}, {"$unwind": {...}}, {"$match": {"_j_owner.country": "DE"}}, {"$unset": ["_j_owner"]},
    ])
```

`qs.query` / `pipeline.to_list()` need no connection at all, so query-building logic can be unit-tested offline.

## How Aggify tests itself

* **Unit tests** (no database): fields, lookups, `Q`, expressions, pipeline stages, update building, security rules.
* **Integration tests** against a real replica set: CRUD, joins, grouping, transactions, signals, indexes (and that
  queries really use them), multiple event loops and threads, 40 concurrent requests...
* **Differential tests** (`tests/integration/test_differential.py`): [Hypothesis](https://hypothesis.works) generates
  random documents and random `Q` trees (AND/OR/NOT over 20+ lookups); the same predicate is evaluated by MongoDB through
  Aggify *and* by a small Python reference implementation - results must be identical, and `exclude()` must be the exact
  complement.
* **Property tests for security**: no string field can ever emit a `$`-keyed value; escaped regexes match literally.
* **Framework tests**: a real FastAPI app over ASGI (including injection attempts and transactions), an in-process
  `grpc.aio` server, Django views (sync and async), forms, pagination, management command, middleware.
* The runnable documentation examples are executed too.

```bash
make mongo          # throw-away replica set in Docker on :27018
make test           # pytest
make test-all       # + lint, mypy, 3.9 - 3.13 via tox
```

Set `AGGIFY_TEST_MONGO_URI` to use another server; tests marked `mongo` skip themselves when none is reachable and
tests marked `replica` skip on a standalone server.
