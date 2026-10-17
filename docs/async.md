# Sync & async

Aggify uses pymongo's native `MongoClient` for sync code and `AsyncMongoClient` for async code. Both are created
lazily from the same `connect()` settings.

| sync | async |
|---|---|
| `qs.get() / first() / last() / count() / exists()` | `aget / afirst / alast / acount / aexists` |
| `list(qs)` / `for x in qs` | `await qs.alist()` / `async for x in qs` |
| `qs.iterator()` | `qs.aiterator()` |
| `qs.aggregate(...)`, `qs.update(...)`, `qs.delete()` | `aaggregate`, `aupdate`, `adelete` |
| `create / get_or_create / update_or_create / bulk_create` | `acreate / aget_or_create / aupdate_or_create / abulk_create` |
| `doc.save() / delete() / refresh_from_db()` | `asave / adelete / arefresh_from_db` |
| `doc.fetch_related("owner")` | `await doc.afetch_related("owner")` |
| `Model.ensure_indexes()` | `await Model.aensure_indexes()` |
| `pipeline.execute()`, `iterate()`, `explain()` | `aexecute`, `aiterate`, `aexplain` |
| `with aggify.transaction():` | `async with aggify.atransaction():` |
| `aggify.ping()` | `await aggify.aping()` |

Chaining methods (`filter`, `order_by`, `annotate`, ...) are shared and synchronous - they only build the query.

## One client per event loop

pymongo's async client belongs to the loop that first used it. Aggify therefore keeps **one `AsyncMongoClient` per
running loop** (and per alias). That makes the following all work without any special code:

* uvicorn/gunicorn workers (one loop per process),
* `asyncio.run()` called repeatedly, pytest-asyncio's loop-per-test,
* Django's `async_to_sync` / `sync_to_async`,
* `grpc.aio` servers, background threads that run their own loop.

Close the current loop's clients on shutdown with `await aggify.aclose_clients()` (or
`await aggify.adisconnect()` to also forget the connection settings). The `lifespan()` helper does it for FastAPI.

## Blocking calls inside the event loop are refused

```python
async def view():
    Account.objects.count()        # SynchronousOnlyOperation: use acount() or asyncio.to_thread
```

Run sync code from a worker thread instead (`await asyncio.to_thread(sync_function)`), where it is perfectly fine - the
sync client is thread-safe. Set `AGGIFY_ALLOW_ASYNC_UNSAFE=1` to turn the guard off (not recommended).

## Concurrency

Everything is plain asyncio, so `asyncio.gather` just works:

```python
users, orders, stats = await asyncio.gather(
    User.objects.filter(active=True).alist(),
    Order.objects.filter(status="new").alist(),
    Order.objects.aaggregate(total=Sum("amount")),
)
```

Tune the pool through `connect(..., maxPoolSize=100)`; Aggify adds no global locks on the async path.
