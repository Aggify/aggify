# Transactions

Multi-document transactions need a **replica set** (or sharded cluster); a standalone `mongod` rejects them.

```python
with aggify.transaction():
    order = Order.objects.create(...)
    Product.objects.filter(pk=pid).update(inc__stock=-1)
    # exception -> rollback, otherwise commit
```

```python
async with aggify.atransaction():
    order = await Order.objects.acreate(...)
    await Product.objects.filter(pk=pid).aupdate(inc__stock=-1)
```

* The session lives in a `ContextVar`: every query, `save()`, `bulk_create()`, `delete()`... executed inside the
  block - in the same thread or asyncio task - joins the transaction automatically. Separate tasks that each open
  their own `atransaction()` get independent transactions.
* A MongoDB session is not safe for *concurrent* operations: inside one transaction run statements one after
  another (do not `asyncio.gather` queries that share the same transaction).
* Nested blocks join the outer transaction (no savepoints in MongoDB).
* `transaction(alias="other", read_concern=..., write_concern=...)` forwards options to `start_transaction`.
* Collections must exist before concurrent transactions touch them for the first time (MongoDB cannot create the
  same collection from two transactions at once): run `ensure_indexes()` at start-up.
* `aggify.current_session()` returns the active `ClientSession` (or `None`).

In Django, `AggifyAtomicMiddleware` wraps each request (sync or async); in FastAPI use
`Depends(transaction_dependency)`.
