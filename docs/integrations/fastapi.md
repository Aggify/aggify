# FastAPI

Aggify is plain asyncio with no thread-locals, so it fits FastAPI as is. `aggify.contrib.fastapi` adds the glue.

```bash
pip install "aggify[fastapi]"
```

```python
from typing import Optional
from fastapi import Depends, FastAPI
from aggify.contrib.fastapi import Page, Pagination, get_or_404, install_exception_handlers, lifespan, paginate
from aggify.contrib.pydantic import pydantic_model

app = FastAPI(lifespan=lifespan("mongodb://localhost:27017", "shop", indexes=[Account]))
install_exception_handlers(app)

AccountIn = pydantic_model(Account, name="AccountIn", for_input=True)          # request body
AccountPatch = pydantic_model(Account, name="AccountPatch", for_input=True, all_optional=True)
AccountOut = pydantic_model(Account, name="AccountOut", exclude=["attrs"])      # response

@app.post("/accounts", response_model=AccountOut, status_code=201)
async def create(body: AccountIn):
    return await Account.objects.acreate(**body.model_dump(exclude_unset=True))

@app.get("/accounts", response_model=Page[AccountOut])
async def listing(page: Pagination = Depends(), q: Optional[str] = None):
    qs = Account.objects.filter(username__icontains=q) if q else Account.objects.all()
    return await paginate(qs, page)             # {"items": [...], "total": n, "limit": .., "offset": ..}

@app.get("/accounts/{pk}", response_model=AccountOut)
async def read(pk: str):
    return await get_or_404(Account.objects, pk=pk)     # malformed ids are a 404, not a 500

@app.patch("/accounts/{pk}", response_model=AccountOut)
async def patch(pk: str, body: AccountPatch):
    obj = await get_or_404(Account.objects, pk=pk)
    for key, value in body.model_dump(exclude_unset=True).items():
        setattr(obj, key, value)
    await obj.asave()                                      # only the changed fields are written
    return obj
```

## What the helpers do

| helper | |
|---|---|
| `lifespan(uri, db, indexes=[...])` | connects, pings, creates indexes at start-up; closes the loop's clients on shutdown |
| `install_exception_handlers(app)` | `ValidationError`->422 (with per-field errors), `DoesNotExist`->404, `NotUniqueError`/`OptimisticLockError`->409, `SecurityError`/`InvalidField`/`InvalidLookup`->400 |
| `get_or_404(qs, **filters)` | `await qs.aget(...)` or `HTTPException(404)` |
| `Pagination`, `paginate(qs, page)`, `Page[T]` | `limit` (1-200) / `offset` query params and the response envelope |
| `transaction_dependency` | `dependencies=[Depends(transaction_dependency)]` wraps the request in a transaction (replica set) |

## Notes

* `pydantic_model()` builds the schemas from the document, including constraints (`max_length`, `ge`, `pattern`,
  `max_digits`), embedded documents, enums and `ObjectId` (24-hex string in JSON). `for_input=True` drops the primary
  key and server-managed fields and forbids unknown keys.
* Do not call blocking methods in `async def` endpoints - Aggify refuses with `SynchronousOnlyOperation`. In plain
  `def` endpoints (FastAPI runs them in a thread pool) the sync API is fine.
* Several uvicorn/gunicorn workers: each process has its own loop and therefore its own pool.
* Testing: `httpx.AsyncClient(transport=ASGITransport(app=app))` and `app.router.lifespan_context(app)`; see
  `tests/contrib/test_fastapi.py`.
