"""FastAPI helpers.

from fastapi import FastAPI, Depends
from aggify.contrib.fastapi import lifespan, install_exception_handlers, Pagination, get_or_404

app = FastAPI(lifespan=lifespan("mongodb://localhost/shop", indexes=[Product]))
install_exception_handlers(app)

@app.get("/products/{pk}", response_model=ProductOut)
async def read(pk: str):
    return await get_or_404(Product.objects, pk=pk)
"""

from __future__ import annotations

import contextlib
from dataclasses import dataclass
from typing import Any, AsyncIterator, Callable, Generic, List, Optional, Sequence, TypeVar

try:
    from fastapi import FastAPI, HTTPException, Query, Request
    from fastapi.responses import JSONResponse
    from pydantic import BaseModel
except ImportError as exc:  # pragma: no cover
    raise ImportError("aggify.contrib.fastapi needs fastapi: pip install 'aggify[fastapi]'") from exc

import aggify
from aggify import exceptions as exc_

T = TypeVar("T")


def lifespan(
    uri: str,
    db: Optional[str] = None,
    *,
    alias: str = aggify.DEFAULT_ALIAS,
    indexes: Sequence[Any] = (),
    **options: Any,
) -> Callable[[FastAPI], Any]:
    """Return a FastAPI ``lifespan``: connect + ping + ensure indexes on start, close on shutdown."""

    @contextlib.asynccontextmanager
    async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
        aggify.connect(uri, db, alias=alias, **options)
        await aggify.aping(alias)
        for model in indexes:
            await model.aensure_indexes()
        try:
            yield
        finally:
            await aggify.adisconnect(alias)

    return _lifespan


def install_exception_handlers(app: FastAPI) -> None:
    """Map Aggify errors to sensible HTTP responses."""

    def make(status: int, detail: Callable[[Exception], Any]) -> Callable[[Request, Exception], Any]:
        async def handler(request: Request, exc: Exception) -> JSONResponse:
            return JSONResponse(status_code=status, content={"detail": detail(exc)})

        return handler

    app.add_exception_handler(exc_.ValidationError, make(422, lambda e: e.as_dict() or e.message))  # type: ignore[attr-defined]
    app.add_exception_handler(exc_.ObjectDoesNotExist, make(404, lambda e: str(e) or "Not found"))
    app.add_exception_handler(exc_.NotUniqueError, make(409, lambda e: str(e)))
    app.add_exception_handler(exc_.OptimisticLockError, make(409, lambda e: str(e)))
    app.add_exception_handler(exc_.SecurityError, make(400, lambda e: "Rejected unsafe input."))
    app.add_exception_handler(exc_.InvalidField, make(400, lambda e: str(e)))
    app.add_exception_handler(exc_.InvalidLookup, make(400, lambda e: str(e)))


async def get_or_404(source: Any, *args: Any, **kwargs: Any) -> Any:
    """``await get_or_404(Model.objects, pk=...)`` -> instance or ``HTTPException(404)``."""
    qs = source.all() if hasattr(source, "all") else source
    try:
        return await qs.aget(*args, **kwargs)
    except exc_.ObjectDoesNotExist:
        raise HTTPException(status_code=404, detail="Not found") from None
    except exc_.ValidationError:
        raise HTTPException(status_code=404, detail="Not found") from None  # malformed ids look like missing rows


@dataclass
class Pagination:
    """Dependency: ``page: Pagination = Depends()`` -> ``qs[page.offset: page.offset + page.limit]``."""

    limit: int = Query(20, ge=1, le=200)
    offset: int = Query(0, ge=0)

    def apply(self, qs: Any) -> Any:
        return qs[self.offset : self.offset + self.limit]


class Page(BaseModel, Generic[T]):
    items: List[T]
    total: int
    limit: int
    offset: int


async def paginate(qs: Any, page: Pagination) -> dict:
    total = await qs.acount()
    items = await page.apply(qs).alist()
    return {"items": items, "total": total, "limit": page.limit, "offset": page.offset}


async def transaction_dependency() -> AsyncIterator[None]:
    """``Depends(transaction_dependency)`` wraps the request in a MongoDB transaction (replica set required)."""
    async with aggify.atransaction():
        yield
