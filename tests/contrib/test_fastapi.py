"""A real FastAPI app on top of Aggify, exercised through httpx's ASGI transport."""

from typing import Optional

import pytest
from fastapi import Depends, FastAPI
from httpx import ASGITransport, AsyncClient

import aggify
from aggify.contrib.fastapi import (
    Page,
    Pagination,
    get_or_404,
    install_exception_handlers,
    lifespan,
    paginate,
    transaction_dependency,
)
from aggify.contrib.pydantic import pydantic_model
from tests.conftest import MONGO_URI
from tests.models import Account

pytestmark = pytest.mark.mongo

AccountIn = pydantic_model(Account, name="AccountIn", for_input=True)
AccountPatch = pydantic_model(Account, name="AccountPatch", for_input=True, all_optional=True)
AccountOut = pydantic_model(Account, name="AccountOut", exclude=["attrs"])


def make_app(db_name):
    app = FastAPI(lifespan=lifespan(MONGO_URI, db_name, indexes=[Account]))
    install_exception_handlers(app)

    @app.post("/accounts", response_model=AccountOut, status_code=201)
    async def create(body: AccountIn):
        return await Account.objects.acreate(**body.model_dump(exclude_unset=True))

    @app.get("/accounts", response_model=Page[AccountOut])
    async def listing(page: Pagination = Depends(), q: Optional[str] = None):
        qs = Account.objects.filter(username__icontains=q) if q else Account.objects.all()
        return await paginate(qs, page)

    @app.get("/accounts/{pk}", response_model=AccountOut)
    async def read(pk: str):
        return await get_or_404(Account.objects, pk=pk)

    @app.patch("/accounts/{pk}", response_model=AccountOut)
    async def patch(pk: str, body: AccountPatch):
        obj = await get_or_404(Account.objects, pk=pk)
        for k, v in body.model_dump(exclude_unset=True).items():
            setattr(obj, k, v)
        await obj.asave()
        return obj

    @app.get("/search")
    async def search(username: str):
        # classic injection attempt: ?username[$ne]=x is a str here; JSON bodies could carry dicts
        return {"n": await Account.objects.filter(username=username).acount()}

    @app.post("/unsafe-search")
    async def unsafe(body: dict):
        return {"n": await Account.objects.filter(username=body["username"]).acount()}

    return app


@pytest.fixture
async def client(db_name, clean_db):
    app = make_app(db_name)
    try:
        async with app.router.lifespan_context(app):
            async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
                yield c
    finally:
        aggify.connect(MONGO_URI, db_name)  # the app's shutdown removed the shared test connection


async def test_crud_roundtrip(client):
    r = await client.post("/accounts", json={"username": "bob", "age": 30, "address": {"city": "Tehran"}})
    assert r.status_code == 201
    body = r.json()
    assert body["username"] == "bob" and len(body["id"]) == 24 and body["address"]["city"] == "Tehran"
    got = await client.get(f"/accounts/{body['id']}")
    assert got.json() == body
    patched = await client.patch(f"/accounts/{body['id']}", json={"age": 31})
    assert patched.json()["age"] == 31 and patched.json()["username"] == "bob"


async def test_pagination_and_search(client):
    for i in range(25):
        await client.post("/accounts", json={"username": f"user{i:02d}", "age": i})
    page = (await client.get("/accounts", params={"limit": 10, "offset": 20})).json()
    assert page["total"] == 25 and len(page["items"]) == 5 and page["items"][0]["username"] == "user20"
    found = (await client.get("/accounts", params={"q": "user1"})).json()
    assert found["total"] == 10
    assert (await client.get("/accounts", params={"limit": 0})).status_code == 422
    assert (await client.get("/accounts", params={"limit": 1000})).status_code == 422


async def test_error_mapping(client):
    assert (await client.get("/accounts/64b7f0c2e1a4c2d9b8a3f001")).status_code == 404
    assert (await client.get("/accounts/not-an-id")).status_code == 404
    assert (await client.post("/accounts", json={"username": "x"})).status_code == 422
    await client.post("/accounts", json={"username": "dup"})
    r = await client.post("/accounts", json={"username": "dup"})
    assert r.status_code == 409


async def test_operator_injection_attempts_are_neutralised(client):
    await client.post("/accounts", json={"username": "victim"})
    assert (await client.get("/search", params={"username": "victim"})).json() == {"n": 1}
    assert (await client.get("/search", params={"username[$ne]": "x", "username": "nobody"})).json() == {"n": 0}
    r = await client.post("/unsafe-search", json={"username": {"$ne": "zzz"}})
    assert r.status_code == 422  # rejected, not "all users"
    r = await client.post("/unsafe-search", json={"username": {"$regex": ".*"}})
    assert r.status_code == 422


async def test_many_concurrent_requests(client):
    import asyncio

    rs = await asyncio.gather(*(client.post("/accounts", json={"username": f"c{i:03d}"}) for i in range(40)))
    assert all(r.status_code == 201 for r in rs)
    assert (await client.get("/accounts", params={"limit": 1})).json()["total"] == 40


@pytest.mark.replica
async def test_transaction_dependency(db_name, clean_db):
    app = make_app(db_name)

    @app.post("/tx", dependencies=[Depends(transaction_dependency)])
    async def tx(name: str = "tx-user", fail: bool = False):
        await Account.objects.acreate(username=name)
        if fail:
            raise RuntimeError("boom")
        return {"ok": True}

    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app, raise_app_exceptions=False), base_url="http://t") as c:
            assert (await c.post("/tx")).status_code == 200
            assert (await c.post("/tx", params={"name": "tx-failed", "fail": "true"})).status_code == 500
        assert await Account.objects.filter(username="tx-user").acount() == 1
        assert await Account.objects.filter(username="tx-failed").acount() == 0
    aggify.connect(MONGO_URI, db_name)
