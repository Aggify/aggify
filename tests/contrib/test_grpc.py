"""Aggify inside a real grpc.aio server (generic JSON handlers: no protoc needed)."""

import json

import grpc
import pytest

from aggify.contrib.grpc import fill_message, grpc_errors, message_to_kwargs, status_for
from tests.models import Account

pytestmark = pytest.mark.mongo


def dumps(obj):
    return json.dumps(obj).encode()


def loads(raw):
    return json.loads(raw.decode())


class AccountService:
    @grpc_errors
    async def Get(self, request, context):
        obj = await Account.objects.aget(pk=request["id"])
        return obj.to_dict(json_safe=True, include=["id", "username", "age"])

    @grpc_errors
    async def Create(self, request, context):
        obj = await Account.objects.acreate(**request)
        return {"id": str(obj.pk)}

    @grpc_errors
    async def Search(self, request, context):
        async for acc in Account.objects.filter(username__icontains=request["q"]).order_by("username").aiterator():
            yield {"username": acc.username}

    @grpc_errors
    async def Boom(self, request, context):
        raise RuntimeError("not an aggify error")


def handlers(svc):
    rpc = grpc.unary_unary_rpc_method_handler
    return grpc.method_handlers_generic_handler(
        "accounts.Accounts",
        {
            "Get": rpc(svc.Get, loads, dumps),
            "Create": rpc(svc.Create, loads, dumps),
            "Boom": rpc(svc.Boom, loads, dumps),
            "Search": grpc.unary_stream_rpc_method_handler(svc.Search, loads, dumps),
        },
    )


@pytest.fixture
async def channel(clean_db):
    server = grpc.aio.server()
    server.add_generic_rpc_handlers((handlers(AccountService()),))
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as ch:
        yield ch
    await server.stop(None)


def unary(channel, name):
    return channel.unary_unary(f"/accounts.Accounts/{name}", request_serializer=dumps, response_deserializer=loads)


async def test_create_and_get(channel):
    created = await unary(channel, "Create")({"username": "bob", "age": 3})
    got = await unary(channel, "Get")({"id": created["id"]})
    assert got == {"id": created["id"], "username": "bob", "age": 3}


async def test_error_codes(channel):
    with pytest.raises(grpc.aio.AioRpcError) as ei:
        await unary(channel, "Get")({"id": "64b7f0c2e1a4c2d9b8a3f001"})
    assert ei.value.code() == grpc.StatusCode.NOT_FOUND
    with pytest.raises(grpc.aio.AioRpcError) as ei:
        await unary(channel, "Create")({"username": "x", "age": -1})
    assert ei.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    with pytest.raises(grpc.aio.AioRpcError) as ei:
        await unary(channel, "Create")({"username": "bob", "is_admin": True})  # mass assignment -> TypeError
    assert ei.value.code() == grpc.StatusCode.UNKNOWN
    await Account.aensure_indexes()
    await unary(channel, "Create")({"username": "dup"})
    with pytest.raises(grpc.aio.AioRpcError) as ei:
        await unary(channel, "Create")({"username": "dup"})
    assert ei.value.code() == grpc.StatusCode.ALREADY_EXISTS
    with pytest.raises(grpc.aio.AioRpcError) as ei:
        await unary(channel, "Boom")({})
    assert ei.value.code() == grpc.StatusCode.UNKNOWN  # non-Aggify errors are not swallowed


async def test_server_streaming(channel):
    for name in ("anna", "bob", "hanna"):
        await unary(channel, "Create")({"username": name})
    call = channel.unary_stream("/accounts.Accounts/Search", request_serializer=dumps, response_deserializer=loads)
    names = [m["username"] async for m in call({"q": "ann"})]
    assert names == ["anna", "hanna"]


async def test_many_concurrent_rpcs(channel):
    import asyncio

    create = unary(channel, "Create")
    ids = await asyncio.gather(*(create({"username": f"user{i:02d}"}) for i in range(30)))
    assert len({i["id"] for i in ids}) == 30
    assert await Account.objects.acount() == 30


def test_status_mapping():
    import aggify

    assert status_for(aggify.ValidationError("x")) == grpc.StatusCode.INVALID_ARGUMENT
    assert status_for(aggify.SecurityError("x")) == grpc.StatusCode.INVALID_ARGUMENT
    assert status_for(aggify.OptimisticLockError("x")) == grpc.StatusCode.ABORTED
    assert status_for(ValueError()) is None


def test_protobuf_helpers_roundtrip():
    from google.protobuf import struct_pb2

    acc = Account(username="bob", age=3)
    msg = fill_message(struct_pb2.Struct(), acc, include=["id", "username", "age"])
    assert msg["username"] == "bob" and msg["id"] == str(acc.pk)
    kwargs = message_to_kwargs(msg)
    assert kwargs["username"] == "bob" and kwargs["age"] == 3
