# gRPC

Aggify works unchanged inside `grpc.aio` servicers: there is no per-thread state, clients are per event loop, and
transactions use `ContextVar`s that follow each RPC's task.

```bash
pip install "aggify[grpc]"
```

```python
import grpc
from aggify.contrib.grpc import grpc_errors, fill_message, message_to_kwargs

class AccountService(accounts_pb2_grpc.AccountsServicer):

    @grpc_errors                       # Aggify errors -> proper gRPC status codes
    async def Get(self, request, context):
        account = await Account.objects.aget(pk=request.id)
        return fill_message(accounts_pb2.Account(), account)

    @grpc_errors
    async def Create(self, request, context):
        account = await Account.objects.acreate(**message_to_kwargs(request))
        return fill_message(accounts_pb2.Account(), account)

    @grpc_errors
    async def Search(self, request, context):             # server streaming
        async for acc in Account.objects.filter(username__icontains=request.q).aiterator():
            yield fill_message(accounts_pb2.Account(), acc)


async def serve():
    aggify.connect("mongodb://localhost:27017/shop")
    await Account.aensure_indexes()
    server = grpc.aio.server()
    accounts_pb2_grpc.add_AccountsServicer_to_server(AccountService(), server)
    server.add_insecure_port("[::]:50051")
    await server.start()
    try:
        await server.wait_for_termination()
    finally:
        await aggify.adisconnect()
```

## Status code mapping

| Aggify | gRPC |
|---|---|
| `ValidationError`, `SecurityError`, `InvalidField`, `QueryError` | `INVALID_ARGUMENT` |
| `Model.DoesNotExist` | `NOT_FOUND` |
| `NotUniqueError` | `ALREADY_EXISTS` |
| `OptimisticLockError` | `ABORTED` |
| `ConnectionNotConfigured` | `UNAVAILABLE` |
| anything else | propagates (gRPC reports `UNKNOWN`) |

## Messages

* `doc.to_dict(json_safe=True, include=[...], exclude=[...], include_none=False)` produces only JSON/protobuf friendly
  primitives (`ObjectId`/`UUID`/`Decimal` as strings, datetimes as ISO-8601) - feed it to
  `google.protobuf.json_format.ParseDict`. `fill_message` does exactly that.
* `message_to_kwargs(msg)` goes the other way using the proto field names, ready for `Model(**kwargs)` - which
  rejects unknown keys.
* For `google.protobuf.Timestamp` fields convert explicitly (`Timestamp().FromDatetime(doc.created_at)`).

See `tests/contrib/test_grpc.py` for a complete in-process server (generic handlers, no `protoc` needed).
