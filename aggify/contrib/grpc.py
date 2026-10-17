"""gRPC (``grpc.aio``) helpers.

Aggify is plain asyncio and keeps no thread-local state, so it works unchanged inside
``grpc.aio`` servicers.  This module adds error mapping and dict <-> protobuf helpers::

    class Users(users_pb2_grpc.UsersServicer):
        @grpc_errors
        async def Get(self, request, context):
            user = await User.objects.aget(pk=request.id)
            return fill_message(users_pb2.User(), user)
"""

from __future__ import annotations

import functools
import inspect
from typing import Any, Callable, Dict, Optional

try:
    import grpc
except ImportError as exc:  # pragma: no cover
    raise ImportError("aggify.contrib.grpc needs grpcio: pip install 'aggify[grpc]'") from exc

from aggify import exceptions as e

_STATUS = [
    (e.ValidationError, grpc.StatusCode.INVALID_ARGUMENT),
    (e.ObjectDoesNotExist, grpc.StatusCode.NOT_FOUND),
    (e.NotUniqueError, grpc.StatusCode.ALREADY_EXISTS),
    (e.OptimisticLockError, grpc.StatusCode.ABORTED),
    (e.SecurityError, grpc.StatusCode.INVALID_ARGUMENT),
    (e.InvalidField, grpc.StatusCode.INVALID_ARGUMENT),
    (e.QueryError, grpc.StatusCode.INVALID_ARGUMENT),
    (e.ConnectionNotConfigured, grpc.StatusCode.UNAVAILABLE),
]


def status_for(exc: BaseException) -> Optional[grpc.StatusCode]:
    for klass, code in _STATUS:
        if isinstance(exc, klass):
            return code
    return None


def grpc_errors(handler: Callable[..., Any]) -> Callable[..., Any]:
    """Decorator for ``async def`` servicer methods (unary and server-streaming)."""

    if inspect.isasyncgenfunction(handler):

        @functools.wraps(handler)
        async def stream_wrapper(self: Any, request: Any, context: Any):
            try:
                async for item in handler(self, request, context):
                    yield item
            except e.AggifyError as exc:
                code = status_for(exc)
                if code is None:
                    raise
                await context.abort(code, str(exc))

        return stream_wrapper

    @functools.wraps(handler)
    async def wrapper(self: Any, request: Any, context: Any):
        try:
            return await handler(self, request, context)
        except e.AggifyError as exc:
            code = status_for(exc)
            if code is None:
                raise
            await context.abort(code, str(exc))

    return wrapper


def fill_message(message: Any, document: Any, **kw: Any) -> Any:
    """Populate a protobuf ``message`` from a document (JSON-safe values, snake_case names)."""
    from google.protobuf.json_format import ParseDict

    data = document.to_dict(json_safe=True, include_none=False, **kw) if hasattr(document, "to_dict") else document
    return ParseDict(data, message, ignore_unknown_fields=True)


def message_to_kwargs(message: Any) -> Dict[str, Any]:
    """protobuf message -> dict usable as ``Model(**kwargs)``."""
    from google.protobuf.json_format import MessageToDict

    return MessageToDict(message, preserving_proto_field_name=True)
