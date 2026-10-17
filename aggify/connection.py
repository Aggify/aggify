"""Connection registry.

* ``connect(...)`` only *registers* settings - clients are created lazily, so
  importing your models never touches the network.
* One synchronous ``MongoClient`` per alias (thread-safe).
* One ``AsyncMongoClient`` per alias **per running event loop** (pymongo's async
  client is bound to its loop), which makes the library safe in FastAPI/uvicorn
  workers, gRPC ``aio`` servers, Django's ``async_to_sync`` and pytest-asyncio.
* Transactions are carried in a ``ContextVar`` so every query in the block joins
  the session automatically, in threads and asyncio tasks alike.
"""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import os
import threading
import weakref
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Dict, Iterator, Optional

from pymongo import AsyncMongoClient, MongoClient
from pymongo.asynchronous.database import AsyncDatabase
from pymongo.database import Database
from pymongo.uri_parser import parse_uri

from aggify.exceptions import ConfigurationError, ConnectionNotConfigured, SynchronousOnlyOperation
from aggify.security import mask_uri

DEFAULT_ALIAS = "default"

_DEFAULT_OPTIONS: Dict[str, Any] = {
    "tz_aware": True,
    "uuidRepresentation": "standard",
    "appname": "aggify",
}


@dataclass
class ConnectionConfig:
    alias: str
    uri: str
    db: str
    options: Dict[str, Any] = field(default_factory=dict)

    def __repr__(self) -> str:  # never leak credentials into logs / tracebacks
        return f"ConnectionConfig(alias={self.alias!r}, uri={mask_uri(self.uri)!r}, db={self.db!r})"

    def client_kwargs(self) -> Dict[str, Any]:
        from aggify.monitoring import LISTENER

        kwargs = {**_DEFAULT_OPTIONS, **self.options}
        kwargs["event_listeners"] = [*kwargs.get("event_listeners", []), LISTENER]
        return kwargs


_lock = threading.RLock()
_configs: Dict[str, ConnectionConfig] = {}
_sync_clients: Dict[str, MongoClient] = {}
_async_clients: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, Dict[str, AsyncMongoClient]] = (
    weakref.WeakKeyDictionary()
)


def connect(
    uri: str = "mongodb://localhost:27017",
    db: Optional[str] = None,
    *,
    alias: str = DEFAULT_ALIAS,
    **options: Any,
) -> ConnectionConfig:
    """Register a connection.  ``db`` defaults to the database in the URI path.

    >>> aggify.connect("mongodb://localhost:27017/shop")
    >>> aggify.connect("mongodb+srv://...", "analytics", alias="analytics", maxPoolSize=50)
    """
    if db is None:
        try:
            db = parse_uri(uri).get("database")
        except Exception as exc:  # invalid uri
            raise ConfigurationError(f"Invalid MongoDB URI: {exc}") from None
    if not db:
        raise ConfigurationError("No database name: pass db=... or put it in the URI path.")
    with _lock:
        previous = _configs.get(alias)
        config = ConnectionConfig(alias, uri, db, dict(options))
        if previous is not None and (previous.uri != uri or previous.options != config.options):
            _close_sync(alias)
        _configs[alias] = config
    return config


def is_connected(alias: str = DEFAULT_ALIAS) -> bool:
    return alias in _configs


def get_config(alias: str = DEFAULT_ALIAS) -> ConnectionConfig:
    try:
        return _configs[alias]
    except KeyError:
        raise ConnectionNotConfigured(alias) from None


def check_sync_allowed() -> None:
    """Refuse blocking driver calls from inside a running event loop (set
    ``AGGIFY_ALLOW_ASYNC_UNSAFE=1`` to disable the guard, as Django's equivalent)."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return
    if os.environ.get("AGGIFY_ALLOW_ASYNC_UNSAFE"):
        return
    raise SynchronousOnlyOperation(
        "A synchronous Aggify call was made from a running event loop. Use the async variant "
        "(acreate/aget/alist/asave/...) or run it in a thread (asyncio.to_thread)."
    )


def get_client(alias: str = DEFAULT_ALIAS) -> MongoClient:
    check_sync_allowed()
    config = get_config(alias)
    with _lock:
        client = _sync_clients.get(alias)
        if client is None:
            client = _sync_clients[alias] = MongoClient(config.uri, **config.client_kwargs())
        return client


def get_async_client(alias: str = DEFAULT_ALIAS) -> AsyncMongoClient:
    config = get_config(alias)
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        raise ConfigurationError("get_async_client() must be called from a running event loop.") from None
    bucket = _async_clients.get(loop)
    if bucket is None:
        bucket = _async_clients[loop] = {}
    client = bucket.get(alias)
    if client is None:
        client = bucket[alias] = AsyncMongoClient(config.uri, **config.client_kwargs())
    return client


def get_database(alias: str = DEFAULT_ALIAS) -> Database:
    return get_client(alias)[get_config(alias).db]


def get_async_database(alias: str = DEFAULT_ALIAS) -> AsyncDatabase:
    return get_async_client(alias)[get_config(alias).db]


def get_collection(model: Any):
    return get_database(model._meta.db_alias)[model._meta.collection]


def get_async_collection(model: Any):
    return get_async_database(model._meta.db_alias)[model._meta.collection]


def _close_sync(alias: str) -> None:
    client = _sync_clients.pop(alias, None)
    if client is not None:
        with contextlib.suppress(Exception):
            client.close()


def disconnect(alias: Optional[str] = None) -> None:
    """Close sync clients and forget the registration (async clients: use :func:`adisconnect`)."""
    with _lock:
        aliases = [alias] if alias else list(_configs)
        for a in aliases:
            _close_sync(a)
            _configs.pop(a, None)
            for bucket in list(_async_clients.values()):
                bucket.pop(a, None)


async def adisconnect(alias: Optional[str] = None) -> None:
    """Close the async clients of the *current* loop (and the sync ones), forget registrations."""
    loop = asyncio.get_running_loop()
    bucket = _async_clients.get(loop, {})
    for a in [alias] if alias else list(bucket):
        client = bucket.pop(a, None)
        if client is not None:
            with contextlib.suppress(Exception):
                await client.close()
    disconnect(alias)


async def aclose_clients(alias: Optional[str] = None) -> None:
    """Close this loop's async clients but keep the configuration (app shutdown hook)."""
    loop = asyncio.get_running_loop()
    bucket = _async_clients.get(loop, {})
    for a in [alias] if alias else list(bucket):
        client = bucket.pop(a, None)
        if client is not None:
            with contextlib.suppress(Exception):
                await client.close()


def ping(alias: str = DEFAULT_ALIAS) -> bool:
    return bool(get_database(alias).command("ping").get("ok"))


async def aping(alias: str = DEFAULT_ALIAS) -> bool:
    return bool((await get_async_database(alias).command("ping")).get("ok"))


# --------------------------------------------------------------------------- #
# sessions / transactions
# --------------------------------------------------------------------------- #
_sessions: contextvars.ContextVar[Optional[Dict[str, Any]]] = contextvars.ContextVar("aggify_sessions", default=None)


def current_session(alias: str = DEFAULT_ALIAS) -> Any:
    return (_sessions.get() or {}).get(alias)


def session_kwargs(alias: str) -> Dict[str, Any]:
    s = (_sessions.get() or {}).get(alias)
    return {"session": s} if s is not None else {}


@contextlib.contextmanager
def transaction(alias: str = DEFAULT_ALIAS, **options: Any) -> Iterator[Any]:
    """``with aggify.transaction(): ...`` - needs a replica set / mongos.

    Nested blocks join the outer transaction.
    """
    existing = current_session(alias)
    if existing is not None:
        yield existing
        return
    client = get_client(alias)
    with client.start_session() as session:
        with session.start_transaction(**options):
            token = _sessions.set({**(_sessions.get() or {}), alias: session})
            try:
                yield session
            finally:
                _sessions.reset(token)


@contextlib.asynccontextmanager
async def atransaction(alias: str = DEFAULT_ALIAS, **options: Any) -> AsyncIterator[Any]:
    """``async with aggify.atransaction(): ...``"""
    existing = current_session(alias)
    if existing is not None:
        yield existing
        return
    client = get_async_client(alias)
    async with client.start_session() as session:
        async with await session.start_transaction(**options):
            token = _sessions.set({**(_sessions.get() or {}), alias: session})
            try:
                yield session
            finally:
                _sessions.reset(token)
