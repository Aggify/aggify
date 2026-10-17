"""Shared fixtures.

MongoDB for integration tests
-----------------------------
``AGGIFY_TEST_MONGO_URI``  default ``mongodb://localhost:27018/?directConnection=true``
(start one with ``make mongo`` / ``docker run -p 27018:27017 mongo:7 --replSet rs0`` + ``rs.initiate``).
Tests marked ``mongo`` are skipped automatically when the server is unreachable;
tests marked ``replica`` additionally need a replica set (transactions).
"""

from __future__ import annotations

import os
import uuid

import pytest

import aggify

MONGO_URI = os.environ.get("AGGIFY_TEST_MONGO_URI", "mongodb://localhost:27018/?directConnection=true")


def _probe() -> tuple[bool, bool]:
    from pymongo import MongoClient

    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=1500)
        hello = client.admin.command("hello")
        replica = bool(hello.get("setName"))
        client.close()
        return True, replica
    except Exception:
        return False, False


_AVAILABLE, _REPLICA = _probe()


def pytest_collection_modifyitems(config, items):
    for item in items:
        if "mongo" in item.keywords and not _AVAILABLE:
            item.add_marker(pytest.mark.skip(reason=f"MongoDB not reachable at {MONGO_URI}"))
        if "replica" in item.keywords and _AVAILABLE and not _REPLICA:
            item.add_marker(pytest.mark.skip(reason="MongoDB is not a replica set"))


@pytest.fixture(scope="session")
def db_name():
    return f"aggify_test_{uuid.uuid4().hex[:8]}"


@pytest.fixture(scope="session", autouse=True)
def _connection(db_name):
    aggify.connect(MONGO_URI, db_name)
    yield
    if _AVAILABLE:
        from pymongo import MongoClient

        client = MongoClient(MONGO_URI)
        client.drop_database(db_name)
        client.close()
    aggify.disconnect()


@pytest.fixture(autouse=True)
def _reset_settings():
    saved = dict(vars(aggify.settings))
    yield
    for k, v in saved.items():
        setattr(aggify.settings, k, v)


@pytest.fixture
def clean_db(request, db_name):
    """Empty every collection used by the test models before the test."""
    from pymongo import MongoClient

    if _AVAILABLE:
        client = MongoClient(MONGO_URI)
        db = client[db_name]
        for name in db.list_collection_names():
            db[name].drop()
        client.close()
    yield
