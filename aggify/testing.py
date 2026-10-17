"""Helpers for testing code that uses Aggify (pytest friendly, framework agnostic).

from aggify.testing import capture_commands, assert_num_queries, assert_pipeline, temporary_database
"""

from __future__ import annotations

import contextlib
import uuid
from typing import Any, Dict, Iterator, List

from aggify.monitoring import CommandLog, CommandRecord, assert_num_queries, capture_commands  # noqa: F401


def assert_pipeline(queryset_or_pipeline: Any, expected: List[Dict[str, Any]]) -> None:
    """Compare the compiled stages with ``expected`` (readable diff on failure)."""
    obj = queryset_or_pipeline
    stages = obj.to_pipeline().to_list() if hasattr(obj, "to_pipeline") else obj.to_list()
    assert stages == expected, f"\nactual:   {stages}\nexpected: {expected}"


def assert_query(queryset: Any, **expected: Any) -> None:
    """Assert on the ``find`` spec of a queryset, e.g. ``assert_query(qs, filter={...}, limit=5)``."""
    spec = queryset.query
    assert "find" in spec, f"Queryset compiles to an aggregation: {spec}"
    for key, value in expected.items():
        assert spec["find"].get(key) == value, f"{key}: {spec['find'].get(key)!r} != {value!r}"


@contextlib.contextmanager
def temporary_database(uri: str, *, alias: str = "default", prefix: str = "aggify_tmp") -> Iterator[str]:
    """Connect to a throw-away database and drop it afterwards."""
    from pymongo import MongoClient

    import aggify

    name = f"{prefix}_{uuid.uuid4().hex[:10]}"
    aggify.connect(uri, name, alias=alias)
    try:
        yield name
    finally:
        client = MongoClient(uri)
        client.drop_database(name)
        client.close()
        aggify.disconnect(alias)
