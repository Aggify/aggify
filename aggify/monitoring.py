"""Command monitoring: ``capture_commands()`` is Aggify's ``assertNumQueries``.

    from aggify.testing import capture_commands, assert_num_queries

    with capture_commands() as cmds:
        list(Post.objects.select_related("owner"))
    assert cmds.names == ["aggregate"]

    with assert_num_queries(1):
        Post.objects.count()

The listener is attached to every client Aggify creates; it is a no-op unless a recorder is active.
"""

from __future__ import annotations

import contextlib
import threading
from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, List, Optional

from pymongo import monitoring

_IGNORED = frozenset(
    {
        "hello",
        "isMaster",
        "ismaster",
        "ping",
        "endSessions",
        "saslStart",
        "saslContinue",
        "buildInfo",
        "getMore",
        "killCursors",
        "commitTransaction",
        "abortTransaction",
        "authenticate",
        "getnonce",
    }
)
_active: List[CommandLog] = []
_lock = threading.Lock()


@dataclass
class CommandRecord:
    name: str
    database: str
    command: Dict[str, Any]
    request_id: int
    duration_micros: Optional[int] = None
    failed: bool = False

    @property
    def collection(self) -> Optional[str]:
        value = self.command.get(self.name)
        return value if isinstance(value, str) else None


@dataclass
class CommandLog:
    records: List[CommandRecord] = field(default_factory=list)

    def __len__(self) -> int:
        return len(self.records)

    def __iter__(self) -> Iterator[CommandRecord]:
        return iter(self.records)

    @property
    def names(self) -> List[str]:
        return [r.name for r in self.records]

    @property
    def count(self) -> int:
        return len(self.records)

    def of(self, name: str) -> List[CommandRecord]:
        return [r for r in self.records if r.name == name]


class _Listener(monitoring.CommandListener):
    def started(self, event: monitoring.CommandStartedEvent) -> None:
        if not _active or event.command_name in _IGNORED:
            return
        rec = CommandRecord(event.command_name, event.database_name, dict(event.command), event.request_id)
        with _lock:
            for log in _active:
                log.records.append(rec)

    def succeeded(self, event: monitoring.CommandSucceededEvent) -> None:
        if not _active:
            return
        with _lock:
            for log in _active:
                for r in reversed(log.records):
                    if r.request_id == event.request_id:
                        r.duration_micros = event.duration_micros
                        break

    def failed(self, event: monitoring.CommandFailedEvent) -> None:
        if not _active:
            return
        with _lock:
            for log in _active:
                for r in reversed(log.records):
                    if r.request_id == event.request_id:
                        r.failed = True
                        break


LISTENER = _Listener()


@contextlib.contextmanager
def capture_commands() -> Iterator[CommandLog]:
    log = CommandLog()
    with _lock:
        _active.append(log)
    try:
        yield log
    finally:
        with _lock:
            _active.remove(log)


@contextlib.contextmanager
def assert_num_queries(expected: int, *, names: Optional[List[str]] = None) -> Iterator[CommandLog]:
    """Fail unless exactly ``expected`` commands (optionally only those in ``names``) hit the server."""
    with capture_commands() as log:
        yield log
    seen = [r for r in log.records if names is None or r.name in names]
    if len(seen) != expected:
        listing = "\n".join(f"  {i + 1}. {r.name} {r.command}" for i, r in enumerate(seen))
        raise AssertionError(f"Expected {expected} command(s), ran {len(seen)}:\n{listing}")
