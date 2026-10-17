"""Tiny signal dispatcher that understands both sync and async receivers.

    @receiver(post_save, sender=Order)
    def on_saved(sender, instance, created, **kw): ...

    @receiver(post_save, sender=Order)
    async def on_saved_async(sender, instance, created, **kw): ...

``send`` calls sync receivers (and returns awaitables for async ones that you must
await yourself - the library does that in the ``a*`` methods).  ``asend`` awaits.
"""

from __future__ import annotations

import inspect
import logging
import weakref
from typing import Any, Callable, List, Tuple

log = logging.getLogger("aggify.signals")


class Signal:
    def __init__(self, name: str = "") -> None:
        self.name = name
        self._receivers: List[Tuple[Callable[..., Any], Any, bool]] = []

    def connect(self, receiver: Callable[..., Any], sender: Any = None, weak: bool = False) -> Callable[..., Any]:
        ref: Any = weakref.WeakMethod(receiver) if weak and inspect.ismethod(receiver) else receiver
        self._receivers.append((ref, sender, weak))
        return receiver

    def disconnect(self, receiver: Callable[..., Any], sender: Any = None) -> bool:
        before = len(self._receivers)
        self._receivers = [
            r for r in self._receivers if not (self._deref(r[0]) is receiver and (sender is None or r[1] is sender))
        ]
        return len(self._receivers) < before

    @staticmethod
    def _deref(ref: Any) -> Any:
        return ref() if isinstance(ref, weakref.WeakMethod) else ref

    def _matching(self, sender: Any):
        for ref, snd, _ in list(self._receivers):
            fn = self._deref(ref)
            if fn is None:
                continue
            if (
                snd is None
                or snd is sender
                or (isinstance(sender, type) and isinstance(snd, type) and issubclass(sender, snd))
            ):
                yield fn

    def send(self, sender: Any, **kwargs: Any) -> List[Tuple[Callable[..., Any], Any]]:
        """Call sync receivers; the value of async receivers is an un-awaited coroutine."""
        return [(fn, fn(sender=sender, signal=self, **kwargs)) for fn in self._matching(sender)]

    async def asend(self, sender: Any, **kwargs: Any) -> List[Tuple[Callable[..., Any], Any]]:
        out = []
        for fn in self._matching(sender):
            result = fn(sender=sender, signal=self, **kwargs)
            if inspect.isawaitable(result):
                result = await result
            out.append((fn, result))
        return out

    def has_receivers(self, sender: Any) -> bool:
        return any(True for _ in self._matching(sender))


def receiver(signal: Any, **kwargs: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    signals = signal if isinstance(signal, (list, tuple)) else [signal]

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        for s in signals:
            s.connect(fn, **kwargs)
        return fn

    return decorator


pre_init = Signal("pre_init")
post_init = Signal("post_init")
pre_save = Signal("pre_save")
post_save = Signal("post_save")
pre_delete = Signal("pre_delete")
post_delete = Signal("post_delete")
