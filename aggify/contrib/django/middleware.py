"""Optional per-request transaction (needs a MongoDB replica set)."""

from __future__ import annotations

from asgiref.sync import iscoroutinefunction, markcoroutinefunction

import aggify


class AggifyAtomicMiddleware:
    """Wrap each request in a MongoDB transaction - works for WSGI and ASGI deployments."""

    sync_capable = True
    async_capable = True

    def __init__(self, get_response):
        self.get_response = get_response
        self._is_async = iscoroutinefunction(get_response)
        if self._is_async:
            markcoroutinefunction(self)

    def __call__(self, request):
        if self._is_async:
            return self.__acall__(request)
        with aggify.transaction():
            return self.get_response(request)

    async def __acall__(self, request):
        async with aggify.atransaction():
            return await self.get_response(request)
