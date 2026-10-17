from __future__ import annotations

from typing import Any, List

from django.http import Http404

from aggify.exceptions import ObjectDoesNotExist, ValidationError


def _qs(klass: Any) -> Any:
    return klass.objects.all() if hasattr(klass, "objects") else (klass.all() if hasattr(klass, "all") else klass)


def get_object_or_404(klass: Any, *args: Any, **kwargs: Any) -> Any:
    try:
        return _qs(klass).get(*args, **kwargs)
    except (ObjectDoesNotExist, ValidationError):
        raise Http404(f"No {getattr(klass, '__name__', 'object')} matches the given query.") from None


async def aget_object_or_404(klass: Any, *args: Any, **kwargs: Any) -> Any:
    try:
        return await _qs(klass).aget(*args, **kwargs)
    except (ObjectDoesNotExist, ValidationError):
        raise Http404(f"No {getattr(klass, '__name__', 'object')} matches the given query.") from None


def get_list_or_404(klass: Any, *args: Any, **kwargs: Any) -> List[Any]:
    items = list(_qs(klass).filter(*args, **kwargs))
    if not items:
        raise Http404("No objects match the given query.")
    return items
