"""Model registry used to resolve string references ("Account", "shop.Account")."""

from __future__ import annotations

from typing import Any, Dict, List, Type, Union

from aggify.exceptions import ConfigurationError

_models: Dict[str, List[type]] = {}


def register(model: Any) -> None:
    for key in {model.__name__, model._meta.label}:
        bucket = _models.setdefault(key, [])
        # re-definition of the same class name (autoreload, tests) replaces the old entry
        bucket[:] = [
            m
            for m in bucket
            if not (
                m.__module__ == model.__module__
                and m.__qualname__ == model.__qualname__
                and getattr(getattr(m, "_meta", None), "label", None)
                == getattr(getattr(model, "_meta", None), "label", None)
            )
        ]
        bucket.append(model)


def resolve(ref: Union[str, type], *, relative_to: Any = None) -> type:
    if not isinstance(ref, str):
        return ref
    if ref == "self" and relative_to is not None:
        return relative_to
    candidates = _models.get(ref, [])
    if not candidates and "." in ref:
        candidates = _models.get(ref.rsplit(".", 1)[-1], [])
    if not candidates:
        raise ConfigurationError(f"Model {ref!r} is not registered (is it imported?).")
    if len(candidates) > 1:
        raise ConfigurationError(
            f"Model name {ref!r} is ambiguous ({[m.__module__ for m in candidates]}); use 'app_label.ModelName'."
        )
    return candidates[0]


Model = Type[Any]
