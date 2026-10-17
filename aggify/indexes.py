"""Index declarations (``Meta.indexes``) and ``ensure_indexes`` support."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from pymongo import ASCENDING, DESCENDING, GEO2D, GEOSPHERE, HASHED, TEXT, IndexModel

from aggify.exceptions import ConfigurationError

_TYPES = {"text": TEXT, "2dsphere": GEOSPHERE, "2d": GEO2D, "hashed": HASHED}


class Index:
    """``Index("owner", "-created_at", unique=True)`` - a leading ``-`` means descending.

    Special types: ``Index("title", type="text")``, ``Index("loc", type="2dsphere")``.
    ``ttl`` (seconds) makes a TTL index, ``partial`` is a query dict for ``partialFilterExpression``.
    """

    def __init__(
        self,
        *fields: str,
        name: Optional[str] = None,
        unique: bool = False,
        sparse: bool = False,
        ttl: Optional[int] = None,
        partial: Optional[Dict[str, Any]] = None,
        type: Optional[str] = None,  # noqa: A002
        background: bool = True,
        collation: Optional[Dict[str, Any]] = None,
        weights: Optional[Dict[str, int]] = None,
        default_language: Optional[str] = None,
    ) -> None:
        if not fields:
            raise ConfigurationError("Index() needs at least one field.")
        if type is not None and type not in _TYPES:
            raise ConfigurationError(f"Unknown index type {type!r}; use one of {sorted(_TYPES)}.")
        self.fields = fields
        self.name, self.unique, self.sparse, self.ttl = name, unique, sparse, ttl
        self.partial, self.type, self.collation = partial, type, collation
        self.weights, self.default_language = weights, default_language

    def to_model(self, model: Any) -> IndexModel:
        keys: List[Tuple[str, Any]] = []
        for spec in self.fields:
            desc = spec.startswith("-")
            plain = spec.lstrip("+-")
            keys.append(
                (_db_path(model, plain), _TYPES[self.type] if self.type else (DESCENDING if desc else ASCENDING))
            )
        kwargs: Dict[str, Any] = {}
        if self.name:
            kwargs["name"] = self.name
        if self.unique:
            kwargs["unique"] = True
        if self.sparse:
            kwargs["sparse"] = True
        if self.ttl is not None:
            kwargs["expireAfterSeconds"] = self.ttl
        if self.partial:
            kwargs["partialFilterExpression"] = self.partial
        if self.collation:
            kwargs["collation"] = self.collation
        if self.weights:
            kwargs["weights"] = {_db_path(model, k): v for k, v in self.weights.items()}
        if self.default_language:
            kwargs["default_language"] = self.default_language
        return IndexModel(keys, **kwargs)


def _db_path(model: Any, path: str) -> str:
    from aggify.schema import Scope, resolve_path

    r = resolve_path(Scope.for_model(model), path, with_lookup=False, allow_joins=False)
    return r.db_path


def collect_index_models(model: Any) -> List[IndexModel]:
    meta = model._meta
    models: List[IndexModel] = []
    for field in meta.fields.values():
        if field.primary_key:
            continue
        if field.unique:
            models.append(
                IndexModel([(field.db_field, ASCENDING)], unique=True, sparse=True if not field.required else False)
            )
        elif field.index:
            models.append(IndexModel([(field.db_field, ASCENDING)]))
    for idx in meta.indexes:
        if isinstance(idx, str):
            idx = Index(idx)
        elif isinstance(idx, (list, tuple)):
            idx = Index(*idx)
        models.append(idx.to_model(model))
    return models
