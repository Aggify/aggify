"""Exception hierarchy.

Everything raised on purpose by Aggify derives from :class:`AggifyError`, so a
single ``except AggifyError`` is enough to catch library errors in a web
handler or a gRPC servicer.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional


class AggifyError(Exception):
    """Base class of every Aggify exception."""

    message: str = ""

    def __init__(self, message: str = "", *args: Any) -> None:
        self.message = message or self.message
        super().__init__(self.message, *args)

    def __str__(self) -> str:
        return self.message


# Backwards compatible name used by Aggify 0.x
AggifyBaseException = AggifyError


# --------------------------------------------------------------------------- #
# configuration / connection
# --------------------------------------------------------------------------- #
class ConfigurationError(AggifyError):
    pass


class ConnectionNotConfigured(ConfigurationError):
    def __init__(self, alias: str = "default") -> None:
        super().__init__(
            f"No connection registered under alias {alias!r}. "
            "Call aggify.connect(...) first (or configure AGGIFY in Django settings)."
        )


class SynchronousOnlyOperation(AggifyError):
    """A blocking call was made from inside a running event loop."""


class AsyncOnlyOperation(AggifyError):
    pass


# --------------------------------------------------------------------------- #
# validation / schema
# --------------------------------------------------------------------------- #
class ValidationError(AggifyError):
    """Raised when a value does not satisfy a field.

    ``errors`` maps a field name to a list of messages, which makes the
    exception trivial to serialise into an HTTP 422 / gRPC INVALID_ARGUMENT.
    """

    def __init__(
        self,
        message: Any = "",
        *,
        field: Optional[str] = None,
        errors: Optional[Dict[str, List[str]]] = None,
    ) -> None:
        if isinstance(message, dict):
            errors = {k: ([v] if isinstance(v, str) else list(v)) for k, v in message.items()}
            message = ""
        self.errors: Dict[str, List[str]] = dict(errors or {})
        if field is not None and message:
            self.errors.setdefault(field, []).append(str(message))
        if not message:
            message = "; ".join(f"{k}: {', '.join(v)}" for k, v in self.errors.items())
        super().__init__(str(message))

    def prefixed(self, prefix: str) -> ValidationError:
        return ValidationError(
            errors={f"{prefix}.{k}" if k else prefix: v for k, v in self.errors.items()} or {prefix: [self.message]}
        )

    def as_dict(self) -> Dict[str, List[str]]:
        return {k: list(v) for k, v in self.errors.items()}


class InvalidField(AggifyError):
    def __init__(self, field: str = "", model: Any = None) -> None:
        self.field = field
        where = f" on {getattr(model, '__name__', model)}" if model is not None else ""
        super().__init__(f"Field {field!r} does not exist{where}.")


FieldDoesNotExist = InvalidField


class InvalidEmbeddedField(AggifyError):
    def __init__(self, field: str = "") -> None:
        super().__init__(f"Field {field!r} is not an embedded document field.")


class AlreadyExistsField(AggifyError):
    def __init__(self, field: str = "") -> None:
        super().__init__(f"Field {field!r} already exists.")


# --------------------------------------------------------------------------- #
# queries
# --------------------------------------------------------------------------- #
class QueryError(AggifyError):
    pass


class InvalidLookup(QueryError):
    def __init__(self, lookup: str = "", field: str = "") -> None:
        super().__init__(f"Unsupported lookup {lookup!r} for field {field!r}.")


class InvalidOperator(QueryError):
    def __init__(self, operator: str = "") -> None:
        super().__init__(f"Invalid operator {operator!r}.")


class InvalidArgument(QueryError):
    def __init__(self, message: str = "", expected_list: Optional[Iterable[Any]] = None) -> None:
        if expected_list is not None and not message:
            message = f"Invalid arguments, expected one of {list(expected_list)!r}."
        super().__init__(message)


class AggifyValueError(QueryError):
    def __init__(self, expected: Iterable[Any] = (), result: Any = None) -> None:
        super().__init__(f"Expected one of {list(expected)!r}, got {result!r}.")


class MongoIndexError(QueryError):
    def __init__(self, message: str = "") -> None:
        super().__init__(message or "Index is invalid, use a non-negative int or a slice without step.")


class PipelineError(QueryError):
    pass


InvalidPipelineStageError = PipelineError


class OutStageError(PipelineError):
    def __init__(self, stage: str = "") -> None:
        super().__init__(f"You cannot add a pipeline stage after $out/$merge (tried: {stage}).")


class AnnotationError(PipelineError):
    pass


class InvalidAnnotateExpression(AnnotationError):
    def __init__(self, message: str = "") -> None:
        super().__init__(message or "Annotation expressions must be aggregation expressions.")


class InvalidProjection(PipelineError):
    def __init__(self, message: str = "") -> None:
        super().__init__(message or "You cannot mix inclusion (1) and exclusion (0) in a projection.")


class ObjectDoesNotExist(AggifyError):
    """Base of every per-model ``DoesNotExist``."""


class MultipleObjectsReturned(AggifyError):
    pass


class NotUniqueError(AggifyError):
    pass


class OptimisticLockError(AggifyError):
    """The document was modified by someone else since it was loaded."""


class OperationFailure(AggifyError):
    pass


# --------------------------------------------------------------------------- #
# security
# --------------------------------------------------------------------------- #
class SecurityError(AggifyError):
    """A query/value was rejected because it looked like an injection attempt."""


class UnsafeQueryError(SecurityError):
    pass
