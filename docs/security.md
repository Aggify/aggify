# Security

Aggify is designed so that the *easy* way to write a query is also the safe one. This page lists the threats it
addresses, what it does about each, and what remains your responsibility.

## 1. NoSQL operator injection

The classic attack: a handler does `User.find({"username": body["username"]})` and the client posts
`{"username": {"$ne": ""}}` - logging in as anyone.

**In Aggify** every value is converted by its field *before* it can reach a query:

```python
User.objects.filter(username=request_json["username"])      # {"$ne": ""} -> ValidationError("Expected a string.")
User.objects.filter(age__in=request_json["ages"])            # a dict / string instead of a list -> error
User.objects.filter(attrs__color=request_json["c"])           # untyped value: any "$" key anywhere -> UnsafeQueryError
```

* `StringField` accepts only `str`, `IntField` only `int` (not `bool`), `ObjectIdField` only 24-hex ids...
* For untyped fields (`DictField`, `AnyField`, computed columns) values are scanned recursively and any mapping with a
  `$`-prefixed key is refused (`UnsafeQueryError`).
* `Q()` rejects dicts: there is no way to smuggle an operator document through `Q({...})`.
* `Raw(...)` is the one explicit escape hatch - grep for it in code review.
* `Model(**request_json)` raises `TypeError` for unknown keys (mass assignment) - unless you disable `strict`.
* Update operators validate their operands (`inc__x="5"`, `push__tags={"$each": ...}` fail), and the primary key
  cannot be updated.

!!! tip "Pair it with request validation"
    For HTTP/gRPC input use `pydantic_model(Model, for_input=True)` (forbids extra keys, enforces types and limits)
    and let Aggify's field validation be the second line of defence.

## 2. Regular-expression injection and ReDoS

`contains`, `startswith`, `endswith`, `iexact` and friends **escape** the user's text:
`name__contains="(a+)+$"` searches for that literal text. Only the explicit `regex` / `iregex` lookups accept a
pattern; they are length-capped (`max_regex_length`, default 256) and must compile.

## 3. Server-side JavaScript

`$where`, `$function` and `$accumulator` run JavaScript inside `mongod`. They are **rejected** in filters, raw
stages, `Raw(...)` and nested pipelines (`SecurityError`) unless you opt in:

```python
aggify.configure(allow_javascript=True)      # strongly discouraged
```

## 4. Field and path injection

Field names used as paths (annotation names, `$out` collections, dictionary keys, `rename__`) are validated: no `$`
prefix, no NUL, no empty segments. `DictField` keys may not start with `$` or contain `.`. Filter paths must exist
on the model (typos raise `InvalidField` instead of silently matching nothing); inside dictionaries a `$`-segment
(`attrs__$where`) is refused.

## 5. Resource exhaustion

| setting | default | effect |
|---|---|---|
| `max_limit` | `None` | largest allowed `limit()` / slice (set it on public APIs) |
| `max_in_list` | 100 000 | longest `__in` / `__nin` / `__all` list |
| `max_regex_length` | 256 | longest user-supplied regex |
| `max_value_depth` | 64 | nesting depth of untyped values |
| `default_max_time_ms` | `None` | applied to every find/aggregate/count (`maxTimeMS`) |

```python
aggify.configure(max_limit=200, default_max_time_ms=5_000)
```

`qs.options(max_time_ms=...)` and `pipeline.options(max_time_ms=..., allow_disk_use=...)` override per query.

## 6. Credentials and logging

`connect()` returns a config whose `repr()` masks the password (`mongodb://user:***@host/db`); the URI is never
included in exception messages. Prefer environment variables or a secrets manager for the URI, enable TLS
(`mongodb+srv://` or `?tls=true`) and give the application user the minimum roles it needs.

## 7. Data integrity

* Strict types: `bool` is not an `int`, `Decimal` is stored exactly (`Decimal128`), datetimes are UTC and timezone
  aware, `NaN` decimals are rejected, integers are range-checked against 64 bits.
* `save()` writes only changed fields, optional optimistic locking prevents lost updates.
* Transactions (replica set) for multi-document invariants.
* `unique=True` creates unique indexes (`ensure_indexes()`); violations surface as `NotUniqueError`.

## What is still on you

* Authentication/authorisation: Aggify filters data, it does not decide *whose* data to filter. Scope querysets by
  tenant/user in a custom `Manager.get_queryset()`.
* Network exposure and MongoDB hardening (bind addresses, authentication, TLS, backups).
* Anything you pass through `Raw(...)`, `Pipeline.stage()` / `raw()` or `allow_javascript=True`.
* Rate limiting and pagination limits on public endpoints.

Found a vulnerability? See [SECURITY.md](https://github.com/Aggify/aggify/blob/main/SECURITY.md).
