# Changelog

## 1.0.0 (unreleased) - complete rewrite

Aggify is now a standalone, async-first MongoDB ODM with a Django-style API. **MongoEngine is no longer used or required.**

### Added
- `Document` / `EmbeddedDocument` with typed fields (strict validation, `Decimal128`, tz-aware UTC datetimes, enums,
  references, GeoJSON points, ...), `Meta` options, abstract bases, managers, signals, indexes.
- `QuerySet` / `Manager`: lazy, immutable, Django-style (`filter/exclude/order_by/values/annotate/aggregate/select_related/
  prefetch_related/only/defer/...`), automatic joins across references, `Q` and `F`, 25 lookups, update operators and
  expression updates, bulk operations. Every I/O method has an async twin (`aget`, `alist`, `asave`, ...).
- Native async via pymongo's `AsyncMongoClient` (one client per event loop), sync API on `MongoClient`.
- Change tracking: `save()` sends only changed fields; optional optimistic locking.
- Transactions: `transaction()` / `atransaction()` (ContextVar based, nestable).
- Expression library (arithmetic, strings, dates, arrays, `Case/When`, `Filter/Map/Reduce`, aggregates, windows).
- Rebuilt `Aggify` pipeline builder: immutable, schema-aware, all common stages (`$facet`, `$bucket`,
  `$setWindowFields`, `$graphLookup`, `$unionWith`, `$merge`, ...), sync + async execution.
- Integrations: Pydantic schema generation, FastAPI helpers, gRPC (`grpc.aio`) helpers, Django app (settings,
  shortcuts, forms, middleware, `aggify_indexes`, `ObjectIdField` for Django ORM models).
- Testing helpers: `capture_commands`, `assert_num_queries`, `assert_pipeline`, `temporary_database`.
- Documentation site (mkdocs) with a cookbook of complex queries; Persian overview.

### Security
- Operator injection prevented by field typing and recursive value scanning; `Raw()` is the only escape hatch.
- Regex lookups escape input (no regex injection / ReDoS); explicit `regex` is length-capped.
- Server-side JavaScript (`$where`, `$function`, `$accumulator`) blocked unless enabled.
- Validated field names/dictionary keys; strict constructors (mass assignment); resource limits
  (`max_limit`, `max_in_list`, `default_max_time_ms`); credentials masked in `repr`.

### Fixed (compared with 0.x)
- `$subtract` / `$divide` with 3 operands, mutation of shared `F` / `Q` objects, `$unwind` using the wrong path,
  silently dropped `$match` conditions, magic `let` substitution, `[n]` meaning `limit n`.

### Breaking
See the [upgrade guide](docs/migration/aggify-0.md).
