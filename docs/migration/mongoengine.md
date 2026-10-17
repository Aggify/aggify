# Migrating from MongoEngine

Aggify is not a fork of MongoEngine: documents, fields and queries were re-designed (async, immutable querysets,
strict validation). The **data stays compatible** - same collections, same BSON - so you can migrate model by model.

## Imports and connection

| MongoEngine | Aggify |
|---|---|
| `mongoengine.connect(db="x", host=...)` | `aggify.connect("mongodb://host/x")` |
| `from mongoengine import Document, fields` | `from aggify import Document, StringField, ...` (no `fields.` namespace; `import aggify.fields as fields` also works) |
| `meta = {"collection": "c", "ordering": ["-a"], "indexes": [...]}` | `class Meta: collection = "c"; ordering = ["-a"]; indexes = [Index(...)]` |
| `EmbeddedDocumentField` | `EmbeddedField` (alias kept) |
| `ListField(StringField())` | same |
| `ReferenceField("Other", db_field="other_id")` | same (stores the `_id`, no `DBRef`) |
| `Doc.objects(a=1)` | `Doc.objects.filter(a=1)` |
| `Doc.objects.get(...)` / `.first()` | same; async: `aget` / `afirst` |
| `doc.save()` | same; async: `await doc.asave()` |
| `Doc.objects.update(set__a=1, inc__b=1)` | same prefixes: `update(a=1, inc__b=1)` (`set__a` also works) |
| `Doc.objects.filter(Q(a=1) \| Q(b=2))` | same `Q` |
| `Doc.objects.aggregate(pipeline)` | `Aggify(Doc)...` builder, or pass raw stages with `Pipeline.raw()` |
| `Doc.objects.no_cache()` | `iterator()` / `aiterator()` |
| `queryset.only('a')` / `exclude('a')` | `only('a')` / `defer('a')` |
| `DoesNotExist` | `Doc.DoesNotExist` |
| `signals.post_save.connect(fn, sender=Doc)` | same names in `aggify` |

## Behavioural differences to know about

1. **Querysets are immutable** - `qs.filter(...)` returns a new queryset (MongoEngine's `__call__` mutated in place).
2. **Strict types**: `StringField` no longer coerces `5` to `"5"`; `filter(name={"$ne": ""})` raises instead of
   running. Conversions are lenient only where unambiguous (`"5"` -> `5`, 24-hex -> `ObjectId`).
3. **`None` is stored as "absent"** (as MongoEngine did) - `filter(x=None)` matches missing and null.
4. **Unknown constructor kwargs raise** (`strict`), unknown *stored* keys are ignored on load.
5. **Datetimes** are always timezone-aware UTC.
6. **Dirty tracking** is value-based: in-place mutation of lists/dicts/embedded documents is detected, and `save()`
   updates only changed fields.
7. **No `allow_inheritance` / `_cls`** (polymorphic documents) yet. Keep such collections on MongoEngine or model them
   with a discriminator field and abstract bases.
8. **References** are not auto-dereferenced: use `select_related`, `prefetch_related` or `afetch_related`. Lazy sync
   access works outside event loops.
9. `DictField` keys cannot start with `$` or contain `.`.
10. `GenericReferenceField`, `GridFS`, `SequenceField`, `CachedReferenceField`, `LazyReferenceField` are not provided.

## A suggested path

1. Install Aggify next to MongoEngine; both can talk to the same database.
2. Re-declare one model with Aggify (same `collection`, same `db_field`s). Document the indexes with `Index` -
   `ensure_indexes()` is a no-op for indexes that already exist with the same definition.
3. Move its call sites over. The sync API is deliberately close to MongoEngine's; the async API comes for free.
4. Replace hand-built `aggregate([...])` calls with querysets/pipelines using the [cookbook](../cookbook.md).
5. Delete the MongoEngine model.
