# Models & fields

## Documents

```python
class Account(Document):
    username = StringField(required=True, unique=True, min_length=2, max_length=30)
    email = EmailField()
    role = EnumField(Role, default=Role.USER)
    tags = ListField(StringField())
    address = EmbeddedField(Address)          # an EmbeddedDocument
    manager = ReferenceField("Account")       # string = lazy / self reference
    created_at = DateTimeField(auto_now_add=True)

    class Meta:
        collection = "accounts"
        ordering = ["username"]
        indexes = [Index("role", "-created_at"), Index("tags")]
```

* The primary key is `id` (`ObjectId`, stored as `_id`) unless a field declares `primary_key=True`. `obj.pk` always works.
* Documents are *strict*: `Account(nope=1)` raises `TypeError` (mass-assignment protection).
  Set `Meta.strict = False` or `aggify.configure(strict_models=False)` to ignore unknown keys.
* Mutable defaults are safe: `default=list` / `default=dict` and any callable are evaluated per instance.
* Values are converted when assigned (`acc.age = "41"` -> `41`) and validated strictly on `save()` / `full_clean()`.

### `Meta` options

| option | meaning |
|---|---|
| `collection` | collection name (default: snake_case class name) |
| `db_alias` | which `connect(alias=...)` connection to use |
| `ordering` | default ordering, `"-field"` for descending |
| `indexes` | list of [`Index`](#indexes) |
| `abstract` | base class only; fields and `Meta` are inherited |
| `version_field` | integer field enabling [optimistic locking](#optimistic-locking) |
| `get_latest_by` | default for `latest()` / `earliest()` |
| `strict` | reject unknown constructor keys |
| `validate_on_save` | run `full_clean()` in `save()` (default `True`) |

## Fields

| field | Python type | stored as | notes |
|---|---|---|---|
| `StringField` | `str` | string | `min_length`, `max_length`, `regex`, `strip`, `choices`; NUL bytes rejected |
| `EmailField`, `URLField`, `SlugField` | `str` | string | validated; e-mails are lower-cased |
| `IntField` (`LongField`) | `int` | int32/int64 | `min_value`, `max_value`; `bool` is *not* an int |
| `FloatField` | `float` | double | |
| `DecimalField` | `Decimal` | `Decimal128` | exact money maths, `max_digits`, `decimal_places` |
| `BooleanField` | `bool` | bool | |
| `DateTimeField` | tz-aware `datetime` (UTC) | date | naive values are assumed UTC; `auto_now`, `auto_now_add`; truncated to milliseconds like BSON |
| `DateField` | `date` | date (midnight UTC) | |
| `UUIDField` | `UUID` | binary subtype 4 | |
| `ObjectIdField` | `ObjectId` | ObjectId | accepts 24-hex strings |
| `BinaryField` | `bytes` | binary | `max_bytes` |
| `EnumField(Enum)` | enum member | `.value` | |
| `ListField(field)` | `list` | array | `min_length`, `max_length`; items validated by `field` |
| `DictField(field=None)` | `dict` | document | keys may not start with `$` or contain `.` |
| `EmbeddedField(Doc)` | `EmbeddedDocument` | sub-document | |
| `ReferenceField(Doc)` | related object / id | the target's `_id` | [see below](#references) |
| `GeoPointField` | `(lng, lat)` | GeoJSON `Point` | works with `Index(..., type="2dsphere")` and `__near` |
| `AnyField` | anything | as is | operator-looking keys are refused |

Common arguments: `db_field`, `default`, `required`, `unique`, `index`, `primary_key`, `choices`, `validators`,
`help_text`, `verbose_name`, `editable`.

!!! note "`None` means *absent*"
    Fields set to `None` are not stored (and are `$unset` on update), so `field=None` filters match both missing and
    null values, and documents stay compact.

## References

```python
class Post(Document):
    owner = ReferenceField(Account, db_field="owner_id")
```

```python
post.owner_id                # always available, never queries
post.owner                   # sync code: fetched on first access and cached
await post.afetch_related("owner")     # async code
Post.objects.select_related("owner")   # one query ($lookup)
Post.objects.prefetch_related("owner") # two queries ($in), good for many rows
Post.objects.filter(owner__country="DE")  # join happens automatically
```

Inside a running event loop `post.owner` raises `SynchronousOnlyOperation` with the fix in the message - lazy I/O
must never block the loop.

## Validation

```python
obj.full_clean()          # converts + validates every field, raises ValidationError(errors={"age": [...]})
```

Override `clean()` for cross-field rules. `ValidationError.errors` is a `{field: [messages]}` dict, ready for an
HTTP 422 or a gRPC `INVALID_ARGUMENT`.

## Saving

`save()`/`asave()` insert new instances and send **only changed fields** for loaded ones; mutations in place
(`doc.tags.append("x")`, `doc.address.city = "y"`) are detected.

```python
doc.save(update_fields=["age"])       # restrict the update
doc.save(validate=False)               # skip validation (values must already have the right types)
Model.objects.bulk_create(objs)        # insert_many in batches, validated up-front
```

Partial documents (`only()` / `defer()`) track and save just the fields that were loaded, so a partial `save()` can
never wipe the rest of the document.

### Optimistic locking

```python
class Wallet(Document):
    balance = IntField()
    version = IntField(default=0)
    class Meta:
        version_field = "version"
```

`save()` filters on the version it loaded and increments it atomically; a concurrent change makes it raise
`OptimisticLockError`, you reload and retry.

## Indexes

```python
class Meta:
    indexes = [
        Index("owner", "-created_at"),                     # compound
        Index("expires_at", ttl=3600),                     # TTL
        Index("title", "body", type="text"),               # text search -> Model.objects.search("...")
        Index("location", type="2dsphere"),
        Index("email", unique=True, partial={"email": {"$type": "string"}}),
    ]
```

`Model.ensure_indexes()` / `await Model.aensure_indexes()` are idempotent. Run them at deploy/start-up
(`aggify_indexes` management command in Django, `lifespan(..., indexes=[...])` in FastAPI).

## Managers & querysets

```python
class PostQuerySet(QuerySet):
    def published(self):
        return self.filter(published=True)

class Post(Document):
    objects = PostQuerySet.as_manager()

Post.objects.published().filter(...)
```

Override `Manager.get_queryset()` for default filters such as soft deletes.

## Signals

```python
from aggify import receiver, post_save

@receiver(post_save, sender=Order)
async def notify(sender, instance, created, **kw): ...
```

`pre_save`, `post_save`, `pre_delete`, `post_delete`, `post_init`. `save()` calls sync receivers, `asave()` awaits
async ones. Bulk operations (`update`, `delete`, `bulk_create`) do not send per-document signals.
