# Django

Aggify's Django support is optional and does not make Django a dependency of the core.

```bash
pip install "aggify[django]"
```

```python
# settings.py
INSTALLED_APPS = [..., "aggify.contrib.django"]

AGGIFY = {
    "default": {"URI": "mongodb://localhost:27017", "NAME": "shop", "OPTIONS": {"maxPoolSize": 50}},
    "analytics": {"URI": "mongodb+srv://...", "NAME": "stats"},      # models choose with Meta.db_alias
}
AGGIFY_SETTINGS = {"default_max_time_ms": 5000, "max_limit": 500}     # optional, see Security
# or simply: MONGODB_URI = "mongodb://localhost/shop"
```

`AggifyConfig.ready()` registers the connections. Documents live anywhere you like (`app/documents.py`, `models.py`).

## Using documents like models

```python
# sync view
from aggify.contrib.django.shortcuts import get_object_or_404, aget_object_or_404, get_list_or_404

def detail(request, username):
    account = get_object_or_404(Account, username=username)
    ...

# async view (Django >= 4.1)
async def stats(request):
    rows = await Order.objects.values("status").annotate(n=Count()).alist()
    return JsonResponse({"rows": rows})
```

* `Model.objects`, `Model.DoesNotExist`, `Q`, `F`, `get_or_create`, `update_or_create`, `in_bulk`, signals, managers.
* **Pagination**: `django.core.paginator.Paginator(Account.objects.order_by("username"), 25)` works as is
  (`count()` + slicing).
* **Forms**: `document_form(Account, fields=["username", "age"])` builds a `django.forms.Form`; `form.save()` /
  `await form.asave()` validate through the document and persist it; pass `instance=` to edit.
* **Management command**: `python manage.py aggify_indexes [Model ...]` creates the declared indexes.
* **Transactions per request** (replica set): add `"aggify.contrib.django.middleware.AggifyAtomicMiddleware"`; it works for
  WSGI and ASGI.
* **Tests**: nothing special; use a throw-away database (`aggify.testing.temporary_database`).

## Connecting to the Django ORM

Mix SQL and Mongo in one project by storing Mongo ids in Django models, and Django pks in documents:

```python
from aggify.contrib.django.fields import ObjectIdField

class Invoice(models.Model):                      # PostgreSQL / MySQL / SQLite
    customer = ObjectIdField(db_index=True)       # Mongo Customer._id (24-char column, returns bson.ObjectId)
    total = models.DecimalField(max_digits=12, decimal_places=2)

customer = await Customer.objects.aget(pk=invoice.customer)

class Event(Document):
    user_id = IntField(index=True)                # Django auth user pk
    payload = DictField()

Event.objects.filter(user_id=request.user.pk)
```

Use `Document.to_dict(json_safe=True)` for DRF/JSON responses, or `pydantic_model` for schema-based APIs (django-ninja
accepts Pydantic models directly).

## Sync, async and WSGI/ASGI

Under WSGI (gunicorn sync workers) use the sync API. Under ASGI use the `a*` methods in async views and the sync API in
sync views (Django runs those in a thread). Aggify keeps one async client per event loop, so `async_to_sync` /
`sync_to_async` boundaries are safe.
