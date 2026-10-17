# Quickstart

## 1. Connect

```python
import aggify

aggify.connect("mongodb://localhost:27017/library")        # database name from the URI
aggify.connect("mongodb+srv://...", "analytics", alias="analytics", maxPoolSize=50)  # a second database
```

`connect()` only *registers* the settings - clients are created lazily the first time you query, so importing your
models never touches the network. Passwords are masked in `repr()` and logs.

## 2. Declare documents

```python
from aggify import Document, StringField, IntField

class Book(Document):
    title = StringField(required=True)
    author = StringField()
    year = IntField(min_value=0)

    class Meta:
        collection = "books"
        ordering = ["year"]
```

## 3. Use them - sync or async

=== "Sync"

    ```python
    Book.objects.create(title="Dune", author="Herbert", year=1965)
    old = Book.objects.filter(year__lt=1950).order_by("-year")
    for book in old: ...
    book = Book.objects.get(title="Dune")
    book.year = 1966
    book.save()                  # sends only {"$set": {"year": 1966}}
    ```

=== "Async"

    ```python
    await Book.objects.acreate(title="Dune", author="Herbert", year=1965)
    old = Book.objects.filter(year__lt=1950).order_by("-year")
    async for book in old: ...
    book = await Book.objects.aget(title="Dune")
    book.year = 1966
    await book.asave()
    ```

!!! tip "Sync code inside `async def`"
    Calling a blocking method from a running event loop raises `SynchronousOnlyOperation` instead of silently
    freezing your server - exactly like Django. Use the `a`-method or `asyncio.to_thread(...)`.

## 4. Query like Django

```python
from aggify import Q, F, Count, Avg

Book.objects.filter(Q(author="Herbert") | Q(year__lt=1900))
Book.objects.exclude(author__in=["A", "B"]).count()
Book.objects.filter(year__gt=F("pages") * 2)           # compare two fields
Book.objects.aggregate(books=Count(), mean_year=Avg("year"))
Book.objects.values("author").annotate(n=Count()).order_by("-n")
```

See what will be sent to MongoDB at any time:

```python
>>> Book.objects.filter(year__lt=1950).order_by("-year").query
{'find': {'filter': {'year': {'$lt': 1950}}, 'sort': [('year', -1)]}}
```

## 5. Indexes

Declare them on fields (`unique=True`, `index=True`) or in `Meta.indexes`, then create them at start-up:

```python
await Book.aensure_indexes()      # or Book.ensure_indexes()
```

The complete runnable version of this page is `docs/examples/quickstart.py` (it is part of the test-suite).

```python
--8<-- "quickstart.py"
```
