"""Quickstart used by docs/tutorial/quickstart.md (executed by the test-suite)."""

import asyncio

import aggify
from aggify import Count, Document, IntField, Q, StringField


class Book(Document):
    title = StringField(required=True)
    author = StringField()
    year = IntField(min_value=0)

    class Meta:
        collection = "books"
        ordering = ["year"]


def sync_demo():
    Book.drop_collection()
    Book.objects.create(title="Dune", author="Herbert", year=1965)
    Book.objects.bulk_create(
        [Book(title="Emma", author="Austen", year=1815), Book(title="Ulysses", author="Joyce", year=1922)]
    )
    old = Book.objects.filter(year__lt=1950).order_by("-year")
    assert [b.title for b in old] == ["Ulysses", "Emma"]
    assert Book.objects.filter(Q(author="Herbert") | Q(year__lt=1900)).count() == 2
    book = Book.objects.get(title="Dune")
    book.year = 1966
    book.save()  # only {"$set": {"year": 1966}} is sent
    assert Book.objects.aggregate(first=aggify.Min("year"), n=Count()) == {"first": 1815, "n": 3}
    return True


async def async_demo():
    book = await Book.objects.acreate(title="Neuromancer", year=1984)
    assert await Book.objects.filter(year__gt=1980).acount() == 1
    async for b in Book.objects.filter(year__lt=1930):
        assert b.year < 1930
    await book.adelete()
    return True


def run():
    assert sync_demo()
    assert asyncio.run(async_demo())
    return True
