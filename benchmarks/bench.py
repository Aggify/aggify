"""Rough overhead of Aggify versus raw pymongo.  Run: python benchmarks/bench.py [mongodb-uri]"""

import asyncio
import statistics
import sys
import time

from pymongo import AsyncMongoClient, MongoClient

import aggify
from aggify import Count, Document, IntField, StringField, Sum

URI = sys.argv[1] if len(sys.argv) > 1 else "mongodb://localhost:27018/?directConnection=true"
N = 5000


class Item(Document):
    name = StringField()
    group = IntField()
    value = IntField()

    class Meta:
        collection = "bench_item"


def timeit(fn, repeat=7):
    samples = []
    for _ in range(repeat):
        t = time.perf_counter()
        fn()
        samples.append(time.perf_counter() - t)
    return statistics.median(samples) * 1000


def main():
    aggify.connect(URI, "aggify_bench")
    client = MongoClient(URI, tz_aware=True)
    coll = client["aggify_bench"]["bench_item"]
    coll.drop()
    coll.insert_many([{"name": f"n{i}", "group": i % 10, "value": i} for i in range(N)])
    rows = []

    rows.append(("read 5000 docs  raw pymongo (dicts)", timeit(lambda: list(coll.find({"value": {"$gte": 0}})))))
    rows.append(("read 5000 docs  Aggify (instances)", timeit(lambda: list(Item.objects.filter(value__gte=0)))))
    rows.append(
        (
            "read 5000 docs  Aggify .values()",
            timeit(lambda: list(Item.objects.filter(value__gte=0).values("name", "value"))),
        )
    )
    rows.append(
        (
            "group-by       raw pipeline",
            timeit(
                lambda: list(coll.aggregate([{"$group": {"_id": "$group", "n": {"$sum": 1}, "s": {"$sum": "$value"}}}]))
            ),
        )
    )
    rows.append(
        (
            "group-by       Aggify",
            timeit(lambda: list(Item.objects.values("group").annotate(n=Count(), s=Sum("value")))),
        )
    )
    rows.append(
        (
            "build query    Aggify compile only (x1000)",
            timeit(
                lambda: [
                    Item.objects.filter(value__gte=1, name__startswith="n").order_by("-value")[:10].query
                    for _ in range(1000)
                ]
            ),
        )
    )
    ins = [{"name": f"w{i}", "group": 1, "value": i} for i in range(1000)]
    rows.append(
        (
            "insert 1000    raw insert_many",
            timeit(
                lambda: (
                    coll.delete_many({"group": 1, "name": {"$regex": "^w"}}),
                    coll.insert_many([dict(d) for d in ins]),
                )
            ),
        )
    )
    rows.append(
        (
            "insert 1000    Aggify bulk_create",
            timeit(
                lambda: (
                    Item.objects.filter(name__startswith="w").delete(),
                    Item.objects.bulk_create([Item(**d) for d in ins]),
                )
            ),
        )
    )

    async def concurrent():
        await asyncio.gather(*(Item.objects.filter(group=i % 10).acount() for i in range(200)))

    rows.append(("200 concurrent async counts", timeit(lambda: asyncio.run(concurrent()))))
    width = max(len(r[0]) for r in rows)
    for name, ms in rows:
        print(f"{name:<{width}}  {ms:9.2f} ms")
    coll.drop()
    client.close()


if __name__ == "__main__":
    main()
