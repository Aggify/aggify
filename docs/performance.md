# Performance notes

* **Writes are minimal**: `save()` sends only changed fields. Use `update_fields=[...]` or queryset `update()` for hot
  paths; `bulk_create()` batches `insert_many`.
* **Reads**: `.only()` / `.defer()` shrink documents; `.values()` / `.values_list()` skip model instantiation
  (about 30 % faster than instances in `benchmarks/bench.py`); `iterator()` / `aiterator()` stream huge results.
* **N+1**: `select_related("owner")` (one `$lookup`) or `prefetch_related("owner")` (one extra `$in` query). Verify with
  `assert_num_queries`.
* **Counting**: `count()` uses `countDocuments`; slices respect skip/limit. `exists()` fetches one `_id`.
* **Indexes**: declare them, run `ensure_indexes()`, and check with `qs.explain()` that the plan is an `IXSCAN`.
  `$lookup` joins want an index on the foreign field (the target's `_id` for references - already indexed).
* **Pipelines**: put selective `filter()` first; MongoDB also pushes `$match` before `$lookup`/`$addFields` when it is
  independent of them. Use `options(allow_disk_use=True)` for big sorts/groups.
* **Pooling**: `connect(..., maxPoolSize=..., minPoolSize=...)` - one pool per event loop for async.
* **Overhead**: instantiating a document costs roughly 3 us on top of the driver; compiling a typical queryset roughly
  25 us. Run the benchmark yourself:

```bash
python benchmarks/bench.py mongodb://localhost:27017
```

Example output on a laptop (5 000 documents, local MongoDB):

```
read 5000 docs  raw pymongo (dicts)       7.5 ms
read 5000 docs  Aggify (instances)       16.3 ms
read 5000 docs  Aggify .values()         11.1 ms
group-by        raw pipeline              4.1 ms
group-by        Aggify                    4.3 ms
```
