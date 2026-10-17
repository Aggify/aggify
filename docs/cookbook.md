# Cookbook: complex MongoDB queries, short

Every snippet below is executed by Aggify's test-suite against a real MongoDB
(`docs/examples/complex_queries.py`), using these models:

```python
--8<-- "shop_models.py"
```

## Revenue per country and month (join + computed group key + HAVING + sort)

=== "Aggify"

    ```python
    Order.objects.filter(status="paid") \
        .values("customer__country", month=DateToString("created_at", "%Y-%m")) \
        .annotate(revenue=Sum("total"), orders=Count()) \
        .filter(orders__gte=10) \
        .order_by("customer__country")
    ```

=== "Raw pymongo (roughly)"

    ```python
    [
      {"$match": {"status": "paid"}},
      {"$lookup": {"from": "customers", "localField": "customer", "foreignField": "_id", "as": "_c"}},
      {"$unwind": {"path": "$_c", "preserveNullAndEmptyArrays": True}},
      {"$addFields": {"month": {"$dateToString": {"date": "$created_at", "format": "%Y-%m"}}}},
      {"$group": {"_id": {"customer__country": "$_c.country", "month": "$month"},
                  "revenue": {"$sum": "$total"}, "orders": {"$sum": 1}}},
      {"$project": {"customer__country": "$_id.customer__country", "month": "$_id.month",
                    "revenue": 1, "orders": 1, "_id": 0}},
      {"$match": {"orders": {"$gte": 10}}},
      {"$sort": {"customer__country": 1}},
    ]
    ```

## Several conditional totals in one pass

```python
Order.objects.aggregate(
    paid=Sum("total", filter=Q(status="paid")),
    refunded=Sum("total", filter=Q(status="refunded")),
    orders=Count(),
    biggest=Max("total"),
)
```

## One line item matching two conditions (`$elemMatch`)

```python
Order.objects.filter(items__elem_match=Q(qty__gte=5, price__lt=10))
```

## Filter through two joins, including a list of embedded references

```python
Order.objects.filter(customer__country="IR", items__product__category="toys")
```

## Latest order of every customer (correlated `$lookup` sub-pipeline)

```python
latest = Aggify(Order).filter(customer=Var("cid")).order_by("-created_at")[:1].project(total=1, status=1, pk=0)
Aggify(Customer).lookup(Order, "latest", let={"cid": F("id")}, pipeline=latest).execute()
```

## Search page: results, brand counts and price buckets in one round trip (`$facet`)

```python
Aggify(Product).filter(stock__gt=0).facet(
    results=Aggify(Product).order_by("title").limit(10),
    brands=Aggify(Product).group("brand", n=Count()),
    prices=Aggify(Product).bucket("price", [0, 10, 100], default="other", n=Count()),
).execute()
```

## Running totals (window functions)

```python
Aggify(Order).filter(status="paid").set_window_fields(
    partition_by="customer", sort_by={"created_at": 1},
    running=Sum("total", window={"documents": ["unbounded", "current"]}), nth=Rank(),
)
```

## Atomic server-side updates

```python
Product.objects.filter(category="office").update(price=Round(F("price") * Value(1.1), 2), inc__stock=-1)
Counter.objects.filter(key="visits").update(upsert=True, inc__n=1)
```

## Array maths without `$unwind`

```python
Order.objects.annotate(lines=Size("items"), units=Sum("items__qty")).filter(lines__gte=2)
```

## Full-text search

```python
Product.ensure_indexes()                       # Index("title", "tags", type="text")
Product.objects.search("robot")
```

## The whole file

```python
--8<-- "complex_queries.py"
```
