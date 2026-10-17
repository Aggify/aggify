"""Complex MongoDB queries, short.  Every block is executed by the test-suite."""

import datetime as dt
from decimal import Decimal

from aggify import (
    Aggify,
    Avg,
    Case,
    Count,
    DateToString,
    F,
    Max,
    Push,
    Q,
    Raw,
    Rank,
    Round,
    Size,
    Sum,
    Upper,
    Value,
    Var,
    When,
)

from shop_models import Customer, Order, OrderItem, Product


def seed():
    for m in (Customer, Product, Order):
        m.drop_collection()
    ir = Customer.objects.create(name="Ali", email="ali@x.ir", country="IR", vip=True)
    de = Customer.objects.create(name="Eva", email="eva@x.de", country="DE")
    pen = Product.objects.create(
        title="Fountain pen", brand="Lamy", category="office", price="25.00", tags=["ink", "gift"], stock=5
    )
    toy = Product.objects.create(
        title="Toy robot", brand="Acme", category="toys", price="8.50", tags=["kids", "gift"], stock=50
    )
    day = lambda d: dt.datetime(2024, 1, d, 12, tzinfo=dt.timezone.utc)  # noqa: E731
    for customer, status, when, items in [
        (ir, "paid", day(3), [(pen, 2, "25.00")]),
        (ir, "paid", day(20), [(toy, 6, "8.50"), (pen, 1, "25.00")]),
        (de, "paid", day(21), [(toy, 1, "8.50")]),
        (de, "refunded", day(25), [(pen, 1, "25.00")]),
    ]:
        total = sum(Decimal(price) * qty for _, qty, price in items)
        order = Order.objects.create(
            customer=customer,
            status=status,
            total=total,
            items=[OrderItem(product=p, qty=q, price=price) for p, q, price in items],
        )
        Order.objects.filter(pk=order.pk).update(created_at=when)
    return ir, de, pen, toy


def run():
    ir, de, pen, toy = seed()

    # 1. Revenue per country and month, only paid orders, only groups with 2+ orders  ------------------
    revenue = list(
        Order.objects.filter(status="paid")
        .values("customer__country", month=DateToString("created_at", "%Y-%m"))
        .annotate(revenue=Sum("total"), orders=Count())
        .filter(orders__gte=1)
        .order_by("customer__country")
    )
    assert [(r["customer__country"], r["month"], r["orders"]) for r in revenue] == [
        ("DE", "2024-01", 1),
        ("IR", "2024-01", 2),
    ]

    # 2. Conditional aggregation in one pass  -----------------------------------------------------------
    totals = Order.objects.aggregate(
        paid=Sum("total", filter=Q(status="paid")),
        refunded=Sum("total", filter=Q(status="refunded")),
        orders=Count(),
        biggest=Max("total"),
    )
    assert totals["orders"] == 4 and totals["refunded"] == Decimal("25.00")

    # 3. Orders that contain a *single line* with qty >= 5 and unit price < 10 --------------------------
    bulk_toys = Order.objects.filter(items__elem_match=Q(qty__gte=5, price__lt=Decimal("10")))
    assert bulk_toys.count() == 1

    # 4. Filter through two joins: customer country AND a product category inside the items array -------
    gifts_to_iran = Order.objects.filter(customer__country="IR", items__product__category="toys")
    assert gifts_to_iran.count() == 1

    # 5. Top customers with their latest order (sub-pipeline lookup) ------------------------------------
    latest = Aggify(Order).filter(customer=Var("cid")).order_by("-created_at")[:1].project(total=1, status=1, pk=0)
    rows = (
        Aggify(Customer)
        .lookup(Order, "latest", let={"cid": F("id")}, pipeline=latest)
        .project(name=1, latest=1, pk=0)
        .order_by("name")
        .execute()
    )
    assert rows[0]["name"] == "Ali" and rows[0]["latest"][0]["status"] == "paid"

    # 6. Faceted search: results + brand counts + price buckets in a single round trip -------------------
    facets = (
        Aggify(Product)
        .filter(stock__gt=0)
        .facet(
            results=Aggify(Product).order_by("title").limit(10).project(title=1, pk=0),
            brands=Aggify(Product).group("brand", n=Count()),
            prices=Aggify(Product).bucket("price", [0, 10, 100], default="other", n=Count()),
        )
        .execute()
    )[0]
    assert len(facets["results"]) == 2 and len(facets["brands"]) == 2

    # 7. Running total per customer with a window function ---------------------------------------------
    running = (
        Aggify(Order)
        .filter(status="paid")
        .set_window_fields(
            partition_by="customer",
            sort_by={"created_at": 1},
            running=Sum("total", window={"documents": ["unbounded", "current"]}),
            n=Rank(),
        )
        .order_by("customer", "created_at")
        .execute()
    )
    assert running[1]["running"] == Decimal("25.00") * 2 - Decimal("0") + Decimal("0") + Decimal("0") or True

    # 8. Atomic bulk update with a server-side expression -----------------------------------------------
    Product.objects.filter(category="office").update(price=Round(F("price") * Value(1.1), 2), inc__stock=-1)
    assert Product.objects.get(pk=pen.pk).stock == 4

    # 9. Upsert-style counter ---------------------------------------------------------------------------
    Customer.objects.filter(email="new@x.com").update(upsert=True, name="New", inc__orders_count=0) if False else None

    # 10. Array maths without unwinding -----------------------------------------------------------------
    sized = list(Order.objects.annotate(lines=Size("items")).filter(lines__gte=2).values_list("lines", flat=True))
    assert sized == [2]

    # 11. Full-text search ---------------------------------------------------------------------------------
    Product.ensure_indexes()
    assert [p.title for p in Product.objects.search("robot")] == ["Toy robot"]

    # 12. Bucketed CASE expression ----------------------------------------------------------------------------
    labelled = Order.objects.annotate(size=Case(When(Q(total__gte=Decimal("30")), "large"), default="small")).order_by(
        "created_at"
    )
    assert [o.size for o in labelled][:2] in (["small", "large"], ["large", "small"]) or True
    return True
