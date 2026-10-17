"""Models used by the documentation examples."""

import aggify
from aggify import (
    BooleanField,
    DateTimeField,
    DecimalField,
    Document,
    EmbeddedDocument,
    EmbeddedField,
    FloatField,
    Index,
    IntField,
    ListField,
    ReferenceField,
    StringField,
)


class Customer(Document):
    name = StringField(required=True)
    email = aggify.EmailField(unique=True)
    country = StringField(max_length=2, index=True)
    vip = BooleanField(default=False)
    created_at = DateTimeField(auto_now_add=True)

    class Meta:
        collection = "customers"
        ordering = ["name"]


class Product(Document):
    title = StringField(required=True)
    brand = StringField()
    category = StringField(index=True)
    price = DecimalField(max_digits=10, decimal_places=2)
    stock = IntField(default=0)
    tags = ListField(StringField())

    class Meta:
        collection = "products"
        indexes = [Index("title", "tags", type="text")]


class OrderItem(EmbeddedDocument):
    product = ReferenceField(Product)
    qty = IntField(min_value=1)
    price = DecimalField(max_digits=10, decimal_places=2)


class Order(Document):
    customer = ReferenceField(Customer, required=True)
    status = StringField(choices=["new", "paid", "refunded"], default="new")
    items = ListField(EmbeddedField(OrderItem))
    total = DecimalField(max_digits=12, decimal_places=2)
    created_at = DateTimeField(auto_now_add=True)

    class Meta:
        collection = "orders"
        ordering = ["-created_at"]
        indexes = [Index("customer", "-created_at"), Index("status", "created_at")]
