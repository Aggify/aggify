"""Fields for *Django ORM* models that point at MongoDB documents.

class Invoice(models.Model):                       # lives in PostgreSQL / MySQL / SQLite
    customer = ObjectIdField(db_index=True)        # -> aggify Customer document _id
    total = models.DecimalField(...)

customer = await Customer.objects.aget(pk=invoice.customer)
"""

from __future__ import annotations

from typing import Any

from bson import ObjectId
from django.core import exceptions
from django.db import models


class ObjectIdField(models.CharField):
    description = "A MongoDB ObjectId stored as a 24 character string"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs["max_length"] = 24
        super().__init__(*args, **kwargs)

    def deconstruct(self):
        name, path, args, kwargs = super().deconstruct()
        kwargs.pop("max_length", None)
        return name, path, args, kwargs

    def from_db_value(self, value: Any, expression: Any, connection: Any) -> Any:
        return None if value is None else ObjectId(value)

    def to_python(self, value: Any) -> Any:
        if value is None or isinstance(value, ObjectId):
            return value
        try:
            return ObjectId(str(value))
        except Exception:
            raise exceptions.ValidationError("Enter a valid ObjectId.", code="invalid") from None

    def get_prep_value(self, value: Any) -> Any:
        value = self.to_python(value)
        return None if value is None else str(value)
