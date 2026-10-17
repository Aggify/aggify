"""Models shared by the test-suite."""

from __future__ import annotations

import enum

from aggify import (
    BooleanField, DateTimeField, DecimalField, DictField, Document, EmailField, EmbeddedDocument, EmbeddedField,
    EnumField, FloatField, GeoPointField, Index, IntField, ListField, ReferenceField, StringField,
)


class Role(enum.Enum):
    USER = "user"
    ADMIN = "admin"


class Address(EmbeddedDocument):
    city = StringField()
    zip_code = StringField(db_field="zip", max_length=10)
    geo = GeoPointField()


class Account(Document):
    username = StringField(required=True, unique=True, min_length=2, max_length=30)
    display_name = StringField()
    email = EmailField()
    age = IntField(min_value=0, max_value=150)
    score = FloatField()
    balance = DecimalField(max_digits=12, decimal_places=2)
    is_verified = BooleanField(default=False)
    role = EnumField(Role, default=Role.USER)
    tags = ListField(StringField())
    address = EmbeddedField(Address)
    attrs = DictField()
    created_at = DateTimeField(auto_now_add=True)
    deleted_at = IntField()

    class Meta:
        collection = "account"
        ordering = ["username"]
        indexes = [Index("age", "-created_at"), Index("tags")]


class PostStat(EmbeddedDocument):
    like_count = IntField(default=0)
    view_count = IntField(default=0)


class Comment(EmbeddedDocument):
    author = ReferenceField(Account, db_field="author_id")
    text = StringField(required=True)
    score = IntField(default=0)


class Post(Document):
    owner = ReferenceField(Account, db_field="owner_id")
    caption = StringField()
    location = StringField()
    hashtags = ListField(StringField())
    stat = EmbeddedField(PostStat)
    comments = ListField(EmbeddedField(Comment))
    likes = IntField(default=0)
    published = BooleanField(default=False)
    created_at = DateTimeField(auto_now_add=True)

    class Meta:
        collection = "post"


class Versioned(Document):
    title = StringField()
    version = IntField(default=0)

    class Meta:
        collection = "versioned"
        version_field = "version"


class Slug(Document):
    code = StringField(primary_key=True)
    label = StringField()

    class Meta:
        collection = "slug"


class SoftDeleteQuerySet:  # defined lazily in the manager test
    pass
