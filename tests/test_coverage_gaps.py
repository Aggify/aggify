"""Smaller behaviours: testing helpers, registry, settings, signals, index specs, exotic lookups."""

import gc

import pytest

import aggify
from aggify import ConfigurationError, Document, Index, Q, StringField, ValidationError, configure
from aggify.indexes import collect_index_models
from aggify.testing import assert_pipeline, assert_query
from tests.models import Account, Post


def test_assert_helpers():
    assert_query(Account.objects.filter(age=3), filter={"age": 3})
    assert_pipeline(
        Post.objects.filter(owner__username="x"),
        [
            {"$lookup": {"from": "account", "localField": "owner_id", "foreignField": "_id", "as": "_j_owner"}},
            {"$unwind": {"path": "$_j_owner", "preserveNullAndEmptyArrays": True}},
            {"$match": {"_j_owner.username": "x"}},
            {"$unset": ["_j_owner"]},
        ],
    )
    with pytest.raises(AssertionError):
        assert_query(Account.objects.filter(age=3), filter={"age": 4})
    with pytest.raises(AssertionError):
        assert_query(Post.objects.filter(owner__username="x"))
    with pytest.raises(AssertionError):
        assert_pipeline(Account.objects.filter(age=3), [])


@pytest.mark.mongo
def test_temporary_database_is_isolated_and_dropped():
    from pymongo import MongoClient
    from aggify.testing import temporary_database
    from tests.conftest import MONGO_URI

    with temporary_database(MONGO_URI, alias="tmp-db") as name:

        class Scratch(Document):
            x = StringField()

            class Meta:
                db_alias = "tmp-db"
                collection = "scratch"

        Scratch.objects.create(x="1")
        assert Scratch.objects.count() == 1
    client = MongoClient(MONGO_URI)
    try:
        assert name not in client.list_database_names()
    finally:
        client.close()


def test_registry_ambiguity_and_missing():
    class Twin(Document):
        class Meta:
            app_label = "one"

    twin_one = Twin

    class Twin(Document):  # noqa: F811
        class Meta:
            app_label = "two"

    from aggify import registry

    assert registry.resolve("one.Twin") is twin_one
    with pytest.raises(ConfigurationError, match="ambiguous"):
        registry.resolve("Twin")
    with pytest.raises(ConfigurationError, match="not registered"):
        registry.resolve("NopeNeverDefined")


def test_configure_rejects_unknown_settings():
    with pytest.raises(TypeError):
        configure(not_a_setting=1)
    assert configure(max_limit=5).max_limit == 5


def test_signal_disconnect_and_weak_receivers():
    from aggify import Signal

    sig = Signal("t")
    calls = []

    class Handler:
        def handle(self, sender, **kw):
            calls.append(sender)

    h = Handler()
    sig.connect(h.handle, weak=True)
    sig.send("a")
    del h
    gc.collect()
    sig.send("b")
    assert calls == ["a"]
    fn = lambda sender, **kw: calls.append("x")  # noqa: E731
    sig.connect(fn)
    assert sig.disconnect(fn) is True and sig.disconnect(fn) is False
    assert not sig.has_receivers("a")


def test_index_specs():
    from aggify import DateTimeField, GeoPointField

    class Place(Document):
        name = StringField(unique=True)
        slug = StringField(index=True)
        seen = DateTimeField()
        where = GeoPointField()

        class Meta:
            indexes = [Index("-seen", "name", sparse=True), "slug", ("name", "slug"), Index("where", type="2dsphere")]

    models = collect_index_models(Place)
    docs = [m.document for m in models]
    assert {"key": {"name": 1}, "name": "name_1", "unique": True, "sparse": True} in docs
    assert any(d["key"] == {"seen": -1, "name": 1} and d.get("sparse") for d in docs)
    assert any(d["key"] == {"where": "2dsphere"} for d in docs)
    with pytest.raises(ConfigurationError):
        Index()
    with pytest.raises(ConfigurationError):
        Index("x", type="bogus")


def test_exotic_lookup_compilation():
    assert Account.objects.filter(age__mod=(2, 0)).query["find"]["filter"] == {"age": {"$mod": [2, 0]}}
    assert Account.objects.filter(age__type="int").query["find"]["filter"] == {"age": {"$type": "int"}}
    with pytest.raises(ValidationError):
        Account.objects.filter(age__type="nonsense").query
    with pytest.raises(ValidationError):
        Account.objects.filter(age__mod=(1,)).query
    assert Post.objects.filter(stat__like_count__gte=1).query["find"]["filter"] == {"stat.like_count": {"$gte": 1}}


@pytest.mark.mongo
def test_geo_near_query_and_pipeline(clean_db):
    from aggify import Aggify, Document, GeoPointField, Index

    class Cafe(Document):
        name = StringField()
        where = GeoPointField()

        class Meta:
            collection = "cafes_geo"
            indexes = [Index("where", type="2dsphere")]

    try:
        Cafe.ensure_indexes()
        Cafe.objects.bulk_create([Cafe(name="near", where=(51.40, 35.70)), Cafe(name="far", where=(10.0, 10.0))])
        assert [c.name for c in Cafe.objects.filter(where__near=(51.401, 35.701, 5000))] == ["near"]
        rows = Aggify(Cafe).geo_near((51.4, 35.7), "dist", max_distance=5000).execute()
        assert [r["name"] for r in rows] == ["near"] and rows[0]["dist"] < 500
    finally:
        Cafe.drop_collection()


@pytest.mark.mongo
def test_binary_uuid_decimal_enum_roundtrip(clean_db):
    import decimal
    import uuid

    from aggify import BinaryField, DecimalField, Document, UUIDField

    class Blob(Document):
        key = UUIDField()
        data = BinaryField()
        amount = DecimalField()

        class Meta:
            collection = "blobs_rt"

    try:
        u = uuid.uuid4()
        Blob.objects.create(key=u, data=b"\x00\x01", amount="12.340")
        b = Blob.objects.get(key=u)
        assert b.data == b"\x00\x01" and b.amount == decimal.Decimal("12.340") and b.key == u
        assert Blob.objects.filter(amount__gt="12.3").count() == 1
    finally:
        Blob.drop_collection()


def test_query_property_of_none_and_repr():
    assert Account.objects.none().query == {"none": True}
    assert "QuerySet Account" in repr(Account.objects.filter(age=1))


def test_q_repr_and_bool():
    assert not Q() and Q(a=1) and repr(~Q(a=1)).startswith("~Q")
