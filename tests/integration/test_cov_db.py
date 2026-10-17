"""Database-backed tests for the remaining queryset / document / connection branches."""

import asyncio
import os

import pytest

import aggify
from aggify import (
    Aggify,
    Count,
    F,
    InvalidArgument,
    InvalidField,
    NotUniqueError,
    Q,
    QuerySet,
    Sum,
    SynchronousOnlyOperation,
)
from aggify.exceptions import ConfigurationError
from aggify.monitoring import capture_commands
from tests.conftest import MONGO_URI
from tests.models import Account, Comment, Post


@pytest.fixture
def seeded(clean_db):
    accs = [Account.objects.create(username=f"user{i}", age=i, tags=["t"]) for i in range(4)]
    for a in accs:
        Post.objects.create(owner=a, caption=f"c{a.age}", likes=a.age)
    return accs


class TestConnectionBranches:
    def test_bad_uri_and_missing_database(self):
        with pytest.raises(ConfigurationError, match="Invalid MongoDB URI"):
            aggify.connect("definitely not a uri", alias="bad")
        with pytest.raises(ConfigurationError, match="No database name"):
            aggify.connect("mongodb://localhost:27017", alias="nodb")

    def test_reconnecting_with_other_options_replaces_client(self):
        aggify.connect(MONGO_URI, "aggify_reconnect", alias="rc")
        try:
            first = aggify.get_client("rc")
            aggify.connect(MONGO_URI, "aggify_reconnect", alias="rc", maxPoolSize=3)
            assert aggify.get_client("rc") is not first and aggify.is_connected("rc")
            assert not aggify.is_connected("never")
        finally:
            aggify.disconnect("rc")

    async def test_unsafe_escape_hatch_env(self, monkeypatch):
        monkeypatch.setenv("AGGIFY_ALLOW_ASYNC_UNSAFE", "1")
        assert await asyncio.to_thread(lambda: True)
        aggify.connection.check_sync_allowed()  # returns instead of raising inside the loop
        monkeypatch.delenv("AGGIFY_ALLOW_ASYNC_UNSAFE")
        with pytest.raises(SynchronousOnlyOperation):
            aggify.connection.check_sync_allowed()

    def test_async_client_needs_a_loop(self):
        with pytest.raises(ConfigurationError, match="running event loop"):
            aggify.get_async_client()

    async def test_aclose_clients_and_nested_atransaction(self, clean_db):
        await Account.objects.acount()
        await aggify.aclose_clients()
        await Account.objects.acount()  # lazily recreated
        await aggify.aclose_clients("default")

    @pytest.mark.replica
    async def test_nested_atransaction_joins_outer(self, clean_db):
        await Account.aensure_indexes()
        async with aggify.atransaction() as outer:
            async with aggify.atransaction() as inner:
                assert inner is outer

    async def test_adisconnect_alias_roundtrip(self):
        aggify.connect(MONGO_URI, "aggify_tmp_disc", alias="tmpd")
        client = aggify.get_async_client("tmpd")
        assert client is not None
        await aggify.adisconnect("tmpd")
        assert not aggify.is_connected("tmpd")


class TestDocumentBranches:
    def test_fetch_related_sync(self, seeded):
        post = Post.objects.get(caption="c1")
        assert post.fetch_related("owner").owner.username == "user1"
        post.fetch_related("owner")  # already loaded: nothing to do

    async def test_async_save_of_deleted_document_reinserts_and_refresh_missing(self, seeded):
        acc = seeded[0]
        await Account.objects.filter(pk=acc.pk).adelete()
        acc.age = 99
        await acc.asave()
        assert (await Account.objects.aget(pk=acc.pk)).age == 99
        await acc.adelete()
        with pytest.raises(Account.DoesNotExist):
            await acc.arefresh_from_db()
        await Account.adrop_collection()
        assert await Account.objects.acount() == 0

    async def test_async_save_without_changes_and_update_fields(self, seeded):
        acc = await Account.objects.aget(username="user1")
        acc.age, acc.display_name = 50, "x"
        await acc.asave(update_fields=["age"])
        again = await Account.objects.aget(username="user1")
        assert (again.age, again.display_name) == (50, None)

    def test_delete_pk_required(self):
        with pytest.raises(ValueError):
            Account(id=None, username="ab").delete()


class TestQuerySetBranches:
    def test_argument_validation(self, seeded):
        with pytest.raises(InvalidArgument):
            Account.objects.values_list("a", "b", flat=True)
        with pytest.raises(InvalidArgument):
            Account.objects.annotate(F("age"))
        with pytest.raises(TypeError):
            Account.objects.all()["x"]
        with pytest.raises(InvalidField):
            list(Account.objects.only("nope"))
        with pytest.raises(InvalidField):
            list(Account.objects.defer("nope"))
        with pytest.raises(InvalidField):
            list(Post.objects.select_related("caption"))
        with pytest.raises(InvalidField):
            list(Account.objects.prefetch_related("age"))
        with pytest.raises(InvalidArgument):
            list(Post.objects.values("caption").prefetch_related("owner"))
        with pytest.raises(InvalidArgument):
            Account.objects.latest()
        with pytest.raises(InvalidArgument):
            Account.objects.aggregate()
        with pytest.raises(InvalidArgument):
            Account.objects.order_by(5).query
        with pytest.raises(InvalidArgument):
            Account.objects.order_by(F("age") + 1).query

    def test_options_hint_and_text_language(self, seeded):
        Account.ensure_indexes()
        qs = Account.objects.hint("username_1").options(max_time_ms=5000, batch_size=2, comment="c")
        assert len(list(qs)) == 4
        assert len(list(Account.objects.options(max_time_ms=5000).filter(F("age") > 1).annotate(x=F("age")))) == 2
        assert (
            Account.objects.search("x", language="english").query["find"]["filter"]["$text"]["$language"] == "english"
        )

    def test_default_max_time_is_applied(self, seeded):
        aggify.configure(default_max_time_ms=9999)
        with capture_commands() as log:
            list(Account.objects.all())
            Account.objects.count()
            Account.objects.annotate(x=F("age")).count()
            Account.objects.distinct("age")
        assert all(r.command.get("maxTimeMS") == 9999 for r in log.records)

    def test_cached_indexing_and_len_count(self, seeded):
        qs = Account.objects.order_by("age")
        list(qs)
        assert qs[1].age == 1 and qs.count() == 4 and qs.list()[0].age == 0

    def test_ordering_with_expressions(self, seeded):
        assert [a.age for a in Account.objects.order_by(F("age").desc())] == [3, 2, 1, 0]
        assert [a.age for a in Account.objects.order_by(F("age"))] == [0, 1, 2, 3]
        assert [a.age for a in Account.objects.order_by(F("age")).reverse()] == [3, 2, 1, 0]
        assert [a.age for a in Account.objects.order_by(F("age").desc()).reverse()] == [0, 1, 2, 3]

    def test_pipeline_skip_with_annotation_and_select_related_with_only(self, seeded):
        assert [a.age for a in Account.objects.annotate(x=F("age")).order_by("age")[2:]] == [2, 3]
        post = Post.objects.select_related("owner").only("caption").get(caption="c2")
        assert post.caption == "c2" and post.owner.username == "user2"

    def test_nested_select_related(self, seeded):
        class ChainNode(aggify.Document):
            parent = aggify.ReferenceField("ChainNode")
            name = aggify.StringField()

            class Meta:
                collection = "node_chain"

        try:
            root = ChainNode.objects.create(name="root")
            mid = ChainNode.objects.create(name="mid", parent=root)
            leaf = ChainNode.objects.create(name="leaf", parent=mid)
            got = ChainNode.objects.select_related("parent__parent").get(pk=leaf.pk)
            assert got.parent.parent.name == "root"
            orphan = ChainNode.objects.select_related("parent__parent").get(pk=root.pk)
            assert orphan.parent_id is None
        finally:
            ChainNode.drop_collection()

    def test_explain(self, seeded):
        assert "queryPlanner" in str(Account.objects.filter(age=1).order_by("age").explain())
        assert Account.objects.annotate(x=F("age")).explain()

    def test_none_iterators_and_streaming_aggregates(self, seeded):
        assert list(Account.objects.none().iterator()) == []
        assert [a.age for a in Account.objects.annotate(x=F("age")).order_by("age").iterator(chunk_size=2)] == [
            0,
            1,
            2,
            3,
        ]

    def test_count_variants(self, seeded):
        aggify.configure(default_max_time_ms=1000)
        assert Account.objects.order_by("age")[1:3].count() == 2
        assert Account.objects.values("age").annotate(n=Count()).count() == 4

    def test_get_or_create_race_is_resolved(self, seeded, monkeypatch):
        Account.ensure_indexes()
        real, calls = QuerySet.get, {"n": 0}

        def fake(self, *a, **k):
            calls["n"] += 1
            if calls["n"] == 1:
                raise Account.DoesNotExist()
            return real(self, *a, **k)

        monkeypatch.setattr(QuerySet, "get", fake)
        obj, created = Account.objects.get_or_create(username="user1")
        assert not created and obj.age == 1

    def test_bulk_create_duplicate_error(self, seeded):
        Account.ensure_indexes()
        with pytest.raises(NotUniqueError):
            Account.objects.bulk_create([Account(username="user1")])

    def test_update_or_create_without_defaults(self, seeded):
        obj, created = Account.objects.update_or_create(username="user1")
        assert not created and obj.age == 1

    def test_prefetch_with_nothing_to_fetch(self, clean_db):
        Post.objects.create(caption="orphan")
        assert [p.owner_id for p in Post.objects.prefetch_related("owner")] == [None]

    def test_annotation_values_with_unconvertible_data(self, seeded):
        class Bad(aggify.Document):
            n = aggify.IntField()

            class Meta:
                collection = "bad_rows"

        try:
            Bad.objects.bulk_create([Bad(n=1)])
            # write a value of the wrong type behind the model's back: loading must not crash
            aggify.get_collection(Bad).update_many({}, {"$set": {"n": "oops"}})
            assert list(Bad.objects.annotate(x=F("n")))[0].x == "oops"
        finally:
            Bad.drop_collection()


class TestAsyncQuerySetBranches:
    async def test_none_and_empty_paths(self, seeded):
        assert await Account.objects.none().alist() == []
        assert await Account.objects.none().acount() == 0 and not await Account.objects.none().aexists()
        assert [x async for x in Account.objects.none().aiterator()] == []

    async def test_count_with_pipeline_slice_and_limits(self, seeded):
        aggify.configure(default_max_time_ms=1000)
        assert await Account.objects.values("age").annotate(n=Count()).acount() == 4
        assert await Account.objects.order_by("age")[1:3].acount() == 2
        assert await Account.objects.annotate(x=F("age")).acount() == 4

    async def test_get_variants_and_edges(self, seeded):
        with pytest.raises(Account.MultipleObjectsReturned):
            await Account.objects.aget(tags="t")
        with pytest.raises(Account.DoesNotExist):
            await Account.objects.aget(username="none")
        assert (await Account.objects.alast()).username == "user3"
        assert (await Account.objects.aearliest("age")).age == 0
        assert (await Account.objects.alatest("age")).age == 3
        with pytest.raises(InvalidArgument):
            await Account.objects.alatest()
        with pytest.raises(Account.DoesNotExist):
            await Account.objects.filter(age=99).alatest("age")

    async def test_aiterator_over_a_pipeline(self, seeded):
        rows = [a.age async for a in Account.objects.annotate(x=F("age")).order_by("age").aiterator(chunk_size=2)]
        assert rows == [0, 1, 2, 3]

    async def test_explain_and_update_one(self, seeded):
        assert "queryPlanner" in str(await Account.objects.filter(age=1).aexplain())
        assert await Account.objects.annotate(x=F("age")).aexplain()
        assert await Account.objects.filter(age=1).aupdate_one(age=10) == 1
        assert await Account.objects.filter(username="fresh").aupdate_one(upsert=True, age=5) == 1

    async def test_async_races_and_duplicates(self, seeded, monkeypatch):
        await Account.aensure_indexes()
        real, calls = QuerySet.aget, {"n": 0}

        async def fake(self, *a, **k):
            calls["n"] += 1
            if calls["n"] == 1:
                raise Account.DoesNotExist()
            return await real(self, *a, **k)

        monkeypatch.setattr(QuerySet, "aget", fake)
        obj, created = await Account.objects.aget_or_create(username="user1")
        assert not created
        monkeypatch.undo()
        with pytest.raises(NotUniqueError):
            await Account.objects.abulk_create([Account(username="user1")])

    async def test_async_prefetch_missing_and_distinct_values(self, seeded):
        await Post.objects.acreate(caption="orphan")
        posts = await Post.objects.prefetch_related("owner").order_by("caption").alist()
        assert any(p.owner_id is None for p in posts)


class TestPipelineExecution:
    def test_iterate_and_options(self, seeded):
        p = Aggify(Account).order_by("age").project(age=1, pk=0)
        assert [r["age"] for r in p.iterate()] == [0, 1, 2, 3]
        aggify.configure(default_max_time_ms=1000)
        assert len(p.execute()) == 4
        assert p.explain()

    async def test_async_explain(self, seeded):
        assert await Aggify(Account).filter(age=1).aexplain()
        assert [r["age"] async for r in Aggify(Account).order_by("age").project(age=1, pk=0).aiterate(raw=True)] == [
            0,
            1,
            2,
            3,
        ]
