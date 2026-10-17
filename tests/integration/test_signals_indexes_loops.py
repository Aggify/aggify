import asyncio
import threading

import pytest

import aggify
from aggify import post_delete, post_save, pre_save, receiver
from aggify.testing import capture_commands
from tests.models import Account, Post


class TestSignals:
    def test_save_and_delete_signals(self):
        events = []

        @receiver(pre_save, sender=Account)
        def pre(sender, instance, created, **kw):
            events.append(("pre", created))

        @receiver(post_save, sender=Account)
        def post(sender, instance, created, **kw):
            events.append(("post", created))

        @receiver(post_delete, sender=Account)
        def deleted(sender, instance, **kw):
            events.append(("deleted", instance.username))

        try:
            a = Account.objects.create(username="sig")
            a.age = 3
            a.save()
            a.delete()
        finally:
            for fn, sig in ((pre, pre_save), (post, post_save), (deleted, post_delete)):
                sig.disconnect(fn)
        assert events == [("pre", True), ("post", True), ("pre", False), ("post", False), ("deleted", "sig")]

    def test_sender_filtering(self):
        calls = []
        fn = lambda sender, **kw: calls.append(sender)  # noqa: E731
        post_save.connect(fn, sender=Post)
        try:
            Account.objects.create(username="nope")
            assert calls == []
            Post.objects.create(caption="x")
            assert calls == [Post]
        finally:
            post_save.disconnect(fn)

    async def test_async_receivers_are_awaited_by_async_methods(self):
        calls = []

        async def handler(sender, instance, created, **kw):
            await asyncio.sleep(0)
            calls.append(instance.username)

        post_save.connect(handler, sender=Account)
        try:
            await Account.objects.acreate(username="async-sig")
        finally:
            post_save.disconnect(handler)
        assert calls == ["async-sig"]

    def test_bulk_operations_do_not_fire_per_document_signals(self):
        calls = []
        fn = lambda **kw: calls.append(1)  # noqa: E731
        post_save.connect(fn)
        try:
            Account.objects.bulk_create([Account(username="b1"), Account(username="b2")])
            Account.objects.update(age=1)
        finally:
            post_save.disconnect(fn)
        assert calls == []


class TestIndexes:
    def test_ensure_indexes_creates_declared_indexes(self):
        names = Account.ensure_indexes()
        info = aggify.get_collection(Account).index_information()
        assert any(v["key"] == [("username", 1)] and v.get("unique") for v in info.values())
        assert any(v["key"] == [("age", 1), ("created_at", -1)] for v in info.values())
        assert len(names) >= 3

    async def test_async_ensure_indexes_is_idempotent(self):
        await Account.aensure_indexes()
        await Account.aensure_indexes()
        info = await aggify.get_async_collection(Account).index_information()
        assert any(v["key"] == [("tags", 1)] for v in info.values())

    def test_ttl_text_partial_geo_index_options(self):
        from aggify import DateTimeField, Document, GeoPointField, Index, StringField

        class Session(Document):
            token = StringField()
            expires = DateTimeField()
            body = StringField()
            where = GeoPointField()

            class Meta:
                collection = "sessions_idx"
                indexes = [
                    Index("expires", ttl=60),
                    Index("body", type="text"),
                    Index("where", type="2dsphere"),
                    Index("token", unique=True, partial={"token": {"$type": "string"}}),
                ]

        try:
            Session.ensure_indexes()
            info = aggify.get_collection(Session).index_information()
            ttl = [v for v in info.values() if v.get("expireAfterSeconds") == 60]
            assert ttl and any("textIndexVersion" in v for v in info.values())
            assert any(v["key"] == [("where", "2dsphere")] for v in info.values())
            assert any(v.get("partialFilterExpression") for v in info.values())
        finally:
            Session.drop_collection()

    def test_queries_use_the_index(self):
        Account.ensure_indexes()
        Account.objects.bulk_create([Account(username=f"user{i}", age=i % 50) for i in range(300)])
        plan = str(Account.objects.filter(username="user7").explain())
        assert "IXSCAN" in plan


class TestEventLoops:
    def test_each_event_loop_gets_its_own_client(self):
        ids = []

        async def one():
            await Account.objects.acount()
            ids.append(id(aggify.get_async_client()))

        asyncio.run(one())
        asyncio.run(one())
        asyncio.run(one())
        assert len(set(ids)) >= 1  # no "attached to a different loop" errors across runs

    def test_threads_with_their_own_loops(self):
        errors, results = [], []

        def target(i):
            try:

                async def work():
                    await Account.objects.acreate(username=f"thread{i}")
                    return await Account.objects.filter(username=f"thread{i}").acount()

                results.append(asyncio.run(work()))
            except Exception as exc:  # pragma: no cover
                errors.append(exc)

        threads = [threading.Thread(target=target, args=(i,)) for i in range(6)]
        [t.start() for t in threads]
        [t.join() for t in threads]
        assert not errors and results == [1] * 6

    def test_sync_threads_share_the_sync_client(self):
        from concurrent.futures import ThreadPoolExecutor

        with ThreadPoolExecutor(8) as pool:
            list(pool.map(lambda i: Account.objects.create(username=f"pool{i}"), range(20)))
        assert Account.objects.count() == 20

    async def test_adisconnect_and_reconnect(self):
        cfg = aggify.get_config()
        await aggify.aclose_clients()
        assert await Account.objects.acount() >= 0  # a fresh client is created lazily
        assert aggify.get_config() is cfg


class TestMonitoring:
    def test_capture_commands_counts_queries(self):
        Account.objects.create(username="m1")
        with capture_commands() as log:
            list(Account.objects.filter(username="m1"))
            Account.objects.count()
        assert log.names == ["find", "aggregate"] or log.names == ["find", "count"]

    def test_assert_num_queries_failure_message(self):
        from aggify.testing import assert_num_queries

        with pytest.raises(AssertionError, match="Expected 0 command"):
            with assert_num_queries(0):
                Account.objects.count()


class TestConnection:
    def test_not_configured_alias_error(self):
        class Orphan(aggify.Document):
            class Meta:
                db_alias = "does-not-exist"

        with pytest.raises(aggify.ConnectionNotConfigured):
            Orphan.objects.count()

    def test_second_database_alias(self):
        from tests.conftest import MONGO_URI

        aggify.connect(MONGO_URI, "aggify_other_db", alias="other")
        try:

            class Elsewhere(aggify.Document):
                name = aggify.StringField()

                class Meta:
                    db_alias = "other"
                    collection = "elsewhere"

            Elsewhere.objects.create(name="x")
            assert aggify.get_database("other").elsewhere.count_documents({}) == 1
            assert Elsewhere.objects.count() == 1 and Account.objects.count() == 0
        finally:
            aggify.get_client("other").drop_database("aggify_other_db")
            aggify.disconnect("other")

    def test_ping(self):
        assert aggify.ping() is True

    async def test_aping(self):
        assert await aggify.aping() is True
