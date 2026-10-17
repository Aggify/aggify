import asyncio
import datetime as dt
import decimal

import pytest

from aggify import NotUniqueError, OptimisticLockError, SynchronousOnlyOperation, ValidationError
from aggify.testing import assert_num_queries, capture_commands
from tests.models import Account, Post, Role, Slug, Versioned


class TestSyncCrud:
    def test_create_get_roundtrip_all_types(self):
        a = Account.objects.create(
            username="bob",
            email="BOB@example.com",
            age=30,
            score=1.5,
            balance="10.25",
            tags=["x", "y"],
            address={"city": "Tehran", "zip_code": "123", "geo": (51.4, 35.7)},
            attrs={"k": {"n": 1}},
            role="admin",
        )
        b = Account.objects.get(pk=a.pk)
        assert b == a and b.email == "bob@example.com" and b.balance == decimal.Decimal("10.25")
        assert b.address.geo == (51.4, 35.7) and b.attrs == {"k": {"n": 1}} and b.role is Role.ADMIN
        assert isinstance(b.created_at, dt.datetime) and b.created_at.tzinfo is not None
        assert (b.age, b.score, b.tags) == (30, 1.5, ["x", "y"])

    def test_save_sends_only_changed_fields(self):
        a = Account.objects.create(username="bob", age=1, display_name="Bob")
        a = Account.objects.get(pk=a.pk)
        with capture_commands() as cmds:
            a.age = 2
            a.display_name = None
            a.save()
        (cmd,) = cmds.of("update")
        upd = cmd.command["updates"][0]
        assert upd["u"] == {"$set": {"age": 2}, "$unset": {"display_name": ""}}
        assert upd["q"] == {"_id": a.pk}

    def test_save_without_changes_does_nothing(self):
        a = Account.objects.create(username="bob")
        with assert_num_queries(0):
            a.save()
            a.save()

    def test_update_fields(self):
        a = Account.objects.create(username="bob", age=1)
        a.age, a.display_name = 5, "x"
        a.save(update_fields=["age"])
        b = Account.objects.get(pk=a.pk)
        assert (b.age, b.display_name) == (5, None)

    def test_unique_violation(self):
        Account.objects.create(username="bob")
        Account.ensure_indexes()
        with pytest.raises(NotUniqueError):
            Account.objects.create(username="bob")

    def test_validation_blocks_save(self):
        with pytest.raises(ValidationError):
            Account(username="x", age=-5).save()
        assert Account.objects.count() == 0

    def test_validate_false_skips_validation(self):
        Account(username="x", age=-5).save(validate=False)
        assert Account.objects.count() == 1

    def test_delete_and_refresh(self):
        a = Account.objects.create(username="bob", age=1)
        Account.objects.filter(pk=a.pk).update(age=9)
        a.refresh_from_db()
        assert a.age == 9
        assert a.delete() is True
        with pytest.raises(Account.DoesNotExist):
            a.refresh_from_db()
        assert Account.objects.count() == 0

    def test_refresh_specific_fields(self):
        a = Account.objects.create(username="bob", age=1, display_name="d")
        Account.objects.update(age=2, display_name="e")
        a.refresh_from_db(fields=["age"])
        assert (a.age, a.display_name) == (2, "d")

    def test_save_after_delete_reinserts_like_django(self):
        a = Account.objects.create(username="bob")
        Account.objects.all().delete()
        a.age = 3
        a.save()
        assert Account.objects.get(pk=a.pk).age == 3

    def test_custom_string_pk(self):
        Slug.objects.create(code="a", label="A")
        assert Slug.objects.get(pk="a").label == "A"
        s = Slug.objects.get(code="a")
        s.label = "B"
        s.save()
        assert Slug.objects.get(pk="a").label == "B"

    def test_auto_now_add_is_set_once(self):
        a = Account.objects.create(username="bob")
        created = a.created_at
        a.age = 3
        a.save()
        assert Account.objects.get(pk=a.pk).created_at.replace(microsecond=0) == created.replace(microsecond=0)

    def test_optimistic_locking(self):
        v = Versioned.objects.create(title="a")
        assert v.version == 1
        one, two = Versioned.objects.get(pk=v.pk), Versioned.objects.get(pk=v.pk)
        one.title = "one"
        one.save()
        assert one.version == 2
        two.title = "two"
        with pytest.raises(OptimisticLockError):
            two.save()
        two.refresh_from_db()
        two.title = "two"
        two.save()
        assert Versioned.objects.get(pk=v.pk).title == "two"

    def test_bulk_create(self):
        accounts = [Account(username=f"u{i}", age=i) for i in range(25)]
        with assert_num_queries(3, names=["insert"]):
            out = Account.objects.bulk_create(accounts, batch_size=10)
        assert len(out) == 25 and Account.objects.count() == 25
        assert all(not a._state.adding for a in accounts)

    def test_bulk_create_validates_up_front(self):
        with pytest.raises(ValidationError):
            Account.objects.bulk_create([Account(username="ok"), Account(username="x", age=-1)])
        assert Account.objects.count() == 0

    def test_get_or_create_and_update_or_create(self):
        a, created = Account.objects.get_or_create(username="bob", defaults={"age": 3})
        assert created and a.age == 3
        b, created = Account.objects.get_or_create(username="bob", defaults={"age": 99})
        assert not created and b.pk == a.pk and b.age == 3
        c, created = Account.objects.update_or_create(username="bob", defaults={"age": 50})
        assert not created and Account.objects.get(username="bob").age == 50
        d, created = Account.objects.update_or_create(username="amy", defaults={"age": 1})
        assert created

    def test_get_errors(self):
        with pytest.raises(Account.DoesNotExist):
            Account.objects.get(username="nobody")
        Account.objects.create(username="a1", age=1)
        Account.objects.create(username="a2", age=1)
        with pytest.raises(Account.MultipleObjectsReturned):
            Account.objects.get(age=1)

    def test_malformed_pk_is_a_validation_error_not_a_crash(self):
        with pytest.raises(ValidationError):
            Account.objects.get(pk="not-an-object-id")

    def test_reference_roundtrip_and_lazy_fetch(self):
        owner = Account.objects.create(username="bob")
        p = Post.objects.create(owner=owner, caption="hi")
        q = Post.objects.get(pk=p.pk)
        assert q.owner_id == owner.pk
        with assert_num_queries(1):
            assert q.owner.username == "bob"
        with assert_num_queries(0):
            assert q.owner.username == "bob"  # cached

    def test_embedded_list_roundtrip(self):
        owner = Account.objects.create(username="bob")
        p = Post.objects.create(owner=owner, comments=[{"text": "a", "author": owner}, {"text": "b"}])
        q = Post.objects.get(pk=p.pk)
        assert [c.text for c in q.comments] == ["a", "b"] and q.comments[0].author_id == owner.pk
        q.comments[1].score = 5
        q.save()
        assert Post.objects.get(pk=p.pk).comments[1].score == 5


class TestAsyncCrud:
    async def test_create_get_save_delete(self):
        a = await Account.objects.acreate(username="bob", age=1)
        b = await Account.objects.aget(pk=a.pk)
        assert b == a
        b.age = 2
        with capture_commands() as cmds:
            await b.asave()
        assert cmds.names == ["update"]
        await b.arefresh_from_db()
        assert b.age == 2
        assert await b.adelete()
        assert await Account.objects.acount() == 0

    async def test_unique_and_validation(self):
        await Account.aensure_indexes()
        await Account.objects.acreate(username="bob")
        with pytest.raises(NotUniqueError):
            await Account.objects.acreate(username="bob")
        with pytest.raises(ValidationError):
            await Account(username="x", age=-1).asave()

    async def test_bulk_create_and_get_or_create(self):
        await Account.objects.abulk_create([Account(username=f"u{i}") for i in range(5)])
        assert await Account.objects.acount() == 5
        obj, created = await Account.objects.aget_or_create(username="new", defaults={"age": 4})
        assert created and obj.age == 4
        obj, created = await Account.objects.aupdate_or_create(username="new", defaults={"age": 5})
        assert not created and (await Account.objects.aget(username="new")).age == 5

    async def test_sync_calls_are_refused_inside_the_event_loop(self):
        with pytest.raises(SynchronousOnlyOperation):
            Account.objects.count()
        with pytest.raises(SynchronousOnlyOperation):
            Account(username="xx").save()

    async def test_lazy_reference_in_async_context_explains_itself(self):
        owner = await Account.objects.acreate(username="bob")
        await Post.objects.acreate(owner=owner)
        post = await Post.objects.afirst()
        with pytest.raises(SynchronousOnlyOperation, match="afetch_related"):
            post.owner
        await post.afetch_related("owner")
        assert post.owner.username == "bob"

    async def test_optimistic_lock_async(self):
        v = await Versioned.objects.acreate(title="a")
        one, two = await Versioned.objects.aget(pk=v.pk), await Versioned.objects.aget(pk=v.pk)
        one.title = "x"
        await one.asave()
        two.title = "y"
        with pytest.raises(OptimisticLockError):
            await two.asave()

    async def test_many_concurrent_writes(self):
        await asyncio.gather(*(Account.objects.acreate(username=f"user{i}", age=i) for i in range(50)))
        assert await Account.objects.acount() == 50
        results = await asyncio.gather(*(Account.objects.filter(age=i).acount() for i in range(50)))
        assert results == [1] * 50


@pytest.mark.parametrize("n", [1])
def test_sync_in_thread_while_async_loop_runs(n):
    """Sync API from a worker thread is fine: the guard only fires on the loop's own thread."""

    async def main():
        return await asyncio.to_thread(lambda: Account.objects.create(username="threaded").pk)

    assert asyncio.run(main()) is not None
    assert Account.objects.get(username="threaded")
