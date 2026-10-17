"""Last database-backed branches."""

import pytest

import aggify
from aggify import Aggify, Count, DictField, Document, F, IntField, Q, ReferenceField, StringField
from tests.models import Account, Post


@pytest.fixture
def data(clean_db):
    accs = [Account.objects.create(username=f"user{i}", age=i) for i in range(3)]
    for a in accs:
        Post.objects.create(owner=a, caption=f"c{a.age}")
    return accs


def test_empty_bulk_create_and_asave_noop(data):
    assert Account.objects.bulk_create([]) == []


async def test_async_branches(data):
    assert await Account.objects.abulk_create([]) == []
    acc = await Account.objects.aget(username="user1")
    await acc.asave()  # nothing changed: no command
    acc.age = None
    await acc.asave()  # only $unset
    assert (await Account.objects.aget(username="user1")).age is None
    qs = Account.objects.order_by("username")
    assert len(await qs.alist()) == 3 and len(await qs.alist()) == 3
    obj, created = await Account.objects.aupdate_or_create(username="user2")
    assert not created
    # prefetch with nothing to fetch, and with an already-loaded relation
    await Post.objects.filter(caption="c0").aupdate(unset__owner=True)
    posts = await Post.objects.prefetch_related("owner").alist()
    assert sum(p.owner_id is None for p in posts) == 1
    p = await Post.objects.afirst()
    await p.afetch_related("owner")
    await p.afetch_related("owner")  # second call has nothing left to do
    await Post.objects.filter(caption="c1").aupdate(unset__owner=True)
    orphan = await Post.objects.aget(caption="c1")
    await orphan.afetch_related("owner")


def test_sync_fetch_related_with_missing_target(data):
    post = Post.objects.get(caption="c2")
    Account.objects.filter(pk=post.owner_id).delete()
    owner_id = post.owner_id
    assert post.fetch_related("owner").owner_id == owner_id  # dangling reference keeps its id


def test_explain_without_sort_and_with_projection(data):
    assert Post.objects.filter(caption="c1").explain()
    assert Account.objects.filter(age=1).only("username").explain()


async def test_aexplain_without_sort(data):
    assert await Post.objects.filter(caption="c1").aexplain()


def test_values_with_pk_and_defer_with_select_related(data):
    rows = list(Account.objects.annotate(x=F("age")).values("pk", "x").order_by("x"))
    assert rows[0]["x"] == 0 and "id" in rows[0]
    post = Post.objects.select_related("owner").defer("location").get(caption="c1")
    assert post.owner.username == "user1"


def test_annotation_missing_from_some_rows(data):
    Account.objects.filter(username="user0").update(unset__age=True)
    got = {a.username: getattr(a, "x", "absent") for a in Account.objects.annotate(x=F("age"))}
    assert got["user1"] == 1
    assert got["user0"] is None


def test_desc_expression_ordering_errors_and_reverse(data):
    with pytest.raises(aggify.InvalidArgument):
        Account.objects.order_by((F("age") + 1).desc()).query
    assert [a.age for a in Account.objects.order_by(F("age").desc()).reverse()] == [0, 1, 2]


def test_pipeline_with_explicit_scope_and_join_in_unwind(data):
    p = Aggify(Post).unwind("owner__username")
    assert p.to_list()[0]["$lookup"]
    p2 = Aggify(Post).order_by("owner__age").limit(1)
    assert len(p2.execute()) == 1


def test_connection_helpers_for_unknown_aliases():
    import asyncio

    async def main():
        await aggify.aclose_clients("nope")
        await aggify.adisconnect("nope")

    asyncio.run(main())


async def test_async_prefetch_when_no_row_has_a_reference(data):
    await Post.objects.aupdate(unset__owner=True)
    assert all(p.owner_id is None for p in await Post.objects.prefetch_related("owner").alist())


def test_child_overrides_inherited_manager(data):
    class Parent(Document):
        objects = aggify.Manager()

    class Child(Parent):
        objects = aggify.Manager()

    assert Child.objects.model is Child


def test_lazy_access_to_a_dangling_reference_returns_none_and_keeps_the_id(data):
    post = Post.objects.get(caption="c1")
    owner_id = post.owner_id
    Account.objects.filter(pk=owner_id).delete()
    assert post.owner is None and post.owner_id == owner_id


async def test_async_fetch_of_a_dangling_reference(data):
    post = await Post.objects.aget(caption="c1")
    owner_id = post.owner_id
    await Account.objects.filter(pk=owner_id).adelete()
    await post.afetch_related("owner")
    assert post.owner_id == owner_id


def test_annotation_with_explicit_null_value(data):
    aggify.get_collection(Account).update_many({"username": "user0"}, {"$set": {"deleted_at": None}})
    got = {a.username: a.d for a in Account.objects.annotate(d=F("deleted_at"))}
    assert got["user0"] is None
