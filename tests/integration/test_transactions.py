import pytest

import aggify
from aggify import atransaction, transaction
from tests.models import Account

pytestmark = pytest.mark.replica


def test_commit_and_rollback_sync():
    with transaction():
        Account.objects.create(username="in-tx")
        assert Account.objects.filter(username="in-tx").exists()
    assert Account.objects.filter(username="in-tx").exists()

    with pytest.raises(RuntimeError):
        with transaction():
            Account.objects.create(username="rolled-back")
            raise RuntimeError("boom")
    assert not Account.objects.filter(username="rolled-back").exists()


async def test_commit_and_rollback_async():
    async with atransaction():
        await Account.objects.acreate(username="a-in-tx")
        await Account.objects.filter(username="a-in-tx").aupdate(age=3)
    assert (await Account.objects.aget(username="a-in-tx")).age == 3

    with pytest.raises(RuntimeError):
        async with atransaction():
            await Account.objects.acreate(username="a-rolled")
            await Account.objects.filter(username="a-in-tx").aupdate(age=99)
            raise RuntimeError("boom")
    assert not await Account.objects.filter(username="a-rolled").aexists()
    assert (await Account.objects.aget(username="a-in-tx")).age == 3


def test_changes_are_invisible_outside_until_commit():
    from pymongo import MongoClient

    from tests.conftest import MONGO_URI

    outside = MongoClient(MONGO_URI)
    try:
        with transaction():
            Account.objects.create(username="isolated")
            assert outside[aggify.get_config().db]["account"].count_documents({"username": "isolated"}) == 0
        assert outside[aggify.get_config().db]["account"].count_documents({"username": "isolated"}) == 1
    finally:
        outside.close()


def test_nested_blocks_join_the_outer_transaction():
    with pytest.raises(RuntimeError):
        with transaction():
            with transaction():
                Account.objects.create(username="inner")
            raise RuntimeError("outer fails")
    assert not Account.objects.filter(username="inner").exists()


async def test_concurrent_tasks_get_their_own_transaction_scope():
    import asyncio

    await Account.aensure_indexes()  # collections must exist before concurrent transactions touch them

    async def worker(name, fail):
        try:
            async with atransaction():
                await Account.objects.acreate(username=name)
                await asyncio.sleep(0.01)
                if fail:
                    raise RuntimeError
        except RuntimeError:
            pass

    await asyncio.gather(worker("ok-1", False), worker("bad-1", True), worker("ok-2", False), worker("bad-2", True))
    assert sorted(a.username for a in await Account.objects.alist()) == ["ok-1", "ok-2"]


def test_session_not_leaked_after_block():
    with transaction():
        assert aggify.current_session() is not None
    assert aggify.current_session() is None
