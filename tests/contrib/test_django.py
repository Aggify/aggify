"""Django integration, run inside this process with a minimal settings module."""

import io

import pytest
from django.core.management import call_command
from django.core.paginator import Paginator
from django.db import connection, models
from django.http import Http404
from django.test import AsyncClient, Client, RequestFactory

import aggify
from aggify.contrib.django.fields import ObjectIdField  # noqa: E402
from aggify.contrib.django.forms import document_form  # noqa: E402
from aggify.contrib.django.shortcuts import aget_object_or_404, get_list_or_404, get_object_or_404  # noqa: E402
from tests.models import Account  # noqa: E402

pytestmark = pytest.mark.mongo


def test_app_ready_registered_connection_from_settings():
    cfg = aggify.get_config("django-test")
    assert cfg.db == "aggify_django_tests" and cfg.options == {"maxPoolSize": 7}
    assert aggify.get_client("django-test").options.pool_options.max_pool_size == 7


@pytest.fixture
def accounts(clean_db):
    return [Account.objects.create(username=f"user{i:02d}", age=i, is_verified=i % 2 == 0) for i in range(25)]


def test_shortcuts(accounts):
    assert get_object_or_404(Account, username="user03").age == 3
    assert get_object_or_404(Account.objects.filter(age__gt=5), username="user10").age == 10
    with pytest.raises(Http404):
        get_object_or_404(Account, username="missing")
    with pytest.raises(Http404):
        get_object_or_404(Account, pk="garbage")
    assert len(get_list_or_404(Account, age__lt=3)) == 3
    with pytest.raises(Http404):
        get_list_or_404(Account, age__lt=0)


async def test_async_shortcut(accounts):
    assert (await aget_object_or_404(Account, username="user04")).age == 4
    with pytest.raises(Http404):
        await aget_object_or_404(Account, username="nope")


def test_django_paginator_works_with_querysets(accounts):
    paginator = Paginator(Account.objects.order_by("username"), 10)
    assert paginator.count == 25 and paginator.num_pages == 3
    page = paginator.page(3)
    assert [a.username for a in page.object_list] == [f"user{i:02d}" for i in range(20, 25)]
    assert page.has_previous() and not page.has_next()


def test_sync_view(accounts):
    c = Client()
    assert c.get("/sync/user05/").json() == {"username": "user05", "age": 5}
    assert c.get("/sync/ghost/").status_code == 404


async def test_async_views(accounts):
    c = AsyncClient()
    assert (await c.get("/async/user06/")).json() == {"username": "user06", "age": 6}
    assert (await c.get("/async/ghost/")).status_code == 404
    rows = (await c.get("/stats/")).json()["rows"]
    assert rows == [{"is_verified": False, "n": 12}, {"is_verified": True, "n": 13}]
    created = (await c.get("/create/", {"u": "brand-new"})).json()
    assert len(created["id"]) == 24


def test_document_form(clean_db):
    Form = document_form(Account, fields=["username", "age", "email", "is_verified"])
    bad = Form({"username": "x", "age": "-3", "email": "nope"})
    assert not bad.is_valid() and {"username", "age", "email"} <= set(bad.errors)
    good = Form({"username": "formuser", "age": "33", "email": "A@B.co", "is_verified": "on"})
    assert good.is_valid(), good.errors
    obj = good.save()
    assert Account.objects.get(pk=obj.pk).email == "a@b.co" and obj.is_verified is True
    editing = Form({"username": "formuser", "age": "34"}, instance=Account.objects.get(username="formuser"))
    assert editing.is_valid()
    assert editing.save().age == 34


async def test_document_form_async_save(clean_db):
    Form = document_form(Account, fields=["username"])
    form = Form({"username": "asyncform"})
    assert form.is_valid()
    obj = await form.asave()
    assert await Account.objects.filter(pk=obj.pk).aexists()


def test_management_command(clean_db):
    out = io.StringIO()
    call_command("aggify_indexes", "Account", stdout=out)
    assert "Account: " in out.getvalue()
    names = {v["key"][0][0] for v in aggify.get_collection(Account).index_information().values()}
    assert {"username", "age"} <= names


@pytest.mark.mongo
def test_objectid_field_in_django_orm_model(clean_db):
    class Invoice(models.Model):
        customer = ObjectIdField(db_index=True)
        total = models.IntegerField()

        class Meta:
            app_label = "aggify"

    with connection.schema_editor() as editor:
        editor.create_model(Invoice)
    try:
        acc = Account.objects.create(username="cust")
        Invoice.objects.create(customer=acc.pk, total=10)
        Invoice.objects.create(customer=str(acc.pk), total=5)
        got = Invoice.objects.filter(customer=acc.pk).order_by("total")
        assert [i.total for i in got] == [5, 10]
        assert got[0].customer == acc.pk and isinstance(got[0].customer, type(acc.pk))
        # the bridge: from the SQL row to the Mongo document
        assert Account.objects.get(pk=got[0].customer).username == "cust"
        with pytest.raises(Exception):
            Invoice(customer="nope").full_clean()
    finally:
        with connection.schema_editor() as editor:
            editor.delete_model(Invoice)


@pytest.mark.replica
def test_atomic_middleware_sync(clean_db):
    from aggify.contrib.django.middleware import AggifyAtomicMiddleware

    Account.ensure_indexes()

    def view_ok(request):
        Account.objects.create(username="mw-ok")
        from django.http import HttpResponse

        return HttpResponse("ok")

    def view_bad(request):
        Account.objects.create(username="mw-bad")
        raise RuntimeError("fail")

    rf = RequestFactory()
    AggifyAtomicMiddleware(view_ok)(rf.get("/"))
    with pytest.raises(RuntimeError):
        AggifyAtomicMiddleware(view_bad)(rf.get("/"))
    assert [a.username for a in Account.objects.all()] == ["mw-ok"]
