"""Remaining branches of the framework integrations."""

import asyncio
import datetime as dt
import uuid

import django
import grpc
import pytest
from django.conf import settings
from django.test import override_settings

from tests.conftest import MONGO_URI

import aggify  # noqa: E402
from aggify import (  # noqa: E402
    BooleanField,
    DateField,
    DateTimeField,
    DecimalField,
    DictField,
    Document,
    EmbeddedDocument,
    EmbeddedField,
    EmailField,
    FloatField,
    IntField,
    ListField,
    ObjectIdField,
    ReferenceField,
    StringField,
    URLField,
    UUIDField,
)
from aggify.contrib.django import conf as dconf  # noqa: E402
from aggify.contrib.django.fields import ObjectIdField as DjangoObjectIdField  # noqa: E402
from aggify.contrib.django.forms import document_form  # noqa: E402
from aggify.contrib.pydantic import pydantic_model  # noqa: E402
from tests.models import Account  # noqa: E402


class TestDjangoConf:
    def test_mongodb_uri_shortcut(self):
        with override_settings(AGGIFY=None, MONGODB_URI=MONGO_URI, MONGODB_NAME="aggify_conf_shortcut"):
            out = dconf.setup_from_settings()
        assert out["default"].db == "aggify_conf_shortcut"

    def test_no_settings_is_a_noop(self):
        with override_settings(AGGIFY=None, MONGODB_URI=None):
            assert dconf.setup_from_settings() == {}

    def test_invalid_settings(self):
        from django.core.exceptions import ImproperlyConfigured

        with override_settings(AGGIFY=["nope"]):
            with pytest.raises(ImproperlyConfigured):
                dconf.setup_from_settings()
        with override_settings(AGGIFY={"x": {"NAME": "db"}}):
            with pytest.raises(ImproperlyConfigured, match="URI"):
                dconf.setup_from_settings()

    def test_extra_aggify_settings(self):
        with override_settings(
            AGGIFY={"conf-extra": {"URI": MONGO_URI, "NAME": "aggify_conf_extra"}}, AGGIFY_SETTINGS={"max_limit": 7}
        ):
            try:
                dconf.setup_from_settings()
                assert aggify.settings.max_limit == 7
            finally:
                aggify.disconnect("conf-extra")


@pytest.fixture(autouse=True)
def _restore_default_connection(db_name):
    yield
    aggify.connect(MONGO_URI, db_name)


def test_django_objectid_field_deconstruct():
    name, path, args, kwargs = DjangoObjectIdField(db_index=True).deconstruct()
    assert "max_length" not in kwargs and kwargs["db_index"] is True


class Sub(EmbeddedDocument):
    x = IntField()


class Everything(Document):
    text = StringField(verbose_name="Free text", choices=[("a", "Alpha"), "b"])
    url = URLField()
    ratio = FloatField(min_value=0, max_value=1)
    money = DecimalField(max_digits=6, decimal_places=2)
    when = DateTimeField()
    day = DateField()
    key = UUIDField()
    oid = ObjectIdField()
    ref = ReferenceField(Account)
    blob = DictField()
    items = ListField(StringField())
    sub = EmbeddedField(Sub)
    flag = BooleanField()
    mail = EmailField()
    anything = aggify.AnyField()
    created = DateTimeField(auto_now_add=True)

    class Meta:
        collection = "everything_form"


def test_form_field_mapping_covers_every_field_type():
    from django import forms as dj

    Form = document_form(Everything)
    f = Form().fields
    assert isinstance(f["text"], dj.ChoiceField) and f["text"].label == "Free text"
    assert dict(f["text"].choices) == {"a": "Alpha", "b": "b"}
    assert isinstance(f["url"], dj.URLField) and isinstance(f["ratio"], dj.FloatField)
    assert isinstance(f["money"], dj.DecimalField) and isinstance(f["when"], dj.DateTimeField)
    assert isinstance(f["day"], dj.DateField) and isinstance(f["key"], dj.UUIDField)
    assert isinstance(f["oid"], dj.CharField) and isinstance(f["ref"], dj.CharField)
    for name in ("blob", "items", "sub"):
        assert isinstance(f[name], dj.JSONField)
    assert isinstance(f["flag"], dj.BooleanField) and isinstance(f["mail"], dj.EmailField)
    assert "anything" not in f and "created" not in f and "id" not in f


def test_form_excludes_and_validates_through_the_document():
    Form = document_form(Everything, exclude=["blob", "items", "sub", "oid", "ref", "key"], name="EForm")
    assert Form.__name__ == "EForm"
    bad = Form({"ratio": "5", "money": "1234567.1", "url": "ftp://x"})
    assert not bad.is_valid() and {"ratio", "url"} <= set(bad.errors)
    good = Form({"ratio": "0.5", "money": "1.25", "text": "a", "when": "2024-01-02 03:04", "day": "2024-01-02"})
    assert good.is_valid(), good.errors
    obj = good.save(commit=False)
    assert obj.ratio == 0.5 and obj._state.adding


async def test_async_middleware_and_markcoroutine():
    from asgiref.sync import iscoroutinefunction
    from django.http import HttpResponse

    from aggify.contrib.django.middleware import AggifyAtomicMiddleware

    async def view(request):
        return HttpResponse("ok")

    mw = AggifyAtomicMiddleware(view)
    assert iscoroutinefunction(mw)
    # no replica set needed to exercise the wiring: stub the transaction context
    import contextlib
    from unittest import mock

    @contextlib.asynccontextmanager
    async def fake(*a, **k):
        yield None

    with mock.patch("aggify.atransaction", fake):
        response = await mw(object())
    assert response.content == b"ok"


class TestGrpcBranches:
    async def test_stream_wrapper_maps_and_reraises(self):
        from aggify.contrib.grpc import grpc_errors

        aborted = []

        class Ctx:
            async def abort(self, code, message):
                aborted.append(code)
                raise RuntimeError("aborted")

        class Svc:
            @grpc_errors
            async def Stream(self, request, context):
                yield 1
                raise aggify.ValidationError("bad")

            @grpc_errors
            async def Other(self, request, context):
                yield 1
                raise KeyError("not aggify")

            @grpc_errors
            async def Odd(self, request, context):
                raise aggify.AggifyError("unmapped")
                yield 0

        got = []
        with pytest.raises(RuntimeError, match="aborted"):
            async for item in Svc().Stream(None, Ctx()):
                got.append(item)
        assert got == [1] and aborted == [grpc.StatusCode.INVALID_ARGUMENT]
        with pytest.raises(KeyError):
            async for _ in Svc().Other(None, Ctx()):
                pass
        with pytest.raises(aggify.AggifyError):
            async for _ in Svc().Odd(None, Ctx()):
                pass

        class Plain:
            @grpc_errors
            async def Unary(self, request, context):
                raise aggify.AggifyError("unmapped")

        with pytest.raises(aggify.AggifyError):
            await Plain().Unary(None, Ctx())


class TestPydanticBranches:
    def test_constraints_are_mapped(self):
        class Strict(Document):
            code = StringField(min_length=2, max_length=5, regex=r"^[a-z]+$")
            score = FloatField(min_value=0, max_value=10)
            n = IntField(min_value=1, max_value=3)
            tags = ListField(StringField(), min_length=1, max_length=2)

            class Meta:
                collection = "strict_schema"

        Schema = pydantic_model(Strict, name="StrictIn", for_input=True)
        props = Schema.model_json_schema()["properties"]
        assert props["code"]["anyOf"][0]["pattern"] == "^[a-z]+$"
        assert props["score"]["anyOf"][0]["minimum"] == 0 and props["score"]["anyOf"][0]["maximum"] == 10
        assert props["n"]["anyOf"][0]["maximum"] == 3
        assert props["tags"]["anyOf"][0]["maxItems"] == 2 and props["tags"]["anyOf"][0]["minItems"] == 1

    def test_include_required_defaults_and_recursion_guard(self):
        class Defaulted(Document):
            a = StringField(required=True, default="x")
            b = IntField(default=3)

            class Meta:
                collection = "defaulted_schema"

        Schema = pydantic_model(Defaulted, name="DefIn", for_input=True, include=["a", "b"])
        assert Schema.model_validate({}).a == "x" or Schema.model_validate({}).a is None
        assert Schema.model_validate({}).b == 3

        class Loop(EmbeddedDocument):
            pass

        Loop._meta.fields["me"] = EmbeddedField(Loop)
        Loop._meta.fields["me"].name = "me"
        with pytest.raises(ValueError, match="Recursive"):
            pydantic_model(Loop, name="LoopOut")


async def test_form_asave_without_commit():
    Form = document_form(Account, fields=["username"])
    form = Form({"username": "nocommit"})
    assert form.is_valid()
    obj = await form.asave(commit=False)
    assert obj._state.adding
