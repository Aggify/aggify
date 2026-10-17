"""The last uncovered branches (unit level)."""

import asyncio

import pytest
from bson import Binary

import aggify
from aggify import (
    Aggify,
    DateToString,
    DecimalField,
    DictField,
    Document,
    EmbeddedDocument,
    EmbeddedField,
    F,
    FloatField,
    GeoPointField,
    IntField,
    ListField,
    Manager,
    Q,
    ReferenceField,
    StringField,
    UUIDField,
    ValidationError,
)
from aggify.expressions import DateAdd, DateDiff, DateTrunc, RegexMatch
from aggify.fields import Field
from aggify.schema import Context, Scope
from aggify.updates import _element_field, _prepare, build_update
from tests.models import Account, Post


def res(expr, model=Account):
    return expr.resolve(Context(Scope.for_model(model)))


def test_field_base_and_float_edges():
    assert Field().to_mongo(5) == 5
    assert FloatField().to_python(True) is True
    with pytest.raises(ValidationError):
        FloatField().to_mongo(True)
    with pytest.raises(ValidationError):
        GeoPointField().to_mongo([1.0, 2.0])


def test_reference_id_accessor_setter():
    post = Post()
    post.owner_id = Account(username="ab").pk
    assert post.owner_id is not None and post.to_mongo()["owner_id"] == post.owner_id


def test_reference_does_not_clobber_existing_id_attribute():
    class Odd(Document):
        thing = ReferenceField(Account)

        @property
        def thing_id(self):
            return "mine"

    # the class defined thing_id itself *after* the field was processed: ensure construction still works
    assert Odd().thing is None

    class Pre(Document):
        thing_id = "custom"
        thing = ReferenceField(Account)

    assert Pre.thing_id == "custom"


def test_list_of_fields_with_explicit_db_field_and_dict_none_values():
    class L(Document):
        xs = ListField(StringField(db_field="custom"))
        d = DictField(IntField())

    assert L._meta.fields["xs"].field.db_field == "custom"
    L(d={"a": None, "b": 2}).full_clean()


def test_diamond_inheritance_and_manager_inheritance():
    class A(Document):
        shared = StringField()

        class Meta:
            abstract = True

    class B(Document):
        shared = StringField()
        other = IntField()

        class Meta:
            abstract = True

    class C(A, B):
        pass

    assert list(C._meta.fields)[1:] == ["shared", "other"]

    class Parent(Document):
        things = Manager()

    class Child(Parent):
        pass

    assert Child.things.model is Child and Parent.things.model is Parent


def test_documents_unset_only_update_plan():
    a = Account.from_mongo(Account(username="ab", age=3).to_mongo())
    a.age = None
    kind, flt, update = a._plan_save(False, None, False)
    assert kind == "update" and update == {"$unset": {"age": ""}}


def test_date_functions_without_optionals():
    assert res(DateToString("created_at", "%Y", timezone="UTC"))["$dateToString"]["timezone"] == "UTC"
    assert "timezone" not in res(DateAdd("created_at", "day", 1))["$dateAdd"]
    assert "timezone" not in res(DateDiff("created_at", "created_at"))["$dateDiff"]
    assert res(DateTrunc("created_at")) == {"$dateTrunc": {"date": "$created_at", "unit": "day"}}
    assert res(DateTrunc("created_at", timezone="UTC"))["$dateTrunc"]["timezone"] == "UTC"
    assert res(DateTrunc("created_at", start_of_week="mon"))["$dateTrunc"]["startOfWeek"] == "mon"
    assert "options" not in res(RegexMatch("username", "^a"))["$regexMatch"]


def test_near_without_distance():
    assert Account.objects.filter(address__geo__near=(1, 2)).query["find"]["filter"] == {
        "address.geo": {"$near": {"$geometry": {"type": "Point", "coordinates": [1, 2]}}}
    }


def test_pipeline_branches():
    custom = Scope.for_model(Account).evolve(only=["age"])
    assert set(Aggify(Account, scope=custom).scope.fields) == {"age"}
    assert Aggify(Account).add_fields().to_list() == []
    assert Aggify(Account).unwind("tags", include_array_index="i").to_list() == [
        {"$unwind": {"path": "$tags", "includeArrayIndex": "i"}}
    ]
    assert Aggify(Account).unwind("address__city").to_list() == [{"$unwind": "$address.city"}]
    p = Aggify(Account).lookup(Post, "empty", let={}, query=Q())
    assert p.to_list()[0]["$lookup"]["pipeline"] == []
    assert "sortBy" not in Aggify(Account).set_window_fields(rank=aggify.Rank()).to_list()[0]["$setWindowFields"]
    assert Aggify(Account).replace_with("address").to_list() == [{"$replaceWith": "$address"}]


def test_pipeline_convert_survives_bad_values():
    from aggify.pipeline import convert_row

    class Keyed(Document):
        key = UUIDField()

    bad = Binary(b"x" * 16, 3)  # legacy UUID subtype cannot be decoded with the standard representation
    assert convert_row(Scope.for_model(Keyed), {"key": bad})["key"] == bad


def test_queryset_hydration_survives_bad_annotation_values():
    from aggify.queryset import QuerySet

    class Keyed(Document):
        key = UUIDField()

        class Meta:
            collection = "keyed_unit"

    qs = Keyed.objects.annotate(k=F("key"))
    scope = Scope.for_model(Keyed).evolve({"k": Keyed._meta.fields["key"]})
    obj = QuerySet._hydrate_row(
        qs,
        {
            "_id": aggify.ObjectId() if hasattr(aggify, "ObjectId") else __import__("bson").ObjectId(),
            "k": Binary(b"x" * 16, 3),
        },
        scope,
        None,
    )
    assert obj.k == Binary(b"x" * 16, 3)


def test_update_builder_branches():
    with pytest.raises(aggify.InvalidArgument, match="updated twice"):
        build_update(Account, {"age": 1, "set__age": 2})
    with pytest.raises(ValidationError):
        build_update(Account, {"age": F("age") + 1, "inc__score": "x"})
    assert build_update(Account, {"age": F("age") + 1, "min__score": 3.0}) == [
        {"$set": {"age": {"$add": ["$age", 1]}, "score": {"$min": ["$score", 3.0]}}}
    ]
    assert _prepare(None, {"a": 1}) == {"a": 1} and _prepare(None, aggify.Raw({"$x": 1})) == {"$x": 1}
    assert isinstance(_element_field(None, "p"), aggify.AnyField)
    only_unset = build_update(Account, {"age": F("age") + 1, "unset__score": True})
    assert only_unset[1] == {"$unset": ["score"]}
    only_unset = build_update(Account, {"unset__score": True, "age": F("age") + 1})
    assert only_unset[0]["$set"] and only_unset[1]
    assert build_update(Account, {"current_date__created_at": True}) == {"$currentDate": {"created_at": True}}
    mm = build_update(Account, {"age": F("age") + 1, "max__score": 3.0})
    assert mm[0]["$set"]["score"] == {"$max": ["$score", 3.0]}


def test_signal_asend_with_sync_receiver():
    sig = aggify.Signal("x")
    seen = []
    sig.connect(lambda sender, **kw: seen.append(sender))
    asyncio.run(sig.asend("s"))
    assert seen == ["s"]


def test_monitoring_matches_the_right_request():
    from types import SimpleNamespace as NS

    from aggify.monitoring import LISTENER, capture_commands

    with capture_commands() as log:
        for rid in (1, 2):
            LISTENER.started(NS(command_name="find", database_name="d", command={"find": "c"}, request_id=rid))
        LISTENER.succeeded(NS(request_id=1, duration_micros=9))
        LISTENER.failed(NS(request_id=1))
    assert log.records[0].duration_micros == 9 and log.records[0].failed and not log.records[1].failed
    with capture_commands() as log2:
        LISTENER.started(NS(command_name="find", database_name="d", command={"find": "c"}, request_id=5))
        LISTENER.succeeded(NS(request_id=99, duration_micros=1))
        LISTENER.failed(NS(request_id=99))
    assert log2.records[0].duration_micros is None and not log2.records[0].failed


def test_pydantic_decimal_without_digits():
    from aggify.contrib.pydantic import pydantic_model

    class Money(Document):
        a = DecimalField()
        b = DecimalField(max_digits=5)

        class Meta:
            collection = "money_schema"

    assert set(pydantic_model(Money, name="MoneyOut").model_fields) == {"id", "a", "b"}


def test_embedded_documents_inside_dict_field_resolve():
    class Inner(EmbeddedDocument):
        n = IntField()

    class Holder2(Document):
        by = DictField(EmbeddedField(Inner))

    from aggify.schema import resolve_path

    assert resolve_path(Scope.for_model(Holder2), "by__k__n").db_path == "by.k.n"
