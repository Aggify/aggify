"""Document behaviour that needs no database."""

import copy
import pickle

import pytest
from bson import ObjectId

import aggify
from aggify import Document, IntField, ListField, StringField, ValidationError
from tests.models import Account, Address, Post, Role, Slug


def test_defaults_and_conversion_on_init():
    a = Account(username="bob", age="30")
    assert a.age == 30 and a.is_verified is False and a.role is Role.USER and a.tags == []
    assert isinstance(a.pk, ObjectId) and a.id == a.pk
    assert a.attrs == {} and a.attrs is not Account(username="x").attrs  # fresh mutable defaults


def test_assignment_converts():
    a = Account(username="bob")
    a.age = "41"
    a.role = "admin"
    assert (a.age, a.role) == (41, Role.ADMIN)


def test_unknown_kwargs_rejected_unless_non_strict():
    with pytest.raises(TypeError):
        Account(nope=1)

    class Loose(Document):
        x = IntField()

        class Meta:
            strict = False

    assert Loose(x=1, y=2).x == 1


def test_to_mongo_maps_db_fields_and_omits_none():
    a = Account(username="bob", address=Address(city="Tehran", zip_code="123"))
    doc = a.to_mongo()
    assert doc["_id"] == a.pk and doc["address"] == {"city": "Tehran", "zip": "123"}
    assert "age" not in doc and "display_name" not in doc
    assert doc["role"] == "user"


def test_from_mongo_roundtrip():
    a = Account(username="bob", age=3, address={"city": "x", "zip_code": "1"}, tags=["a"], role="admin")
    a.full_clean()
    b = Account.from_mongo(a.to_mongo())
    assert b == a and b.to_mongo() == a.to_mongo()
    assert b.address.zip_code == "1" and b.role is Role.ADMIN and b._state.adding is False


def test_dirty_tracking_without_a_database():
    a = Account.from_mongo(Account(username="bob", age=3, tags=["a"]).to_mongo())
    assert a.changed_fields == []
    a.age = 4
    a.tags.append("b")  # in-place mutation is detected
    a.display_name = "Bob"
    a.username = None  # removing a value
    sets, unsets = a.get_changes()
    assert sets == {"age": 4, "tags": ["a", "b"], "display_name": "Bob"} and unsets == ["username"]
    assert sorted(a.changed_fields) == ["age", "display_name", "tags", "username"]


def test_embedded_mutation_is_detected():
    a = Account.from_mongo(Account(username="bob", address=Address(city="x")).to_mongo())
    a.address.city = "y"
    assert a.changed_fields == ["address"]


def test_partial_documents_only_track_loaded_fields():
    raw = Account(username="bob", age=3).to_mongo()
    partial = Account.from_mongo({"_id": raw["_id"], "username": "bob"}, loaded={"id", "username"})
    assert partial.changed_fields == []  # age was not loaded: it is not 'removed'
    partial.username = "bobby"
    assert partial.get_changes() == ({"username": "bobby"}, [])


def test_equality_and_hash():
    pk = ObjectId()
    a, b = Account(id=pk, username="a"), Account(id=pk, username="b")
    assert a == b and hash(a) == hash(b) and a != Post(id=pk)
    assert len({a, b}) == 1
    e1, e2 = Address(city="x"), Address(city="x")
    assert e1 == e2
    with pytest.raises(TypeError):
        hash(e1)


def test_pk_helpers_and_custom_primary_key():
    s = Slug(code="hello", label="Hi")
    assert s.pk == "hello" and s.to_mongo()["_id"] == "hello" and Slug._meta.pk.name == "code"
    s.pk = "bye"
    assert s.code == "bye"


def test_to_dict():
    a = Account(username="bob", age=3, address=Address(city="x"))
    d = a.to_dict()
    assert d["username"] == "bob" and d["address"]["city"] == "x" and d["role"] is Role.USER
    js = a.to_dict(json_safe=True, include=["id", "username", "role", "created_at", "balance"])
    assert js["id"] == str(a.pk) and js["role"] == "user"
    assert a.to_dict(exclude=["attrs", "tags"], include_none=False).keys() >= {"username"}
    import json

    json.dumps(a.to_dict(json_safe=True))


def test_pickle_and_copy():
    a = Account(username="bob", age=3)
    b = pickle.loads(pickle.dumps(a))
    assert b == a and b.age == 3
    c = copy.deepcopy(a)
    c.age = 9
    assert a.age == 3


def test_meta_options_and_inheritance():
    class Base(Document):
        created = IntField()

        class Meta:
            abstract = True
            ordering = ["-created"]

    class Child(Base):
        name = StringField()

    class Other(Base):
        class Meta:
            collection = "others"

    assert Child._meta.ordering == ["-created"] and list(Child._meta.fields)[:1] == ["id"]
    assert "created" in Child._meta.fields and Child._meta.collection == "child"
    assert Other._meta.collection == "others" and Other._meta.ordering == []
    assert Base._meta.abstract and not hasattr(Base, "objects")


def test_does_not_exist_hierarchy():
    assert issubclass(Account.DoesNotExist, aggify.ObjectDoesNotExist)
    assert Account.DoesNotExist is not Post.DoesNotExist


def test_string_model_references_resolve_lazily():
    class Node(Document):
        parent = aggify.ReferenceField("Node")
        kids = ListField(aggify.ReferenceField("Node"))

    n = Node()
    assert Node._meta.fields["parent"].document_type is Node
    assert n.parent_id is None


def test_manager_not_accessible_from_instances():
    with pytest.raises(AttributeError):
        Account(username="x").objects


def test_custom_manager_and_queryset():
    from aggify import Manager, QuerySet

    class ActiveQS(QuerySet):
        def active(self):
            return self.filter(deleted_at=None)

    class Thing(Document):
        deleted_at = IntField()
        objects = Manager.from_queryset(ActiveQS)()

    assert Thing.objects.active().query["find"]["filter"] == {"deleted_at": None}
    assert Thing.objects.filter(deleted_at=None).__class__ is ActiveQS

    class Soft(Document):
        deleted_at = IntField()
        objects = ActiveQS.as_manager()

    assert Soft.objects.active().model is Soft


def test_non_default_manager_filtering():
    class OnlyVisible(Manager if (Manager := aggify.Manager) else object):
        def get_queryset(self):
            return super().get_queryset().filter(deleted_at=None)

    class Item(Document):
        deleted_at = IntField()
        objects = OnlyVisible()

    assert Item.objects.all().query["find"]["filter"] == {"deleted_at": None}


def test_validation_hook():
    class Range(Document):
        low = IntField()
        high = IntField()

        def clean(self):
            if self.low is not None and self.high is not None and self.low > self.high:
                raise ValidationError("low must be <= high")

    Range(low=1, high=2).full_clean()
    with pytest.raises(ValidationError, match="low must be"):
        Range(low=3, high=2).full_clean()


def test_reference_assignment_forms():
    owner = Account(username="bob")
    p = Post(owner=owner)
    assert p.owner is owner and p.owner_id == owner.pk and p.to_mongo()["owner_id"] == owner.pk
    p2 = Post(owner=str(owner.pk))
    assert p2.owner_id == owner.pk
    p3 = Post(owner_id=owner.pk)
    assert p3.owner_id == owner.pk
    with pytest.raises(ValidationError):
        Post(owner=Post()).to_mongo()


def test_nested_dict_mutation_is_detected_after_load():
    """The change-tracking snapshot must never alias values handed to the user."""
    a = Account.from_mongo(Account(username="bob", attrs={"deep": {"list": [1, 2]}}).to_mongo())
    assert a.changed_fields == []
    a.attrs["deep"]["list"].append(3)
    assert a.changed_fields == ["attrs"]
    assert a.get_changes()[0] == {"attrs": {"deep": {"list": [1, 2, 3]}}}


def test_datetime_is_truncated_to_milliseconds():
    import datetime as dt

    a = Account(username="bob", created_at=dt.datetime(2024, 1, 1, 0, 0, 0, 123456))
    assert a.created_at.microsecond == 123000
