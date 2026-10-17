import decimal
import json

import pytest
from bson import ObjectId
from pydantic import ValidationError as PydanticError

from aggify.contrib.pydantic import PyObjectId, from_pydantic, pydantic_model, to_pydantic
from tests.models import Account, Post, Role


def test_output_schema_mirrors_fields():
    Out = pydantic_model(Account, name="AccountOut", exclude=["attrs"])
    fields = Out.model_fields
    assert "attrs" not in fields and set(fields) >= {"id", "username", "age", "address", "role", "balance"}
    schema = Out.model_json_schema()
    assert schema["properties"]["id"]["pattern"] == "^[0-9a-fA-F]{24}$"
    assert schema["properties"]["username"]["anyOf"][0]["maxLength"] == 30


def test_input_schema_drops_server_fields_and_forbids_extras():
    In = pydantic_model(Account, name="AccountIn", for_input=True)
    assert "id" not in In.model_fields and "created_at" not in In.model_fields
    body = In.model_validate({"username": "bob", "age": 3, "address": {"city": "x"}})
    assert body.model_dump(exclude_unset=True) == {"username": "bob", "age": 3, "address": {"city": "x"}}
    with pytest.raises(PydanticError):
        In.model_validate({"username": "bob", "is_admin": True})
    with pytest.raises(PydanticError):
        In.model_validate({"age": 3})  # username required
    with pytest.raises(PydanticError):
        In.model_validate({"username": "x"})  # min_length=2
    with pytest.raises(PydanticError):
        In.model_validate({"username": "bob", "age": 500})


def test_patch_schema_all_optional():
    Patch = pydantic_model(Account, name="AccountPatch", for_input=True, all_optional=True)
    assert Patch.model_validate({}).model_dump(exclude_unset=True) == {}


def test_instance_to_schema_and_json_roundtrip():
    a = Account(username="bob", age=3, balance="1.50", role="admin", address={"city": "x"})
    Out = pydantic_model(Account)
    out = to_pydantic(a, Out)
    assert out.id == a.pk and out.role is Role.ADMIN and out.balance == decimal.Decimal("1.50")
    data = json.loads(out.model_dump_json())
    assert data["id"] == str(a.pk) and data["role"] == "admin"


def test_from_pydantic_creates_documents():
    In = pydantic_model(Account, name="AccountIn2", for_input=True)
    doc = from_pydantic(Account, In.model_validate({"username": "bob", "age": 4}))
    assert isinstance(doc, Account) and doc.age == 4 and doc.pk is not None


def test_pyobjectid_validation():
    from pydantic import TypeAdapter

    ta = TypeAdapter(PyObjectId)
    oid = ObjectId()
    assert ta.validate_python(str(oid)) == oid and ta.validate_python(oid) == oid
    with pytest.raises(PydanticError):
        ta.validate_python("nope")
    assert ta.dump_python(oid, mode="json") == str(oid)


def test_references_and_embedded_lists():
    Out = pydantic_model(Post)
    owner = Account(username="bob")
    post = Post(owner=owner, comments=[{"text": "hi", "author": owner}])
    out = to_pydantic(post, Out)
    assert out.owner == owner.pk and out.comments[0].text == "hi" and out.comments[0].author == owner.pk


def test_schema_is_cached():
    assert pydantic_model(Account, name="X1") is pydantic_model(Account, name="X1")
