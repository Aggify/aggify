import pytest

from aggify import F, InvalidArgument, InvalidField, ValidationError
from aggify.updates import build_update
from tests.models import Account, Post


def test_plain_assignment_is_set():
    assert build_update(Account, {"age": "5", "display_name": "x"}) == {"$set": {"age": 5, "display_name": "x"}}


def test_operators():
    out = build_update(
        Account,
        {
            "inc__age": 2,
            "mul__score": 1.5,
            "min__age": 3,
            "max__age": 9,
            "unset__display_name": True,
            "push__tags": "a",
            "add_to_set__tags": "b",
            "pull__tags": "c",
            "pop__tags": -1,
            "rename__display_name": "name",
            "current_date__created_at": True,
        },
    )
    assert out["$inc"] == {"age": 2} and out["$mul"] == {"score": 1.5}
    assert out["$push"] == {"tags": "a"} and out["$addToSet"] == {"tags": "b"}
    assert out["$pull"] == {"tags": "c"} and out["$pop"] == {"tags": -1}
    assert out["$unset"] == {"display_name": ""} and out["$rename"] == {"display_name": "name"}
    assert out["$currentDate"] == {"created_at": True}


def test_many_variants():
    out = build_update(Account, {"push_all__tags": ["a", "b"], "add_to_set_all__tags": ["c"], "pull_all__tags": ["d"]})
    assert out["$push"] == {"tags": {"$each": ["a", "b"]}}
    assert out["$addToSet"] == {"tags": {"$each": ["c"]}}
    assert out["$pullAll"] == {"tags": ["d"]}


def test_nested_paths_use_db_names():
    assert build_update(Account, {"address__zip_code": "1", "inc__attrs__count": 1}) == {
        "$set": {"address.zip": "1"},
        "$inc": {"attrs.count": 1},
    }


def test_expression_switches_to_pipeline_update():
    assert build_update(Account, {"age": F("age") + 1, "display_name": "x"}) == [
        {"$set": {"age": {"$add": ["$age", 1]}, "display_name": "x"}}
    ]


def test_expressions_cannot_mix_with_unsupported_operators():
    with pytest.raises(InvalidArgument):
        build_update(Account, {"age": F("age") + 1, "pull__tags": "x"})
    with pytest.raises(InvalidArgument):
        build_update(Account, {"inc__age": F("age")})


def test_operators_are_rewritten_inside_pipeline_updates():
    out = build_update(Account, {"age": F("age") * 2, "inc__score": 1, "push__tags": "t", "unset__display_name": True})
    assert out == [
        {
            "$set": {
                "age": {"$multiply": ["$age", 2]},
                "score": {"$add": [{"$ifNull": ["$score", 0]}, 1]},
                "tags": {"$concatArrays": [{"$ifNull": ["$tags", []]}, [{"$literal": "t"}]]},
            }
        },
        {"$unset": ["display_name"]},
    ]


def test_pipeline_update_literals_are_wrapped():
    assert build_update(Account, {"age": F("age") + 1, "display_name": "$username"}) == [
        {"$set": {"age": {"$add": ["$age", 1]}, "display_name": {"$literal": "$username"}}}
    ]


def test_pk_cannot_be_updated():
    with pytest.raises(InvalidArgument):
        build_update(Account, {"id": "64b7f0c2e1a4c2d9b8a3f001"})


def test_validation():
    for bad in (
        {"inc__age": "x"},
        {"inc__age": True},
        {"pop__tags": 2},
        {"push_all__tags": "abc"},
        {"push__age": 1},
        {"age": "old"},
        {"username": 5},
    ):
        with pytest.raises((ValidationError, InvalidArgument)):
            build_update(Account, bad)
    with pytest.raises(InvalidField):
        build_update(Account, {"nope": 1})
    with pytest.raises(InvalidArgument):
        build_update(Account, {})


def test_unset_has_no_value_requirements():
    assert build_update(Account, {"unset__age": 1}) == {"$unset": {"age": ""}}


def test_set_to_none_is_allowed():
    assert build_update(Account, {"age": None}) == {"$set": {"age": None}}


def test_references_accept_documents_and_ids():
    a = Account(username="bob")
    assert build_update(Post, {"owner": a}) == {"$set": {"owner_id": a.pk}}
    assert build_update(Post, {"owner": str(a.pk)}) == {"$set": {"owner_id": a.pk}}
