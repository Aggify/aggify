from dataclasses import dataclass

import pytest
from mongoengine import Document, fields

from aggify import Aggify, F, Q  # noqa


class AccountDocument(Document):
    username = fields.StringField()
    display_name = fields.StringField()
    phone = fields.StringField()
    is_verified = fields.BooleanField()
    disabled_at = fields.LongField()
    deleted_at = fields.LongField()
    banned_at = fields.LongField()

    meta = {
        "collection": "account",
        "ordering": ["-_id"],
        "indexes": [
            "username",
            "phone",
            "display_name",
            "deleted_at",
            "disabled_at",
            "banned_at",
        ],
    }


class PostStat(fields.EmbeddedDocument):
    like_count = fields.IntField(default=0)
    view_count = fields.IntField(default=0)
    comment_count = fields.IntField(default=0)

    meta = {"allow_inheritance": True}


class PostDocument(Document):
    owner = fields.ReferenceField("AccountDocument", db_field="owner_id")
    caption = fields.StringField()
    location = fields.StringField()
    comment_disabled = fields.BooleanField()
    stat_disabled = fields.BooleanField()
    hashtags = fields.ListField()
    archived_at = fields.LongField()
    deleted_at = fields.LongField()
    stat = fields.EmbeddedDocumentField(PostStat)


@dataclass
class ParameterTestCase:
    compiled_query: Aggify
    expected_query: list


cases = [
    ParameterTestCase(
        compiled_query=Aggify(PostDocument).filter(
            caption__contains="hello", owner__deleted_at=None
        ),
        expected_query=[
            {"$match": {"caption": {"$options": "i", "$regex": ".*hello.*"}}},
            {
                "$lookup": {
                    "as": "owner",
                    "foreignField": "_id",
                    "from": "account",
                    "localField": "owner_id",
                }
            },
            {
                "$unwind": {
                    "includeArrayIndex": None,
                    "path": "$owner",
                    "preserveNullAndEmptyArrays": True,
                }
            },
            {"$match": {"owner.deleted_at": None}},
        ],
    ),
    ParameterTestCase(
        compiled_query=Aggify(PostDocument)
        .filter(caption__contains="hello")
        .project(caption=1, deleted_at=0),
        expected_query=[
            {"$match": {"caption": {"$options": "i", "$regex": ".*hello.*"}}},
            {"$project": {"caption": 1, "deleted_at": 0}},
        ],
    ),
    ParameterTestCase(
        compiled_query=Aggify(PostDocument).filter(
            (Q(caption__contains=["hello"]) | Q(location__contains="test"))
            & Q(deleted_at=None)
        ),
        expected_query=[
            {
                "$match": {
                    "$and": [
                        {
                            "$or": [
                                {
                                    "caption": {
                                        "$options": "i",
                                        "$regex": ".*['hello'].*",
                                    }
                                },
                                {"location": {"$options": "i", "$regex": ".*test.*"}},
                            ]
                        },
                        {"deleted_at": None},
                    ]
                }
            }
        ],
    ),
    ParameterTestCase(
        compiled_query=Aggify(PostDocument).filter(caption="hello")[3:10],
        expected_query=[{"$match": {"caption": "hello"}}, {"$skip": 3}, {"$limit": 7}],
    ),
    ParameterTestCase(
        compiled_query=Aggify(PostDocument).filter(caption="hello").order_by("-_id"),
        expected_query=[{"$match": {"caption": "hello"}}, {"$sort": {"_id": -1}}],
    ),
    ParameterTestCase(
        compiled_query=(
            Aggify(PostDocument).add_fields(
                **{
                    "new_field_1": "some_string",
                    "new_field_2": F("existing_field") + 10,
                    "new_field_3": F("field_a") * F("field_b"),
                }
            )
        ),
        expected_query=[
            {
                "$addFields": {
                    "new_field_1": {"$literal": "some_string"},
                    "new_field_2": {"$add": ["$existing_field", 10]},
                    "new_field_3": {"$multiply": ["$field_a", "$field_b"]},
                }
            }
        ],
    ),
    ParameterTestCase(
        compiled_query=(
            Aggify(PostDocument).lookup(
                AccountDocument,
                query=[  # noqa
                    Q(_id__ne="owner") & Q(username__ne="seyed"),
                ],
                let=["owner"],
                as_name="_posts",
            )
        ),
        expected_query=[
            {
                "$lookup": {
                    "as": "_posts",
                    "from": "account",
                    "let": {"owner": "$owner_id"},
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {
                                    "$and": [
                                        {"$ne": ["$_id", "$$owner"]},
                                        {"$ne": ["$username", "seyed"]},
                                    ]
                                }
                            }
                        }
                    ],
                }
            }
        ],
    ),
    ParameterTestCase(
        compiled_query=(
            Aggify(PostDocument)
            .lookup(
                AccountDocument,
                query=[  # noqa
                    Q(_id__ne="owner") & Q(username__ne="seyed"),
                ],
                let=["owner"],
                as_name="_posts",
            )
            .filter(_posts__ne=[])
        ),
        expected_query=[
            {
                "$lookup": {
                    "as": "_posts",
                    "from": "account",
                    "let": {"owner": "$owner_id"},
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {
                                    "$and": [
                                        {"$ne": ["$_id", "$$owner"]},
                                        {"$ne": ["$username", "seyed"]},
                                    ]
                                }
                            }
                        }
                    ],
                }
            },
            {"$match": {"_posts": {"$ne": []}}},
        ],
    ),
    ParameterTestCase(
        compiled_query=(
            Aggify(PostDocument)
            .lookup(
                AccountDocument,
                query=[Q(_id__exact="owner"), Q(username__exact="caption")],  # noqa
                let=["owner", "caption"],
                as_name="_posts",
            )
            .filter(_posts__ne=[])
        ),
        expected_query=[
            {
                "$lookup": {
                    "as": "_posts",
                    "from": "account",
                    "let": {"caption": "$caption", "owner": "$owner_id"},
                    "pipeline": [
                        {"$match": {"$expr": {"$eq": ["$_id", "$$owner"]}}},
                        {"$match": {"$expr": {"$eq": ["$username", "$$caption"]}}},
                    ],
                }
            },
            {"$match": {"_posts": {"$ne": []}}},
        ],
    ),
    ParameterTestCase(
        compiled_query=(Aggify(PostDocument).replace_root(embedded_field="stat")),
        expected_query=[{"$replaceRoot": {"$newRoot": "$stat"}}],
    ),
    ParameterTestCase(
        compiled_query=(Aggify(PostDocument).replace_with(embedded_field="stat")),
        expected_query=[{"$replaceWith": "$stat"}],
    ),
    ParameterTestCase(
        compiled_query=(
            Aggify(PostDocument).replace_with(
                embedded_field="stat",
                merge={"like_count": 0, "view_count": 0, "comment_count": 0},
            )
        ),
        expected_query=[
            {
                "$replaceWith": {
                    "$mergeObjects": [
                        {"comment_count": 0, "like_count": 0, "view_count": 0},
                        "$stat",
                    ]
                }
            }
        ],
    ),
    ParameterTestCase(
        compiled_query=(
            Aggify(PostDocument).replace_root(
                embedded_field="stat",
                merge={"like_count": 0, "view_count": 0, "comment_count": 0},
            )
        ),
        expected_query=[
            {
                "$replaceRoot": {
                    "newRoot": {
                        "$mergeObjects": [
                            {"comment_count": 0, "like_count": 0, "view_count": 0},
                            "$stat",
                        ]
                    }
                }
            }
        ],
    ),
    ParameterTestCase(
        compiled_query=(
            Aggify(PostDocument).lookup(
                AccountDocument,
                local_field="owner",
                foreign_field="id",
                as_name="_owner",
            )
        ),
        expected_query=[
            {
                "$lookup": {
                    "as": "_owner",
                    "foreignField": "_id",
                    "from": "account",
                    "localField": "owner_id",
                }
            }
        ],
    ),
    ParameterTestCase(
        compiled_query=(
            Aggify(PostDocument)
            .lookup(
                AccountDocument,
                local_field="owner",
                foreign_field="id",
                as_name="_owner",
            )
            .filter(_owner__username="Aggify")
        ),
        expected_query=[
            {
                "$lookup": {
                    "as": "_owner",
                    "foreignField": "_id",
                    "from": "account",
                    "localField": "owner_id",
                }
            },
            {"$match": {"_owner.username": "Aggify"}},
        ],
    ),
]


@pytest.mark.parametrize("case", cases)
def test_query_compiler(case: ParameterTestCase):
    assert case.compiled_query.pipelines == case.expected_query, case
