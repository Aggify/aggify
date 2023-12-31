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
            {"$match": {"caption": {"$regex": "hello"}}},
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
                    "path": "$owner_id",
                    "preserveNullAndEmptyArrays": True,
                }
            },
            {"$match": {"owner.deleted_at": None}},
        ],
    ),
    ParameterTestCase(
        compiled_query=Aggify(PostDocument)
        .filter(caption__contains="hello")
        .project(caption=1, deleted_at=1),
        expected_query=[
            {"$match": {"caption": {"$regex": "hello"}}},
            {"$project": {"caption": 1, "deleted_at": 1}},
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
                                        "$regex": "['hello']",
                                    }
                                },
                                {"location": {"$regex": "test"}},
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
            .unwind("_posts")
        ),
        expected_query=[
            {
                "$lookup": {
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
                    "as": "_posts",
                }
            },
            {"$unwind": "$_posts"},
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
        expected_query=[{"$replaceRoot": {"newRoot": "$stat"}}],
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
    ParameterTestCase(
        compiled_query=(
            Aggify(AccountDocument).redact("$username", ">=", "$age", "prune", "keep")
        ),
        expected_query=[
            {
                "$redact": {
                    "$cond": {
                        "if": {"$gte": ["$username", "$age"]},
                        "then": "$$PRUNE",
                        "else": "$$KEEP",
                    }
                }
            }
        ],
    ),
    ParameterTestCase(
        compiled_query=(
            Aggify(AccountDocument).redact(
                "$username", ">=", "$age", "PRUne", "$$$keep"
            )
        ),
        expected_query=[
            {
                "$redact": {
                    "$cond": {
                        "if": {"$gte": ["$username", "$age"]},
                        "then": "$$PRUNE",
                        "else": "$$KEEP",
                    }
                }
            }
        ],
    ),
    ParameterTestCase(
        compiled_query=(
            Aggify(PostDocument)
            .lookup(
                AccountDocument,
                as_name="post_owner",
                query=[Q(owner__exact="owner")],
                let=["owner"],
            )
            .lookup(
                AccountDocument,
                query=[Q(username__exact="post_owner__username")],
                let=["post_owner__username"],
                as_name="test",
            )
        ),
        expected_query=[
            {
                "$lookup": {
                    "from": "account",
                    "let": {"owner": "$owner_id"},
                    "pipeline": [
                        {"$match": {"$expr": {"$eq": ["$$owner", "$$owner"]}}}
                    ],
                    "as": "post_owner",
                }
            },
            {
                "$lookup": {
                    "from": "account",
                    "let": {"post_owner__username": "$post_owner.username"},
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {
                                    "$eq": ["$username", "$$post_owner__username"]
                                }
                            }
                        }
                    ],
                    "as": "test",
                }
            },
        ],
    ),
    ParameterTestCase(
        compiled_query=(
            Aggify(PostDocument)
            .group("owner")
            .annotate("likes", "first", "stat__like_count")
        ),
        expected_query=[
            {"$group": {"_id": "$owner_id", "likes": {"$first": "$stat.like_count"}}}
        ],
    ),
    ParameterTestCase(
        compiled_query=(
            Aggify(PostDocument).group("owner").annotate("sss", "first", "sss")
        ),
        expected_query=[{"$group": {"_id": "$owner_id", "sss": {"$first": "sss"}}}],
    ),
    ParameterTestCase(
        compiled_query=(
            Aggify(PostDocument)
            .group("stat__like_count")
            .annotate("sss", "first", "sss")
        ),
        expected_query=[
            {"$group": {"_id": "$stat.like_count", "sss": {"$first": "sss"}}}
        ],
    ),
    ParameterTestCase(
        compiled_query=(
            Aggify(PostDocument).lookup(
                AccountDocument,
                let=["caption"],
                raw_let={
                    "latest_story_id": {"$last": {"$slice": ["$owner.story", -1]}}
                },
                query=[
                    Q(end__exact="caption") & Q(start__exact="$$latest_story_id._id")
                ],
                as_name="is_seen",
            )
        ),
        expected_query=[
            {
                "$lookup": {
                    "from": "account",
                    "let": {
                        "caption": "$caption",
                        "latest_story_id": {"$last": {"$slice": ["$owner.story", -1]}},
                    },
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {
                                    "$and": [
                                        {"$eq": ["$end", "$$caption"]},
                                        {"$eq": ["$start", "$$latest_story_id._id"]},
                                    ]
                                }
                            }
                        }
                    ],
                    "as": "is_seen",
                }
            }
        ],
    ),
    ParameterTestCase(
        compiled_query=(
            Aggify(PostDocument)
            .lookup(
                PostDocument,
                local_field="stat",
                foreign_field="id",
                as_name="saved_post",
            )
            .replace_root(embedded_field="saved_post")
        ),
        expected_query=[
            {
                "$lookup": {
                    "as": "saved_post",
                    "foreignField": "_id",
                    "from": "post_document",
                    "localField": "stat",
                }
            },
            {"$replaceRoot": {"newRoot": "$saved_post"}},
        ],
    ),
    ParameterTestCase(
        compiled_query=(Aggify(PostDocument).filter(stat__like_count=2)),
        expected_query=[{"$match": {"stat.like_count": 2}}],
    ),
]


@pytest.mark.parametrize("case", cases)
def test_query_compiler(case: ParameterTestCase):
    assert case.compiled_query.pipelines == case.expected_query, case
