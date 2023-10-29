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


class PostDocument(Document):
    owner = fields.ReferenceField("AccountDocument", db_field="owner_id")
    caption = fields.StringField()
    location = fields.StringField()
    comment_disabled = fields.BooleanField()
    stat_disabled = fields.BooleanField()
    hashtags = fields.ListField()
    archived_at = fields.LongField()
    deleted_at = fields.LongField()


@dataclass
class TestCase:
    compiled_query: Aggify
    expected_query: list


cases = [
    TestCase(
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
            {"$unwind": {"path": "$owner", "preserveNullAndEmptyArrays": True}},
            {"$match": {"owner.deleted_at": None}},
        ],
    ),
    TestCase(
        compiled_query=Aggify(PostDocument)
        .filter(caption__contains="hello")
        .project(caption=1, deleted_at=0),
        expected_query=[
            {"$match": {"caption": {"$options": "i", "$regex": ".*hello.*"}}},
            {"$project": {"caption": 1, "deleted_at": 0}},
        ],
    ),
    TestCase(
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
    TestCase(
        compiled_query=Aggify(PostDocument).filter(caption="hello")[3:10],
        expected_query=[{"$match": {"caption": "hello"}}, {"$skip": 3}, {"$limit": 7}],
    ),
    TestCase(
        compiled_query=Aggify(PostDocument).filter(caption="hello").order_by("-_id"),
        expected_query=[{"$match": {"caption": "hello"}}, {"$sort": {"_id": -1}}],
    ),
    TestCase(
        compiled_query=(
            Aggify(PostDocument).add_fields(
                {
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
    TestCase(
        compiled_query=(
            Aggify(PostDocument).lookup(
                AccountDocument, query=[  # noqa
                    Q(_id__ne='owner') & Q(username__ne='seyed'),
                ], let=['owner'], as_name='posts'
            )
        ),
        expected_query=[
            {'$lookup': {'as': 'posts',
                         'from': 'account',
                         'let': {'owner': '$owner_id'},
                         'pipeline': [{'$match': {'$expr': {'$and': [{'$ne': ['$_id',
                                                                              '$$owner']},
                                                                     {'$ne': ['$username',
                                                                              'seyed']}]}}}]}}
        ],
    ),
    TestCase(
        compiled_query=(
            Aggify(PostDocument).lookup(
                AccountDocument, query=[  # noqa
                    Q(_id__ne='owner') & Q(username__ne='seyed'),
                ], let=['owner'], as_name='posts'
            ).filter(posts__ne=[])
        ),
        expected_query=[
            {'$lookup': {'as': 'posts',
                         'from': 'account',
                         'let': {'owner': '$owner_id'},
                         'pipeline': [{'$match': {'$expr': {'$and': [{'$ne': ['$_id',
                                                                              '$$owner']},
                                                                     {'$ne': ['$username',
                                                                              'seyed']}]}}}]}},
            {'$match': {'posts': {'$ne': []}}}
        ],
    ),
    TestCase(
        compiled_query=(
            Aggify(PostDocument).lookup(
                AccountDocument, query=[  # noqa
                    Q(_id__exact='owner'),
                    Q(username__exact='caption')
                ], let=['owner', 'caption'], as_name='posts'
            ).filter(posts__ne=[])
        ),
        expected_query=[
            {'$lookup': {'as': 'posts',
                         'from': 'account',
                         'let': {'caption': '$caption', 'owner': '$owner_id'},
                         'pipeline': [{'$match': {'$expr': {'$eq': ['$_id', '$$owner']}}},
                                      {'$match': {'$expr': {'$eq': ['$username',
                                                                    '$$caption']}}}]}},
            {'$match': {'posts': {'$ne': []}}}
        ],
    ),
]


@pytest.mark.parametrize("case", cases)
def test_query_compiler(case: TestCase):
    print(str(case.compiled_query))
    print(case.compiled_query.pipelines)
    print(case.expected_query)
    assert case.compiled_query.pipelines == case.expected_query
