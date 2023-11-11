<p align="center">
  <img src="https://i.imgur.com/BBp9vUQ.png" alt="Aggify">
</p>
<p align="center">
    <em>Aggify is a Python library to generate MongoDB aggregation pipelines</em>
</p>

[![Package version](https://img.shields.io/pypi/v/aggify?color=%2334D058&label=pypi%20package)](https://pypi.org/project/aggify)
[![Downloads](https://img.shields.io/pypi/dm/aggify)](https://pypi.org/project/aggify)
[![Supported Python versions](https://img.shields.io/pypi/pyversions/aggify.svg?color=%2334D058)](https://pypi.org/project/aggify)
[![Coverage](https://img.shields.io/codecov/c/github/Aggify/aggify)](https://app.codecov.io/gh/Aggify/aggify)
[![License](https://img.shields.io/github/license/Aggify/aggify.svg)](https://github.com/Aggify/aggify/blob/main/LICENSE)
[![Contributors](https://img.shields.io/github/contributors/Aggify/aggify.svg)](https://github.com/Aggify/aggify/graphs/contributors)
[![Telegram](https://img.shields.io/badge/-telegram-red?color=white&logo=telegram&logoColor=blue)](https://t.me/Aggify)

# Aggify

Aggify is a Python library for generating MongoDB aggregation pipelines, designed to work seamlessly with Mongoengine.
This library simplifies the process of constructing complex MongoDB queries and aggregations using an intuitive and
organized interface.

## Features

- Programmatically build MongoDB aggregation pipelines.
- Filter, project, group, and perform various aggregation operations with ease.
- Supports querying nested documents and relationships defined using Mongoengine.
- Encapsulates aggregation stages for a more organized and maintainable codebase.
- Designed to simplify the process of constructing complex MongoDB queries.

## TODO



## Installation

You can install Aggify using pip:

```bash
pip install aggify
```

## Sample Usage

Here's a code snippet that demonstrates how to use Aggify to construct a MongoDB aggregation pipeline:

```python
from mongoengine import Document, fields

class AccountDocument(Document):
    username = fields.StringField()
    display_name = fields.StringField()
    phone = fields.StringField()
    is_verified = fields.BooleanField()
    disabled_at = fields.LongField()
    deleted_at = fields.LongField()
    banned_at = fields.LongField()

class FollowAccountEdge(Document):
    start = fields.ReferenceField("AccountDocument")
    end = fields.ReferenceField("AccountDocument")
    accepted = fields.BooleanField()
    meta = {
        "collection": "edge.follow.account",
    }

class BlockEdge(Document):
    start = fields.ObjectIdField()
    end = fields.ObjectIdField()
    meta = {
        "collection": "edge.block",
    }
```

Aggify query:

```python
from models import *
from aggify import Aggify, F, Q
from bson import ObjectId

aggify = Aggify(AccountDocument)

pipelines = list(
    (
        aggify.filter(
            phone__in=[],
            id__ne=ObjectId(),
            disabled_at=None,
            banned_at=None,
            deleted_at=None,
            network_id=ObjectId(),
        )
        .lookup(
            FollowAccountEdge,
            let=["id"],
            query=[Q(start__exact=ObjectId()) & Q(end__exact="id")],
            as_name="followed",
        )
        .lookup(
            BlockEdge,
            let=["id"],
            as_name="blocked",
            query=[
                (Q(start__exact=ObjectId()) & Q(end__exact="id"))
                | (Q(end__exact=ObjectId()) & Q(start__exact="id"))
            ],
        )
        .filter(followed=[], blocked=[])
        .group("username")
        .annotate(annotate_name="phone", accumulator="first", f=F("phone") + 10)
        .redact(
            value1="phone",
            condition="==",
            value2="132",
            then_value="keep",
            else_value="prune",
        )
        .project(username=0)[5:10]
        .out(coll="account")
    )
)
```

Mongoengine equivalent query:

```python
[
    {
        "$match": {
            "phone": {"$in": []},
            "_id": {"$ne": ObjectId("65486eae04cce43c5469e0f1")},
            "disabled_at": None,
            "banned_at": None,
            "deleted_at": None,
            "network_id": ObjectId("65486eae04cce43c5469e0f2"),
        }
    },
    {
        "$lookup": {
            "from": "edge.follow.account",
            "let": {"id": "$_id"},
            "pipeline": [
                {
                    "$match": {
                        "$expr": {
                            "$and": [
                                {
                                    "$eq": [
                                        "$start",
                                        ObjectId("65486eae04cce43c5469e0f3"),
                                    ]
                                },
                                {"$eq": ["$end", "$$id"]},
                            ]
                        }
                    }
                }
            ],
            "as": "followed",
        }
    },
    {
        "$lookup": {
            "from": "edge.block",
            "let": {"id": "$_id"},
            "pipeline": [
                {
                    "$match": {
                        "$expr": {
                            "$or": [
                                {
                                    "$and": [
                                        {
                                            "$eq": [
                                                "$start",
                                                ObjectId("65486eae04cce43c5469e0f4"),
                                            ]
                                        },
                                        {"$eq": ["$end", "$$id"]},
                                    ]
                                },
                                {
                                    "$and": [
                                        {
                                            "$eq": [
                                                "$end",
                                                ObjectId("65486eae04cce43c5469e0f5"),
                                            ]
                                        },
                                        {"$eq": ["$start", "$$id"]},
                                    ]
                                },
                            ]
                        }
                    }
                }
            ],
            "as": "blocked",
        }
    },
    {"$match": {"followed": [], "blocked": []}},
    {"$group": {"_id": "$username", "phone": {"$first": {"$add": ["$phone", 10]}}}},
    {
        "$redact": {
            "$cond": {
                "if": {"$eq": ["phone", "132"]},
                "then": "$$KEEP",
                "else": "$$PRUNE",
            }
        }
    },
    {"$project": {"username": 0}},
    {"$skip": 5},
    {"$limit": 5},
    {"$out": "account"},
]
```

In the sample usage above, you can see how Aggify simplifies the construction of MongoDB aggregation pipelines by
allowing you to chain filters, lookups, and other operations to build complex queries.
For more details and examples, please refer to the documentation and codebase.
