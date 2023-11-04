<p align="center">
  <img src="https://i.imgur.com/LIcqDY1.png?3" alt="Aggify">
</p>
<p align="center">
    <em>Aggify is a Python library to generate MongoDB aggregation pipelines</em>
</p>

[![Package version](https://img.shields.io/pypi/v/aggify?color=%2334D058&label=pypi%20package)](https://pypi.org/project/aggify)
[![Downloads](https://img.shields.io/pypi/dm/aggify)](https://pypi.org/project/aggify)
[![Supported Python versions](https://img.shields.io/pypi/pyversions/aggify.svg?color=%2334D058)](https://pypi.org/project/aggify)
[![Coverage](https://img.shields.io/codecov/c/github/Aggify/aggify)](https://coverage-badge.samuelcolvin.workers.dev/redirect/Aggify/aggify)
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

class PostDocument(Document):
    owner = fields.ReferenceField('AccountDocument', db_field='owner_id')
    caption = fields.StringField()
    location = fields.StringField()
    comment_disabled = fields.BooleanField()
    stat_disabled = fields.BooleanField()
    hashtags = fields.ListField()
    archived_at = fields.LongField()
    deleted_at = fields.LongField()
```

Aggify query:

```python
from aggify import Aggify, Q, F

query = Aggify(PostDocument)

query.filter(deleted_at=None, caption__contains='Aggify').order_by('-_id').lookup(
        AccountDocument, query=[
            Q(_id__exact='owner') & Q(deleted_at=None),
            Q(is_verified__exact=True)
        ], let=['owner'], as_name='owner'
    ).filter(owner__ne=[]).add_fields({
        "aggify": "Aggify is lovely",
    }
    ).project(caption=0).out("post").pipelines
```

Mongoengine equivalent query:

```python
[
        {
            '$match': {
                'caption': {
                    '$options': 'i',
                    '$regex': '.*Aggify.*'
                },
                'deleted_at': None
            }
        },
        {
            '$sort': {
                '_id': -1
            }
        },
        {
            '$lookup': {
                'as': 'owner',
                'from': 'account',
                'let': {
                    'owner': '$owner_id'
                },
                'pipeline': [
                    {
                        '$match': {
                            '$expr': {
                                '$and': [
                                    {
                                        '$eq': ['$_id', '$$owner']
                                    },
                                    {
                                        'deleted_at': None
                                    }
                                ]
                            }
                        }
                    },
                    {
                        '$match': {
                            '$expr': {
                                '$eq': ['$is_verified', True]
                            }
                        }
                    }
                ]
            }
        },
        {
            '$match': {
                'owner': {'$ne': []}
            }
        },
        {
            '$addFields': {
                'aggify': {
                    '$literal': 'Aggify is lovely'
                }
            }
        },
        {
            '$project': {
                'caption': 0
                }
        },
        {
            '$out': 'post'
        }
]
```

In the sample usage above, you can see how Aggify simplifies the construction of MongoDB aggregation pipelines by
allowing you to chain filters, lookups, and other operations to build complex queries.
For more details and examples, please refer to the documentation and codebase.

