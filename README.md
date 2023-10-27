# Aggify

Aggify is a Python library for generating MongoDB aggregation pipelines, designed to work seamlessly with Mongoengine. This library simplifies the process of constructing complex MongoDB queries and aggregations using an intuitive and organized interface.

## Features

- Programmatically build MongoDB aggregation pipelines.
- Filter, project, group, and perform various aggregation operations with ease.
- Supports querying nested documents and relationships defined using Mongoengine.
- Encapsulates aggregation stages for a more organized and maintainable codebase.
- Designed to simplify the process of constructing complex MongoDB queries.

## TODO

- [x] `$match`: Filters the documents to include only those that match a specified condition.
- [x] `$project`: Reshapes and selects specific fields from the documents.
- [x] `$group`: Groups documents by a specified field and performs aggregation operations within each group.
- [x] `$unwind`: Deconstructs arrays within documents, creating multiple documents for each array element.
- [x] `$limit`: Limits the number of documents in the result.
- [x] `$skip`: Skips a specified number of documents in the result.
- [x] `$lookup`: Performs a left outer join to combine documents from two collections.
- [x] `$sort`: Sorts the documents in the aggregation pipeline.
- [x] `$conf`:
- [ ] `$addFields`: Adds new fields to the documents in the pipeline.
- [ ] `$replaceRoot`: Replaces the document structure with a new one.
- [x] `$group` (with accumulators): Performs various aggregation operations like counting, summing, averaging, and more.
- [ ] `$project` (with expressions): Allows you to use expressions to reshape and calculate values.
- [ ] `$redact`: Controls document inclusion during the aggregation pipeline.
- [ ] `$out`: Writes the result of the aggregation pipeline to a new collection.

- [x] Q function : object is primarily used for complex queries that require logical operations
- [x] F function : object represents the value of a model field, its transformed value, or an annotated column

## Installation

You can install Aggify using pip:

```bash
pip install aggify
```

## Sample Usage

Here's a code snippet that demonstrates how to use Aggify to construct a MongoDB aggregation pipeline:

```python
from aggify import Aggify, Q
from mongoengine import Document, fields
from pprint import pprint

class AccountDocument(Document):
    username = fields.StringField()
    display_name = fields.StringField()
    phone = fields.StringField()
    is_verified = fields.BooleanField()
    disabled_at = fields.LongField()
    deleted_at = fields.LongField()
    banned_at = fields.LongField()
    
    meta = {
        'collection': 'account',
        'ordering': ['-_id'],
        'indexes': [
            'username', 'phone', 'display_name',
            'deleted_at', 'disabled_at', 'banned_at'
        ],
    }
    
class PostDocument(Document):
    owner = fields.ReferenceField('AccountDocument', db_field='owner_id')
    caption = fields.StringField()
    location = fields.StringField()
    comment_disabled = fields.BooleanField()
    stat_disabled = fields.BooleanField()
    hashtags = fields.ListField()
    archived_at = fields.LongField()
    deleted_at = fields.LongField()


# Create Aggify instance with the base model (e.g., PostDocument)
query = Aggify(PostDocument)

pprint(query.filter(caption__contains="hello").pipelines)
# output :
#    [{'$match': {'caption': {'$options': 'i', '$regex': '.*hello.*'}}}]


pprint(query.filter(caption__contains="hello", owner__deleted_at=None).pipelines)
# output :
#         [{'$match': {'caption': {'$options': 'i', '$regex': '.*hello.*'}}},
#          {'$lookup': {'as': 'owner',
#                       'foreignField': '_id',
#                       'from': 'account',
#                       'localField': 'owner_id'}},
#          {'$unwind': {'path': '$owner', 'preserveNullAndEmptyArrays': True}},
#          {'$match': {'owner.deleted_at': None}}]


pprint(
    query.filter(caption__contains="hello").project(caption=1, deleted_at=0).pipelines
)

# output :
#         [{'$match': {'caption': {'$options': 'i', '$regex': '.*hello.*'}}},
#          {'$project': {'caption': 1, 'deleted_at': 0}}]

pprint(
    query.filter(
        (Q(caption__contains=['hello']) | Q(location__contains='test')) & Q(deleted_at=None)
    ).pipelines
)

# output :
        # [{'$match': {'$and': [{'$or': [{'caption': {'$options': 'i',
        #                                             '$regex': ".*['hello'].*"}},
        #                                {'location': {'$options': 'i',
        #                                              '$regex': '.*test.*'}}]},
        #                       {'deleted_at': None}]}}]

pprint(
    query.filter(caption='hello')[3:10].pipelines
)

# output:
#         [{'$match': {'caption': 'hello'}}, {'$skip': 3}, {'$limit': 7}]

pprint(
    query.filter(caption='hello').order_by('-_id').pipelines
)

# output:
#         [{'$match': {'caption': 'hello'}}, {'$sort': {'_id': -1}}]

```

In the sample usage above, you can see how Aggify simplifies the construction of MongoDB aggregation pipelines by allowing you to chain filters, projections, and other operations to build complex queries. The pprint(query.pipelines) line demonstrates how you can inspect the generated aggregation pipeline for debugging or analysis.

For more details and examples, please refer to the documentation and codebase.

