Aggify Example
==============

This is an example of using the "aggify" library with MongoDB and "mongoengine."

Aggify is a Python library that helps you create aggregation pipelines for MongoDB using a more Pythonic syntax.

Usage
-----

First, import the necessary modules and create MongoDB Document classes:

.. code-block:: python

    from aggify import Aggify, Q
    from mongoengine import Document, fields

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

       meta = {
           'collection': 'post'
       }


Create an Aggify instance with the base model (e.g., PostDocument):

.. code-block:: python

    query = Aggify(PostDocument)

The following examples demonstrate different aggregation pipeline scenarios:

1. Simple filter based on caption:

.. code-block:: python

   pprint(query.filter(caption__contains="hello").pipelines)

   Output:
   [{'$match': {'caption': {'$options': 'i', '$regex': '.*hello.*'}}}]

2. Filter with a lookup and unwind:

.. code-block:: python

   pprint(query.filter(caption__contains="hello", owner__deleted_at=None).pipelines)

   Output:
   [{'$match': {'caption': {'$options': 'i', '$regex': '.*hello.*'}},
    {'$lookup': {'as': 'owner',
                 'foreignField': '_id',
                 'from': 'account',
                 'localField': 'owner_id'}},
    {'$unwind': {'path': '$owner', 'preserveNullAndEmptyArrays': True}},
    {'$match': {'owner.deleted_at': None}}]

3. Projection:

.. code-block:: python

   pprint(query.filter(caption__contains="hello").project(caption=1, deleted_at=0).pipelines)

   Output:
   [{'$match': {'caption': {'$options': 'i', '$regex': '.*hello.*'}},
    {'$project': {'caption': 1, 'deleted_at': 0}}]

4. Complex filter using Q objects:

.. code-block:: python

   pprint(
        query.filter(
            (Q(caption__contains=['hello']) | Q(location__contains='test')) & Q(deleted_at=None)
        ).pipelines
   )

   Output:
   [{'$match': {'$and': [{'$or': [{'caption': {'$options': 'i',
                                       '$regex': ".*['hello'].*"}},
                                  {'location': {'$options': 'i', '$regex': '.*test.*'}}]},
                        {'deleted_at': None}]}]

5. Slicing and ordering:

.. code-block:: python

   pprint(query.filter(caption='hello')[3:10].pipelines)

   Output:
   [{'$match': {'caption': 'hello'}}, {'$skip': 3}, {'$limit': 7}]

.. code-block:: python

   pprint(query.filter(caption='hello').order_by('-_id').pipelines)

   Output:
   [{'$match': {'caption': 'hello'}}, {'$sort': {'_id': -1}}]

This documentation provides examples of how to use the Aggify library to create MongoDB aggregation pipelines using Pythonic syntax.
