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
- [ ] `$sort`: Sorts the documents in the aggregation pipeline.
- [ ] `$addFields`: Adds new fields to the documents in the pipeline.
- [ ] `$replaceRoot`: Replaces the document structure with a new one.
- [ ] `$group` (with accumulators): Performs various aggregation operations like counting, summing, averaging, and more.
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
from models import PostDocument
from pprint import pprint

# Create a Aggify instance with the base model (e.g., PostDocument)
query = Aggify(PostDocument)

# Define a complex query with filters and projections
var = query.filter(
    caption__contains="ssad",
    location__in=[1, 2, 3],
    archived_at=False,
    owner__deleted_at=None,
    owner__disabled_at=None
).project(
    caption=1,
    deleted_at=0
).filter(
    location__in=[1, 2, 3],
    owner__deleted_at=None
).filter(
    (Q(caption__in=[1, 2, 3]) | Q(location__contains='test')) & Q(deleted_at=None)
).order_by('-code')[3:7]

# Get the aggregated results
result = query.aggregate()

# Print the generated aggregation pipelines
pprint(query.pipelines)
```

In the sample usage above, you can see how Aggify simplifies the construction of MongoDB aggregation pipelines by allowing you to chain filters, projections, and other operations to build complex queries. The pprint(query.pipelines) line demonstrates how you can inspect the generated aggregation pipeline for debugging or analysis.

For more details and examples, please refer to the documentation and codebase.

