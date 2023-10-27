import pytest

from aggify import Aggify, F, Q, Cond
from mongoengine import Document, StringField, IntField


class BaseModel(Document):
    # Define your fields here
    name = StringField(max_length=100)
    age = IntField()

    meta = {
        'allow_inheritance': True,
        'abstract': True
    }

# This defines a base document model for MongoDB using MongoEngine, with 'name' and 'age' fields.
# The 'allow_inheritance' and 'abstract' options ensure it's used as a base class for other documents.


class TestAggify:
    def test__getitem__int_zero(self):
        aggify = Aggify(BaseModel)
        thing = aggify[0]
        assert not thing.pipelines

    def test__getitem__int_non_zero(self):
        aggify = Aggify(BaseModel)
        thing = aggify[1]
        assert isinstance(thing, Aggify)
        assert len(thing.pipelines) == 1
        assert thing.pipelines[-1]['$limit'] == 1

        thing = aggify[2]
        assert thing.pipelines[-1]['$limit'] == 2
        assert len(thing.pipelines) == 2

        thing = thing[3]
        assert thing.pipelines[-1]['$limit'] == 3
        assert len(thing.pipelines) == 3

    def test__getitem__slice(self):
        aggify = Aggify(BaseModel)
        thing = aggify[0:10]
        assert isinstance(thing, Aggify)
        assert thing.pipelines[-1]['$limit'] == 10
        assert len(thing.pipelines) == 1  # cause start is zero and it is falsy

        aggify = Aggify(BaseModel)
        thing = aggify[2:10]
        assert len(thing.pipelines) == 2
        skip, limit = thing.pipelines[-2:]
        assert skip['$skip'] == 2
        assert limit['$limit'] == 8

    def test__getitem__value_error(self):
        with pytest.raises(ValueError) as err:
            Aggify(BaseModel)['hello']

        assert 'invalid' in err.__str__().lower()

    # Test filtering and projection
    def test_filter_and_project(self):
        aggify = Aggify(BaseModel)
        aggify.filter(age__gte=30).project(name=1, age=1)
        assert len(aggify.pipelines) == 2
        assert aggify.pipelines[1]["$project"] == {"name": 1, "age": 1}

    # Test filtering and ordering
    def test_filter_and_order(self):
        aggify = Aggify(BaseModel)
        aggify.filter(age__gte=30).order_by("-age")
        assert len(aggify.pipelines) == 2
        assert aggify.pipelines[1]["$sort"] == {"age": -1}

    # Test multiple filters and complex conditions
    def test_multiple_filters_and_conditions(self):
        aggify = Aggify(BaseModel)
        age = F("age") * 2
        aggify.filter(Q(name="John") | Q(name="Alice")).project(name=1, age=age.to_dict())
        assert len(aggify.pipelines) == 2
        assert aggify.pipelines[1]["$project"]["age"] == {"$multiply": ["$age", 2]}

    # Test raw aggregation stage
    def test_raw_aggregation_stage(self):
        aggify = Aggify(BaseModel)
        aggify.raw({"$customStage": {"field": "$name"}})
        assert len(aggify.pipelines) == 1
        assert "$customStage" in aggify.pipelines[0]

    # Test combining multiple filters with AND
    def test_combine_filters_with_and(self):
        aggify = Aggify(BaseModel)
        aggify.filter(Q(name="John") & Q(age__gte=30))
        assert len(aggify.pipelines) == 1

    # Test raw aggregation stage followed by filtering
    def test_raw_aggregation_stage_before_filtering(self):
        aggify = Aggify(BaseModel)
        aggify.raw({"$customStage": {"field": "$name"}}).filter(age__gte=30)
        assert len(aggify.pipelines) == 2
        assert "$customStage" in aggify.pipelines[0]

    # Test combining multiple filters with OR
    def test_combine_filters_with_or(self):
        aggify = Aggify(BaseModel)
        aggify.filter(Q(name="John") | Q(name="Alice"))
        assert len(aggify.pipelines) == 1

    # Test ordering by multiple fields
    def test_order_by_multiple_fields(self):
        aggify = Aggify(BaseModel)
        aggify.order_by("name").order_by("-age")
        assert len(aggify.pipelines) == 2
        assert aggify.pipelines[0]["$sort"] == {"name": 1}
        assert aggify.pipelines[1]["$sort"] == {"age": -1}

    # Test complex conditional expression in projection
    def test_complex_conditional_expression_in_projection(self):
        aggify = Aggify(BaseModel)
        aggify.project(name=1, age=1, custom_field=dict(Cond(F("age").to_dict(), '>', 30, "Adult", "Child")))
        assert len(aggify.pipelines) == 1
        assert "custom_field" in aggify.pipelines[0]["$project"]
        assert aggify.pipelines[0]["$project"]["custom_field"]["$cond"]["if"] == {"$gt": ["$age", 30]}
        assert aggify.pipelines[0]["$project"]["custom_field"]["$cond"]["then"] == "Adult"
        assert aggify.pipelines[0]["$project"]["custom_field"]["$cond"]["else"] == "Child"

    # Test filtering using not operator
    def test_filter_with_not_operator(self):
        aggify = Aggify(BaseModel)
        aggify.filter(~Q(name="John"))
        assert len(aggify.pipelines) == 1
        assert aggify.pipelines[0]["$match"]["$not"][0]["name"] == "John"
