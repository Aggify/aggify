import pytest
from mongoengine import Document, IntField, StringField

from aggify import Aggify, Cond, F, Q
from aggify.exceptions import AggifyValueError, AnnotationError, OutStageError


class BaseModel(Document):
    # Define your fields here
    name = StringField(max_length=100)
    age = IntField()

    meta = {"allow_inheritance": True, "abstract": True}


# This defines a base document model for MongoDB using MongoEngine, with 'name' and 'age' fields.
# The 'allow_inheritance' and 'abstract' options ensure it's used as a base class for other documents.


class TestAggify:
    def test__getitem__zero(self):
        aggify = Aggify(BaseModel)
        aggify[0]

    def test__getitem__slice(self):
        aggify = Aggify(BaseModel)
        thing = aggify[0:10]
        assert isinstance(thing, Aggify)
        assert thing.pipelines[-1]["$limit"] == 10
        assert thing.pipelines[-2]["$skip"] == 0

    def test__getitem__value_error(self):
        with pytest.raises(AggifyValueError) as err:
            Aggify(BaseModel)["hello"]  # type: ignore # noqa

        assert "str" in err.__str__(), "wrong type was not detected"

    def test_filtering_and_projection(self):
        aggify = Aggify(BaseModel)
        aggify.filter(age__gte=30).project(name=1, age=1)
        assert len(aggify.pipelines) == 2
        assert aggify.pipelines[1]["$project"] == {"name": 1, "age": 1}

    def test_filtering_and_ordering(self):
        aggify = Aggify(BaseModel)
        aggify.filter(age__gte=30).order_by("-age")
        assert len(aggify.pipelines) == 2
        assert aggify.pipelines[1]["$sort"] == {"age": -1}

    # Test multiple filters and complex conditions
    def test_multiple_filters_and_conditions(self):
        aggify = Aggify(BaseModel)
        age = F("age") * 2
        aggify.filter(Q(name="John") | Q(name="Alice")).project(
            name=1, age=age.to_dict()
        )
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
        aggify.project(
            name=1,
            age=1,
            custom_field=dict(Cond(F("age").to_dict(), ">", 30, "Adult", "Child")),
        )
        assert len(aggify.pipelines) == 1
        assert "custom_field" in aggify.pipelines[0]["$project"]
        assert aggify.pipelines[0]["$project"]["custom_field"]["$cond"]["if"] == {
            "$gt": ["$age", 30]
        }
        assert (
                aggify.pipelines[0]["$project"]["custom_field"]["$cond"]["then"] == "Adult"
        )
        assert (
                aggify.pipelines[0]["$project"]["custom_field"]["$cond"]["else"] == "Child"
        )

    # Test filtering using not operator
    def test_filter_with_not_operator(self):
        aggify = Aggify(BaseModel)
        aggify.filter(~Q(name="John"))
        assert len(aggify.pipelines) == 1
        assert aggify.pipelines[0]["$match"]["$not"][0]["name"] == "John"

    def test_add_field_value_error(self):
        with pytest.raises(AggifyValueError):
            aggify = Aggify(BaseModel)
            fields = {
                "new_field_1": True,
            }
            aggify.add_fields(**fields)

    def test_add_fields_string_literal(self):
        aggify = Aggify(BaseModel)
        fields = {"new_field_1": "some_string", "new_field_2": "another_string"}
        add_fields_stage = aggify.add_fields(**fields)

        expected_stage = {
            "$addFields": {
                "new_field_1": {"$literal": "some_string"},
                "new_field_2": {"$literal": "another_string"},
            }
        }

        assert add_fields_stage.pipelines[0] == expected_stage

    def test_add_fields_with_f_expression(self):
        aggify = Aggify(BaseModel)
        fields = {
            "new_field_1": F("existing_field") + 10,
            "new_field_2": F("field_a") * F("field_b"),
        }
        add_fields_stage = aggify.add_fields(**fields)

        expected_stage = {
            "$addFields": {
                "new_field_1": {"$add": ["$existing_field", 10]},
                "new_field_2": {"$multiply": ["$field_a", "$field_b"]},
            }
        }
        assert add_fields_stage.pipelines[0] == expected_stage

    def test_filter_value_error(self):
        with pytest.raises(AggifyValueError):
            Aggify(BaseModel).filter(arg="Hi")

    def test_group(self):
        aggify = Aggify(BaseModel)
        thing = aggify.group("name")
        assert len(thing.pipelines) == 1
        assert thing.pipelines[-1]["$group"] == {"_id": "$name"}

    def test_annotate_empty_pipeline_value_error(self):
        with pytest.raises(AnnotationError) as err:
            Aggify(BaseModel).annotate("size", "sum", None)

        assert "your pipeline is empty" in err.__str__().lower()

    def test_annotate_not_group_value_error(self):
        with pytest.raises(AnnotationError) as err:
            Aggify(BaseModel)[1].annotate("size", "sum", None)

        assert "not to $limit" in err.__str__().lower()

    def test_annotate_invalid_accumulator(self):
        with pytest.raises(AnnotationError):
            Aggify(BaseModel).group("name").annotate("size", "mahdi", None)

    @pytest.mark.parametrize(
        "accumulator",
        (
                "sum",
                "avg",
                "stdDevPop",
                "stdDevSamp",
                "push",
                "addToSet",
                "count",
                "first",
                "last",
                "max",
                "accumulator",
                "min",
                "median",
                "mergeObjects",
                "top",
                "bottom",
                "topN",
                "bottomN",
                "firstN",
                "lastN",
                "maxN",
        ),
    )
    def test_annotate_with_raw_f(self, accumulator):
        aggify = Aggify(BaseModel)
        thing = aggify.group().annotate("price", accumulator, F("price"))
        assert len(thing.pipelines) == 1
        assert thing.pipelines[-1]["$group"]["price"] == {f"${accumulator}": "$price"}

    @pytest.mark.parametrize(
        "accumulator",
        (
                "sum",
                "avg",
                "stdDevPop",
                "stdDevSamp",
                "push",
                "addToSet",
                "count",
                "first",
                "last",
                "max",
                "accumulator",
                "min",
                "median",
                "mergeObjects",
                "top",
                "bottom",
                "topN",
                "bottomN",
                "firstN",
                "lastN",
                "maxN",
        ),
    )
    def test_annotate_with_f(self, accumulator):
        aggify = Aggify(BaseModel)
        thing = aggify.group().annotate("price", accumulator, F("price") * 10)
        assert len(thing.pipelines) == 1
        assert thing.pipelines[-1]["$group"]["price"] == {
            f"${accumulator}": {"$multiply": ["$price", 10]}
        }

    @pytest.mark.parametrize(
        "accumulator",
        (
                "sum",
                "avg",
                "stdDevPop",
                "stdDevSamp",
                "push",
                "addToSet",
                "count",
                "first",
                "last",
                "max",
                "accumulator",
                "min",
                "median",
                "mergeObjects",
                "top",
                "bottom",
                "topN",
                "bottomN",
                "firstN",
                "lastN",
                "maxN",
        ),
    )
    def test_annotate_raw_value(self, accumulator):
        aggify = Aggify(BaseModel)
        thing = aggify.group().annotate("some_name", accumulator, "name")
        assert len(thing.pipelines) == 1
        assert thing.pipelines[-1]["$group"]["some_name"] == {
            f"${accumulator}": "$name"
        }

    @pytest.mark.parametrize(
        "accumulator",
        (
                "sum",
                "avg",
                "stdDevPop",
                "stdDevSamp",
                "push",
                "addToSet",
                "count",
                "first",
                "last",
                "max",
                "accumulator",
                "min",
                "median",
                "mergeObjects",
                "top",
                "bottom",
                "topN",
                "bottomN",
                "firstN",
                "lastN",
                "maxN",
        ),
    )
    def test_annotate_raw_value_not_model_field(self, accumulator):
        aggify = Aggify(BaseModel)
        thing = aggify.group().annotate("some_name", accumulator, "some_value")
        assert len(thing.pipelines) == 1
        assert thing.pipelines[-1]["$group"]["some_name"] == {
            f"${accumulator}": "some_value"
        }

    @pytest.mark.parametrize(
        "accumulator",
        (
                "sum",
                "avg",
                "stdDevPop",
                "stdDevSamp",
                "push",
                "addToSet",
                "count",
                "first",
                "last",
                "max",
                "accumulator",
                "min",
                "median",
                "mergeObjects",
                "top",
                "bottom",
                "topN",
                "bottomN",
                "firstN",
                "lastN",
                "maxN",
        ),
    )
    def test_annotate_add_annotated_field_to_base_model(self, accumulator):
        aggify = Aggify(BaseModel)
        thing = aggify.group().annotate("some_name", accumulator, "some_value")
        assert len(thing.pipelines) == 1
        assert thing.pipelines[-1]["$group"]["some_name"] == {
            f"${accumulator}": "some_value"
        }
        assert aggify.filter(some_name=123).pipelines[-1] == {
            "$match": {"some_name": 123}
        }

    def test_out_with_project_stage_error(self):
        with pytest.raises(OutStageError):
            Aggify(BaseModel).out("Hi").project(age=1)

    @pytest.mark.parametrize(
        ("method", "args"),
        (
                ("group", ("_id",)),
                ("order_by", ("field",)),
                ("raw", ({"$query": "query"},)),
                ("add_fields", ({"$field": "value"},)),
                ("filter", (Q(age=20),)),
                ("__getitem__", (slice(2, 10),)),
                ("unwind", ("path",)),
        ),
    )
    def test_out_stage_error(self, method, args):
        aggify = Aggify(BaseModel)
        aggify.out("coll")
        with pytest.raises(OutStageError):
            getattr(Aggify, method)(aggify, *args)

    def test_out_db_none(self):
        aggify = Aggify(BaseModel)
        aggify.out("collection")
        assert len(aggify.pipelines) == 1
        assert aggify.pipelines[-1]["$out"] == "collection"

    def test_out(self):
        aggify = Aggify(BaseModel)
        aggify.out("collection", "db_name")
        assert len(aggify.pipelines) == 1
        assert aggify.pipelines[-1]["$out"]["db"] == "db_name"
        assert aggify.pipelines[-1]["$out"]["coll"] == "collection"

    def test_unwind_just_path(self):
        aggify = Aggify(BaseModel)
        thing = aggify.unwind(path="Hello")
        assert len(thing.pipelines) == 1
        assert thing.pipelines[-1]["$unwind"] == "$Hello"

    @pytest.mark.parametrize(
        "params",
        (
                {"include_index_array": "Mahdi"},
                {"preserve": True},
                {"include_index_array": "Mahdi", "preserve": True},
        ),
    )
    def test_unwind_with_parameters(self, params):
        aggify = Aggify(BaseModel)
        thing = aggify.unwind("Hi", **params)
        assert len(thing.pipelines) == 1
        include = params.get("include_index_array")
        preserve = params.get("preserve")
        if include is not None:
            assert thing.pipelines[-1]["$unwind"]["includeArrayIndex"] == "Mahdi"
        if preserve is not None:
            assert thing.pipelines[-1]["$unwind"]["preserveNullAndEmptyArrays"] is True

    def test_regex_exact(self):
        aggify = Aggify(BaseModel)
        thing = list(aggify.filter(name__exact="Aggify"))
        assert thing[-1]["$match"]["name"] == {"$eq": "Aggify"}

    def test_regex_iexact(self):
        aggify = Aggify(BaseModel)
        thing = list(aggify.filter(name__iexact="Aggify"))
        assert thing[-1]["$match"]["name"] == {"$regex": "^Aggify$", "$options": "i"}

    def test_regex_contains(self):
        aggify = Aggify(BaseModel)
        thing = list(aggify.filter(name__contains="Aggify"))
        assert thing[-1]["$match"]["name"] == {"$regex": "Aggify"}

    def test_regex_icontains(self):
        aggify = Aggify(BaseModel)
        thing = list(aggify.filter(name__icontains="Aggify"))
        assert thing[-1]["$match"]["name"] == {"$regex": "Aggify", "$options": "i"}

    def test_regex_startwith(self):
        aggify = Aggify(BaseModel)
        thing = list(aggify.filter(name__startswith="Aggify"))
        assert thing[-1]["$match"]["name"] == {"$regex": "^Aggify"}

    def test_regex_istarstwith(self):
        aggify = Aggify(BaseModel)
        thing = list(aggify.filter(name__istartswith="Aggify"))
        assert thing[-1]["$match"]["name"] == {"$regex": "^Aggify", "$options": "i"}

    def test_regex_endswith(self):
        aggify = Aggify(BaseModel)
        thing = list(aggify.filter(name__endswith="Aggify"))
        assert thing[-1]["$match"]["name"] == {"$regex": "Aggify$"}

    def test_regex_iendswith(self):
        aggify = Aggify(BaseModel)
        thing = list(aggify.filter(name__iendswith="Aggify"))
        assert thing[-1]["$match"]["name"] == {"$regex": "Aggify$", "$options": "i"}

    def test_regex_f_with_exact(self):
        aggify = Aggify(BaseModel)
        thing = list(aggify.filter(name__exact=F("age")))
        assert thing[-1]["$match"] == {"$expr": {"$eq": ["$name", "$age"]}}

    def test_regex_f_with_others(self):
        aggify = Aggify(BaseModel)
        with pytest.raises(ValueError):
            aggify.filter(name__contains=F("age"))
