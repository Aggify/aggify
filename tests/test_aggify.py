import pytest
from mongoengine import Document, IntField, StringField

from aggify import Aggify, Cond, F, Q
from aggify.exceptions import (
    AggifyValueError,
    AnnotationError,
    OutStageError,
    InvalidArgument,
    InvalidField,
    InvalidOperator,
    AlreadyExistsField,
    InvalidEmbeddedField,
    MongoIndexError,
)


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
        assert aggify[0]

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
            # noinspection PyTypeChecker
            Aggify(BaseModel).filter(arg="Hi")

    def test_group(self):
        aggify = Aggify(BaseModel)
        thing = aggify.group("name")
        assert len(thing.pipelines) == 1
        assert thing.pipelines[-1]["$group"] == {"_id": "$name"}

    def test_annotate_empty_pipeline_value_error(self):
        with pytest.raises(AnnotationError) as err:
            # noinspection PyTypeChecker
            Aggify(BaseModel).annotate("size", "sum", None)

        assert "your pipeline is empty" in err.__str__().lower()

    def test_annotate_not_group_value_error(self):
        with pytest.raises(AnnotationError) as err:
            # noinspection PyTypeChecker
            Aggify(BaseModel)[1].annotate("size", "sum", None)

        assert "not to $limit" in err.__str__().lower()

    def test_annotate_invalid_accumulator(self):
        with pytest.raises(AnnotationError):
            # noinspection PyTypeChecker
            Aggify(BaseModel).group("name").annotate("size", "mahdi", None)

    # noinspection SpellCheckingInspection
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

    # noinspection SpellCheckingInspection
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

    # noinspection SpellCheckingInspection
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

    # noinspection SpellCheckingInspection
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

    # noinspection SpellCheckingInspection
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
        thing = aggify.unwind(path="name")
        assert len(thing.pipelines) == 1
        assert thing.pipelines[-1]["$unwind"] == "$name"

    @pytest.mark.parametrize(
        "params",
        (
            {"include_array_index": "Mahdi"},
            {"preserve": True},
            {"include_array_index": "Mahdi", "preserve": True},
        ),
    )
    def test_unwind_with_parameters(self, params):
        aggify = Aggify(BaseModel)
        thing = aggify.unwind("name", **params)
        assert len(thing.pipelines) == 1
        include = params.get("include_array_index")
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

    # noinspection SpellCheckingInspection
    def test_regex_icontains(self):
        aggify = Aggify(BaseModel)
        thing = list(aggify.filter(name__icontains="Aggify"))
        assert thing[-1]["$match"]["name"] == {"$regex": "Aggify", "$options": "i"}

    # noinspection SpellCheckingInspection
    def test_regex_startwith(self):
        aggify = Aggify(BaseModel)
        thing = list(aggify.filter(name__startswith="Aggify"))
        assert thing[-1]["$match"]["name"] == {"$regex": "^Aggify"}

    # noinspection SpellCheckingInspection
    def test_regex_istarstwith(self):
        aggify = Aggify(BaseModel)
        thing = list(aggify.filter(name__istartswith="Aggify"))
        assert thing[-1]["$match"]["name"] == {"$regex": "^Aggify", "$options": "i"}

    def test_regex_endswith(self):
        aggify = Aggify(BaseModel)
        thing = list(aggify.filter(name__endswith="Aggify"))
        assert thing[-1]["$match"]["name"] == {"$regex": "Aggify$"}

    # noinspection SpellCheckingInspection
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

    def test_aggregate_failed_connection(self):
        aggify = Aggify(BaseModel)
        with pytest.raises(ValueError):
            aggify.filter(name__contains=F("age")).aggregate()

    def test_annotate_str_field_as_value_but_not_base_model_field(self):
        thing = list(Aggify(BaseModel).group("name").annotate("age", "sum", "test"))
        assert thing[0]["$group"] == {"_id": "$name", "age": {"$sum": "test"}}

    def test_lookup_not_pass_let_and_local_field_and_foreign_field(self):
        aggify = Aggify(BaseModel)
        with pytest.raises(InvalidArgument):
            aggify.lookup(BaseModel, as_name="test")

    def test_lookup_pass_let_and_not_pass_query(self):
        aggify = Aggify(BaseModel)
        with pytest.raises(InvalidArgument):
            aggify.lookup(BaseModel, as_name="test", let=["123"])

    def test_redact_invalid_value(self):
        aggify = Aggify(BaseModel)
        with pytest.raises(InvalidArgument):
            aggify.redact("name", "==", "age", "123", "456")

    def test_redact(self):
        aggify = Aggify(BaseModel)
        thing = list(aggify.redact("name", "==", "age", "PRune", "$$$$keep"))
        assert thing[0]["$redact"] == {
            "$cond": {
                "if": {"$eq": ["name", "age"]},
                "then": "$$PRUNE",
                "else": "$$KEEP",
            }
        }

    def test_lookup_use_aggify_instance_in_query(self):
        aggify = Aggify(BaseModel)
        thing = list(
            aggify.lookup(
                BaseModel,
                let=["name"],
                as_name="__",
                query=[Aggify(BaseModel).filter(name=123)],
            )
        )
        assert thing[0]["$lookup"] == {
            "from": None,
            "let": {"name": "$name"},
            "pipeline": [{"$match": {"name": 123}}],
            "as": "__",
        }

    def test_unwind_invalid_field(self):
        aggify = Aggify(BaseModel)
        with pytest.raises(InvalidField):
            aggify.unwind("invalid")

    def test_in_operator(self):
        thing = list(Aggify(BaseModel).filter(name__in=[]))
        assert thing[0]["$match"] == {"name": {"$in": []}}

    def test_nin_operator(self):
        thing = list(Aggify(BaseModel).filter(name__nin=[]))
        assert thing[0]["$match"] == {"name": {"$nin": []}}

    def test_eq_operator(self):
        thing = list(Aggify(BaseModel).filter(name__exact=[]))
        assert thing[0]["$match"] == {"name": {"$eq": []}}

    def test_invalid_operator(self):
        aggify = Aggify(BaseModel)
        with pytest.raises(InvalidOperator):
            aggify.filter(name__aggify="test")

    def test_lookup_with_duplicate_as_name(self):
        aggify = Aggify(BaseModel)
        with pytest.raises(AlreadyExistsField):
            aggify.lookup(
                BaseModel, local_field="name", foreign_field="name", as_name="name"
            )

    def test_project_delete_id(self):
        thing = list(Aggify(BaseModel).project(id=0))
        assert thing[0]["$project"] == {"_id": 0}

    def test_add_field_list_as_expression(self):
        thing = list(Aggify(BaseModel).add_fields(new=[]))
        assert thing[0]["$addFields"] == {"new": []}

    def test_add_field_cond_as_expression(self):
        thing = list(Aggify(BaseModel).add_fields(new=Cond("name", "==", "name", 0, 1)))
        assert thing[0]["$addFields"] == {
            "new": {"$cond": {"if": {"$eq": ["name", "name"]}, "then": 0, "else": 1}}
        }

    def test_annotate_int_field(self):
        thing = list(Aggify(BaseModel).group("name").annotate("name", "first", 2))
        assert thing[0]["$group"] == {"_id": "$name", "name": {"$first": 2}}

    def test_sequential_matches_combine(self):
        thing = list(Aggify(BaseModel).filter(name=123).filter(age=123))
        assert thing[0]["$match"] == {"name": 123, "age": 123}

    def test_get_model_field_invalid_field(self):
        aggify = Aggify(BaseModel)
        with pytest.raises(InvalidField):
            aggify.get_model_field(BaseModel, "username")

    def test_replace_base_invalid_embedded_field(self):
        aggify = Aggify(BaseModel)
        with pytest.raises(InvalidEmbeddedField):
            aggify._replace_base("name")

    def test_aggify_get_item_negative_index(self):
        aggify = Aggify(BaseModel)
        with pytest.raises(MongoIndexError):
            var = aggify.filter(name=1)[-10]

    def test_aggify_get_item_slice_step_not_none(self):
        aggify = Aggify(BaseModel)
        with pytest.raises(MongoIndexError):
            var = aggify.filter(name=1)[slice(1, 3, 2)]

    def test_aggify_get_item_slice_start_gte_stop(self):
        aggify = Aggify(BaseModel)
        with pytest.raises(MongoIndexError):
            var = aggify.filter(name=1)[slice(3, 1)]

    def test_aggify_get_item_slice_negative_start(self):
        aggify = Aggify(BaseModel)
        with pytest.raises(MongoIndexError):
            var = aggify.filter(name=1)[slice(-5, -1)]
