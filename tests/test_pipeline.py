"""Pipeline builder output (no database needed)."""

import pytest

from aggify import (
    Aggify,
    AlreadyExistsField,
    AnnotationError,
    Concat,
    Cond,
    Count,
    F,
    First,
    InvalidArgument,
    InvalidEmbeddedField,
    InvalidField,
    InvalidProjection,
    MongoIndexError,
    OutStageError,
    PipelineError,
    Q,
    Rank,
    SecurityError,
    Sum,
    Value,
    Var,
)
from tests.models import Account, Post


def P(model=Account):
    return Aggify(model)


class TestImmutability:
    def test_methods_return_new_pipelines(self):
        base = P().filter(username="a")
        a = base.filter(age__gt=1)
        b = base.order_by("age")
        assert base.to_list() == [{"$match": {"username": "a"}}]
        assert a.to_list() != b.to_list()

    def test_repr_and_iteration_protocol(self):
        p = P().filter(username="a").limit(3)
        assert list(p) == p.to_list() and len(p) == 2 and "Account" in repr(p)


class TestMatch:
    def test_adjacent_matches_merge(self):
        p = P().filter(username="a").filter(age__gt=1).filter(age__lt=5)
        assert p.to_list() == [{"$match": {"username": "a", "age": {"$gt": 1, "$lt": 5}}}]

    def test_conflicting_keys_use_and_instead_of_overwriting(self):
        p = P().filter(username="a").filter(username="b")
        assert p.to_list() == [{"$match": {"username": "a", "$and": [{"username": "b"}]}}]

    def test_exclude(self):
        assert P().exclude(username="a").to_list() == [{"$match": {"$nor": [{"username": "a"}]}}]

    def test_empty_filter_adds_nothing(self):
        assert P().filter().to_list() == [] and P().filter(Q()).to_list() == []

    def test_join_stages_for_reference_paths(self):
        p = P(Post).filter(caption__contains="hi", owner__deleted_at=None)
        assert p.to_list() == [
            {"$lookup": {"from": "account", "localField": "owner_id", "foreignField": "_id", "as": "_j_owner"}},
            {"$unwind": {"path": "$_j_owner", "preserveNullAndEmptyArrays": True}},
            {"$match": {"caption": {"$regex": "hi"}, "_j_owner.deleted_at": None}},
        ]

    def test_join_is_not_repeated(self):
        p = P(Post).filter(owner__age__gt=1).order_by("-owner__age").filter(owner__username="x")
        assert [next(iter(s)) for s in p.to_list()].count("$lookup") == 1

    def test_invalid_field(self):
        with pytest.raises(InvalidField):
            P().filter(nope=1)


class TestSortSkipLimit:
    def test_order_by(self):
        assert P().order_by("-age", "username").to_list() == [{"$sort": {"age": -1, "username": 1}}]
        assert P().order_by(F("age").desc()).to_list() == [{"$sort": {"age": -1}}]

    def test_order_by_replaces_previous_sort(self):
        assert P().order_by("age").order_by("-age").to_list() == [{"$sort": {"age": -1}}]

    @pytest.mark.parametrize(
        "sl,expected",
        [
            (slice(5, 10), [{"$skip": 5}, {"$limit": 5}]),
            (slice(None, 3), [{"$limit": 3}]),
            (slice(2, None), [{"$skip": 2}]),
            (4, [{"$skip": 4}, {"$limit": 1}]),
        ],
    )
    def test_slicing(self, sl, expected):
        assert P()[sl].to_list() == expected

    @pytest.mark.parametrize("bad", [-1, slice(-1, 3), slice(5, 2), slice(0, 10, 2), "x", 1.5])
    def test_bad_indexes(self, bad):
        with pytest.raises((MongoIndexError, Exception)):
            P()[bad]

    def test_max_limit_setting(self):
        import aggify

        aggify.configure(max_limit=10)
        with pytest.raises(InvalidArgument):
            P().limit(11)
        P().limit(10)


class TestProject:
    def test_inclusion_keeps_pk_by_default(self):
        p = P().project(username=1, age=1)
        assert p.to_list() == [{"$project": {"username": 1, "age": 1}}]
        assert set(p.scope.fields) == {"id", "username", "age"}

    def test_exclude_pk(self):
        p = P().project(username=1, pk=0)
        assert p.to_list() == [{"$project": {"_id": 0, "username": 1}}]
        assert set(p.scope.fields) == {"username"}

    def test_exclusion(self):
        p = P().project(username=0)
        assert "username" not in p.scope.fields and "age" in p.scope.fields

    def test_mixing_is_an_error(self):
        with pytest.raises(InvalidProjection):
            P().project(username=1, age=0)

    def test_computed_fields_enter_the_scope(self):
        p = P().project(full=Concat("username", Value(" "), "display_name"), pk=0)
        assert p.to_list() == [{"$project": {"full": {"$concat": ["$username", " ", "$display_name"]}, "_id": 0}}]
        p.filter(full="x")  # usable afterwards

    def test_fields_vanish_after_project(self):
        with pytest.raises(InvalidField):
            P().project(username=1).filter(age=3)

    def test_only_defer(self):
        assert P().only("username").to_list() == [{"$project": {"username": 1}}]
        assert P().defer("age").to_list() == [{"$project": {"age": 0}}]


class TestAddFieldsAndAnnotate:
    def test_add_fields_literals_and_expressions(self):
        p = P().add_fields(label="x", doubled=F("age") * 2, flag=Cond(Q(age__gt=1), 1, 0), meta__src="y")
        assert p.to_list() == [
            {
                "$addFields": {
                    "label": "x",
                    "doubled": {"$multiply": ["$age", 2]},
                    "flag": {"$cond": {"if": {"$gt": ["$age", 1]}, "then": 1, "else": 0}},
                    "meta.src": "y",
                }
            }
        ]
        assert "doubled" in p.scope.fields

    def test_string_values_cannot_inject_expressions(self):
        p = P().add_fields(label="$username")
        assert p.to_list() == [{"$addFields": {"label": {"$literal": "$username"}}}]

    def test_added_field_usable_in_later_stages(self):
        P().add_fields(x=F("age") + 1).filter(x__gt=3).order_by("x")

    def test_annotate_scalar(self):
        assert P().annotate(n=F("age") + 1).to_list() == [{"$addFields": {"n": {"$add": ["$age", 1]}}}]

    def test_aggregate_annotation_requires_group(self):
        with pytest.raises(AnnotationError):
            P().annotate(n=Count())

    def test_unset(self):
        assert P().unset("age").to_list() == [{"$unset": ["age"]}]


class TestGroup:
    def test_group_by_none_field_list_dict(self):
        assert P().group(None, n=Count()).to_list() == [{"$group": {"_id": None, "n": {"$sum": 1}}}]
        assert P().group("age", n=Count()).to_list() == [{"$group": {"_id": "$age", "n": {"$sum": 1}}}]
        assert P().group(["age", "role"]).to_list() == [{"$group": {"_id": {"age": "$age", "role": "$role"}}}]
        assert P().group({"decade": F("age") / 10}).to_list() == [
            {"$group": {"_id": {"decade": {"$divide": ["$age", 10]}}}}
        ]

    def test_accumulators_must_be_aggregates(self):
        with pytest.raises(AnnotationError):
            P().group("age", x=F("age"))

    def test_annotate_after_group_merges_into_group(self):
        p = P().group("username").annotate(total=Sum("age"), first_score=First("score"))
        assert p.to_list() == [
            {"$group": {"_id": "$username", "total": {"$sum": "$age"}, "first_score": {"$first": "$score"}}}
        ]

    def test_legacy_annotate_signature(self):
        p = P().group("username").annotate(annotate_name="phone", accumulator="first", f=F("age") + 10)
        assert p.to_list()[0]["$group"]["phone"] == {"$first": {"$add": ["$age", 10]}}
        with pytest.raises(AnnotationError):
            P().group("username").annotate(annotate_name="x", accumulator="bogus", f="age")

    def test_scope_after_group(self):
        p = P().group("username", n=Count())
        assert set(p.scope.fields) == {"_id", "n"}
        with pytest.raises(InvalidField):
            p.filter(age=1)
        p.filter(n__gt=2).order_by("-n")  # HAVING / sort on accumulators


class TestUnwindLookup:
    def test_unwind_variants(self):
        assert P().unwind("tags").to_list() == [{"$unwind": "$tags"}]
        assert P().unwind("tags", preserve=True, include_array_index="i").to_list() == [
            {"$unwind": {"path": "$tags", "includeArrayIndex": "i", "preserveNullAndEmptyArrays": True}}
        ]

    def test_unwind_unwraps_list_type(self):
        from aggify import StringField

        assert isinstance(P().unwind("tags").scope.fields["tags"], StringField)

    def test_unwind_unknown_field(self):
        with pytest.raises(InvalidField):
            P().unwind("nope")

    def test_lookup_equality_form(self):
        p = P().lookup(Post, "posts", local_field="id", foreign_field="owner")
        assert p.to_list() == [
            {"$lookup": {"from": "post", "localField": "_id", "foreignField": "owner_id", "as": "posts"}}
        ]
        p.filter(posts__caption="x")  # joined collection stays addressable

    def test_lookup_let_pipeline_form(self):
        p = P().lookup(Post, "paid", let={"uid": F("id")}, query=Q(owner=Var("uid"), published=True))
        (stage,) = p.to_list()
        assert stage["$lookup"]["let"] == {"uid": "$_id"}
        assert stage["$lookup"]["pipeline"] == [
            {"$match": {"$expr": {"$eq": ["$owner_id", "$$uid"]}, "published": True}}
        ]

    def test_lookup_with_sub_pipeline(self):
        sub = Aggify(Post).filter(published=True).limit(3)
        p = P().lookup(Post, "latest", let={"uid": F("id")}, pipeline=sub)
        assert p.to_list()[0]["$lookup"]["pipeline"] == sub.to_list()

    def test_lookup_unwind_convenience(self):
        p = P(Post).lookup(Account, "owner_doc", local_field="owner", foreign_field="id", unwind=True)
        assert [next(iter(s)) for s in p.to_list()] == ["$lookup", "$unwind"]
        p.filter(owner_doc__username="x")

    def test_lookup_duplicate_alias(self):
        with pytest.raises(AlreadyExistsField):
            P().lookup(Post, "username", local_field="id", foreign_field="owner")

    def test_lookup_requires_arguments(self):
        with pytest.raises(InvalidArgument):
            P().lookup(Post, "x")

    def test_graph_lookup(self):
        p = P().graph_lookup(Account, F("age"), "age", "score", "chain", max_depth=2)
        assert p.to_list()[0]["$graphLookup"]["maxDepth"] == 2


class TestRootsAndRedact:
    def test_replace_root_embedded(self):
        p = P().replace_root("address")
        assert p.to_list() == [{"$replaceRoot": {"newRoot": "$address"}}]
        assert set(p.scope.fields) == {"city", "zip_code", "geo"}

    def test_replace_with_merge_defaults(self):
        p = P().replace_with("address", merge={"city": "n/a"})
        assert p.to_list() == [{"$replaceWith": {"$mergeObjects": [{"city": "n/a"}, "$address"]}}]

    def test_replace_root_requires_embedded(self):
        with pytest.raises(InvalidEmbeddedField):
            P().replace_root("username")

    def test_redact(self):
        p = P().redact(Q(age__gt=3), "descend", "PRUNE")
        assert p.to_list() == [
            {"$redact": {"$cond": {"if": {"$gt": ["$age", 3]}, "then": "$$DESCEND", "else": "$$PRUNE"}}}
        ]
        with pytest.raises(InvalidArgument):
            P().redact(Q(age=1), "keep", "explode")


class TestOtherStages:
    def test_facet_bucket_count_sample(self):
        p = P().facet(young=Aggify(Account).filter(age__lt=30).count("n"), all=[{"$count": "n"}])
        assert p.to_list()[0]["$facet"]["young"] == [{"$match": {"age": {"$lt": 30}}}, {"$count": "n"}]
        assert P().bucket("age", [0, 18, 65], default="other", n=Count()).to_list() == [
            {
                "$bucket": {
                    "groupBy": "$age",
                    "boundaries": [0, 18, 65],
                    "default": "other",
                    "output": {"n": {"$sum": 1}},
                }
            }
        ]
        assert P().sample(5).to_list() == [{"$sample": {"size": 5}}]
        assert P().sort_by_count("role").to_list() == [{"$sortByCount": "$role"}]

    def test_window_fields(self):
        p = P().set_window_fields(
            partition_by="role",
            sort_by={"age": 1},
            rank=Rank(),
            running=Sum("score", window={"documents": ["unbounded", "current"]}),
        )
        spec = p.to_list()[0]["$setWindowFields"]
        assert spec["partitionBy"] == "$role" and spec["sortBy"] == {"age": 1}
        assert spec["output"]["rank"] == {"$rank": {}}
        assert spec["output"]["running"] == {"$sum": "$score", "window": {"documents": ["unbounded", "current"]}}

    def test_union_with(self):
        assert P().union_with(Post).to_list() == [{"$unionWith": {"coll": "post"}}]

    def test_geo_near_must_be_first(self):
        assert "$geoNear" in P().geo_near((1.0, 2.0), "d").to_list()[0]
        with pytest.raises(PipelineError):
            P().filter(age=1).geo_near((1.0, 2.0))


class TestOutAndTerminalStages:
    def test_out(self):
        assert P().out("archive").to_list() == [{"$out": "archive"}]
        assert P().out("archive", db="other").to_list() == [{"$out": {"db": "other", "coll": "archive"}}]

    @pytest.mark.parametrize(
        "method,args",
        [
            ("filter", ()),
            ("order_by", ("age",)),
            ("limit", (3,)),
            ("skip", (3,)),
            ("unwind", ("tags",)),
            ("project", ()),
            ("add_fields", ()),
            ("group", ()),
        ],
    )
    def test_nothing_after_out(self, method, args):
        p = P().out("x")
        kwargs = (
            {"username": "a"}
            if method == "filter"
            else ({"username": 1} if method == "project" else ({"x": 1} if method == "add_fields" else {}))
        )
        with pytest.raises(OutStageError):
            getattr(p, method)(*args, **kwargs)

    def test_out_collection_name_validated(self):
        with pytest.raises(Exception):
            P().out("$bad")

    def test_merge(self):
        assert P().merge("target", on="_id").to_list() == [
            {"$merge": {"into": "target", "on": "_id", "whenMatched": "merge", "whenNotMatched": "insert"}}
        ]


class TestRawAndSecurity:
    def test_raw_stage_and_open_scope(self):
        p = P().raw({"$limit": 3}).filter(anything_after=1)
        assert p.to_list()[0] == {"$limit": 3}

    def test_raw_blocks_javascript_by_default(self):
        for stage in (
            {"$match": {"$where": "this.a == 1"}},
            {"$addFields": {"x": {"$function": {"body": "function(){}", "args": [], "lang": "js"}}}},
            {"$group": {"_id": None, "x": {"$accumulator": {}}}},
        ):
            with pytest.raises(SecurityError):
                P().raw(stage)

    def test_raw_allows_javascript_when_enabled_explicitly(self):
        import aggify

        aggify.configure(allow_javascript=True)
        P().raw({"$match": {"$where": "this.a == 1"}})

    def test_raw_shape(self):
        with pytest.raises(InvalidArgument):
            P().raw({"$limit": 1, "$skip": 2})

    def test_nested_pipeline_is_scanned(self):
        with pytest.raises(SecurityError):
            P().facet(x=[{"$match": {"$where": "1"}}])
        with pytest.raises(SecurityError):
            P().lookup(Post, "x", let={}, pipeline=[{"$match": {"$where": "1"}}])

    def test_options(self):
        p = P().options(allow_disk_use=True, max_time_ms=1000)
        assert p._aggregate_kwargs() == {"allow_disk_use": True, "max_time_ms": 1000}
        with pytest.raises(InvalidArgument):
            P().options(bogus=1)
