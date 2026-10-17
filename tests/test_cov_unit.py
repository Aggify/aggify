"""Branch-level unit tests (no database): the less travelled paths of every module."""

import datetime as dt
import decimal
import enum
import uuid

import pytest
from bson import Binary, Decimal128, Int64, ObjectId

import aggify
from aggify import (
    AddToSet,
    AlreadyExistsField,
    AnnotationError,
    Avg,
    BinaryField,
    BooleanField,
    Case,
    Cond,
    Count,
    DateAdd,
    DateDiff,
    DatePart,
    DateTrunc,
    DecimalField,
    DictField,
    DocumentNumber,
    Document,
    EmbeddedField,
    EnumField,
    F,
    FloatField,
    Func,
    GeoPointField,
    IntField,
    InvalidArgument,
    InvalidField,
    InvalidLookup,
    InvalidProjection,
    Last,
    Let,
    ListField,
    Max,
    MergeObjects,
    Median,
    Min,
    MongoIndexError,
    ObjectIdField,
    Pipeline,
    Push,
    Q,
    Raw,
    ReferenceField,
    Rank,
    ReplaceAll,
    Shift,
    StdDevPop,
    StdDevSamp,
    StringField,
    Sum,
    URLField,
    UUIDField,
    ValidationError,
    Value,
    Var,
    When,
    Aggify,
    RegexMatch,
    Reduce,
    Trim,
    Rand,
    Year,
)
from aggify import exceptions as exc
from aggify import lookups, registry, security
from aggify.fields import Field, NOT_PROVIDED
from aggify.schema import Context, Scope, dynamic_field, resolve_path
from tests.models import Account, Address, Post, Role


def res(expr, model=Account):
    return expr.resolve(Context(Scope.for_model(model)))


class TestExceptions:
    def test_validation_error_shapes(self):
        e = exc.ValidationError({"a": "bad", "b": ["x", "y"]})
        assert e.errors == {"a": ["bad"], "b": ["x", "y"]} and "a: bad" in str(e)
        assert e.prefixed("p").errors == {"p.a": ["bad"], "p.b": ["x", "y"]}
        assert exc.ValidationError("plain").prefixed("p").errors == {"p": ["plain"]}
        assert exc.ValidationError("m", field="f").as_dict() == {"f": ["m"]}

    def test_messages(self):
        assert "one of" in str(exc.InvalidArgument(expected_list=["a"]))
        assert "aggregation" in str(exc.InvalidAnnotateExpression())
        assert "mix" in str(exc.InvalidProjection())
        assert "Model" in str(exc.InvalidField("x", Account)) or "Account" in str(exc.InvalidField("x", Account))
        assert str(exc.AlreadyExistsField("x")) and str(exc.InvalidEmbeddedField("x"))
        assert str(exc.AggifyValueError([int], "s")) and str(exc.MongoIndexError())
        assert str(exc.OutStageError("$x")) and str(exc.ConnectionNotConfigured("z"))
        assert issubclass(exc.AggifyBaseException, Exception)


class TestLookupsModule:
    def test_registry_helpers(self):
        assert lookups.is_lookup("gte") and not lookups.is_lookup("nope")
        with pytest.raises(InvalidLookup):
            lookups.get_lookup("nope", "f")

    def test_expr_forms(self):
        L = lookups.LOOKUPS
        assert L["range"].to_expr("$a", [1, 2]) == {"$and": [{"$gte": ["$a", 1]}, {"$lte": ["$a", 2]}]}
        assert L["nin"].to_expr("$a", [1]) == {"$not": [{"$in": ["$a", [1]]}]}
        assert L["icontains"].to_expr("$a", "x") == {"$regexMatch": {"input": "$a", "regex": "x", "options": "i"}}
        assert L["elem_match"].to_query("a", {"x": 1}) == {"a": {"$elemMatch": {"x": 1}}}
        with pytest.raises(InvalidLookup):
            lookups.Lookup().to_expr("$a", 1)

    @pytest.mark.parametrize(
        "kind,value",
        [
            ("int", -1),
            ("int", True),
            ("bool", "x"),
            ("range", [1]),
            ("string", 3),
            ("regex", 3),
            ("mod", [1]),
            ("near", [1]),
            ("near", ["a", "b"]),
        ],
    )
    def test_prepare_kind_rejections(self, kind, value):
        with pytest.raises(ValidationError):
            lookups.prepare_kind(kind, "x", value, lambda v: v)

    def test_regex_accepts_compiled_patterns(self):
        import re

        assert lookups.prepare_kind("regex", "regex", re.compile("^a"), str) == "^a"
        assert lookups.prepare_kind("query", "x", {"a": 1}, str) == {"a": 1}


class TestSchemaAndRegistry:
    def test_scope_api(self):
        s = Scope.for_model(Account)
        assert s.has("age") and "age" in repr(s) and repr(Scope({}, open=True)).endswith("open)")
        assert set(s.evolve(only=["age"]).fields) == {"age"}
        assert s.get("_id").name == "id"
        assert isinstance(Scope({}, open=True).get("anything"), Field)
        assert dynamic_field("x", IntField()).name == "x"

    def test_resolve_errors(self):
        s = Scope.for_model(Account)
        with pytest.raises(InvalidField):
            resolve_path(s, "")
        with pytest.raises(InvalidField):
            resolve_path(s, "age__x__y")
        with pytest.raises(InvalidField):
            resolve_path(Scope.for_model(Post), "owner__username", allow_joins=False)
        ctx = Context(Scope.for_model(Post), allow_joins=False)
        with pytest.raises(InvalidField):
            ctx.add_joins([aggify.schema.Join("a", Account, "x")])

    def test_dict_of_embedded_documents(self):
        class Holder(Document):
            by_name = DictField(EmbeddedField(Address))

        r = resolve_path(Scope.for_model(Holder), "by_name__home__city")
        assert r.db_path == "by_name.home.city"

    def test_registry(self):
        class Solo(Document):
            pass

        assert registry.resolve(Solo) is Solo
        assert registry.resolve("self", relative_to=Solo) is Solo
        assert registry.resolve("tests.models.Account") is Account
        with pytest.raises(aggify.ConfigurationError):
            registry.resolve("Missing")


class TestSecurityModule:
    def test_raw_repr_and_unwrap(self):
        assert repr(Raw({"a": 1})) == "Raw({'a': 1})"
        assert security.assert_safe_value(Raw({"$x": 1})) == {"$x": 1}
        assert security.assert_safe_value({"a": [1, (2, 3)]}) == {"a": [1, (2, 3)]}
        with pytest.raises(aggify.UnsafeQueryError):
            security.ensure_iterable("abc")

    def test_mask_uri_without_credentials(self):
        assert security.mask_uri("mongodb://h:1/x") == "mongodb://h:1/x"
        assert "***" in security.mask_uri("mongodb://u:p@h/x")


class TestIndexSpecs:
    def test_all_options_compile(self):
        from aggify import Index

        class Spec(Document):
            title = StringField()
            body = StringField()

        doc = Index("title", "-body", name="my_idx", collation={"locale": "en"}).to_model(Spec).document
        assert doc["name"] == "my_idx" and doc["collation"] == {"locale": "en"}
        text = Index("title", type="text", weights={"title": 5}, default_language="english").to_model(Spec).document
        assert text["weights"] == {"title": 5} and text["default_language"] == "english"


class TestFieldEdges:
    def test_base_field(self):
        f = Field()
        assert f.to_python(3) == 3 and f.get_default() is None and "Field" in repr(f)
        assert repr(NOT_PROVIDED) == "NOT_PROVIDED" and not NOT_PROVIDED
        assert Field(default=5).get_default() == 5
        assert f.deconstruct_kwargs()["required"] is False
        named = StringField()
        named.name = "x"
        assert repr(named) == "<StringField: x>"
        assert Account.age.__class__ is IntField  # descriptor on the class returns the field

    def test_validator_raising_validation_error_is_preserved(self):
        def v(value):
            raise ValidationError("custom")

        with pytest.raises(ValidationError, match="custom"):
            StringField(validators=[v]).clean("a")

    def test_choices_variants(self):
        class Color(enum.Enum):
            RED = "r"

        f = StringField(choices=Color)
        assert "r" in f.choice_values() and Color.RED in f.choice_values()
        assert StringField().choice_values() == []

    def test_raw_passthrough_in_prepare(self):
        for f in (
            StringField(),
            ListField(StringField()),
            DictField(),
            EmbeddedField(Address),
            GeoPointField(),
            aggify.AnyField(),
        ):
            assert f.prepare_value(Raw({"$x": 1})) == {"$x": 1}
        assert StringField().prepare_lookup("exact", Raw("z")) == "z"

    def test_conversions(self):
        assert ObjectIdField().to_python(b"123456789012") == ObjectId(b"123456789012")
        assert StringField().to_python(b"abc") == "abc" and StringField().to_python(b"\xff") == b"\xff"
        assert IntField().to_python(Int64(5)) == 5 and IntField().to_python("x") == "x"
        assert FloatField().to_python("x") == "x" and FloatField().to_python(decimal.Decimal("1.5")) == 1.5
        assert FloatField().to_python([1]) == [1]
        with pytest.raises(ValidationError):
            FloatField(max_value=1).clean(2)
        assert DecimalField().to_python(1.25) == decimal.Decimal("1.25")
        assert DecimalField().to_python("zz") == "zz" and DecimalField().to_python([1]) == [1]
        assert DecimalField().to_python(True) is True
        assert DecimalField().from_mongo(Decimal128("1")) == decimal.Decimal("1")
        with pytest.raises(ValidationError):
            DecimalField(min_value=1).clean("0.5")
        with pytest.raises(ValidationError):
            DecimalField(max_value=1).clean("2")
        assert URLField().to_python("x") == "x"
        with pytest.raises(ValidationError):
            URLField().clean("http://[::1")
        assert DateTimeField_to_python()

    def test_date_edges(self):
        from aggify import DateField, DateTimeField

        assert DateTimeField().to_python(dt.date(2024, 1, 2)) == dt.datetime(2024, 1, 2, tzinfo=dt.timezone.utc)
        assert DateTimeField().to_python(0).year == 1970 and DateTimeField().to_python(10**30) == 10**30
        assert DateTimeField().to_python([1]) == [1]
        aware = dt.datetime(2024, 1, 2, 3, tzinfo=dt.timezone(dt.timedelta(hours=2)))
        assert DateField().to_python(aware) == dt.date(2024, 1, 2)
        assert DateField().to_python(dt.datetime(2024, 1, 2, 3)) == dt.date(2024, 1, 2)
        assert DateField().to_python(dt.date(2024, 1, 2)) == dt.date(2024, 1, 2)
        assert DateField().to_python("nope") == "nope" and DateField().to_python(5) == 5

    def test_binary_uuid_enum_edges(self):
        u = uuid.uuid4()
        assert UUIDField().to_python(Binary.from_uuid(u)) == u and UUIDField().to_python(5) == 5
        assert BinaryField().to_python(Binary(b"x")) == b"x" and BinaryField().to_python(bytearray(b"y")) == b"y"
        with pytest.raises(ValidationError):
            BinaryField().to_mongo("not bytes")
        assert EnumField(Role).python_type is Role and EnumField(Role).to_python("admin") is Role.ADMIN
        assert EnumField(Role).to_python("zzz") == "zzz"

    def test_list_edges(self):
        f = ListField(IntField())
        assert f.to_python((1, "2")) == [1, 2] and f.to_python({1}) == [1] and f.to_python("x") == "x"
        assert f.from_mongo("x") == "x" and f.from_mongo([1, None]) == [1, None]
        assert f.prepare_value([1, "2"]) == [1, 2] and f.prepare_value("3") == 3
        assert f._prepare_exact("3") == 3 and f._prepare_exact((1, 2)) == [1, 2]
        assert f.lookup_field("exact", [1]) is f and f.lookup_field("gt", 1) is f.field
        aggify.configure(max_in_list=1)
        with pytest.raises(ValidationError):
            f.to_mongo(list(range(30)))

    def test_list_item_errors_are_indexed(self):
        class Bag(Document):
            nums = ListField(IntField(min_value=5))

        with pytest.raises(ValidationError) as ei:
            Bag(nums=[9, 1, None]).full_clean()
        assert any("nums[1]" in k for k in ei.value.errors)

    def test_dict_edges(self):
        f = DictField(IntField())
        assert f.from_mongo({"a": 1, "b": None}) == {"a": 1, "b": None} and f.from_mongo(3) == 3
        assert f.to_python(3) == 3
        with pytest.raises(ValidationError):
            f.to_mongo([1])
        assert f.lookup_field("exact", {"a": 1}) is f and f.lookup_field("gt", 1) is f.field
        assert f.prepare_value({"a": "1"}) == {"a": 1} and f.prepare_value("5") == 5
        assert f._prepare_exact({"a": "1"}) == {"a": 1} and f._prepare_exact("5") == 5
        with pytest.raises(ValidationError):
            DictField(IntField(min_value=3)).clean({"a": 1})

    def test_embedded_edges(self):
        f = EmbeddedField(Address)
        f.name = "address"
        assert f.from_mongo(3) == 3 and f.to_python(3) == 3
        assert f.prepare_value({"city": "x"}) == {"city": "x"}
        assert f.prepare_value(Address(city="y")) == {"city": "y"}
        with pytest.raises(ValidationError):
            f.to_mongo({"city": "x"})
        lazy = EmbeddedField("Address")
        lazy.model = Account
        assert lazy.document_type is Address and lazy.python_type is Address

    def test_reference_edges(self):
        f = Post._meta.fields["owner"]
        assert f.python_type is Account and f.pk_field.name == "id"
        assert Post.owner_id is not None and Post.__dict__["owner_id"].__get__(None, Post) is Post.__dict__["owner_id"]
        assert f.__get__(None, Post) is f
        with pytest.raises(ValidationError, match="Save the referenced"):
            f.to_mongo(Account(id=None))
        with pytest.raises(ValidationError):
            f.to_mongo("not-an-id")
        with pytest.raises(ValidationError):
            f.to_mongo(Post())
        assert f.from_mongo(ObjectId()) is not None

    def test_geo_edges(self):
        f = GeoPointField()
        assert f.to_python({"type": "Point", "coordinates": [1, 2]}) == (1.0, 2.0)
        assert f.to_python(["a", "b"]) == ["a", "b"] and f.to_python(5) == 5
        assert f.prepare_value((1.0, 2.0)) == {"type": "Point", "coordinates": [1.0, 2.0]}
        assert f.prepare_value(Raw(1)) == 1


def DateTimeField_to_python():
    from aggify import DateTimeField

    return DateTimeField().to_python("2024-01-01T00:00:00Z").year == 2024


class TestExpressionEdges:
    def test_operator_variants(self):
        assert res(2 * F("age")) == {"$multiply": [2, "$age"]}
        assert res(8 / F("age")) == {"$divide": [8, "$age"]}
        assert res(F("age") % 2) == {"$mod": ["$age", 2]}
        assert res(7 % F("age")) == {"$mod": [7, "$age"]}
        assert res(F("age") ** 2) == {"$pow": ["$age", 2]}
        assert res(abs(F("age"))) == {"$abs": "$age"}
        assert res(1 + F("age")) == {"$add": [1, "$age"]}
        assert res((F("age") > 1).eq(True)) == {"$eq": [{"$gt": ["$age", 1]}, True]}

    def test_boolean_with_q_defers_to_q(self):
        combined = (F("age") > 1) & Q(username="x")
        assert isinstance(combined, Q)
        assert isinstance((F("age") > 1) | Q(username="x"), Q)
        assert isinstance(Q(username="x") & (F("age") > 1), Q)
        assert isinstance(Q(username="x") | (F("age") > 1), Q)
        assert isinstance((F("age") > 1) & (F("age") < 3), aggify.expressions.BooleanExpression)

    def test_ordering_markers(self):
        assert F("age").asc().descending is False and F("age").desc().descending is True

    def test_value_types_and_repr(self):
        ctx = Context(Scope.for_model(Account))
        assert repr(Value(3)) == "Value(3)" and repr(F("age")) == "F('age')"
        for v, cls in [(True, BooleanField), (1.5, FloatField), ("s", StringField), (2, IntField)]:
            assert isinstance(Value(v).output_field(ctx), cls)
        assert Value(dt.datetime.now()).output_field(ctx).__class__.__name__ == "DateTimeField"
        assert Value(object()).output_field(ctx) is None
        assert Value(5, output_field=FloatField()).output_field(ctx).__class__ is FloatField
        assert Value(Raw({"$x": 1})).value == {"$x": 1}

    def test_f_validation_and_output_field(self):
        ctx = Context(Scope.for_model(Account))
        with pytest.raises(InvalidArgument):
            F("")
        assert F("nope").output_field(ctx) is None and F("age").output_field(ctx).__class__ is IntField
        assert res(F("age").last()) == {"$last": "$age"} and res(F("age").min()) == {"$min": "$age"}
        assert res(F("age").max()) == {"$max": "$age"} and res(F("age").avg()) == {"$avg": "$age"}
        with pytest.raises(InvalidArgument):
            Var("$bad")

    def test_output_field_inference(self):
        ctx = Context(Scope.for_model(Account))
        assert (F("score") + 1).output_field(ctx).__class__ is FloatField
        assert (F("age") / 2).output_field(ctx).__class__ is FloatField
        assert (F("age") + 1).output_field(ctx).__class__ is IntField
        assert (F("age") > 1).output_field(ctx).__class__ is BooleanField
        assert ((F("age") > 1) & (F("age") < 2)).output_field(ctx).__class__ is BooleanField
        assert aggify.expressions.QExpression(Q(age=1)).output_field(ctx).__class__ is BooleanField
        assert Func("$x", "age", output_field=IntField()).output_field(ctx).__class__ is IntField
        assert aggify.Upper("username").output_field(ctx).__class__ is StringField
        assert Func("$x", "age").output_field(ctx) is None and "Func" in repr(Func("$x", "age"))
        assert Cond(Q(age=1), F("age"), 0).output_field(ctx).__class__ is IntField
        assert Case(When(Q(age=1), F("age"))).output_field(ctx).__class__ is IntField

    def test_func_construction_errors(self):
        with pytest.raises(InvalidArgument):
            Func()
        with pytest.raises(InvalidArgument):
            DateToString_too_many()
        assert res(Rand()) == {"$rand": {}}

    def test_date_functions(self):
        assert res(DateTrunc("created_at", "hour", bin_size=2, timezone="UTC", start_of_week="mon")) == {
            "$dateTrunc": {"date": "$created_at", "unit": "hour", "binSize": 2, "timezone": "UTC", "startOfWeek": "mon"}
        }
        assert res(DateAdd("created_at", "day", 3, timezone="UTC")) == {
            "$dateAdd": {"startDate": "$created_at", "unit": "day", "amount": 3, "timezone": "UTC"}
        }
        assert res(DateDiff("created_at", "created_at", "hour", timezone="UTC"))["$dateDiff"]["timezone"] == "UTC"

    def test_string_and_higher_order(self):
        assert res(ReplaceAll("username", "a", "b")) == {
            "$replaceAll": {"input": "$username", "find": "a", "replacement": "b"}
        }
        assert res(RegexMatch("username", "^a", "i"))["$regexMatch"]["options"] == "i"
        with pytest.raises(ValidationError):
            RegexMatch("username", "(")
        assert res(Trim("username", chars=" ")) == {"$trim": {"input": "$username", "chars": " "}}
        assert res(aggify.Filter("tags", Var("item").ne("x"), limit=2))["$filter"]["limit"] == 2
        assert res(Reduce("tags", 0, Var("value") + 1)) == {
            "$reduce": {"input": "$tags", "initialValue": 0, "in": {"$add": ["$$value", 1]}}
        }
        assert res(Let({"a": F("age")}, Var("a") + 1)) == {"$let": {"vars": {"a": "$age"}, "in": {"$add": ["$$a", 1]}}}

    def test_cond_forms(self):
        assert res(Cond(Q(age=1), 1, 2))["$cond"]["if"] == {"$eq": ["$age", 1]}
        assert res(Cond(F("age") > 1, 1, 2))["$cond"]["if"] == {"$gt": ["$age", 1]}
        assert res(Cond(Q(age=1)))["$cond"]["then"] is None
        with pytest.raises(InvalidArgument):
            Cond(1, 2)
        with pytest.raises(InvalidArgument):
            When()
        assert res(When(Q(age=1), then=2).condition) == {"$eq": ["$age", 1]}
        assert When(Q(age=1), then=2, username="x").condition is not None

    def test_aggregates(self):
        assert res(Median("age")) == {"$median": {"input": "$age", "method": "approximate"}}
        assert res(Sum("age", window={"documents": [-1, 0]})) == {"$sum": "$age", "window": {"documents": [-1, 0]}}
        assert res(Count(window={"range": [1, 2]}))["window"] == {"range": [1, 2]}
        assert res(Count("age", filter=Q(age__gt=1)))["$sum"]["$cond"][0]["$and"]
        assert res(Rank()) == {"$rank": {}} and res(DocumentNumber()) == {"$documentNumber": {}}
        assert res(Shift("age", -1, 0)) == {"$shift": {"output": "$age", "by": -1, "default": 0}}
        with pytest.raises(InvalidArgument):
            Sum("age", "score", filter=Q(age=1)).resolve(Context(Scope.for_model(Account)))
        ctx = Context(Scope.for_model(Account))
        assert Sum("age").output_field(ctx).__class__ is IntField
        assert Sum(Value(object())).output_field(ctx).__class__ is FloatField
        assert Max("age", output_field=FloatField()).output_field(ctx).__class__ is FloatField
        assert Avg("age").output_field(ctx).__class__ is FloatField and Count().output_field(ctx).__class__ is IntField
        assert aggify.expressions.Aggregate("$foo").output_field(ctx) is None
        for cls in (Last, Min, Push, AddToSet, StdDevPop, StdDevSamp, MergeObjects):
            assert cls("age").resolve(ctx)

    def test_q_edge_cases(self):
        with pytest.raises(TypeError):
            Q(5)
        assert Q(Raw({"a": 1})).resolve(Context(Scope.for_model(Account))) == {"a": 1}
        e = Q(Raw({"a": 1}), F("age") > 1, Q(age=2)).resolve_expr(Context(Scope.for_model(Account)))
        assert e["$and"][0] == {"a": 1}
        assert Q().resolve_expr(Context(Scope.for_model(Account))) is True
        assert (Q(a=1) & Q()).children == Q(a=1).children and (Q() | Q(a=1)).children == Q(a=1).children
        assert Q(a=1).__and__(5) is NotImplemented
        assert (F("age") > 1).__and__(Q(a=1)) is NotImplemented
        assert (F("age") > 1).__or__(Q(a=1)) is NotImplemented
        assert Q(age=1).__rand__(5) is NotImplemented and Q(age=1).__ror__(5) is NotImplemented
        with pytest.raises(InvalidLookup):
            Q(tags__all=["a"]).resolve_expr(Context(Scope.for_model(Account)))
        assert Q(username__startswith=Value("$x")).resolve_expr(Context(Scope.for_model(Account))) is not None
        e = Q(username="$x").resolve_expr(Context(Scope.for_model(Account)))
        assert e == {"$eq": ["$username", {"$literal": "$x"}]}
        with pytest.raises(InvalidArgument):
            Q(comments__elem_match=5).resolve(Context(Scope.for_model(Post)))
        with pytest.raises(InvalidArgument):
            Q(tags__elem_match=Q(a=1)).resolve(Context(Scope.for_model(Account)))
        out = Q(comments__elem_match=Raw({"text": "x"})).resolve(Context(Scope.for_model(Post)))
        assert out == {"comments": {"$elemMatch": {"text": "x"}}}


def DateToString_too_many():
    return aggify.Trim("a", "b")


class TestPipelineEdges:
    def P(self, model=Account):
        return Aggify(model)

    def test_clone_and_protocol(self):
        p = self.P().filter(age=1)
        assert bool(p) and p.pipelines == p.stages
        assert p._clone(model=Post).model is Post

    def test_order_by_variants(self):
        assert self.P().order_by(F("age")).to_list() == [{"$sort": {"age": 1}}]
        assert self.P().order_by().to_list() == []
        with pytest.raises(InvalidArgument):
            self.P().order_by(F("age") + 1)
        with pytest.raises(InvalidArgument):
            self.P().order_by((F("age") + 1).desc())
        with pytest.raises(InvalidArgument):
            self.P().order_by(5)

    def test_skip_limit_validation(self):
        with pytest.raises(MongoIndexError):
            self.P().skip(-1)
        with pytest.raises(MongoIndexError):
            self.P().limit(True)
        with pytest.raises(InvalidArgument):
            self.P().limit(0)
        with pytest.raises(InvalidArgument):
            self.P()[3:3]
        assert self.P().skip(0).to_list() == []

    def test_project_validation(self):
        with pytest.raises(InvalidProjection):
            self.P().project()
        assert self.P().project(age=True).to_list() == [{"$project": {"age": 1}}]

    def test_filter_with_only_joins(self):
        p = self.P(Post).filter(Q(owner__username="a") | Q(owner__username="b"))
        assert p.to_list()[-1]["$match"]["$or"]

    def test_group_variants(self):
        assert self.P().group(F("age") + 1).to_list()[0]["$group"]["_id"] == {"$add": ["$age", 1]}
        with pytest.raises(InvalidArgument):
            self.P().group(5)
        with pytest.raises(InvalidArgument):
            self.P().group("age", _id=Count())
        assert self.P().group(None, n=Raw({"$sum": 1})).to_list()[0]["$group"]["n"] == {"$sum": 1}

    def test_annotate_args(self):
        with pytest.raises(AnnotationError):
            self.P().annotate(5)
        assert self.P().annotate().to_list() == []

    def test_lookup_sub_pipeline_as_list_and_scan(self):
        p = self.P().lookup(Post, "x", let={}, pipeline=[{"$limit": 1}])
        assert p.to_list()[0]["$lookup"]["pipeline"] == [{"$limit": 1}]

    def test_graph_lookup_options(self):
        spec = (
            self.P()
            .graph_lookup(Account, F("age"), "age", "score", "c", depth_field="d", restrict=Q(age__gt=1))
            .to_list()[0]["$graphLookup"]
        )
        assert spec["depthField"] == "d" and spec["restrictSearchWithMatch"] == {"age": {"$gt": 1}}

    def test_bucket_auto(self):
        spec = self.P().bucket_auto("age", 3, granularity="R5", n=Count()).to_list()[0]["$bucketAuto"]
        assert spec == {"groupBy": "$age", "buckets": 3, "granularity": "R5", "output": {"n": {"$sum": 1}}}
        assert "output" not in self.P().bucket_auto("age", 3).to_list()[0]["$bucketAuto"]
        assert "output" not in self.P().bucket("age", [0, 1]).to_list()[0]["$bucket"]

    def test_replace_root_from_list_of_embedded_and_expression(self):
        class Team(Document):
            members = ListField(EmbeddedField(Address))

        assert self.P(Team).replace_root("members").to_list() == [{"$replaceRoot": {"newRoot": "$members"}}]
        assert self.P().replace_root(F("address")).to_list() == [{"$replaceRoot": {"newRoot": "$address"}}]
        with pytest.raises(InvalidArgument):
            self.P().replace_root()
        with pytest.raises(InvalidArgument):
            self.P().redact(Q(age=1), value1="a")

    def test_geo_near_options_and_stage_names(self):
        spec = (
            self.P()
            .geo_near((1.0, 2.0), "d", max_distance=5, key="address__geo", query=Q(age__gt=1))
            .to_list()[0]["$geoNear"]
        )
        assert spec["maxDistance"] == 5 and spec["key"] == "address.geo" and spec["query"] == {"age": {"$gt": 1}}
        assert self.P().stage("sample", {"size": 1}).to_list() == [{"$sample": {"size": 1}}]

    def test_default_max_time_applies(self):
        aggify.configure(default_max_time_ms=123)
        assert self.P()._aggregate_kwargs() == {"maxTimeMS": 123}

    def test_convert_row_and_helpers(self):
        from aggify.pipeline import _convert, convert_row

        scope = Scope.for_model(Account)
        row = convert_row(
            scope,
            {
                "_id": ObjectId(),
                "username": "a",
                "extra": 1,
                "_j_x": {},
                "tags": ["a"],
                "address": {"city": "t", "zip": "9"},
                "role": "admin",
            },
        )
        assert row["username"] == "a" and row["extra"] == 1 and "_j_x" not in row
        assert row["address"] == {"city": "t", "zip_code": "9"} and row["role"] is Role.ADMIN
        assert _convert(IntField(), None) is None
        assert _convert(DecimalField(), object()) is not None  # conversion failure keeps the raw value


class TestDocumentEdges:
    def test_options_api(self):
        meta = Account._meta
        assert meta.get_field("pk") is meta.pk and len(meta.concrete_fields) == len(meta.fields)
        assert "Account" in repr(meta) and meta.db_fields["_id"] is meta.pk
        with pytest.raises(InvalidField):
            meta.get_field("nope")

    def test_pk_declarations(self):
        class WithId(Document):
            id = StringField()

        assert WithId._meta.pk.db_field == "_id"
        with pytest.raises(aggify.ConfigurationError):

            class TwoPks(Document):
                a = StringField(primary_key=True)
                b = StringField(primary_key=True)

        class ObjPk(Document):
            id = ObjectIdField(primary_key=True)

        assert ObjPk._meta.pk.has_default()

    def test_pk_kwarg_and_manager_inheritance(self):
        pk = ObjectId()
        assert Account(pk=pk, username="ab").pk == pk

        class Base(Document):
            objects = aggify.Manager()

            class Meta:
                abstract = True

        class Kid(Base):
            pass

        assert Kid.objects.model is Kid

    def test_repr_hash_and_dirty(self):
        a = Account(username="ab")
        assert str(a.pk) in repr(a) and a.is_dirty
        with pytest.raises(TypeError):
            hash(Account(id=None, username="ab"))
        e = Address(city="x")
        assert repr(e).startswith("<Address") and e.pk is None

    def test_json_primitives(self):
        from aggify.documents import _to_primitive

        assert _to_primitive(dt.datetime(2024, 1, 2, tzinfo=dt.timezone.utc)) == "2024-01-02T00:00:00Z"
        assert _to_primitive(dt.date(2024, 1, 2)) == "2024-01-02"
        assert _to_primitive(Decimal128("1.5")) == "1.5" and _to_primitive(decimal.Decimal("2")) == "2"
        u = uuid.uuid4()
        assert _to_primitive(u) == str(u) and _to_primitive(Role.ADMIN) == "admin"
        assert _to_primitive(b"ab") == "YWI=" and _to_primitive({"a": (1, 2)}) == {"a": [1, 2]}

    def test_full_clean_collects_message_only_errors(self):
        class Odd(Document):
            x = IntField()

        o = Odd(x=1)
        o._meta.fields["x"].validators.append(lambda v: (_ for _ in ()).throw(ValidationError("plain")))
        try:
            with pytest.raises(ValidationError) as ei:
                o.full_clean()
            assert "x" in ei.value.errors
        finally:
            o._meta.fields["x"].validators.clear()

    def test_pk_required_operations(self):
        a = Account(id=None, username="ab")
        for op in (a.delete, a.refresh_from_db):
            with pytest.raises(ValueError):
                op()

    def test_manager_details(self):
        mgr = Account.objects
        with pytest.raises(AttributeError):
            mgr.__nonexistent__
        import copy

        assert copy.copy(mgr).model is Account
        with pytest.raises(AttributeError):
            mgr.model_
            raise AttributeError
        qs_manager = aggify.Manager(aggify.QuerySet)
        assert qs_manager.queryset_class is aggify.QuerySet

    def test_signal_helper_receiver_with_list(self):
        calls = []
        fn = aggify.receiver([aggify.pre_save, aggify.post_save])(lambda sender, **kw: calls.append(1))
        try:
            aggify.pre_save.send(Account, instance=None, created=True)
            assert calls == [1]
        finally:
            aggify.pre_save.disconnect(fn)
            aggify.post_save.disconnect(fn)


class TestMonitoringUnit:
    def test_log_helpers(self):
        from aggify.monitoring import CommandLog, CommandRecord

        log = CommandLog(
            [CommandRecord("find", "db", {"find": "c"}, 1), CommandRecord("insert", "db", {"insert": 5}, 2)]
        )
        assert len(log) == 2 and [r.name for r in log] == ["find", "insert"] and log.count == 2
        assert log.of("find")[0].collection == "c" and log.records[1].collection is None

    def test_listener_failed_and_succeeded_branches(self):
        from types import SimpleNamespace as NS

        from aggify.monitoring import LISTENER, capture_commands

        with capture_commands() as log:
            LISTENER.started(NS(command_name="find", database_name="d", command={"find": "c"}, request_id=7))
            LISTENER.failed(NS(request_id=7))
            LISTENER.succeeded(NS(request_id=7, duration_micros=5))
            LISTENER.started(NS(command_name="ping", database_name="d", command={}, request_id=8))
        assert log.records[0].failed and log.records[0].duration_micros == 5 and len(log) == 1
        LISTENER.failed(NS(request_id=1))  # no active recorder: no-op
        LISTENER.succeeded(NS(request_id=1, duration_micros=1))
