import pytest

from aggify import (
    Abs,
    Avg,
    Case,
    Coalesce,
    Concat,
    Cond,
    Count,
    DateToString,
    F,
    Filter,
    InvalidArgument,
    InvalidOperator,
    MapArray,
    Max,
    Q,
    Raw,
    Round,
    SecurityError,
    Size,
    Sum,
    Upper,
    Value,
    Var,
    When,
    Year,
)
from aggify.schema import Context, Scope
from tests.models import Account


def r(expr, model=Account):
    return expr.resolve(Context(Scope.for_model(model)))


class TestArithmetic:
    def test_flattened_add_and_multiply(self):
        assert r(F("age") + F("score") + 3) == {"$add": ["$age", "$score", 3]}
        assert r(F("age") * F("score") * 2) == {"$multiply": ["$age", "$score", 2]}

    def test_subtract_divide_are_binary(self):
        # $subtract / $divide accept exactly two arguments in MongoDB: they must nest.
        assert r(F("age") - F("score") - 1) == {"$subtract": [{"$subtract": ["$age", "$score"]}, 1]}
        assert r(F("age") / 2 / 3) == {"$divide": [{"$divide": ["$age", 2]}, 3]}

    def test_reverse_operators(self):
        assert r(10 - F("age")) == {"$subtract": [10, "$age"]}
        assert r(2 ** F("age")) == {"$pow": [2, "$age"]}
        assert r(-F("age")) == {"$multiply": [-1, "$age"]}

    def test_no_mutation_between_uses(self):
        base = F("age") + 1
        a = base + 2
        b = base + 3
        assert r(a) == {"$add": ["$age", 1, 2]} and r(b) == {"$add": ["$age", 1, 3]}
        assert r(base) == {"$add": ["$age", 1]}

    def test_string_rhs_is_literal(self):
        assert r(Concat("username", Value("-"), "display_name")) == {"$concat": ["$username", "-", "$display_name"]}
        assert r(F("age") + "$evil") == {"$add": ["$age", {"$literal": "$evil"}]}

    def test_field_names_are_mapped(self):
        assert r(F("address__zip_code")) == "$address.zip"
        assert r(F("pk")) == "$_id"

    def test_unknown_field_is_an_error(self):
        from aggify import InvalidField

        with pytest.raises(InvalidField):
            r(F("nope"))


class TestValue:
    def test_literal_wrapping(self):
        assert r(Value("$x")) == {"$literal": "$x"}
        assert r(Value({"$gt": 1})) == {"$literal": {"$gt": 1}}
        assert r(Value([1, 2])) == [1, 2]
        assert r(Value(["$a"])) == {"$literal": ["$a"]}
        assert r(Value(5)) == 5


class TestComparisonAndBoolean:
    def test_comparisons(self):
        assert r(F("age") > 3) == {"$gt": ["$age", 3]}
        assert r(F("age") <= F("score")) == {"$lte": ["$age", "$score"]}
        assert r(F("age").eq(3)) == {"$eq": ["$age", 3]}
        assert r(F("age").ne(3)) == {"$ne": ["$age", 3]}
        assert r(F("age").isin([1, 2])) == {"$in": ["$age", [1, 2]]}

    def test_boolean_composition_flattens(self):
        e = (F("age") > 1) & (F("age") < 9) & (F("score") > 0)
        assert r(e) == {"$and": [{"$gt": ["$age", 1]}, {"$lt": ["$age", 9]}, {"$gt": ["$score", 0]}]}
        assert r(~(F("age") > 1)) == {"$not": [{"$gt": ["$age", 1]}]}
        assert r((F("age") > 1) | (F("age") < 0)) == {"$or": [{"$gt": ["$age", 1]}, {"$lt": ["$age", 0]}]}


class TestFunctions:
    def test_unary_and_nary(self):
        assert r(Upper("username")) == {"$toUpper": "$username"}
        assert r(Coalesce("display_name", Value("anon"))) == {"$ifNull": ["$display_name", "anon"]}
        assert r(Size("tags")) == {"$size": "$tags"}
        assert r(Abs(F("age") - 10)) == {"$abs": {"$subtract": ["$age", 10]}}
        assert r(Round("score", 2)) == {"$round": ["$score", 2]}

    def test_date_parts(self):
        assert r(Year("created_at")) == {"$year": "$created_at"}
        assert r(Year("created_at", timezone="Asia/Tehran")) == {
            "$year": {"date": "$created_at", "timezone": "Asia/Tehran"}
        }
        assert r(DateToString("created_at", "%Y-%m")) == {"$dateToString": {"date": "$created_at", "format": "%Y-%m"}}

    def test_filter_and_map(self):
        assert r(Filter("tags", Var("item").ne("x"))) == {
            "$filter": {"input": "$tags", "as": "item", "cond": {"$ne": ["$$item", "x"]}}
        }
        assert r(MapArray("tags", Upper(Var("item")))) == {
            "$map": {"input": "$tags", "as": "item", "in": {"$toUpper": "$$item"}}
        }

    def test_generic_func(self):
        from aggify import Func

        assert r(Func("$toLower", "username")) == {"$toLower": "$username"}

    def test_raw_expression_is_scanned_for_js(self):
        from aggify.expressions import wrap

        with pytest.raises(SecurityError):
            wrap(Raw({"$function": {"body": "x", "args": [], "lang": "js"}}))
        assert r(wrap(Raw({"$sum": 1}))) == {"$sum": 1}


class TestConditionals:
    def test_cond_new_style(self):
        c = Cond(Q(age__gte=18), "adult", "minor")
        assert r(c) == {"$cond": {"if": {"$gte": ["$age", 18]}, "then": "adult", "else": "minor"}}

    @pytest.mark.parametrize(
        "op,mongo", [(">", "$gt"), (">=", "$gte"), ("<", "$lt"), ("<=", "$lte"), ("==", "$eq"), ("!=", "$ne")]
    )
    def test_cond_legacy_signature(self, op, mongo):
        assert dict(Cond(25, op, 20, "High", "Low")) == {
            "$cond": {"if": {mongo: [25, 20]}, "then": "High", "else": "Low"}
        }

    def test_cond_legacy_bad_operator(self):
        with pytest.raises(InvalidOperator):
            Cond(1, "=>", 2, "a", "b")

    def test_case_when(self):
        e = Case(When(Q(age__gte=65), "senior"), When(Q(age__gte=18), then="adult"), default="minor")
        assert r(e) == {
            "$switch": {
                "branches": [
                    {"case": {"$gte": ["$age", 65]}, "then": "senior"},
                    {"case": {"$gte": ["$age", 18]}, "then": "adult"},
                ],
                "default": "minor",
            }
        }

    def test_when_with_kwargs(self):
        assert r(When(age=3, then=1).condition) == {"$eq": ["$age", 3]}

    def test_case_needs_branches(self):
        with pytest.raises(InvalidArgument):
            Case()

    def test_q_expression_form_all_lookups(self):
        e = Q(username__startswith="a", tags__size=2, age__in=[1, 2], age__isnull=False).resolve_expr(
            Context(Scope.for_model(Account))
        )
        assert e["$and"][0] == {"$regexMatch": {"input": "$username", "regex": "^a"}}
        assert {"$eq": [{"$size": "$tags"}, 2]} in e["$and"]
        assert {"$in": ["$age", [1, 2]]} in e["$and"]
        assert {"$gt": ["$age", None]} in e["$and"]


class TestAggregates:
    def test_basic(self):
        assert r(Sum("age")) == {"$sum": "$age"}
        assert r(Count()) == {"$sum": 1}
        assert r(Count("age")) == {"$sum": {"$cond": [{"$ne": [{"$ifNull": ["$age", None]}, None]}, 1, 0]}}
        assert r(Max("age", "score")) == {"$max": ["$age", "$score"]}

    def test_filtered_aggregates(self):
        assert r(Sum("age", filter=Q(is_verified=True))) == {
            "$sum": {"$cond": [{"$eq": ["$is_verified", True]}, "$age", 0]}
        }
        assert r(Avg("age", filter=Q(is_verified=True))) == {
            "$avg": {"$cond": [{"$eq": ["$is_verified", True]}, "$age", "$$REMOVE"]}
        }
        assert r(Count(filter=Q(age__gt=1))) == {"$sum": {"$cond": [{"$gt": ["$age", 1]}, 1, 0]}}

    def test_legacy_shortcuts(self):
        assert r(F("age").first()) == {"$first": "$age"}
        assert r(F("age").sum()) == {"$sum": "$age"}
