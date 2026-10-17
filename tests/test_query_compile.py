"""Q / lookups / path resolution -> MongoDB query documents (no database needed)."""

import pytest

from aggify import F, InvalidField, InvalidLookup, Q, Raw, SecurityError, UnsafeQueryError, ValidationError, Value
from aggify.schema import Context, Scope, resolve_path
from tests.models import Account, Post


def compile_q(model, *args, **kwargs):
    ctx = Context(Scope.for_model(model))
    return Q(*args, **kwargs).resolve(ctx), ctx


def q(model, *a, **kw):
    return compile_q(model, *a, **kw)[0]


class TestLookups:
    def test_exact_is_plain(self):
        assert q(Account, username="x") == {"username": "x"}
        assert q(Account, username__exact="x") == {"username": "x"}

    @pytest.mark.parametrize(
        "lookup,expected",
        [
            ("gt", {"$gt": 3}),
            ("gte", {"$gte": 3}),
            ("lt", {"$lt": 3}),
            ("lte", {"$lte": 3}),
            ("ne", {"$ne": 3}),
        ],
    )
    def test_comparisons(self, lookup, expected):
        assert q(Account, **{f"age__{lookup}": 3}) == {"age": expected}

    def test_in_nin(self):
        assert q(Account, age__in=[1, "2"]) == {"age": {"$in": [1, 2]}}
        assert q(Account, age__nin=[]) == {"age": {"$nin": []}}

    def test_in_requires_iterable_not_string(self):
        for bad in ("abc", {"$ne": 1}, 5):
            with pytest.raises(Exception):
                q(Account, username__in=bad)

    def test_range_size_exists_isnull(self):
        assert q(Account, age__range=(1, 5)) == {"age": {"$gte": 1, "$lte": 5}}
        assert q(Account, tags__size=2) == {"tags": {"$size": 2}}
        assert q(Account, age__isnull=True) == {"age": None}
        assert q(Account, age__isnull=False) == {"age": {"$ne": None}}
        assert q(Account, age__exists=True) == {"age": {"$exists": True}}

    def test_multiple_comparisons_same_field_merge(self):
        assert q(Account, age__gte=1, age__lt=9) == {"age": {"$gte": 1, "$lt": 9}}

    def test_conflicting_same_field_conditions_use_and(self):
        out = q(Account, Q(username="a") & Q(username="b"))
        assert out == {"username": "a", "$and": [{"username": "b"}]} or "$and" in out

    @pytest.mark.parametrize(
        "lookup,pattern,opts",
        [
            ("contains", "a\\.b", None),
            ("icontains", "a\\.b", "i"),
            ("startswith", "^a\\.b", None),
            ("istartswith", "^a\\.b", "i"),
            ("endswith", "a\\.b$", None),
            ("iendswith", "a\\.b$", "i"),
            ("iexact", "^a\\.b$", "i"),
        ],
    )
    def test_string_lookups_escape_input(self, lookup, pattern, opts):
        spec = {"$regex": pattern}
        if opts:
            spec["$options"] = opts
        assert q(Account, **{f"username__{lookup}": "a.b"}) == {"username": spec}

    def test_regex_lookup_is_explicit_and_bounded(self):
        assert q(Account, username__regex="^a.*") == {"username": {"$regex": "^a.*"}}
        with pytest.raises(UnsafeQueryError):
            q(Account, username__regex="a" * 1000)
        with pytest.raises(ValidationError):
            q(Account, username__regex="(")

    def test_regex_metachar_cannot_escape_contains(self):
        assert q(Account, username__contains=".*")["username"]["$regex"] == "\\.\\*"

    def test_unknown_lookup_or_field(self):
        with pytest.raises(InvalidField):
            q(Account, nope=1)
        with pytest.raises(InvalidField):
            q(Account, username__nope=1)  # nope is not a lookup => path into a scalar

    def test_isnull_requires_bool(self):
        with pytest.raises(ValidationError):
            q(Account, age__isnull="yes")

    def test_list_field_semantics(self):
        assert q(Account, tags="x") == {"tags": "x"}
        assert q(Account, tags=["x", "y"]) == {"tags": ["x", "y"]}
        assert q(Account, tags__all=["x"]) == {"tags": {"$all": ["x"]}}
        assert q(Account, tags__in=["x"]) == {"tags": {"$in": ["x"]}}

    def test_elem_match(self):
        out = q(Post, comments__elem_match=Q(text="hi", score__gt=2))
        assert out == {"comments": {"$elemMatch": {"text": "hi", "score": {"$gt": 2}}}}
        with pytest.raises(Exception):
            q(Post, comments__elem_match={"$ne": 1})

    def test_embedded_and_db_field_names(self):
        assert q(Account, address__zip_code="1") == {"address.zip": "1"}
        assert q(Account, address__city__istartswith="t") == {"address.city": {"$regex": "^t", "$options": "i"}}

    def test_dict_field_free_keys(self):
        assert q(Account, attrs__color="red") == {"attrs.color": "red"}
        assert q(Account, attrs__size__gt=3) == {"attrs.size": {"$gt": 3}}

    def test_pk_alias(self):
        out = q(Account, pk__in=["64b7f0c2e1a4c2d9b8a3f001"])
        assert list(out) == ["_id"]

    def test_expression_rhs_becomes_expr(self):
        assert q(Account, age__gt=F("score")) == {"$expr": {"$gt": ["$age", "$score"]}}

    def test_expression_rhs_with_unsupported_lookup(self):
        with pytest.raises(InvalidLookup):
            q(Account, tags__all=F("score"))

    def test_value_wrapper_is_a_literal(self):
        assert q(Account, username=Value("$weird")) == {"username": "$weird"}


class TestQCombinators:
    def test_and_or_not(self):
        out = q(Account, Q(age__gt=1) | Q(age__lt=0) & ~Q(username="z"))
        assert out == {"$or": [{"age": {"$gt": 1}}, {"age": {"$lt": 0}, "$nor": [{"username": "z"}]}]}

    def test_q_is_immutable(self):
        a = Q(x=1)
        b = a | Q(y=2)
        c = a & Q(z=3)
        assert repr(a) == "Q(('x', 1))" and a is not b and a is not c
        assert len(a.children) == 1

    def test_empty_q_is_identity(self):
        assert (Q() & Q(a=1)).children == Q(a=1).children
        assert q(Account) == {}

    def test_double_negation(self):
        assert q(Account, ~~Q(age=1)) == {"age": 1}

    def test_flattening(self):
        merged = Q(a=1) | Q(b=2) | Q(c=3)
        assert len(merged.children) == 3

    def test_q_rejects_dicts(self):
        with pytest.raises(TypeError):
            Q({"$where": "1"})

    def test_raw_requires_explicit_wrapper_and_scans_js(self):
        assert q(Account, Raw({"age": {"$exists": True}})) == {"age": {"$exists": True}}
        with pytest.raises(SecurityError):
            q(Account, Raw({"$where": "sleep(1000)"}))

    def test_expression_as_filter(self):
        out = q(Account, F("age") > F("score"))
        assert out == {"$expr": {"$gt": ["$age", "$score"]}}


class TestPathResolution:
    def test_reference_creates_join(self):
        out, ctx = compile_q(Post, owner__username="bob")
        assert out == {"_j_owner.username": "bob"}
        (join,) = ctx.joins.values()
        assert join.alias == "_j_owner" and join.local_path == "owner_id" and not join.many

    def test_reference_pk_needs_no_join(self):
        out, ctx = compile_q(Post, owner__id="64b7f0c2e1a4c2d9b8a3f001")
        assert list(out) == ["owner_id"] and not ctx.joins
        assert not compile_q(Post, owner="64b7f0c2e1a4c2d9b8a3f001")[1].joins

    def test_reference_in_embedded_list_is_many(self):
        _, ctx = compile_q(Post, comments__author__username="x")
        (join,) = ctx.joins.values()
        assert join.many and join.local_path == "comments.author_id"

    def test_resolve_path_lookup_split(self):
        r = resolve_path(Scope.for_model(Account), "address__city__icontains")
        assert (r.db_path, r.lookup) == ("address.city", "icontains")

    def test_unknown_embedded_field(self):
        with pytest.raises(InvalidField):
            q(Account, address__nope=1)

    def test_dollar_key_in_dict_path_is_refused(self):
        with pytest.raises(UnsafeQueryError):
            q(Account, **{"attrs__$ne": 1})
