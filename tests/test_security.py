"""Security properties: injection, ReDoS, mass assignment, JS, credential leaks."""

import pytest
from hypothesis import given
from hypothesis import settings as hsettings
from hypothesis import strategies as st

import aggify
from aggify import (
    Aggify,
    Raw,
    SecurityError,
    UnsafeQueryError,
    ValidationError,
)
from aggify.connection import ConnectionConfig
from aggify.security import (
    assert_safe_value,
    escape_regex,
    mask_uri,
    scan_stages,
    validate_dict_key,
    validate_field_name,
)
from tests.models import Account

OPERATOR_PAYLOADS = [
    {"$ne": ""},
    {"$gt": ""},
    {"$regex": ".*"},
    {"$where": "1"},
    {"$exists": True},
    {"$in": ["a"]},
    {"a": {"$ne": 1}},
    [{"$ne": 1}],
    ["ok", {"$ne": 1}],
]


class TestOperatorInjection:
    @pytest.mark.parametrize("payload", OPERATOR_PAYLOADS)
    @pytest.mark.parametrize("lookup", ["", "__exact", "__ne", "__gt", "__contains", "__in"])
    def test_typed_fields_refuse_operator_payloads(self, payload, lookup):
        for field in ("username", "age"):
            with pytest.raises(Exception):
                Account.objects.filter(**{field + lookup: payload}).query

    @pytest.mark.parametrize("payload", [{"$ne": 1}, {"x": {"$gt": 1}}, [{"$ne": 1}]])
    def test_untyped_dict_values_are_scanned(self, payload):
        with pytest.raises(UnsafeQueryError):
            Account.objects.filter(attrs__color=payload).query

    def test_dollar_path_segments(self):
        with pytest.raises(Exception):
            Account.objects.filter(**{"attrs__$where": 1}).query

    def test_raw_is_the_only_escape_hatch(self):
        assert Account.objects.filter(Raw({"age": {"$gt": 1}})).query["find"]["filter"] == {"age": {"$gt": 1}}

    def test_update_values_are_validated(self):
        from aggify.updates import build_update

        with pytest.raises(ValidationError):
            build_update(Account, {"username": {"$set": 1}})
        with pytest.raises(ValidationError):
            build_update(Account, {"inc__age": "5"})
        with pytest.raises(Exception):
            build_update(Account, {"attrs": {"$where": "1"}})
        with pytest.raises(Exception):
            build_update(Account, {"push__tags": {"$each": [1]}})

    def test_models_reject_unknown_kwargs(self):
        with pytest.raises(TypeError, match="unexpected field"):
            Account(username="x", is_admin=True)
        with pytest.raises(TypeError):
            Account(**{"$set": 1})

    def test_dict_field_keys(self):
        acc = Account(username="xx", attrs={"$where": 1})
        with pytest.raises(Exception):
            acc.full_clean()
            acc.to_mongo()


class TestRegex:
    @given(st.text(max_size=40))
    @hsettings(max_examples=200, deadline=None)
    def test_escaped_pattern_matches_literally(self, text):
        import re

        pattern = escape_regex(text)
        assert re.fullmatch(pattern, text, flags=re.DOTALL)
        assert re.search(pattern, "xx" + text + "yy", flags=re.DOTALL)
        if text and text != text.upper() and not text.isdigit():
            assert re.fullmatch(pattern, text + "z", flags=re.DOTALL) is None

    def test_contains_neutralises_redos_style_input(self):
        q = Account.objects.filter(username__contains="(a+)+$").query["find"]["filter"]
        assert q["username"]["$regex"] == "\\(a\\+\\)\\+\\$"

    def test_user_regex_length_cap(self):
        aggify.configure(max_regex_length=10)
        with pytest.raises(UnsafeQueryError):
            Account.objects.filter(username__regex="a" * 11).query


class TestJavaScript:
    def test_blocked_everywhere(self):
        for doc in ({"$where": "1"}, {"a": [{"$function": {}}]}, {"x": {"y": {"$accumulator": 1}}}):
            with pytest.raises(SecurityError):
                scan_stages(doc)
        scan_stages({"a": 1})

    def test_filter_with_raw_js_blocked_at_execution_path(self):
        with pytest.raises(SecurityError):
            Account.objects.filter(Raw({"$where": "1"})).query

    def test_enabling_is_explicit(self):
        aggify.configure(allow_javascript=True)
        scan_stages({"$where": "1"})


class TestFieldNames:
    @pytest.mark.parametrize("bad", ["", "$x", "a\x00b", "$$ROOT", None, 5])
    def test_invalid_field_names(self, bad):
        with pytest.raises(UnsafeQueryError):
            validate_field_name(bad)

    def test_dotted_names_only_when_allowed(self):
        validate_field_name("a.b", allow_dots=True)
        with pytest.raises(UnsafeQueryError):
            validate_field_name("a.b")
        with pytest.raises(UnsafeQueryError):
            validate_field_name("a.$b", allow_dots=True)

    @pytest.mark.parametrize("bad", ["$x", "a.b", "", "x\x00"])
    def test_dict_keys(self, bad):
        with pytest.raises(UnsafeQueryError):
            validate_dict_key(bad)

    def test_annotation_names_are_validated(self):
        with pytest.raises(UnsafeQueryError):
            Aggify(Account).add_fields(**{"$bad": 1})
        with pytest.raises(UnsafeQueryError):
            Aggify(Account).group("age", **{"$x": aggify.Count()})


class TestCredentials:
    def test_mask_uri(self):
        assert mask_uri("mongodb://user:s3cret@host:27017/db?x=1") == "mongodb://user:***@host:27017/db?x=1"
        assert mask_uri("mongodb://host/db") == "mongodb://host/db"
        assert "s3cret" not in mask_uri("mongodb+srv://u:s3cret@c.example.net/x")

    def test_repr_never_contains_password(self):
        cfg = ConnectionConfig("default", "mongodb://u:pw123@h/db", "db", {"password": "x"})
        assert "pw123" not in repr(cfg) and "pw123" not in str(cfg)

    def test_connect_returns_masked_repr(self):
        cfg = aggify.connect("mongodb://u:topsecret@localhost/x", alias="masked-test")
        try:
            assert "topsecret" not in repr(cfg)
        finally:
            aggify.disconnect("masked-test")


class TestLimits:
    def test_in_list_cap(self):
        aggify.configure(max_in_list=3)
        with pytest.raises(UnsafeQueryError):
            Account.objects.filter(age__in=[1, 2, 3, 4]).query

    def test_value_depth_cap(self):
        deep = cur = {}
        for _ in range(200):
            cur["a"] = {}
            cur = cur["a"]
        with pytest.raises(UnsafeQueryError):
            assert_safe_value(deep)

    def test_pipeline_depth_cap(self):
        deep = cur = {}
        for _ in range(300):
            cur["a"] = {}
            cur = cur["a"]
        with pytest.raises(UnsafeQueryError):
            scan_stages(deep)

    def test_max_limit_applies_to_slices(self):
        aggify.configure(max_limit=5)
        with pytest.raises(aggify.InvalidArgument):
            Account.objects.all()[0:50].query


class TestNoSqlInjectionProperty:
    @given(
        st.recursive(
            st.none() | st.booleans() | st.integers(-5, 5) | st.text(max_size=5),
            lambda c: st.lists(c, max_size=3) | st.dictionaries(st.text(max_size=3), c, max_size=3),
            max_leaves=8,
        )
    )
    @hsettings(max_examples=300, deadline=None)
    def test_no_string_field_ever_emits_a_dollar_key(self, value):
        """Whatever the input, a StringField filter either rejects it or yields a plain string."""
        try:
            doc = Account.objects.filter(username=value).query["find"]["filter"]
        except Exception:
            return
        assert isinstance(doc["username"], str) or doc["username"] is None

    @given(st.dictionaries(st.sampled_from(["$ne", "$gt", "$regex", "a", "b"]), st.integers(), min_size=1, max_size=3))
    @hsettings(max_examples=100, deadline=None)
    def test_dict_values_with_operators_never_reach_the_query(self, value):
        has_operator = any(k.startswith("$") for k in value)
        try:
            Account.objects.filter(attrs__x=value).query
            ok = True
        except UnsafeQueryError:
            ok = False
        assert ok != has_operator
