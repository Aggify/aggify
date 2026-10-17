import datetime as dt
import decimal
import uuid

import pytest
from bson import Decimal128, ObjectId

from aggify import (
    AnyField,
    BinaryField,
    BooleanField,
    DateField,
    DateTimeField,
    DecimalField,
    DictField,
    EmailField,
    EnumField,
    FloatField,
    GeoPointField,
    IntField,
    ListField,
    ObjectIdField,
    SlugField,
    StringField,
    URLField,
    UUIDField,
    ValidationError,
)
from tests.models import Account, Role


class TestString:
    def test_accepts_str_only(self):
        f = StringField()
        assert f.to_mongo("x") == "x"
        for bad in (1, {"$ne": ""}, None.__class__, ["a"]):
            with pytest.raises(ValidationError):
                f.to_mongo(bad)

    def test_nul_rejected(self):
        with pytest.raises(ValidationError):
            StringField().to_mongo("a\x00b")

    def test_constraints(self):
        f = StringField(min_length=2, max_length=4, regex=r"^[a-z]+$")
        f.clean("abc")
        for bad in ("a", "abcde", "AB1"):
            with pytest.raises(ValidationError):
                f.clean(bad)

    def test_required(self):
        with pytest.raises(ValidationError, match="required"):
            StringField(required=True).clean(None)
        assert StringField().clean(None) is None

    def test_choices(self):
        f = StringField(choices=["a", ("b", "Bee")])
        f.clean("a")
        f.clean("b")
        with pytest.raises(ValidationError):
            f.clean("c")

    def test_validators(self):
        def no_x(v):
            if "x" in v:
                raise ValueError("no x")

        with pytest.raises(ValidationError, match="no x"):
            StringField(validators=[no_x]).clean("xx")

    def test_strip(self):
        assert StringField(strip=True).clean("  hi ") == "hi"


class TestNumbers:
    def test_int_coercion_and_strictness(self):
        f = IntField()
        assert f.clean("42") == 42 and f.clean(3.0) == 3
        for bad in ("x", 3.5, True, [1]):
            with pytest.raises(ValidationError):
                f.clean(bad)

    def test_int_range_and_64bit(self):
        f = IntField(min_value=1, max_value=10)
        for bad in (0, 11):
            with pytest.raises(ValidationError):
                f.clean(bad)
        with pytest.raises(ValidationError):
            IntField().to_mongo(2**63)

    def test_float(self):
        assert FloatField().clean(1) == 1.0 and FloatField().clean("2.5") == 2.5
        with pytest.raises(ValidationError):
            FloatField(min_value=0).clean(-1)

    def test_decimal_roundtrip_and_digits(self):
        f = DecimalField(max_digits=5, decimal_places=2)
        d = f.clean("12.34")
        assert d == decimal.Decimal("12.34")
        assert isinstance(f.to_mongo(d), Decimal128)
        assert f.from_mongo(Decimal128("1.50")) == decimal.Decimal("1.50")
        for bad in ("12345.6", "1.234", "NaN"):
            with pytest.raises(ValidationError):
                f.clean(bad)

    def test_bool(self):
        f = BooleanField()
        assert f.clean("true") is True and f.clean("0") is False and f.clean(1) is True
        with pytest.raises(ValidationError):
            f.clean("maybe")


class TestDatesAndIds:
    def test_datetime_is_utc_aware(self):
        f = DateTimeField()
        naive = dt.datetime(2024, 1, 1, 12)
        out = f.clean(naive)
        assert out.tzinfo is dt.timezone.utc
        tehran = dt.timezone(dt.timedelta(hours=3, minutes=30))
        assert f.clean(dt.datetime(2024, 1, 1, 12, tzinfo=tehran)).hour == 8
        assert f.clean("2024-01-01T10:00:00Z") == dt.datetime(2024, 1, 1, 10, tzinfo=dt.timezone.utc)
        with pytest.raises(ValidationError):
            f.clean("yesterday")

    def test_date(self):
        f = DateField()
        assert f.clean("2024-02-03") == dt.date(2024, 2, 3)
        assert f.to_mongo(dt.date(2024, 2, 3)) == dt.datetime(2024, 2, 3, tzinfo=dt.timezone.utc)
        with pytest.raises(ValidationError):
            f.to_mongo(dt.datetime(2024, 2, 3))

    def test_objectid(self):
        f = ObjectIdField()
        oid = ObjectId()
        assert f.clean(str(oid)) == oid
        for bad in ("zzz", 12, "x" * 24):
            with pytest.raises(ValidationError):
                f.clean(bad)

    def test_uuid(self):
        u = uuid.uuid4()
        assert UUIDField().clean(str(u)) == u
        with pytest.raises(ValidationError):
            UUIDField().clean("nope")

    def test_binary(self):
        f = BinaryField(max_bytes=3)
        f.clean(b"abc")
        with pytest.raises(ValidationError):
            f.clean(b"abcd")


class TestSpecialStrings:
    def test_email_normalises_and_validates(self):
        assert EmailField().clean("  Foo@Example.COM ") == "foo@example.com"
        with pytest.raises(ValidationError):
            EmailField().clean("not-an-email")

    def test_url(self):
        URLField().clean("https://example.com/a?b=1")
        for bad in ("ftp://x.com", "javascript:alert(1)", "https://"):
            with pytest.raises(ValidationError):
                URLField().clean(bad)

    def test_slug(self):
        SlugField().clean("hello-world_1")
        with pytest.raises(ValidationError):
            SlugField().clean("no spaces")


class TestContainers:
    def test_list_converts_items(self):
        f = ListField(IntField())
        assert f.clean(["1", 2]) == [1, 2]
        with pytest.raises(ValidationError):
            f.clean(["x"])
        with pytest.raises(ValidationError):
            f.clean("12")

    def test_list_limits(self):
        f = ListField(StringField(), min_length=1, max_length=2)
        with pytest.raises(ValidationError):
            f.clean([])
        with pytest.raises(ValidationError):
            f.clean(["a", "b", "c"])

    def test_dict_keys_cannot_be_operators_or_paths(self):
        f = DictField()
        f.to_mongo({"ok": 1})
        for key in ("$set", "a.b", ""):
            with pytest.raises(Exception):
                f.to_mongo({key: 1})

    def test_dict_values_typed(self):
        f = DictField(IntField())
        assert f.clean({"a": "1"}) == {"a": 1}

    def test_enum(self):
        f = EnumField(Role)
        assert f.clean("admin") is Role.ADMIN
        assert f.to_mongo(Role.ADMIN) == "admin"
        with pytest.raises(ValidationError):
            f.clean("root")

    def test_geopoint(self):
        f = GeoPointField()
        assert f.to_mongo(f.clean([51.4, 35.7])) == {"type": "Point", "coordinates": [51.4, 35.7]}
        with pytest.raises(ValidationError):
            f.clean([200, 10])

    def test_any_rejects_operator_keys(self):
        with pytest.raises(Exception):
            AnyField().to_mongo({"a": {"$gt": 1}})


class TestDocumentFields:
    def test_error_collects_all_fields(self):
        acc = Account(username="x", age=-1, email="bad")
        with pytest.raises(ValidationError) as ei:
            acc.full_clean()
        assert set(ei.value.errors) >= {"username", "age", "email"}

    def test_embedded_validation_is_prefixed(self):
        acc = Account(username="okay", address={"city": "Tehran", "zip_code": "x" * 20})
        with pytest.raises(ValidationError) as ei:
            acc.full_clean()
        assert any(k.startswith("address") for k in ei.value.errors)
