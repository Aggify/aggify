"""Differential testing: random documents + random Q trees evaluated by MongoDB (via Aggify)
and by a tiny reference interpreter in Python.  Any divergence is a bug in lookup compilation."""

from __future__ import annotations

import operator
from typing import Any, Callable, Dict, List

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from aggify import BooleanField, Count, Document, F, IntField, ListField, Q, StringField, Sum

NAMES = st.text(alphabet="aAb.*(c ", min_size=0, max_size=4)


class Probe(Document):
    name = StringField()
    n = IntField()
    flag = BooleanField()
    tags = ListField(StringField())

    class Meta:
        collection = "probe"


doc_strategy = st.fixed_dictionaries(
    {
        "name": NAMES,
        "n": st.none() | st.integers(0, 6),
        "flag": st.booleans(),
        "tags": st.lists(st.sampled_from(["x", "y", "z"]), max_size=3, unique=True),
    }
)


def _cmp(op: Callable[[Any, Any], bool]) -> Callable[[Dict[str, Any], Any], bool]:
    return lambda d, v: d["n"] is not None and op(d["n"], v)


LEAVES = {
    "name": (lambda d, v: d["name"] == v, NAMES),
    "name__ne": (lambda d, v: d["name"] != v, NAMES),
    "name__contains": (lambda d, v: v in d["name"], NAMES),
    "name__icontains": (lambda d, v: v.lower() in d["name"].lower(), NAMES),
    "name__startswith": (lambda d, v: d["name"].startswith(v), NAMES),
    "name__endswith": (lambda d, v: d["name"].endswith(v), NAMES),
    "name__in": (lambda d, v: d["name"] in v, st.lists(NAMES, max_size=3)),
    "n": (lambda d, v: d["n"] == v, st.none() | st.integers(0, 6)),
    "n__ne": (lambda d, v: d["n"] != v, st.integers(0, 6)),
    "n__gt": (_cmp(operator.gt), st.integers(0, 6)),
    "n__gte": (_cmp(operator.ge), st.integers(0, 6)),
    "n__lt": (_cmp(operator.lt), st.integers(0, 6)),
    "n__lte": (_cmp(operator.le), st.integers(0, 6)),
    "n__in": (lambda d, v: d["n"] in v, st.lists(st.integers(0, 6), max_size=3)),
    "n__nin": (lambda d, v: d["n"] not in v, st.lists(st.integers(0, 6), max_size=3)),
    "n__range": (
        lambda d, v: d["n"] is not None and v[0] <= d["n"] <= v[1],
        st.tuples(st.integers(0, 3), st.integers(3, 6)),
    ),
    "n__isnull": (lambda d, v: (d["n"] is None) == v, st.booleans()),
    "flag": (lambda d, v: d["flag"] == v, st.booleans()),
    "tags": (lambda d, v: v in d["tags"], st.sampled_from(["x", "y", "z"])),
    "tags__in": (lambda d, v: any(t in v for t in d["tags"]), st.lists(st.sampled_from(["x", "y", "z"]), max_size=2)),
    "tags__all": (
        lambda d, v: all(t in d["tags"] for t in v),
        st.lists(st.sampled_from(["x", "y", "z"]), min_size=1, max_size=2),
    ),
    "tags__size": (lambda d, v: len(d["tags"]) == v, st.integers(0, 3)),
}


@st.composite
def leaf(draw):
    key = draw(st.sampled_from(sorted(LEAVES)))
    fn, values = LEAVES[key]
    value = draw(values)
    return Q(**{key: value}), (lambda d, fn=fn, value=value: fn(d, value))


def tree():
    def extend(children):
        return st.one_of(
            st.tuples(children, children).map(
                lambda p: (p[0][0] & p[1][0], lambda d, a=p[0][1], b=p[1][1]: a(d) and b(d))
            ),
            st.tuples(children, children).map(
                lambda p: (p[0][0] | p[1][0], lambda d, a=p[0][1], b=p[1][1]: a(d) or b(d))
            ),
            children.map(lambda c: (~c[0], lambda d, f=c[1]: not f(d))),
        )

    return st.recursive(leaf(), extend, max_leaves=5)


@pytest.mark.mongo
@settings(
    max_examples=250, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture, HealthCheck.too_slow]
)
@given(docs=st.lists(doc_strategy, min_size=0, max_size=12), query=tree())
def test_filter_matches_python_reference(docs: List[Dict[str, Any]], query):
    q, predicate = query
    Probe.drop_collection()
    objs = Probe.objects.bulk_create([Probe(**d) for d in docs], validate=False) if docs else []
    expected = {o.pk for o, d in zip(objs, docs) if predicate(d)}
    got = {o.pk for o in Probe.objects.filter(q)}
    assert got == expected, f"\nQ={q!r}\nquery={Probe.objects.filter(q).query}"
    # the same condition used as exclude() must be the exact complement
    assert {o.pk for o in Probe.objects.exclude(q)} == {o.pk for o in objs} - expected
    # and count() agrees with iteration
    assert Probe.objects.filter(q).count() == len(expected)


@pytest.mark.mongo
@settings(
    max_examples=80, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture, HealthCheck.too_slow]
)
@given(docs=st.lists(doc_strategy, min_size=0, max_size=12))
def test_group_by_matches_python(docs):
    Probe.drop_collection()
    if docs:
        Probe.objects.bulk_create([Probe(**d) for d in docs], validate=False)
    got = {r["flag"]: (r["c"], r["s"]) for r in Probe.objects.values("flag").annotate(c=Count(), s=Sum("n"))}
    expected: Dict[bool, List[int]] = {}
    for d in docs:
        c, s = expected.setdefault(d["flag"], [0, 0])
        expected[d["flag"]] = [c + 1, s + (d["n"] or 0)]
    assert got == {k: (v[0], v[1]) for k, v in expected.items()}


@pytest.mark.mongo
@settings(
    max_examples=80, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture, HealthCheck.too_slow]
)
@given(docs=st.lists(doc_strategy, min_size=1, max_size=12), a=st.integers(0, 6), b=st.integers(0, 6))
def test_field_expression_comparison(docs, a, b):
    Probe.drop_collection()
    objs = Probe.objects.bulk_create([Probe(**d) for d in docs], validate=False)
    got = {o.pk for o in Probe.objects.filter(F("n") + a > b)}
    expected = {o.pk for o, d in zip(objs, docs) if d["n"] is not None and d["n"] + a > b}
    assert got == expected
