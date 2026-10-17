"""Pipelines executed against a real server."""

import pytest

from aggify import (
    AddToSet,
    Aggify,
    Case,
    Concat,
    Cond,
    Count,
    F,
    Filter,
    MapArray,
    Q,
    Rank,
    Size,
    Sum,
    Value,
    Var,
    When,
)
from tests.models import Account, Post


@pytest.fixture
def data(clean_db):
    ac = {
        n: Account.objects.create(username=n, age=a, score=a / 10, tags=t, is_verified=v)
        for n, a, t, v in [
            ("alice", 30, ["a", "b"], True),
            ("bob", 25, ["b"], False),
            ("carol", 41, [], True),
            ("dave", 25, ["c", "a"], True),
        ]
    }
    for owner, likes, pub in [("alice", 10, True), ("alice", 5, False), ("bob", 7, True), ("carol", 1, True)]:
        Post.objects.create(owner=ac[owner], caption=f"{owner}-{likes}", likes=likes, published=pub)
    return ac


def test_group_sort_slice_roundtrip(data):
    rows = (
        Aggify(Post)
        .filter(published=True)
        .group("owner", total=Sum("likes"), n=Count())
        .order_by("-total")[:2]
        .execute()
    )
    assert [(r["total"], r["n"]) for r in rows] == [(10, 1), (7, 1)]
    assert all(isinstance(r["_id"], type(data["alice"].pk)) for r in rows)


async def test_async_execute_and_iterate(data):
    p = Aggify(Account).filter(age__gte=25).order_by("username").project(username=1, age=1, pk=0)
    assert await p.aexecute() == [
        {"username": "alice", "age": 30},
        {"username": "bob", "age": 25},
        {"username": "carol", "age": 41},
        {"username": "dave", "age": 25},
    ]
    seen = [row["username"] async for row in p.aiterate()]
    assert seen[0] == "alice" and len(seen) == 4


def test_lookup_with_let_and_query(data):
    rows = (
        Aggify(Account)
        .lookup(Post, "published_posts", let={"uid": F("id")}, query=Q(owner=Var("uid"), published=True))
        .order_by("username")
        .execute()
    )
    counts = {r["username"]: len(r["published_posts"]) for r in rows}
    assert counts == {"alice": 1, "bob": 1, "carol": 1, "dave": 0}


def test_lookup_equality_form_and_unwind(data):
    p = Aggify(Post).lookup(Account, "who", local_field="owner", foreign_field="id", unwind=True)
    rows = p.filter(who__username="bob").execute()
    assert len(rows) == 1 and rows[0]["who"]["username"] == "bob"


def test_lookup_with_sub_pipeline(data):
    sub = Aggify(Post).filter(Q(owner=Var("uid"))).order_by("-likes")[:1].project(caption=1, pk=0)
    rows = Aggify(Account).lookup(Post, "best", let={"uid": F("id")}, pipeline=sub).filter(username="alice").execute()
    assert rows[0]["best"] == [{"caption": "alice-10"}]


def test_expressions_in_stages(data):
    rows = (
        Aggify(Account)
        .add_fields(
            label=Concat("username", Value(":"), Cond(Q(age__gte=30), "old", "young")),
            n_tags=Size("tags"),
            grade=Case(When(Q(age__gte=40), "A"), default="B"),
        )
        .filter(n_tags__gte=1)
        .order_by("username")
        .project(label=1, n_tags=1, grade=1, pk=0)
        .execute()
    )
    assert rows == [
        {"label": "alice:old", "n_tags": 2, "grade": "B"},
        {"label": "bob:young", "n_tags": 1, "grade": "B"},
        {"label": "dave:young", "n_tags": 2, "grade": "B"},
    ]


def test_filter_and_map_array_expressions(data):
    rows = (
        Aggify(Account)
        .filter(username="alice")
        .add_fields(no_a=Filter("tags", Var("item").ne("a")), up=MapArray("tags", Concat(Var("item"), Value("!"))))
        .execute()
    )
    assert rows[0]["no_a"] == ["b"] and rows[0]["up"] == ["a!", "b!"]


def test_unwind_and_group(data):
    rows = Aggify(Account).unwind("tags").group("tags", n=Count(), who=AddToSet("username")).order_by("-n").execute()
    top = rows[0]
    assert top["n"] == 2 and top["_id"] in ("a", "b") and len(rows) == 3


def test_facet_and_bucket(data):
    res = (
        Aggify(Account)
        .facet(by_verified=Aggify(Account).group("is_verified", n=Count()), total=[{"$count": "n"}])
        .execute()[0]
    )
    assert res["total"] == [{"n": 4}] and {r["_id"]: r["n"] for r in res["by_verified"]} == {True: 3, False: 1}
    rows = Aggify(Account).bucket("age", [0, 30, 50], default="other", n=Count()).execute()
    assert {r["_id"]: r["n"] for r in rows} == {0: 2, 30: 2}


def test_window_fields(data):
    rows = (
        Aggify(Account)
        .set_window_fields(
            sort_by={"age": 1}, rank=Rank(), running=Sum("age", window={"documents": ["unbounded", "current"]})
        )
        .order_by("age", "username")
        .execute()
    )
    assert [r["rank"] for r in rows] == [1, 1, 3, 4] and [r["running"] for r in rows] == [25, 50, 80, 121]


def test_replace_root_and_redact(data):
    Account.objects.filter(username="alice").update(address__city="Tehran", address__zip_code="1")
    rows = Aggify(Account).filter(username="alice").replace_root("address").execute()
    assert rows == [{"city": "Tehran", "zip_code": "1"}]


def test_sort_by_count_and_sample_and_count(data):
    rows = Aggify(Account).sort_by_count("age").execute()
    assert rows[0]["count"] == 2 and rows[0]["_id"] == 25
    assert len(Aggify(Account).sample(2).execute()) == 2
    assert Aggify(Account).filter(age=25).count("n").execute() == [{"n": 2}]


def test_out_and_merge(data):
    Aggify(Account).filter(age__gte=30).project(username=1, pk=0).out("archive").execute()
    from aggify import get_database

    assert sorted(d["username"] for d in get_database().archive.find()) == ["alice", "carol"]
    get_database().archive.drop()


def test_union_with(data):
    rows = (
        Aggify(Account)
        .filter(username="alice")
        .project(username=1, pk=0)
        .union_with(Post, pipeline=Aggify(Post).filter(likes=1).project(caption=1, pk=0))
        .execute()
    )
    assert {tuple(r) for r in rows} == {("username",), ("caption",)}


def test_explain_runs(data):
    plan = Aggify(Account).filter(age=25).explain()
    assert "stages" in str(plan) or "queryPlanner" in plan or "ok" in plan


def test_python_value_conversion_in_results(data):
    rows = Aggify(Account).filter(username="alice").project(username=1, created_at=1, pk=0).execute()
    assert rows[0]["created_at"].tzinfo is not None


def test_pipeline_is_reusable(data):
    base = Aggify(Account).filter(is_verified=True)
    a = base.filter(age__gt=29).execute()
    b = base.execute()
    assert len(a) == 2 and len(b) == 3


def test_raw_stage_runs(data):
    rows = Aggify(Account).raw({"$match": {"age": 25}}).raw({"$count": "n"}).execute()
    assert rows == [{"n": 2}]
