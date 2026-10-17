import datetime as dt

import pytest

from aggify import (
    Avg,
    Case,
    Concat,
    Count,
    F,
    InvalidArgument,
    Max,
    Min,
    MongoIndexError,
    Push,
    Q,
    Size,
    Sum,
    Upper,
    ValidationError,
    Value,
    When,
    Year,
)
from aggify.testing import assert_num_queries
from tests.models import Account, Comment, Post


@pytest.fixture
def data(clean_db):
    ac = {}
    for name, age, verified, tags, city in [
        ("alice", 30, True, ["a", "b"], "Tehran"),
        ("bob", 25, False, ["b"], "Shiraz"),
        ("carol", 41, True, [], "Tehran"),
        ("dave", 25, True, ["c", "a"], None),
    ]:
        ac[name] = Account.objects.create(
            username=name,
            age=age,
            is_verified=verified,
            tags=tags,
            score=age / 10,
            address={"city": city} if city else None,
            display_name=name.title(),
        )
    posts = []
    for i, (owner, likes, tags, pub) in enumerate(
        [
            ("alice", 10, ["x"], True),
            ("alice", 5, ["x", "y"], False),
            ("bob", 7, [], True),
            ("carol", 1, ["y"], True),
        ]
    ):
        posts.append(
            Post.objects.create(
                owner=ac[owner],
                caption=f"post{i}",
                likes=likes,
                hashtags=tags,
                published=pub,
                comments=[{"text": "c", "score": likes}],
            )
        )
    return ac, posts


def names(qs):
    return [a.username for a in qs]


class TestFiltering:
    def test_basic_lookups(self, data):
        assert names(Account.objects.filter(age__gt=25)) == ["alice", "carol"]
        assert names(Account.objects.filter(age__in=[25, 41])) == ["bob", "carol", "dave"]
        assert names(Account.objects.filter(age=25, is_verified=True)) == ["dave"]
        assert names(Account.objects.filter(username__startswith="c")) == ["carol"]
        assert names(Account.objects.filter(username__icontains="LI")) == ["alice"]
        assert names(Account.objects.filter(address__city="Tehran")) == ["alice", "carol"]
        assert names(Account.objects.filter(address__isnull=True)) == ["dave"]
        assert names(Account.objects.filter(tags="a")) == ["alice", "dave"]
        assert names(Account.objects.filter(tags__all=["a", "b"])) == ["alice"]
        assert names(Account.objects.filter(tags__size=0)) == ["carol"]
        assert names(Account.objects.filter(age__range=(26, 41))) == ["alice", "carol"]

    def test_q_combinations(self, data):
        qs = Account.objects.filter(Q(age__lt=26) & ~Q(username="dave") | Q(username="carol"))
        assert names(qs) == ["bob", "carol"]
        assert names(Account.objects.exclude(age=25)) == ["alice", "carol"]
        assert names(Account.objects.exclude(Q(age=25) | Q(age=30))) == ["carol"]

    def test_field_to_field_comparison(self, data):
        assert names(Account.objects.filter(age__gt=F("score") * 10)) == []
        assert names(Account.objects.filter(F("age") * 0.1 >= F("score"))) == ["alice", "bob", "carol", "dave"]
        assert names(Account.objects.filter(age__lte=F("score") * 10)) == ["alice", "bob", "carol", "dave"]

    def test_chained_filters_and(self, data):
        assert names(Account.objects.filter(age=25).filter(is_verified=True)) == ["dave"]
        assert names(Account.objects.filter(age__gte=25).filter(age__lt=31)) == ["alice", "bob", "dave"]
        assert names(Account.objects.filter(username="alice").filter(username="bob")) == []

    def test_elem_match(self, data):
        _, posts = data
        assert Post.objects.filter(comments__elem_match=Q(text="c", score__gte=10)).count() == 1

    def test_regex_special_chars_are_literal(self, data):
        Account.objects.create(username="a.b")
        assert names(Account.objects.filter(username__contains=".")) == ["a.b"]
        assert names(Account.objects.filter(username__contains=".*")) == []
        assert names(Account.objects.filter(username__iexact="A.B")) == ["a.b"]

    def test_none_filters_and_empty_in(self, data):
        assert names(Account.objects.filter(display_name__in=[])) == []
        assert names(Account.objects.none()) == []
        assert Account.objects.none().count() == 0 and not Account.objects.none().exists()

    def test_operator_injection_returns_error_not_data(self, data):
        with pytest.raises(ValidationError):
            list(Account.objects.filter(username={"$ne": "zzz"}))

    def test_isnull_semantics(self, data):
        Account.objects.filter(username="bob").update(unset__age=True)
        assert names(Account.objects.filter(age__isnull=True)) == ["bob"]
        assert len(Account.objects.filter(age__isnull=False)) == 3


class TestJoins:
    def test_filter_across_reference(self, data):
        qs = Post.objects.filter(owner__username="alice")
        assert sorted(p.caption for p in qs) == ["post0", "post1"]
        assert Post.objects.filter(owner__address__city="Tehran").count() == 3
        assert Post.objects.filter(owner__age__gt=26, published=True).count() == 2

    def test_order_by_joined_field(self, data):
        assert [p.caption for p in Post.objects.order_by("-owner__age", "caption")] == [
            "post3",
            "post0",
            "post1",
            "post2",
        ]

    def test_instances_do_not_leak_join_fields(self, data):
        p = Post.objects.filter(owner__username="bob").get()
        assert p.caption == "post2" and not hasattr(p, "_j_owner")

    def test_select_related(self, data):
        with assert_num_queries(1):
            posts = list(Post.objects.select_related("owner").order_by("caption"))
            assert [p.owner.username for p in posts] == ["alice", "alice", "bob", "carol"]

    def test_prefetch_related_uses_two_queries(self, data):
        with assert_num_queries(2):
            posts = list(Post.objects.prefetch_related("owner").order_by("caption"))
            assert [p.owner.username for p in posts] == ["alice", "alice", "bob", "carol"]

    def test_reference_id_filters_need_no_join(self, data):
        ac, _ = data
        qs = Post.objects.filter(owner=ac["alice"])
        assert "aggregate" not in qs.query and qs.count() == 2
        assert Post.objects.filter(owner__id=ac["bob"].pk).count() == 1
        assert Post.objects.filter(owner__in=[ac["bob"], ac["carol"].pk]).count() == 2

    def test_reference_inside_embedded_list(self, data):
        ac, _ = data
        Post.objects.filter(caption="post0").first()
        p = Post.objects.get(caption="post0")
        p.comments = [Comment(text="by bob", author=ac["bob"])]
        p.save()
        assert Post.objects.filter(comments__author__username="bob").count() == 1


class TestOrderingSlicing:
    def test_meta_ordering_default_and_override(self, data):
        assert names(Account.objects.all()) == ["alice", "bob", "carol", "dave"]
        assert names(Account.objects.order_by("-age", "username")) == ["carol", "alice", "bob", "dave"]
        assert names(Account.objects.order_by("-username").reverse()) == ["alice", "bob", "carol", "dave"]

    def test_slicing(self, data):
        qs = Account.objects.order_by("username")
        assert names(qs[1:3]) == ["bob", "carol"] and names(qs[:2]) == ["alice", "bob"] and names(qs[3:]) == ["dave"]
        assert qs[2].username == "carol" and names(qs[1:][1:2]) == ["carol"]
        with pytest.raises(IndexError):
            qs[10]
        for bad in (slice(-1, 2), slice(0, 4, 2), -1):
            with pytest.raises(MongoIndexError):
                qs[bad]

    def test_slice_count_and_exists(self, data):
        qs = Account.objects.order_by("username")
        assert qs[1:3].count() == 2 and qs[:10].count() == 4 and qs[2:2].count() == 0

    def test_first_last_earliest_latest(self, data):
        assert Account.objects.first().username == "alice" and Account.objects.last().username == "dave"
        assert Account.objects.order_by("-age").first().username == "carol"
        assert Account.objects.latest("age").username == "carol"
        assert Account.objects.earliest("age").age == 25
        assert Account.objects.filter(age=1000).first() is None
        with pytest.raises(Account.DoesNotExist):
            Account.objects.filter(age=1000).latest("age")

    def test_cannot_filter_after_slice(self, data):
        with pytest.raises(InvalidArgument):
            Account.objects.all()[:2].filter(age=1)

    def test_queryset_caching(self, data):
        qs = Account.objects.all()
        with assert_num_queries(1):
            assert len(qs) == 4
            assert len(list(qs)) == 4
            assert bool(qs)

    def test_exists_and_count(self, data):
        assert Account.objects.filter(age=41).exists() and not Account.objects.filter(age=99).exists()
        assert Account.objects.filter(age__lt=30).count() == 2

    def test_in_bulk(self, data):
        ac, _ = data
        got = Account.objects.in_bulk([ac["bob"].pk, ac["dave"].pk])
        assert set(got) == {ac["bob"].pk, ac["dave"].pk}
        assert set(Account.objects.in_bulk(["alice", "bob"], field_name="username")) == {"alice", "bob"}

    def test_iterator_streams(self, data):
        assert [a.username for a in Account.objects.iterator(chunk_size=2)] == ["alice", "bob", "carol", "dave"]


class TestProjection:
    def test_only_and_defer_give_partial_instances(self, data):
        a = Account.objects.only("username").get(username="alice")
        assert a.username == "alice" and a.age is None and a._state.loaded == {"id", "username"}
        a.username = "alicia"
        assert a.changed_fields == ["username"]
        a.save()
        full = Account.objects.get(pk=a.pk)
        assert full.username == "alicia" and full.age == 30  # untouched field survived

        b = Account.objects.defer("tags", "address").get(username="bob")
        assert b.age == 25 and b.tags is None and b._state.loaded is not None

    def test_values_and_values_list(self, data):
        assert list(Account.objects.values("username", "age").order_by("username"))[0] == {
            "username": "alice",
            "age": 30,
        }
        assert list(Account.objects.order_by("username").values_list("username", flat=True)) == [
            "alice",
            "bob",
            "carol",
            "dave",
        ]
        assert list(Account.objects.order_by("username").values_list("username", "age"))[1] == ("bob", 25)
        row = list(Account.objects.order_by("username").values_list("username", "age", named=True))[0]
        assert (row.username, row.age) == ("alice", 30)
        assert "id" in Account.objects.values("id", "username").first()

    def test_values_over_joins_and_embedded(self, data):
        rows = list(Post.objects.order_by("caption").values("caption", "owner__username"))
        assert rows[0] == {"caption": "post0", "owner__username": "alice"}
        rows = list(Account.objects.order_by("username").values("address__city"))
        assert rows[0] == {"address__city": "Tehran"}

    def test_values_with_pk(self, data):
        row = (
            Account.objects.filter(username="bob").values("pk", "username").get()
            if False
            else list(Account.objects.filter(username="bob").values("pk", "username"))[0]
        )
        assert row["username"] == "bob" and row["id"] is not None

    def test_distinct(self, data):
        assert sorted(Account.objects.distinct("age")) == [25, 30, 41]
        assert sorted(Account.objects.filter(is_verified=True).distinct("tags")) == ["a", "b", "c"]
        assert sorted(Post.objects.distinct("owner")) != []


class TestAnnotateAggregate:
    def test_annotate_scalar_expression(self, data):
        qs = Account.objects.annotate(double=F("age") * 2, label=Upper("username")).order_by("-double")
        top = qs.first()
        assert (top.username, top.double, top.label) == ("carol", 82, "CAROL")

    def test_filter_and_order_on_annotation(self, data):
        qs = Account.objects.annotate(double=F("age") * 2).filter(double__gt=55).order_by("double")
        assert names(qs) == ["alice", "carol"]
        assert [a.double for a in qs] == [60, 82]

    def test_case_when_annotation(self, data):
        qs = Account.objects.annotate(
            bucket=Case(When(Q(age__gte=40), "senior"), When(Q(age__gte=28), "mid"), default="young")
        ).order_by("username")
        assert [(a.username, a.bucket) for a in qs] == [
            ("alice", "mid"),
            ("bob", "young"),
            ("carol", "senior"),
            ("dave", "young"),
        ]

    def test_annotate_from_array_functions(self, data):
        qs = Account.objects.annotate(ntags=Size("tags")).filter(ntags__gte=2).order_by("username")
        assert [(a.username, a.ntags) for a in qs] == [("alice", 2), ("dave", 2)]

    def test_aggregate(self, data):
        res = Account.objects.aggregate(n=Count(), total=Sum("age"), mean=Avg("age"), lo=Min("age"), hi=Max("age"))
        assert res == {"n": 4, "total": 121, "mean": 30.25, "lo": 25, "hi": 41}

    def test_aggregate_with_filter_and_filtered_aggregates(self, data):
        res = Account.objects.filter(age__gte=30).aggregate(n=Count())
        assert res == {"n": 2}
        res = Account.objects.aggregate(
            verified=Count(filter=Q(is_verified=True)), v_age=Sum("age", filter=Q(is_verified=True))
        )
        assert res == {"verified": 3, "v_age": 96}

    def test_aggregate_on_empty_set(self, data):
        assert Account.objects.filter(age=999).aggregate(n=Count(), total=Sum("age")) == {"n": 0, "total": None}

    def test_group_by_values_annotate(self, data):
        rows = list(Account.objects.values("age").annotate(n=Count(), names=Push("username")).order_by("age"))
        assert [(r["age"], r["n"]) for r in rows] == [(25, 2), (30, 1), (41, 1)]
        assert sorted(rows[0]["names"]) == ["bob", "dave"]

    def test_having_filter_after_annotate(self, data):
        rows = list(Account.objects.values("age").annotate(n=Count()).filter(n__gt=1))
        assert rows == [{"age": 25, "n": 2}]

    def test_where_before_annotate(self, data):
        rows = list(Account.objects.filter(is_verified=True).values("age").annotate(n=Count()).order_by("-age"))
        assert rows == [{"age": 41, "n": 1}, {"age": 30, "n": 1}, {"age": 25, "n": 1}]

    def test_group_by_joined_field(self, data):
        rows = list(Post.objects.values("owner__username").annotate(total=Sum("likes")).order_by("-total"))
        assert rows == [
            {"owner__username": "alice", "total": 15},
            {"owner__username": "bob", "total": 7},
            {"owner__username": "carol", "total": 1},
        ]

    def test_group_by_multiple_and_count_of_grouped(self, data):
        qs = Account.objects.values("is_verified", "age").annotate(n=Count())
        assert qs.count() == 4 and len(list(qs)) == 4

    def test_annotation_name_cannot_shadow_field(self, data):
        with pytest.raises(InvalidArgument):
            Account.objects.annotate(age=F("score"))

    def test_slicing_a_grouped_queryset(self, data):
        rows = list(Account.objects.values("age").annotate(n=Count()).order_by("age")[:2])
        assert [r["age"] for r in rows] == [25, 30]

    def test_date_functions(self, data):
        rows = list(
            Account.objects.values("username").annotate(year=Year("created_at"))
            if False
            else Account.objects.annotate(year=Year("created_at")).values("username", "year").order_by("username")
        )
        assert rows[0]["year"] == dt.datetime.now(dt.timezone.utc).year


class TestBulkWrites:
    def test_update_operators(self, data):
        assert Account.objects.filter(age=25).update(inc__age=1, push__tags="z", display_name="X") == 2
        assert sorted(a.age for a in Account.objects.filter(display_name="X")) == [26, 26]
        assert all("z" in a.tags for a in Account.objects.filter(display_name="X"))

    def test_update_with_expression_is_atomic_pipeline_update(self, data):
        Account.objects.update(age=F("age") * 2, display_name=Concat("username", Value("!")))
        a = Account.objects.get(username="bob")
        assert (a.age, a.display_name) == (50, "bob!")

    def test_update_one_and_upsert(self, data):
        assert Account.objects.filter(age=25).update_one(display_name="ONE") == 1
        assert Account.objects.filter(display_name="ONE").count() == 1
        assert Account.objects.filter(username="newbie").update(upsert=True, age=3) == 1
        assert Account.objects.get(username="newbie").age == 3

    def test_pull_and_add_to_set(self, data):
        Account.objects.filter(username="alice").update(pull__tags="a")
        Account.objects.filter(username="alice").update(add_to_set__tags="b")
        Account.objects.filter(username="alice").update(add_to_set_all__tags=["q", "r", "b"])
        assert sorted(Account.objects.get(username="alice").tags) == ["b", "q", "r"]

    def test_update_validates(self, data):
        with pytest.raises(ValidationError):
            Account.objects.update(age="old")

    def test_update_on_sliced_queryset_is_refused(self, data):
        with pytest.raises(InvalidArgument):
            Account.objects.all()[:2].update(age=1)

    def test_delete(self, data):
        assert Account.objects.filter(age=25).delete() == 2
        assert Account.objects.count() == 2
        assert Account.objects.all().delete() == 2

    def test_filter_on_joined_field_in_write_is_refused(self, data):
        with pytest.raises(Exception):
            Post.objects.filter(owner__username="alice").update(likes=0)


class TestAsyncQueries:
    async def test_basic_async_queries(self, data):
        assert [a.username async for a in Account.objects.filter(age__gt=25)] == ["alice", "carol"]
        assert await Account.objects.filter(age=25).acount() == 2
        assert (await Account.objects.alatest("age")).username == "carol"
        assert await Account.objects.filter(age=41).aexists()
        assert [a.username for a in await Account.objects.order_by("-age")[:2].alist()] == ["carol", "alice"]

    async def test_async_joins_and_aggregates(self, data):
        posts = await Post.objects.select_related("owner").filter(owner__age__gt=26).order_by("caption").alist()
        assert [(p.caption, p.owner.username) for p in posts] == [
            ("post0", "alice"),
            ("post1", "alice"),
            ("post3", "carol"),
        ]
        assert await Account.objects.aaggregate(n=Count(), total=Sum("age")) == {"n": 4, "total": 121}
        rows = await Account.objects.values("age").annotate(n=Count()).order_by("age").alist()
        assert rows[0] == {"age": 25, "n": 2}

    async def test_async_writes(self, data):
        assert await Account.objects.filter(age=25).aupdate(inc__age=1) == 2
        assert await Account.objects.filter(age=26).adelete() == 2
        assert await Account.objects.acount() == 2

    async def test_aiterator_and_prefetch(self, data):
        got = [a.username async for a in Account.objects.aiterator(chunk_size=1)]
        assert got == ["alice", "bob", "carol", "dave"]
        posts = await Post.objects.prefetch_related("owner").order_by("caption").alist()
        assert [p.owner.username for p in posts][:2] == ["alice", "alice"]

    async def test_distinct_and_in_bulk(self, data):
        assert sorted(await Account.objects.adistinct("age")) == [25, 30, 41]
        assert len(await Account.objects.ain_bulk([a.pk for a in await Account.objects.alist()])) == 4


class TestMisc:
    def test_group_by_expression_in_values(self, data):
        from aggify import DateToString

        rows = list(Account.objects.values(day=DateToString("created_at", "%Y")).annotate(n=Count()))
        assert rows == [{"day": str(dt.datetime.now(dt.timezone.utc).year), "n": 4}]

    def test_group_by_joined_name_then_filter_and_order_by_it(self, data):
        qs = (
            Post.objects.values("owner__username")
            .annotate(n=Count())
            .filter(owner__username__in=["alice", "bob"])
            .order_by("owner__username")
        )
        assert list(qs) == [{"owner__username": "alice", "n": 2}, {"owner__username": "bob", "n": 1}]

    def test_text_search(self, data):
        from aggify import Document, Index, StringField

        class Article(Document):
            title = StringField()
            body = StringField()

            class Meta:
                collection = "article_search"
                indexes = [Index("title", "body", type="text")]

        try:
            Article.ensure_indexes()
            Article.objects.bulk_create(
                [Article(title="Mongo tips", body="fast"), Article(title="Cooking", body="soup")]
            )
            assert [a.title for a in Article.objects.search("mongo")] == ["Mongo tips"]
            with pytest.raises(InvalidArgument):
                Article.objects.search("  ")
        finally:
            Article.drop_collection()

    def test_pipeline_update_with_inc_and_expression(self, data):
        Account.objects.filter(username="alice").update(age=F("age") + 1, inc__score=10, push__tags="new")
        a = Account.objects.get(username="alice")
        assert (a.age, a.score, a.tags[-1]) == (31, 13.0, "new")

    def test_first_and_last_on_grouped_and_values_querysets(self, data):
        grouped = Account.objects.values("age").annotate(n=Count()).order_by("age")
        assert grouped.first() == {"age": 25, "n": 2} and grouped.last() == {"age": 41, "n": 1}
        assert Account.objects.values("age").annotate(n=Count()).first() is not None
        assert Account.objects.values_list("username", flat=True).first() in ("alice", "bob", "carol", "dave")
