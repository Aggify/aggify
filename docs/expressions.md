# Expressions & functions

Expressions describe computations that run **inside MongoDB** (aggregation expressions). They are plain Python
objects - building them never touches a model - and are compiled against the model when used, so field names are
validated and translated (`db_field`, embedded paths, joins).

```python
from aggify import F, Value, Q, Case, When, Cond, Var
```

## `F` and `Value`

```python
F("price")                         # "$price"
F("address__city")                 # "$address.city"
F("customer__country")             # adds the join for you
Value("literal")                   # "literal"; strings starting with "$" are wrapped in $literal
```

In function arguments a bare string is a **field name** (like Django): `Upper("name")`. On the right-hand side of an
operator it is a **constant**: `F("name") == ...` is spelled `F("name").eq("x")`. Use `Value()` when in doubt.

## Operators

```python
F("price") * F("qty") + 5          # {"$add": [{"$multiply": ["$price", "$qty"]}, 5]}
-F("x"), abs(F("x")), F("a") % 3, F("a") ** 2, 10 - F("a")
F("a") > 3, F("a") <= F("b")       # comparison expressions (use .eq() / .ne() instead of == / !=)
(F("a") > 1) & (F("b") < 2) | ~(F("c") > 3)      # $and / $or / $not
F("tags").isin(["a", "b"])
```

!!! note
    `$subtract` and `$divide` only accept two operands in MongoDB, so `a - b - c` compiles to nested operators
    (`$add`/`$multiply` are flattened). Expressions are immutable: reusing `base = F("a") + 1` in two places is safe.

## Conditionals

```python
Cond(Q(age__gte=18), "adult", "minor")                              # $cond
Case(When(Q(score__gte=90), "A"), When(Q(score__gte=80), "B"), default="C")   # $switch
When(status="paid", then=1)                                         # kwargs are a Q
```

## Aggregates (accumulators)

`Sum, Avg, Min, Max, First, Last, Count, Push, AddToSet, StdDevPop, StdDevSamp, MergeObjects, Median`

```python
Sum("total", filter=Q(status="paid"))     # conditional aggregate, compiled with $cond
Count()                                    # counts documents;  Count("field") counts documents where it is set
Max("a", "b")                              # several arguments -> row-wise max
```

Usable in `aggregate()`, `values().annotate()`, `Pipeline.group()` and window functions (`window=...`).

## Function library

| group | functions |
|---|---|
| math | `Abs Ceil Floor Round Trunc Sqrt Pow Mod Exp Ln Log10 Rand` |
| strings | `Upper Lower Concat Length Substr Split Trim ReplaceAll RegexMatch IndexOfStr` |
| arrays / sets | `Size ArrayElemAt ConcatArrays SetUnion SetIntersection SetDifference IsArray ReverseArray Slice IndexOfArray RangeOf ArrayToObject ObjectToArray` |
| higher-order | `Filter(array, cond, as_="item")`, `MapArray(array, expr)`, `Reduce(array, initial, expr)`, `Let(vars, expr)` |
| dates | `Year Month DayOfMonth DayOfWeek DayOfYear Hour Minute Second Week IsoWeek` (all take `timezone=`), `DateToString DateTrunc DateAdd DateDiff` |
| types | `ToInt ToLong ToDouble ToString ToBool ToDate ToObjectId ToDecimal TypeOf IsNumber` |
| logic | `Coalesce Not Cond Case When` |
| objects | `GetField MergeObjectsExpr Literal` |
| windows | `Rank DenseRank DocumentNumber Shift` |
| anything else | `Func("$operator", arg, ...)` |

Inside `Filter`/`MapArray`/`Reduce` refer to the loop variable with `Var`:

```python
Account.objects.annotate(
    big=Filter("orders", Var("item.total") > 100),
    names=MapArray("friends", Upper(Var("item.name"))),
)
```

## Getting at everything else: `Raw`

```python
from aggify import Raw
Account.objects.filter(Raw({"location": {"$geoWithin": {...}}}))
pipeline.add_fields(x=Raw({"$sum": "$items.qty"}))
```

`Raw` is the explicit "I know what I'm doing" marker. Even raw values are scanned for server-side JavaScript
(`$where`, `$function`, `$accumulator`), which is blocked unless you call
`aggify.configure(allow_javascript=True)`.
