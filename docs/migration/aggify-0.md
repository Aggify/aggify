# Upgrading from Aggify 0.x

Aggify 0.x was a pipeline *generator* for MongoEngine models. 1.0 is a standalone ODM: the pipeline builder is still
here (`from aggify import Aggify`) but it works on Aggify documents and no longer depends on MongoEngine.

| 0.x | 1.0 |
|---|---|
| `from mongoengine import Document, fields` | `from aggify import Document, StringField, ...` |
| `Aggify(Model)` mutated and returned `self` | **immutable**: each call returns a new pipeline |
| `list(pipeline)` -> stages | still works (`__iter__`); `pipeline.to_list()` is explicit |
| `[start:stop]` and `[n]` (n = *limit n*) | slicing as in Django; `[n]` is the n-th document (`$skip n, $limit 1`) |
| `.filter(a__exact=1, b__contains="x")` | same; `contains` & co. now **escape** the value |
| lookups added `as_name` = the reference field (overwriting the id) | joins use hidden `_j_<field>` aliases |
| `.lookup(Model, let=["id"], query=[Q(end__exact="id")], ...)` (magic string replacement) | `lookup(Model, "as", let={"id": F("id")}, query=Q(end=Var("id")))` - variables are explicit `Var(...)` |
| `.annotate("name", "sum", "field")` / `annotate(annotate_name=, accumulator=, f=)` | `annotate(name=Sum("field"))` (old keyword form still accepted) |
| `.group("field")` (default `"id"`) | `.group("field", n=Count(), ...)`, `by=None` for one bucket |
| `.redact(value1, condition, value2, then_value, else_value)` | `.redact(Q(...) or expression, "keep", "prune")` |
| `.replace_root(embedded_field="x", merge={})` | `.replace_root("x", merge={})` (old keyword still accepted) |
| `Cond(v1, ">", v2, then, else)` | still works; also `Cond(Q(...), then, else)` |
| `F(...)` mutated itself on `+ - * /` and produced invalid 3-operand `$subtract`/`$divide` | immutable; nested binary operators |
| `Q(...)` mutated on `\| &` | immutable |
| `F("x").to_dict()` | `F("x").resolve(ctx)`; use `pipeline.to_list()` |
| `.raw(stage)` accepted anything | scanned for `$where`/`$function`/`$accumulator` |
| string values in `add_fields` were `$literal` | unchanged |
| `annotate` required a preceding `group` | still true for aggregates; plain expressions become `$addFields` |

## Bugs fixed on the way

* `contains`/`startswith`... put raw user text into a regex (regex injection / ReDoS).
* `$subtract` / `$divide` with three operands (rejected by MongoDB), mutation of shared `F`/`Q` objects.
* `convert_match_query` missed `$` in `$lte`; the `let` substitution replaced *any* equal-looking value in a query.
* `$unwind` used the local field name instead of the alias after joins.
* Consecutive `$match` stages with the same key silently dropped the first condition.
* `[n]` meant "limit n".

## Moving your models

```python
# before
from mongoengine import Document, fields
class Account(Document):
    username = fields.StringField()
    meta = {"collection": "account"}

# after
from aggify import Document, StringField
class Account(Document):
    username = StringField()
    class Meta:
        collection = "account"
```

Then `Aggify(Account).filter(...)...` keeps working, and you gain `Account.objects`, async execution
(`await pipeline.aexecute()`) and validation. The comparison table in the [MongoEngine guide](mongoengine.md) covers the
rest of the model API.
