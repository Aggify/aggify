# Pydantic

```python
from aggify.contrib.pydantic import pydantic_model, to_pydantic, from_pydantic, PyObjectId

Out = pydantic_model(Account, name="AccountOut", exclude=["password_hash"])
In = pydantic_model(Account, name="AccountIn", for_input=True)
Patch = pydantic_model(Account, name="AccountPatch", for_input=True, all_optional=True)

to_pydantic(account, Out)          # document -> model
from_pydantic(Account, body)       # model -> unsaved document (unset fields stay default)
```

Mapping: `StringField(max_length=..)`->`str` + constraints, `IntField(min_value=..)`->`ge/le`, `DecimalField`->`Decimal`
with digits, `ListField`->`List[...]`, `DictField`->`Dict[str, ...]`, `EmbeddedField`->nested model,
`ReferenceField`->`PyObjectId`, `EnumField`->the enum, `GeoPointField`->`Tuple[float, float]`.

`PyObjectId` validates 24-hex strings, serialises as a string in JSON and renders as
`{"type": "string", "pattern": "^[0-9a-fA-F]{24}$"}` in OpenAPI.

Models are cached per `(document, options)`, so calling `pydantic_model` in several modules is cheap. Documents can also
be read by Pydantic directly (`from_attributes=True`).
