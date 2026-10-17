"""``document_form(Model)``: a Django ``Form`` generated from an Aggify document."""

from __future__ import annotations

from typing import Any, Dict, Iterable, Optional, Type

from django import forms

from aggify import fields as f
from aggify.exceptions import ValidationError as AggifyValidationError


def _form_field(field: f.Field) -> Optional[forms.Field]:
    kw: Dict[str, Any] = {"required": field.required, "help_text": field.help_text}
    if field.verbose_name:
        kw["label"] = field.verbose_name
    if field.choices is not None and not isinstance(field.choices, type):
        kw["choices"] = [(c[0], c[1]) if isinstance(c, (tuple, list)) else (c, c) for c in field.choices]
        return forms.ChoiceField(**kw)
    if isinstance(field, f.EmailField):
        return forms.EmailField(max_length=field.max_length, **kw)
    if isinstance(field, f.URLField):
        return forms.URLField(max_length=field.max_length, **kw)
    if isinstance(field, f.StringField):
        return forms.CharField(min_length=field.min_length, max_length=field.max_length, strip=field.strip, **kw)
    if isinstance(field, f.IntField):
        return forms.IntegerField(min_value=field.min_value, max_value=field.max_value, **kw)
    if isinstance(field, f.FloatField):
        return forms.FloatField(min_value=field.min_value, max_value=field.max_value, **kw)
    if isinstance(field, f.DecimalField):
        return forms.DecimalField(max_digits=field.max_digits, decimal_places=field.decimal_places, **kw)
    if isinstance(field, f.BooleanField):
        kw["required"] = False
        return forms.BooleanField(**kw)
    if isinstance(field, f.DateTimeField):
        return forms.DateTimeField(**kw)
    if isinstance(field, f.DateField):
        return forms.DateField(**kw)
    if isinstance(field, f.UUIDField):
        return forms.UUIDField(**kw)
    if isinstance(field, (f.ObjectIdField, f.ReferenceField)):
        return forms.CharField(min_length=24, max_length=24, **kw)
    if isinstance(field, (f.DictField, f.ListField, f.EmbeddedField)):
        return forms.JSONField(**kw)
    return None


def document_form(
    model: Any,
    *,
    fields: Optional[Iterable[str]] = None,
    exclude: Iterable[str] = (),
    name: Optional[str] = None,
) -> Type[forms.Form]:
    """Build a form class; ``form.save()`` creates (or updates ``instance``) the document."""
    wanted = set(fields) if fields is not None else None
    skip = set(exclude)
    declared: Dict[str, forms.Field] = {}
    for fname, fld in model._meta.fields.items():
        if fld.primary_key or not fld.editable or fname in skip or (wanted is not None and fname not in wanted):
            continue
        ff = _form_field(fld)
        if ff is not None:
            declared[fname] = ff

    def __init__(self: forms.Form, *args: Any, instance: Any = None, **kwargs: Any) -> None:
        self.instance = instance
        if instance is not None and "initial" not in kwargs:
            kwargs["initial"] = {k: getattr(instance, k) for k in declared}
        forms.Form.__init__(self, *args, **kwargs)

    def clean(self: forms.Form) -> Dict[str, Any]:
        cleaned = forms.Form.clean(self) or self.cleaned_data
        probe = self.instance if self.instance is not None else model()
        data = {k: v for k, v in self.cleaned_data.items() if v not in (None, "")}
        for k, v in data.items():
            setattr(probe, k, v)
        try:
            probe.full_clean()
        except AggifyValidationError as exc:
            for key, messages in exc.errors.items():
                for message in messages:
                    self.add_error(key if key in self.fields else None, message)
        self._probe = probe
        return cleaned

    def save(self: forms.Form, commit: bool = True) -> Any:
        obj = self._probe
        if commit:
            obj.save()
        return obj

    async def asave(self: forms.Form, commit: bool = True) -> Any:
        obj = self._probe
        if commit:
            await obj.asave()
        return obj

    return type(
        name or f"{model.__name__}Form",
        (forms.Form,),
        {**declared, "__init__": __init__, "clean": clean, "save": save, "asave": asave},
    )
