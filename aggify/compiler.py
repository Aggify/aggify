from typing import Any, Type, Union, Dict

from mongoengine import Document, EmbeddedDocumentField
from mongoengine.base import TopLevelDocumentMetaclass

from aggify.exceptions import InvalidOperator
from aggify.utilty import get_db_field, get_nested_field_model


class Operators:
    # noinspection SpellCheckingInspection
    QUERY_OPERATORS = {
        "exact": "$eq",
        "iexact": "$regex",
        "contains": "$regex",
        "icontains": "$regex",
        "startswith": "$regex",
        "istartswith": "$regex",
        "endswith": "$regex",
        "iendswith": "$regex",
        "in": "$in",
        "nin": "$nin",
        "ne": "$ne",
        "not": "$not",
    }

    COMPARISON_OPERATORS = {
        "lt": "$lt",
        "lte": "$lte",
        "gt": "$gt",
        "gte": "$gte",
    }

    ALL_OPERATORS = {
        **QUERY_OPERATORS,
        **COMPARISON_OPERATORS,
    }

    # noinspection SpellCheckingInspection
    REGEX_PATTERNS = {
        "iexact": "^{value}$",
        "contains": "{value}",
        "icontains": "{value}",
        "startswith": "^{value}",
        "istartswith": "^{value}",
        "endswith": "{value}$",
        "iendswith": "{value}$",
    }

    # noinspection SpellCheckingInspection
    REGEX_OPTIONS = {
        "iexact": "i",
        "icontains": "i",
        "istartswith": "i",
        "iendswith": "i",
    }

    def __init__(self, match_query: Dict[str, Any]):
        self.match_query = match_query

    def compile_match(self, operator: str, value, field: str):
        # TODO: i don't like this, we can refactor it later.
        # I think there should be easier way to inject comparison operators to be defined per each
        # like map an existing template to each operator

        if operator in Operators.REGEX_PATTERNS:
            if isinstance(value, F):
                raise ValueError("Not implemented yet")
            pattern = Operators.REGEX_PATTERNS[operator].format(value=value)
            # Create the base query with the pattern
            query = {Operators.ALL_OPERATORS[operator]: pattern}

            # If there's an option for the operator, add it to the query
            if operator in Operators.REGEX_OPTIONS:
                query["$options"] = Operators.REGEX_OPTIONS[operator]

            self.match_query[field] = query
        elif operator in Operators.ALL_OPERATORS:
            if isinstance(value, F):
                self.match_query["$expr"] = {
                    Operators.ALL_OPERATORS[operator]: [f"${field}", value.to_dict()]
                }
            else:
                self.match_query[field] = {Operators.ALL_OPERATORS[operator]: value}

        return self.match_query


class Q:
    def __init__(self, pipeline: Union[list, None] = None, **conditions):
        pipeline = pipeline or []
        self.conditions: dict[str, list] = (
            Match(
                matches=conditions,
                base_model=None,
            )
            .compile(pipeline)
            .get("$match", {})
        )

    def __iter__(self):
        yield "$match", self.conditions

    def __or__(self, other):
        if self.conditions.get("$or"):
            self.conditions["$or"].append(dict(other)["$match"])
            combined_conditions = self.conditions

        else:
            combined_conditions = {"$or": [self.conditions, dict(other)["$match"]]}
        return Q(**combined_conditions)

    def __and__(self, other):
        if self.conditions.get("$and"):
            self.conditions["$and"].append(dict(other)["$match"])
            combined_conditions = self.conditions
        else:
            combined_conditions = {"$and": [self.conditions, dict(other)["$match"]]}
        return Q(**combined_conditions)

    def __invert__(self):
        combined_conditions = {"$not": [self.conditions]}
        return Q(**combined_conditions)


class F:
    def __init__(self, field: Union[str, Dict[str, list]]):
        if isinstance(field, str):
            self.field = f"${field.replace('__', '.')}"
        else:
            self.field = field

    def to_dict(self):
        return self.field

    def __add__(self, other):
        if isinstance(other, F):
            other = other.field

        if isinstance(self.field, dict) and self.field.get("$add") is not None:
            self.field["$add"].append(other)
            combined_field = self.field
        else:
            combined_field = {"$add": [self.field, other]}

        return F(combined_field)

    def __sub__(self, other):
        if isinstance(other, F):
            other = other.field

        if isinstance(self.field, dict) and self.field.get("$subtract") is not None:
            self.field["$subtract"].append(other)
            combined_field = self.field
        else:
            combined_field = {"$subtract": [self.field, other]}
        return F(combined_field)

    def __mul__(self, other):
        if isinstance(other, F):
            other = other.field

        if isinstance(self.field, dict) and self.field.get("$multiply") is not None:
            self.field["$multiply"].append(other)
            combined_field = self.field
        else:
            combined_field = {"$multiply": [self.field, other]}
        return F(combined_field)

    def __truediv__(self, other):
        if isinstance(other, F):
            other = other.field

        if isinstance(self.field, dict) and self.field.get("$divide") is not None:
            self.field["$divide"].append(other)
            combined_field = self.field
        else:
            combined_field = {"$divide": [self.field, other]}
        return F(combined_field)

    @staticmethod
    def is_suitable_for_match(key: str) -> bool:
        if "__" not in key:
            return False
        return True

    def first(self):
        return {"$first": self.field}

    def last(self):
        return {"$last": self.field}

    def min(self):
        return {"$min": self.field}

    def max(self):
        return {"$max": self.field}

    def sum(self):
        return {"$sum": self.field}

    def avg(self):
        return {"$avg": self.field}


class Cond:
    """
    input: Cond(23, '>', 20, 'hi', 'bye')
    return: {'$cond': {'if': {'$gt': [23, 20]}, 'then': 'hi', 'else': 'bye'}}
    """

    OPERATOR_MAPPING = {
        ">": "$gt",
        ">=": "$gte",
        "<": "$lt",
        "<=": "$lte",
        "==": "$eq",
        "!=": "$ne",
    }

    def __init__(self, value1, condition, value2, then_value, else_value):
        self.value1 = value1
        self.value2 = value2
        self.condition = self._map_condition(condition)
        self.then_value = then_value
        self.else_value = else_value

    def _map_condition(self, condition):
        if condition in self.OPERATOR_MAPPING:
            return self.OPERATOR_MAPPING[condition]
        raise InvalidOperator(condition)

    def __iter__(self):
        """Iterator used by `dict` to create a dictionary from a `Cond` object

        With this method we are now able to do this:
        c = Cond(...)
        dict_of_c = dict(c)

        instead of c.to_dict()

        Returns:
            A tuple of '$cond' and its value
        """
        yield (
            "$cond",
            {
                "if": {self.condition: [self.value1, self.value2]},
                "then": self.then_value,
                "else": self.else_value,
            },
        )


class Match:
    def __init__(
        self, matches: Dict[str, Any], base_model: Union[Type[Document], None]
    ):
        self.matches = matches
        self.base_model = base_model

    @staticmethod
    def validate_operator(key: str):
        _op = key.rsplit("__", 1)
        try:
            operator = _op[1]
        except IndexError:
            raise InvalidOperator(str(_op)) from None

        if operator not in Operators.COMPARISON_OPERATORS:
            raise InvalidOperator(operator)

    def is_base_model_field(self, field) -> bool:
        """
        Check if a field in the base model class is of a specific type.
        EmbeddedDocumentField: Field which is embedded.
        TopLevelDocumentMetaclass: Field which is added by lookup stage.

        Args:
            field (str): The name of the field to check.

        Returns:
            bool: True if the field is of type EmbeddedDocumentField or TopLevelDocumentMetaclass
                  and the base_model is not None, otherwise False.
        """
        return self.base_model is not None and (
            isinstance(
                self.base_model._fields.get(field),  # noqa
                (EmbeddedDocumentField, TopLevelDocumentMetaclass),
            )
        )

    def compile(self, pipelines: list) -> Dict[str, Dict[str, list]]:
        match_query = {}
        for key, value in self.matches.items():
            if isinstance(value, F):
                if F.is_suitable_for_match(key) is False:
                    raise InvalidOperator(key)

            if "__" not in key:
                key = get_db_field(self.base_model, key)
                match_query[key] = value
                continue

            field, operator, *others = key.split("__")
            if (
                self.is_base_model_field(field)
                and operator not in Operators.ALL_OPERATORS
            ):
                field_db_name = get_db_field(self.base_model, field)

                nested_field_name = get_db_field(
                    get_nested_field_model(self.base_model, field), operator
                )
                key = (
                    f"{field_db_name}.{nested_field_name}__" + "__".join(others)
                ).rstrip("__")
                pipelines.append(Match({key: value}, self.base_model).compile([]))
                continue

            if operator not in Operators.ALL_OPERATORS:
                raise InvalidOperator(operator)
            db_field = get_db_field(self.base_model, field)
            match_query = Operators(match_query).compile_match(
                operator, value, db_field
            )

        return {"$match": match_query}
