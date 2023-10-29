from typing import Any, Type

from mongoengine import Document, EmbeddedDocumentField

from aggify.exceptions import InvalidOperator


class Operators:
    QUERY_OPERATORS = {
        "exact": "$eq",
        "iexact": "$eq",
        "contains": "$regex",
        "icontains": "$regex",  # noqa
        "startswith": "$regex",
        "istartswith": "$regex",  # noqa
        "endswith": "$regex",
        "iendswith": "$regex",  # noqa
        "in": "$in",
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

    def __init__(self, match_query: dict[str, Any]):
        self.match_query = match_query

    def compile_match(self, operator: str, value, field: str):
        # TODO: i don't like this, we can refactor it later.
        # I think there should be easier way to inject comparison operators to be defined per each
        # like map an existing template to each operator

        if operator in ["exact", "iexact"]:
            self.match_query[field] = {Operators.ALL_OPERATORS[operator]: value}

        elif operator in [
            "contains",
            "startswith",
            "endswith",
            "icontains",
            "istartswith",
            "iendswith",
        ]:  # noqa
            self.match_query[field] = {
                Operators.ALL_OPERATORS[operator]: f".*{value}.*",
                "$options": "i",
            }

        elif operator in Operators.ALL_OPERATORS[operator]:
            if isinstance(value, F):
                self.match_query["$expr"] = {
                    Operators.ALL_OPERATORS[operator]: [
                        f"${field}",
                        value.to_dict(),
                    ]
                }

            else:
                self.match_query[field] = {Operators.ALL_OPERATORS[operator]: value}
        else:
            self.match_query[field] = {Operators.ALL_OPERATORS[operator]: value}

        return self.match_query


class Q:
    def __init__(self, pipeline: list | None = None, **conditions):
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
        if self.conditions.get("$or", None):
            self.conditions["$or"].append(dict(other)["$match"])
            combined_conditions = self.conditions

        else:
            combined_conditions = {"$or": [self.conditions, dict(other)["$match"]]}
        return Q(**combined_conditions)

    def __and__(self, other):
        if self.conditions.get("$and", None):
            self.conditions["$and"].append(dict(other)["$match"])
            combined_conditions = self.conditions
        else:
            combined_conditions = {"$and": [self.conditions, dict(other)["$match"]]}
        return Q(**combined_conditions)

    def __invert__(self):
        combined_conditions = {"$not": [self.conditions]}
        return Q(**combined_conditions)


class F:
    def __init__(self, field: str | dict[str, list]):
        if isinstance(field, str):
            self.field = f"${field}"
        else:
            self.field = field

    def to_dict(self):
        return self.field

    def __add__(self, other):  # TODO: add type for 'other'
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
    def __init__(self, matches: dict[str, Any], base_model: Type[Document] | None):
        self.matches = matches
        self.base_model = base_model

    @staticmethod
    def validate_operator(key: str):
        if (operator := key.rsplit("__", 1)[1]) not in Operators.COMPARISON_OPERATORS:
            raise InvalidOperator(operator)

    def is_base_model_field(self, field) -> bool:
        return self.base_model is not None and isinstance(
            self.base_model._fields.get(field),  # type: ignore # noqa
            EmbeddedDocumentField,
        )

    def compile(self, pipelines: list) -> dict[str, dict[str, list]]:
        match_query = {}
        for key, value in self.matches.items():
            if "__" not in key:
                match_query[key] = value
                continue

            if isinstance(value, F):
                if F.is_suitable_for_match(key) is False:
                    raise InvalidOperator(key)

            field, operator, *_ = key.split("__")
            if self.is_base_model_field(field):
                # TODO: find a better way instead of recursive function call
                pipelines.append(self.compile([(key.replace("__", ".", 1), value)]))
                continue

            if operator not in Operators.ALL_OPERATORS:
                raise InvalidOperator(operator)

            match_query = Operators(match_query).compile_match(operator, value, field)

        return {"$match": match_query}
