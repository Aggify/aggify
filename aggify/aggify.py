from typing import Type

from mongoengine import Document, EmbeddedDocument, EmbeddedDocumentField

from aggify.exceptions import AggifyValueError
from aggify.match import F, Q  # noqa keep
from aggify.types import QueryParams
from aggify.utilty import to_mongo_positive_index


class Aggify:
    def __init__(self, base_model: Type[Document]):
        """
        Initializes the Aggify class.

        Args:
            base_model: The base model class.
        """
        self.base_model = base_model
        self.pipelines = []
        self.start = None
        self.stop = None
        self.q = None

    def __getitem__(self, index: slice | int) -> "Aggify":
        """
        # TODO: missing docs
        """
        if isinstance(index, (int, slice)) is False:
            raise AggifyValueError([int, slice], type(index))

        index = to_mongo_positive_index(index)
        self.pipelines.append({"$skip": index.start})
        self.pipelines.append({"$limit": int(index.stop - index.start)})
        return self

    def filter(self, arg: Q | None = None, **kwargs: QueryParams) -> "Aggify":
        """
        # TODO: missing docs
        """
        if arg is not None and isinstance(arg, Q) is not True:
            raise AggifyValueError([Q, None], type(arg))

        if arg is None:
            self.q = kwargs
            self.to_aggregate()
            self.pipelines = self.combine_sequential_matches()

        if isinstance(arg, Q):
            self.pipelines.append(dict(arg))

        return self

    def match(self, matches):
        """
        Generates a MongoDB match pipeline stage.

        Args:
            matches: The match criteria.

        Returns:
            A MongoDB match pipeline stage.
        """
        mongo_operators = {
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

        mongo_comparison_operators = {
            "lt": "$lt",
            "lte": "$lte",
            "gt": "$gt",
            "gte": "$gte",
        }

        mongo_operators |= mongo_comparison_operators

        match_query = {}
        for match in matches:
            key, value = match
            if isinstance(value, F):
                if "__" not in key:
                    raise ValueError(
                        "You should use comparison operators with F function"
                    )
                if (
                    operator := key.rsplit("__", 1)[1]
                ) not in mongo_comparison_operators:
                    raise ValueError(f"Invalid operator: {operator}")
            if "__" not in key:
                match_query[key] = value
                continue
            field, operator, *_ = key.split("__")
            if self.base_model and isinstance(
                self.base_model._fields.get(field),  # type: ignore
                EmbeddedDocumentField,
            ):  # noqa
                self.pipelines.append(self.match([(key.replace("__", ".", 1), value)]))
                continue
            if operator not in mongo_operators:
                raise ValueError(f"Unsupported operator: {operator}")

            if operator in ["exact", "iexact"]:
                match_query[field] = {mongo_operators[operator]: value}
            elif operator in [
                "contains",
                "startswith",
                "endswith",
                "icontains",
                "istartswith",
                "iendswith",
            ]:  # noqa
                match_query[field] = {
                    mongo_operators[operator]: f".*{value}.*",
                    "$options": "i",
                }
            elif operator in mongo_comparison_operators:
                if isinstance(value, F):
                    match_query["$expr"] = {
                        mongo_operators[operator]: [f"${field}", value.to_dict()]
                    }
                else:
                    match_query[field] = {mongo_operators[operator]: value}
            else:
                match_query[field] = {mongo_operators[operator]: value}

        return {"$match": match_query}

    @staticmethod
    def lookup(from_collection, local_field, as_name, foreign_field="_id"):
        """
        Generates a MongoDB lookup pipeline stage.

        Args:
            from_collection: The name of the collection to lookup.
            local_field: The local field to join on.
            as_name: The name of the new field to create.
            foreign_field: The foreign field to join on.

        Returns:
            A MongoDB lookup pipeline stage.
        """
        return {
            "$lookup": {
                "from": from_collection,
                "localField": local_field,
                "foreignField": foreign_field,
                "as": as_name,
            }
        }

    @staticmethod
    def unwind(path, preserve=True):
        """
        Generates a MongoDB unwind pipeline stage.

        Args:
            path: The path to unwind.
            preserve: Whether to preserve null and empty arrays.

        Returns:
            A MongoDB unwind pipeline stage.
        """
        return {"$unwind": {"path": f"${path}", "preserveNullAndEmptyArrays": preserve}}

    def to_aggregate(self):
        """Builds the pipelines list based on the query parameters."""
        skip_list = []
        for key, value in self.q.items():  # type: ignore
            if key in skip_list:
                continue
            split_query = key.split("__")
            join_field = self.base_model._fields.get(split_query[0])  # type: ignore
            if not join_field:
                raise ValueError(f"Invalid field: {split_query[0]}")
            # This is a nested query.
            if "document_type_obj" not in join_field.__dict__ or issubclass(
                join_field.document_type, EmbeddedDocument
            ):
                match = self.match([(key, value)])
                if (match.get("$match")) != {}:
                    self.pipelines.append(match)
            else:
                from_collection = join_field.document_type._meta["collection"]  # noqa
                local_field = join_field.db_field
                as_name = join_field.name
                matches = []
                for k, v in self.q.items():  # type: ignore
                    if k.split("__")[0] == split_query[0]:
                        skip_list.append(k)
                        if (
                            match := self.match([(k.replace("__", ".", 1), v)]).get(
                                "$match"
                            )
                        ) != {}:
                            matches.append(match)
                self.pipelines.extend(
                    [
                        self.lookup(
                            from_collection=from_collection,
                            local_field=local_field,
                            as_name=as_name,
                        ),
                        self.unwind(as_name),
                        *[{"$match": match} for match in matches],
                    ]
                )

    def combine_sequential_matches(self):
        merged_pipeline = []
        match_stage = None

        for stage in self.pipelines:
            if stage.get("$match"):
                if match_stage is None:
                    match_stage = stage["$match"]
                else:
                    match_stage.update(stage["$match"])
            else:
                if match_stage:
                    merged_pipeline.append({"$match": match_stage})
                    match_stage = None
                merged_pipeline.append(stage)

        if match_stage:
            merged_pipeline.append({"$match": match_stage})

        return merged_pipeline

    def project(self, **kwargs):
        projects = {}
        for k, v in kwargs.items():
            projects[k] = v
        self.pipelines.append({"$project": projects})
        return self

    def group(self, key="_id"):
        self.pipelines.append({"$group": {"_id": f"${key}"}})
        return self

    def annotate(self, annotate_name, accumulator, f):
        try:
            if (stage := list(self.pipelines[-1].keys())[0]) != "$group":
                raise ValueError(f"Annotations apply only to $group, not to {stage}.")
        except IndexError:
            raise ValueError(f"Annotations apply only to $group, you're pipeline is empty.")

        accumulator_dict = {
            "sum": "$sum",
            "avg": "$avg",
            "first": "$first",
            "last": "$last",
            "max": "$max",
            "min": "$min",
            "push": "$push",
            "addToSet": "$addToSet",
            "stdDevPop": "$stdDevPop",
            "stdDevSamp": "$stdDevSamp",  # noqa
        }

        try:
            acc = accumulator_dict[accumulator]
        except KeyError:
            raise ValueError(f"Invalid accumulator: {accumulator}") from None

        if isinstance(f, F):
            value = f.to_dict()
        else:
            value = f"${f}"
        self.pipelines[-1]["$group"] |= {annotate_name: {acc: value}}
        return self

    def order_by(self, field):
        self.pipelines.append(
            {"$sort": {f'{field.replace("-", "")}': -1 if field.startswith("-") else 1}}
        )
        return self

    def raw(self, raw_query):
        self.pipelines.append(raw_query)
        return self

    def addFields(self, fields):  # noqa
        """
        Generates a MongoDB addFields pipeline stage.

        Args:
            fields: A dictionary of field expressions and values.

        Returns:
            A MongoDB addFields pipeline stage.
        """
        add_fields_stage = {"$addFields": {}}

        for field, expression in fields.items():
            if isinstance(expression, str):
                add_fields_stage["$addFields"][field] = {"$literal": expression}
            elif isinstance(expression, F):
                add_fields_stage["$addFields"][field] = expression.to_dict()
            else:
                raise ValueError("Invalid field expression")

        self.pipelines.append(add_fields_stage)
        return self

    def aggregate(self):
        """
        Returns the aggregated results.

        Returns:
            The aggregated results.
        """
        return self.base_model.objects.aggregate(*self.pipelines)  # type: ignore
