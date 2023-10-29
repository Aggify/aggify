from typing import Any, Literal, Type

from mongoengine import Document, EmbeddedDocument, fields

from aggify.compiler import F, Match, Q  # noqa keep
from aggify.exceptions import AggifyValueError, AnnotationError, InvalidField, InvalidEmbeddedField
from aggify.types import QueryParams
from aggify.utilty import to_mongo_positive_index, check_fields_exist, replace_values_recursive, convert_match_query


class Aggify:
    def __init__(self, base_model: Type[Document]):
        """
        Initializes the Aggify class.

        Args:
            base_model: The base model class.
        """
        self.base_model = base_model
        self.pipelines: list[dict[str, dict | Any]] = []
        self.start = None
        self.stop = None
        self.q = None

    @staticmethod
    def unwind(
            path: str, preserve: bool = True
    ) -> dict[
        Literal["$unwind"],
        dict[Literal["path", "preserveNullAndEmptyArrays"], str | bool],
    ]:
        """
        Generates a MongoDB unwind pipeline stage.

        Args:
            path: The path to unwind.
            preserve: Whether to preserve null and empty arrays.

        Returns:
            A MongoDB unwind pipeline stage.
        """
        return {"$unwind": {"path": f"${path}", "preserveNullAndEmptyArrays": preserve}}

    def project(self, **kwargs: QueryParams) -> "Aggify":
        self.pipelines.append({"$project": kwargs})
        return self

    def group(self, key: str = "_id") -> "Aggify":
        self.pipelines.append({"$group": {"_id": f"${key}"}})
        return self

    def order_by(self, field: str) -> "Aggify":
        self.pipelines.append(
            {"$sort": {f'{field.replace("-", "")}': -1 if field.startswith("-") else 1}}
        )
        return self

    def raw(self, raw_query: dict) -> "Aggify":
        self.pipelines.append(raw_query)
        return self

    def add_fields(self, fields: dict) -> "Aggify":  # noqa
        """
        Generates a MongoDB addFields pipeline stage.

        Args:
            fields: A dictionary of field expressions and values.

        Returns:
            A MongoDB add_fields pipeline stage.
        """
        add_fields_stage = {"$addFields": {}}

        for field, expression in fields.items():
            if isinstance(expression, str):
                add_fields_stage["$addFields"][field] = {"$literal": expression}
            elif isinstance(expression, F):
                add_fields_stage["$addFields"][field] = expression.to_dict()
            else:
                raise AggifyValueError([str, F], type(expression))

        self.pipelines.append(add_fields_stage)
        return self

    def aggregate(self):
        """
        Returns the aggregated results.

        Returns:
            The aggregated results.
        """
        return self.base_model.objects.aggregate(*self.pipelines)  # type: ignore

    def filter(self, arg: Q | None = None, **kwargs: QueryParams) -> "Aggify":
        """
        # TODO: missing docs
        """
        if arg is not None and isinstance(arg, Q) is not True:
            raise AggifyValueError([Q, None], type(arg))

        if arg is None:
            self.q = kwargs
            self.__to_aggregate(self.q)
            self.pipelines = self.__combine_sequential_matches()

        if isinstance(arg, Q):
            self.pipelines.append(dict(arg))

        return self

    def annotate(self, annotate_name, accumulator, f):
        try:
            if (stage := list(self.pipelines[-1].keys())[0]) != "$group":
                raise AnnotationError(
                    f"Annotations apply only to $group, not to {stage}."
                )

        except IndexError as error:
            raise AnnotationError(
                "Annotations apply only to $group, you're pipeline is empty."
            ) from error

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
            "stdDevSamp": "$stdDevSamp",
            "merge": "$mergeObjects",
        }

        # Determine the data type based on the accumulator
        if accumulator in ["sum", "avg", "stdDevPop", "stdDevSamp"]:
            field_type = fields.FloatField()
        elif accumulator in ["push", "addToSet"]:
            field_type = fields.ListField()
        else:
            field_type = fields.StringField()

        try:
            acc = accumulator_dict[accumulator]
        except KeyError as error:
            raise AnnotationError(f"Invalid accumulator: {accumulator}") from error

        if isinstance(f, F):
            value = f.to_dict()
        else:
            value = f"${f}"
        self.pipelines[-1]["$group"] |= {annotate_name: {acc: value}}
        self.base_model._fields[annotate_name] = field_type  # noqa
        return self

    def __match(self, matches: dict[str, Any]):
        """
        Generates a MongoDB match pipeline stage.

        Args:
            matches: The match criteria.

        Returns:
            A MongoDB match pipeline stage.
        """
        return Match(matches, self.base_model).compile(self.pipelines)

    @staticmethod
    def __lookup(
            from_collection: str, local_field: str, as_name: str, foreign_field: str = "_id"
    ) -> dict[str, dict[str, str]]:
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

    def __to_aggregate(self, query: dict[str, Any]) -> None:
        """
        Builds the pipelines list based on the query parameters.
        """
        skip_list = []

        for key, value in query.items():
            if key in skip_list:
                continue

            split_query = key.split("__")
            join_field = self.get_model_field(self.base_model, split_query[0])  # type: ignore # noqa
            if not join_field:
                raise ValueError(f"Invalid field: {split_query[0]}")
            # This is a nested query.
            if "document_type_obj" not in join_field.__dict__ or issubclass(
                    join_field.document_type, EmbeddedDocument
            ):
                match = self.__match({key: value})
                if (match.get("$match")) != {}:
                    self.pipelines.append(match)
            else:
                from_collection = join_field.document_type._meta["collection"]  # noqa
                local_field = join_field.db_field
                as_name = join_field.name
                matches = []
                for k, v in query.items():
                    if k.split("__")[0] == split_query[0]:
                        skip_list.append(k)
                        _key = k.replace("__", ".", 1)
                        match = self.__match({_key: v}).get("$match")
                        if match != {}:
                            matches.append(match)

                self.pipelines.extend(
                    [
                        self.__lookup(
                            from_collection=from_collection,
                            local_field=local_field,
                            as_name=as_name,
                        ),
                        self.unwind(as_name),  # type: ignore
                        *[{"$match": match} for match in matches],
                    ]
                )

    def __combine_sequential_matches(self) -> list[dict[str, dict | Any]]:
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

    def __getitem__(self, index: slice | int) -> "Aggify":
        """
        # TODO: missing docs
        """
        if isinstance(index, (int, slice)) is False:
            raise AggifyValueError([int, slice], type(index))

        index = to_mongo_positive_index(index)
        self.pipelines.append({"$skip": int(index.start)})
        self.pipelines.append({"$limit": int(index.stop - index.start)})
        return self

    def lookup(self, from_collection: Document, let: list[str], query: list[Q], as_name: str) -> "Aggify":
        """
        Generates a MongoDB lookup pipeline stage.

        Args:
            from_collection (Document): The name of the collection to lookup.
            let (list): The local field(s) to join on.
            query (list[Q]): List of desired queries with Q function.
            as_name (str): The name of the new field to create.

        Returns:
            Aggify: A MongoDB lookup pipeline stage.
        """
        check_fields_exist(self.base_model, let)  # noqa

        let_dict = {field: f"${self.base_model._fields[field].db_field}" for field in let}  # noqa
        from_collection = from_collection._meta.get('collection')  # noqa

        lookup_stages = []

        for q in query:
            # Construct the match stage for each query
            if isinstance(q, Q):
                replaced_values = replace_values_recursive(convert_match_query(dict(q)),  # noqa
                                                           {field: f'$${field}' for field in let})
                match_stage = {
                    "$match": {
                        "$expr": replaced_values.get('$match')
                    }
                }
                lookup_stages.append(match_stage)
            elif isinstance(q, Aggify):
                lookup_stages.extend(replace_values_recursive(convert_match_query(q.pipelines),  # noqa
                                                              {field: f'$${field}' for field in let}))

        # Append the lookup stage with multiple match stages to the pipeline
        lookup_stage = {
            "$lookup": {
                "from": from_collection,
                "let": let_dict,
                "pipeline": lookup_stages,  # List of match stages
                "as": as_name
            }
        }

        self.pipelines.append(lookup_stage)

        # Add this new field to base model fields, which we can use it in the next stages.
        self.base_model._fields[as_name] = fields.StringField()  # noqa

        return self

    @staticmethod
    def get_model_field(model: Document, field: str) -> fields:
        """
        Get the field definition of a specified field in a MongoDB model.

        Args:
            model (Document): The MongoDB model.
            field (str): The name of the field to retrieve.

        Returns:
            fields.BaseField: The field definition.

        Raises:
            InvalidField: If the specified field does not exist in the model.
        """
        model_field = model._fields.get(field, None)  # noqa
        if not model_field:
            raise InvalidField(field=field)
        return model_field

    def _replace_base(self, embedded_field) -> str:
        """
           Replace the root document with a specified embedded field.

           Args:
               embedded_field (str): The name of the embedded field to use as the new root.

           Returns:
               str: The MongoDB aggregation expression for replacing the root.

           Raises:
               InvalidEmbeddedField: If the specified embedded field is not found or is not of the correct type.
        """
        model_field = self.get_model_field(self.base_model, embedded_field)  # noqa

        if not hasattr(model_field, 'document_type') or not issubclass(model_field.document_type, EmbeddedDocument):
            raise InvalidEmbeddedField(field=embedded_field)

        return f"${model_field.db_field}"

    def replace_root(self, *, embedded_field: str, merge: dict | None = None) -> "Aggify":
        """
        Replace the root document in the aggregation pipeline with a specified embedded field or a merged result.

        Args:
            embedded_field (str): The name of the embedded field to use as the new root.
            merge (dict | None, optional): A dictionary for merging with the new root. Default is None.

        Returns:
            Aggify: The modified Aggify instance.

        Usage:
            Aggify().replace_root(embedded_field="myEmbeddedField")
            Aggify().replace_root(embedded_field="myEmbeddedField", merge={"child_field": default_value_if_not_exists})
        """
        name = self._replace_base(embedded_field)

        if not merge:
            new_root = {
                "$replaceRoot": {
                    "$newRoot": name
                }
            }
        else:
            new_root = {
                '$replaceRoot': {
                    'newRoot': {
                        '$mergeObjects': [
                            merge, name
                        ]
                    }
                }
            }
        self.pipelines.append(new_root)

        return self

    def replace_with(self, *, embedded_field: str, merge: dict | None = None) -> "Aggify":
        """
        Replace the root document in the aggregation pipeline with a specified embedded field or a merged result.

        Args:
            embedded_field (str): The name of the embedded field to use as the new root.
            merge (dict | None, optional): A dictionary for merging with the new root. Default is None.

        Returns:
            Aggify: The modified Aggify instance.

        Usage:
            Aggify().replace_root(embedded_field="myEmbeddedField")
            Aggify().replace_root(embedded_field="myEmbeddedField", merge={"child_field": default_value_if_not_exists})
        """
        name = self._replace_base(embedded_field)

        if not merge:
            new_root = {
                "$replaceWith": name
            }
        else:
            new_root = {
                '$replaceWith': {
                    '$mergeObjects': [
                        merge, name
                    ]
                }
            }
        self.pipelines.append(new_root)

        return self
