from typing import Any, Literal, Type

from mongoengine import Document, EmbeddedDocument

from aggify.compiler import F, Match, Q  # noqa keep
from aggify.exceptions import AggifyValueError
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

    def project(self, **kwargs: QueryParams | dict) -> "Aggify":
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

    def annotate(self, annotate_name, accumulator, f) -> "Aggify":
        raise NotImplementedError(
            "annotate is not implemented in this version of Aggify"
        )

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
            join_field = self.base_model._fields.get(split_query[0])  # type: ignore
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
