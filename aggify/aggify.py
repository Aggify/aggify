import functools
from typing import Any, Dict, Type

from mongoengine import Document, EmbeddedDocument, fields
from mongoengine.base import TopLevelDocumentMetaclass

from aggify.compiler import F, Match, Q, Operators  # noqa keep
from aggify.exceptions import (
    AggifyValueError,
    AnnotationError,
    InvalidField,
    InvalidEmbeddedField,
    OutStageError,
    InvalidArgument,
)
from aggify.types import QueryParams
from aggify.utilty import (
    to_mongo_positive_index,
    check_fields_exist,
    replace_values_recursive,
    convert_match_query,
    check_field_exists,
    get_db_field,
)


def last_out_stage_check(method):
    """Check if the last stage is $out or not

    This decorator check if the last stage is $out or not
    MongoDB does not allow adding aggregation pipeline stage after $out stage
    """

    @functools.wraps(method)
    def decorator(*args, **kwargs):
        try:
            if last_is_out := bool(args[0].pipelines[-1].get("$out")):
                raise OutStageError(method.__name__)
        except IndexError:
            return method(*args, **kwargs)
        else:
            return method(*args, **kwargs)

    return decorator


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

    @last_out_stage_check
    def project(self, **kwargs: QueryParams) -> "Aggify":
        self.pipelines.append({"$project": kwargs})
        return self

    @last_out_stage_check
    def group(self, expression: str | None = "_id") -> "Aggify":
        expression = f"${expression}" if expression else None
        self.pipelines.append({"$group": {"_id": expression}})
        return self

    @last_out_stage_check
    def order_by(self, field: str) -> "Aggify":
        self.pipelines.append(
            {"$sort": {f'{field.replace("-", "")}': -1 if field.startswith("-") else 1}}
        )
        return self

    @last_out_stage_check
    def raw(self, raw_query: dict) -> "Aggify":
        self.pipelines.append(raw_query)
        return self

    @last_out_stage_check
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

    @last_out_stage_check
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

    def out(self, coll: str, db: str | None = None) -> "Aggify":
        """Write the documents returned by the aggregation pipeline into specified collection.

        Starting in MongoDB 4.4, you can specify the output database.
        The $out stage must be the last stage in the pipeline.
        The $out operator lets the aggregation framework return result sets of any size.


        Arguments:
            coll: The output collection name.
            db: The output database name.
              For a replica set or a standalone, if the output database does not exist,
              $out also creates the database.

              For a sharded cluster, the specified output database must already exist.

        if db is None:
          { $out: "<output-collection>" } // Output collection is in the same database

        Notes:
            $out replaces the specified collection if it exists.

            You cannot specify a sharded collection as the output collection.
            The input collection for a pipeline can be sharded.
            To output to a sharded collection, see $merge.

            The $out operator cannot write results to a capped collection.

            If you modify a collection with an Atlas Search index,
            you must first delete and then re-create the search index.
            Consider using $merge instead.

        from: https://www.mongodb.com/docs/manual/reference/operator/aggregation/out/
        """
        if db is None:
            stage = {"$out": coll}
        else:
            stage = {"$out": {"db": db, "coll": coll}}
        self.pipelines.append(stage)
        return self

    def __to_aggregate(self, query: dict[str, Any]) -> None:
        """
        Builds the pipelines list based on the query parameters.
        """
        skip_list = []

        for key, value in query.items():
            if key in skip_list:
                continue

            # Split the key to access the field information.
            split_query = key.split("__")

            # Retrieve the field definition from the model.
            join_field = self.get_model_field(self.base_model, split_query[0])  # type: ignore
            # Check conditions for creating a 'match' pipeline stage.
            if (
                isinstance(
                    join_field, TopLevelDocumentMetaclass
                )  # check whether field is added by lookup stage or not
                or "document_type_obj"
                not in join_field.__dict__  # Check whether this field is a join field or not.
                or issubclass(
                    join_field.document_type, EmbeddedDocument
                )  # Check whether this field is embedded field or not
                or len(split_query) == 1
                or (len(split_query) == 2 and split_query[1] in Operators.ALL_OPERATORS)
            ):
                # Create a 'match' pipeline stage.
                match = self.__match({key: value})

                # Check if the 'match' stage is not empty before adding it to the pipelines.
                if match.get("$match"):
                    self.pipelines.append(match)

            else:
                from_collection = join_field.document_type
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

                self.pipelines.append(
                    self.__lookup(
                        from_collection=from_collection._meta["collection"],  # noqa
                        local_field=local_field,
                        as_name=as_name,
                    )
                )
                self.unwind(as_name, preserve=True)
                self.pipelines.extend([{"$match": match} for match in matches])

    @last_out_stage_check
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

    @last_out_stage_check
    def unwind(
        self, path: str, include_index_array: str | None = None, preserve: bool = False
    ) -> "Aggify":
        """Generates a MongoDB unwind pipeline stage.

        Args:
            path: Field path to an array field.
              To specify a field path, prefix the field name with a dollar sign $
              and enclose in quotes.

            include_index_array: The name of a new field to hold the array index of the element.
              The name cannot start with a dollar sign $.

            preserve: Whether to preserve null and empty arrays.
                  If true, if the path is null, missing, or an empty array,
                  $unwind outputs the document.

                  If false, if path is null, missing, or an empty array,
                  $unwind does not output a document.

        Stages:
            { $unwind: <field path> }

            {
              $unwind:
                {
                  path: <field path>,
                  includeArrayIndex: <string>,
                  preserveNullAndEmptyArrays: <boolean>
                }
            }

        docs: https://www.mongodb.com/docs/manual/reference/operator/aggregation/unwind/
        """

        if include_index_array is None and preserve is False:
            self.pipelines.append({"$unwind": f"${path}"})
            return self
        self.pipelines.append(
            {
                "$unwind": {
                    "path": f"${path}",
                    "includeArrayIndex": include_index_array,
                    "preserveNullAndEmptyArrays": preserve,
                }
            }
        )
        return self

    def aggregate(self):
        """
        Returns the aggregated results.

        Returns:
            The aggregated results.
        """
        return self.base_model.objects.aggregate(*self.pipelines)  # type: ignore

    def annotate(
        self, annotate_name: str, accumulator: str, f: str | dict | F | int
    ) -> "Aggify":
        """
        Annotate a MongoDB aggregation pipeline with a new field.
        Usage: https://www.mongodb.com/docs/manual/reference/operator/aggregation/group/#accumulator-operator

        Args:
            annotate_name (str): The name of the new annotated field.
            accumulator (str): The aggregation accumulator operator (e.g., "$sum", "$avg").
            f (str | dict | F | int): The value for the annotated field.

        Returns:
            self.

        Raises:
            AnnotationError: If the pipeline is empty or if an invalid accumulator is provided.

        Example:
            annotate("totalSales", "sum", "sales")
        """

        # Some of the accumulator fields might be false and should be checked.
        aggregation_mapping: Dict[str, Type] = {
            "sum": (fields.FloatField(), "$sum"),
            "avg": (fields.FloatField(), "$avg"),
            "stdDevPop": (fields.FloatField(), "$stdDevPop"),
            "stdDevSamp": (fields.FloatField(), "$stdDevSamp"),
            "push": (fields.ListField(), "$push"),
            "addToSet": (fields.ListField(), "$addToSet"),
            "count": (fields.IntField(), "$count"),
            "first": (fields.EmbeddedDocumentField(fields.EmbeddedDocument), "$first"),
            "last": (fields.EmbeddedDocumentField(fields.EmbeddedDocument), "$last"),
            "max": (fields.DynamicField(), "$max"),
            "accumulator": (fields.DynamicField(), "$accumulator"),
            "min": (fields.DynamicField(), "$min"),
            "median": (fields.DynamicField(), "$median"),
            "mergeObjects": (fields.DictField(), "$mergeObjects"),
            "top": (fields.EmbeddedDocumentField(fields.EmbeddedDocument), "$top"),
            "bottom": (
                fields.EmbeddedDocumentField(fields.EmbeddedDocument),
                "$bottom",
            ),
            "topN": (fields.ListField(), "$topN"),
            "bottomN": (fields.ListField(), "$bottomN"),
            "firstN": (fields.ListField(), "$firstN"),
            "lastN": (fields.ListField(), "$lastN"),
            "maxN": (fields.ListField(), "$maxN"),
        }

        try:
            stage = list(self.pipelines[-1].keys())[0]
            if stage != "$group":
                raise AnnotationError(
                    f"Annotations apply only to $group, not to {stage}"
                )
        except IndexError:
            raise AnnotationError(
                "Annotations apply only to $group, your pipeline is empty"
            )

        try:
            field_type, acc = aggregation_mapping[accumulator]
        except KeyError as error:
            raise AnnotationError(f"Invalid accumulator: {accumulator}") from error

        if isinstance(f, F):
            value = f.to_dict()
        else:
            if isinstance(f, str):
                try:
                    self.get_model_field(self.base_model, f)
                    value = f"${f}"
                except InvalidField:
                    value = f
            else:
                value = f

        # Determine the data type based on the aggregation operator
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

    @last_out_stage_check
    def lookup(
        self,
        from_collection: Document,
        as_name: str,
        query: list[Q] | Q | None = None,
        let: list[str] | None = None,
        local_field: str | None = None,
        foreign_field: str | None = None,
    ) -> "Aggify":
        """
        Generates a MongoDB lookup pipeline stage.

        Args:
            from_collection (Document): The document representing the collection to perform the lookup on.
            as_name (str): The name of the new field to create.
            query (list[Q] | Q | None, optional): List of desired queries with Q function or a single query.
            let (list[str] | None, optional): The local field(s) to join on. If provided, localField and foreignField are not used.
            local_field (str | None, optional): The local field to join on when let is not provided.
            foreign_field (str | None, optional): The foreign field to join on when let is not provided.

        Returns:
            Aggify: An instance of the Aggify class representing a MongoDB lookup pipeline stage.
        """

        lookup_stages = []
        check_field_exists(self.base_model, as_name)
        from_collection_name = from_collection._meta.get("collection")  # noqa

        if not let and not (local_field and foreign_field):
            raise InvalidArgument(
                expected_list=[["local_field", "foreign_field"], "let"]
            )
        elif not let:
            if not (local_field and foreign_field):
                raise InvalidArgument(expected_list=["local_field", "foreign_field"])
            lookup_stage = {
                "$lookup": {
                    "from": from_collection_name,
                    "localField": get_db_field(self.base_model, local_field),  # noqa
                    "foreignField": get_db_field(
                        from_collection, foreign_field
                    ),  # noqa
                    "as": as_name,
                }
            }
        else:
            if not query:
                raise InvalidArgument(expected_list=["query"])
            check_fields_exist(self.base_model, let)  # noqa
            let_dict = {
                field: f"${get_db_field(self.base_model, field)}"
                for field in let  # noqa
            }
            for q in query:
                # Construct the match stage for each query
                if isinstance(q, Q):
                    replaced_values = replace_values_recursive(
                        convert_match_query(dict(q)),
                        {field: f"$${field}" for field in let},
                    )
                    match_stage = {"$match": {"$expr": replaced_values.get("$match")}}
                    lookup_stages.append(match_stage)
                elif isinstance(q, Aggify):
                    lookup_stages.extend(
                        replace_values_recursive(
                            convert_match_query(q.pipelines),  # noqa
                            {field: f"$${field}" for field in let},
                        )
                    )

            # Append the lookup stage with multiple match stages to the pipeline
            lookup_stage = {
                "$lookup": {
                    "from": from_collection_name,
                    "let": let_dict,
                    "pipeline": lookup_stages,  # List of match stages
                    "as": as_name,
                }
            }

        self.pipelines.append(lookup_stage)

        # Add this new field to base model fields, which we can use it in the next stages.
        self.base_model._fields[as_name] = from_collection  # noqa

        return self

    @staticmethod
    def get_model_field(model: Type[Document], field: str) -> fields:
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
        model_field = self.get_model_field(self.base_model, embedded_field)

        if not hasattr(model_field, "document_type") or not issubclass(
            model_field.document_type, EmbeddedDocument
        ):
            raise InvalidEmbeddedField(field=embedded_field)

        return f"${model_field.db_field}"

    @last_out_stage_check
    def replace_root(
        self, *, embedded_field: str, merge: dict | None = None
    ) -> "Aggify":
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
            new_root = {"$replaceRoot": {"$newRoot": name}}
        else:
            new_root = {"$replaceRoot": {"newRoot": {"$mergeObjects": [merge, name]}}}
        self.pipelines.append(new_root)

        return self

    @last_out_stage_check
    def replace_with(
        self, *, embedded_field: str, merge: dict | None = None
    ) -> "Aggify":
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
            new_root = {"$replaceWith": name}
        else:
            new_root = {"$replaceWith": {"$mergeObjects": [merge, name]}}
        self.pipelines.append(new_root)

        return self
