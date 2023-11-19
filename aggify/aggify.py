import functools
from typing import Any, Dict, Type, Union, List, TypeVar, Callable, Tuple

from mongoengine import Document, EmbeddedDocument, fields as mongoengine_fields
from mongoengine.base import TopLevelDocumentMetaclass

from aggify.compiler import F, Match, Q, Operators, Cond  # noqa keep
from aggify.exceptions import (
    AggifyValueError,
    AnnotationError,
    InvalidField,
    InvalidEmbeddedField,
    OutStageError,
    InvalidArgument,
    InvalidProjection,
    InvalidAnnotateExpression,
)
from aggify.types import QueryParams, CollectionType
from aggify.utilty import (
    to_mongo_positive_index,
    validate_field_existence,
    replace_values_recursive,
    convert_match_query,
    check_field_already_exists,
    get_db_field,
    copy_class,
)

AggifyType = TypeVar("AggifyType", bound=Callable[..., "Aggify"])


def last_out_stage_check(method: AggifyType) -> AggifyType:
    """Check if the last stage is $out or not

    This decorator check if the last stage is $out or not
    MongoDB does not allow adding aggregation pipeline stage after $out stage
    """

    @functools.wraps(method)
    def decorator(*args, **kwargs):
        try:
            if bool(args[0].pipelines[-1].get("$out")):
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
        # Create a separate copy of the main class for safety and flexibility
        self.base_model = copy_class(base_model)
        self.pipelines: List[Dict[str, Union[dict, Any]]] = []
        self.start = None
        self.stop = None
        self.q = None

    def __iter__(self):
        # Return a generator or iterator for the data you want to represent as a list
        return iter(self.pipelines)

    @last_out_stage_check
    def project(self, **kwargs: QueryParams) -> "Aggify":
        """
        Adjusts the base model's fields based on the given keyword arguments.

        Fields to be retained are set to 1 in kwargs.
        Fields to be deleted are set to 0 in kwargs, except for _id which is controlled by the delete_id flag.

        Args:
            **kwargs: Fields to be retained or removed.
                      For example: {"field1": 1, "field2": 0}
                      _id field behavior: {"id": 0} means delete _id.

        Returns:
            Aggify: Returns an instance of the Aggify class for potential method chaining.
        """
        filtered_kwargs = dict(kwargs)
        filtered_kwargs.pop("id", None)
        if all([i in filtered_kwargs.values() for i in [0, 1]]):
            raise InvalidProjection()

        # Extract fields to keep and check if _id should be deleted
        to_keep_values = {"id"}
        projection = {}

        # Add missing fields to the base model
        for key, value in kwargs.items():
            if value == 1:
                to_keep_values.add(key)
            elif key not in self.base_model._fields and isinstance(  # noqa
                kwargs[key], (str, dict)
            ):
                to_keep_values.add(key)
                self.base_model._fields[key] = mongoengine_fields.IntField()  # noqa
            projection[get_db_field(self.base_model, key)] = value  # noqa
            if value == 0:
                del self.base_model._fields[key]  # noqa

        # Remove fields from the base model, except the ones in to_keep_values and possibly _id
        if to_keep_values != {"id"}:
            keys_for_deletion = self.base_model._fields.keys() - to_keep_values  # noqa
            for key in keys_for_deletion:
                del self.base_model._fields[key]  # noqa
        # Append the projection stage to the pipelines
        self.pipelines.append({"$project": projection})
        # Return the instance for method chaining
        return self

    @last_out_stage_check
    def group(self, expression: Union[str, Dict, List, None] = "id") -> "Aggify":
        if isinstance(expression, list):
            expression = {
                field: f"${self.get_field_name_recursively(field)}"
                for field in expression
            }
        if expression and not isinstance(expression, dict):
            try:
                expression = "$" + self.get_field_name_recursively(expression)
            except InvalidField:
                pass
        self.pipelines.append({"$group": {"_id": expression}})
        return self

    @last_out_stage_check
    def order_by(self, *order_fields: Union[str, List[str]]) -> "Aggify":
        sort_dict = {
            get_db_field(self.base_model, field.replace("-", "")): -1
            if field.startswith("-")
            else 1
            for field in order_fields
        }
        self.pipelines.append({"$sort": sort_dict})
        return self

    @last_out_stage_check
    def raw(self, raw_query: dict) -> "Aggify":
        self.pipelines.append(raw_query)
        self.pipelines = self.__combine_sequential_matches()
        return self

    @last_out_stage_check
    def add_fields(self, **fields) -> "Aggify":  # noqa
        """Generates a MongoDB addFields pipeline stage.

        Args:
            fields: A dictionary of field expressions and values.

        Returns:
            A MongoDB add_fields pipeline stage.
        """
        add_fields_stage = {"$addFields": {}}

        for field, expression in fields.items():
            field = field.replace("__", ".")
            if isinstance(expression, str):
                add_fields_stage["$addFields"][field] = {"$literal": expression}
            elif isinstance(expression, F):
                add_fields_stage["$addFields"][field] = expression.to_dict()
            elif isinstance(expression, (list, dict)):
                add_fields_stage["$addFields"][field] = expression
            elif isinstance(expression, Cond):
                add_fields_stage["$addFields"][field] = dict(expression)
            elif isinstance(expression, Q):
                add_fields_stage["$addFields"][field] = convert_match_query(
                    dict(expression)
                )["$match"]
            else:
                raise AggifyValueError([str, F, list], type(expression))
            # TODO: Should be checked if new field is embedded, create embedded field.
            self.base_model._fields[  # noqa
                field.replace("$", "")
            ] = mongoengine_fields.IntField()

        self.pipelines.append(add_fields_stage)
        return self

    @last_out_stage_check
    def filter(
        self, arg: Union[Q, None] = None, **kwargs: Union[QueryParams, F, list]
    ) -> "Aggify":
        """
        # TODO: missing docs
        """
        if arg is not None and isinstance(arg, Q) is not True:
            raise AggifyValueError([Q, None], type(arg))

        if isinstance(arg, Q):
            self.pipelines.append(dict(arg))

        self.q = kwargs
        self.__to_aggregate(self.q)
        self.pipelines = self.__combine_sequential_matches()

        return self

    def out(self, coll: str, db: Union[str, None] = None) -> "Aggify":
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

    def __to_aggregate(self, query: Dict[str, Any]) -> None:
        """
        Builds the pipelines list based on the query parameters.
        """

        for key, value in query.items():
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
                    join_field.document_type, EmbeddedDocument  # noqa
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
    def __getitem__(self, index: Union[slice, int]) -> "Aggify":
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
        self,
        path: str,
        include_array_index: Union[str, None] = None,
        preserve: bool = False,
    ) -> "Aggify":
        """Generates a MongoDB unwind pipeline stage.

        Args:
            path: Field path to an array field.
              To specify a field path, prefix the field name with a dollar sign $
              and enclose in quotes.

            include_array_index: The name of a new field to hold the array index of the element.
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
        path = self.get_field_name_recursively(path)
        if include_array_index is None and preserve is False:
            unwind_stage = {"$unwind": f"${path}"}
        else:
            unwind_stage = {"$unwind": {"path": f"${path}"}}
            if preserve:
                unwind_stage["$unwind"]["preserveNullAndEmptyArrays"] = preserve
            if include_array_index:
                unwind_stage["$unwind"][
                    "includeArrayIndex"
                ] = include_array_index.replace("$", "")
        self.pipelines.append(unwind_stage)
        return self

    def annotate(
        self,
        annotate_name: Union[str, None] = None,
        accumulator: Union[str, None] = None,
        f: Union[Union[str, Dict], F, int, None] = None,
        **kwargs,
    ) -> "Aggify":
        """
        Annotate a MongoDB aggregation pipeline with a new field.
        Usage: https://www.mongodb.com/docs/manual/reference/operator/aggregation/group/#accumulator-operator

        Args:
            annotate_name (str): The name of the new annotated field.
            accumulator (str): The aggregation accumulator operator (e.g., "$sum", "$avg").
            f (Union[str, Dict] | F | int): The value for the annotated field.
            kwargs: Use F expressions.

        Returns:
            self.

        Raises:
            AnnotationError: If the pipeline is empty or if an invalid accumulator is provided.

        Example:
            annotate("totalSales", "sum", "sales")
            or
            annotate(first_field = F('field').first())
        """

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

        # Check either use F expression or not.
        base_model_fields = self.base_model._fields  # noqa
        if not kwargs:
            field_type, acc = self._get_field_type_and_accumulator(accumulator)

            # Get the annotation value: If the value is a string object, then it will be validated in the case of
            # embedded fields; otherwise, if it is an F expression object, simply return it.
            value = self._get_annotate_value(f)
            annotate = {annotate_name: {acc: value}}
            # Determine the data type based on the aggregation operator
            if not base_model_fields.get(annotate_name, None):
                base_model_fields[annotate_name] = field_type
        else:
            annotate, fields = self._do_annotate_with_expression(
                kwargs, base_model_fields
            )

        self.pipelines[-1]["$group"].update(annotate)
        return self

    @staticmethod
    def _get_field_type_and_accumulator(
        accumulator: str,
    ) -> Tuple[Type, str]:
        """
        Retrieves the accumulator name and returns corresponding MongoDB accumulator field type and name.

        Args:
            accumulator (str): The name of the accumulator.

        Returns: (Tuple): containing the field type and MongoDB accumulator string.

        Raises:
            AnnotationError: If the accumulator name is invalid.
        """

        # Some of the accumulator fields might be false and should be checked.
        # noinspection SpellCheckingInspection
        aggregation_mapping: Dict[str, Tuple] = {
            "sum": (mongoengine_fields.FloatField(), "$sum"),
            "avg": (mongoengine_fields.FloatField(), "$avg"),
            "stdDevPop": (mongoengine_fields.FloatField(), "$stdDevPop"),
            "stdDevSamp": (mongoengine_fields.FloatField(), "$stdDevSamp"),
            "push": (mongoengine_fields.ListField(), "$push"),
            "addToSet": (mongoengine_fields.ListField(), "$addToSet"),
            "count": (mongoengine_fields.IntField(), "$count"),
            "first": (
                mongoengine_fields.EmbeddedDocumentField(
                    mongoengine_fields.EmbeddedDocument
                ),
                "$first",
            ),
            "last": (
                mongoengine_fields.EmbeddedDocumentField(
                    mongoengine_fields.EmbeddedDocument
                ),
                "$last",
            ),
            "max": (mongoengine_fields.DynamicField(), "$max"),
            "accumulator": (mongoengine_fields.DynamicField(), "$accumulator"),
            "min": (mongoengine_fields.DynamicField(), "$min"),
            "median": (mongoengine_fields.DynamicField(), "$median"),
            "mergeObjects": (mongoengine_fields.DictField(), "$mergeObjects"),
            "top": (
                mongoengine_fields.EmbeddedDocumentField(
                    mongoengine_fields.EmbeddedDocument
                ),
                "$top",
            ),
            "bottom": (
                mongoengine_fields.EmbeddedDocumentField(
                    mongoengine_fields.EmbeddedDocument
                ),
                "$bottom",
            ),
            "topN": (mongoengine_fields.ListField(), "$topN"),
            "bottomN": (mongoengine_fields.ListField(), "$bottomN"),
            "firstN": (mongoengine_fields.ListField(), "$firstN"),
            "lastN": (mongoengine_fields.ListField(), "$lastN"),
            "maxN": (mongoengine_fields.ListField(), "$maxN"),
        }
        try:
            return aggregation_mapping[accumulator]
        except KeyError as error:
            raise AnnotationError(f"Invalid accumulator: {accumulator}") from error

    def _get_annotate_value(self, f: Union[F, str]) -> Union[Dict, str]:
        """
        Determines the annotation value based on the type of the input 'f'.

        If 'f' is an instance of F, it converts it to a dictionary.
        If 'f' is a string, it attempts to retrieve the corresponding field name recursively.
        If it encounters an InvalidField exception, it retains 'f' as the value.
        Otherwise, 'f' is returned as is.

        Args:
            f: The input value, which can be an instance of F, a string, or any other type.

        Returns:
            The determined annotation value, which could be a dictionary, a formatted string, or the original input.
        """
        if isinstance(f, F):
            value = f.to_dict()
        else:
            if isinstance(f, str):
                try:
                    value = f"${self.get_field_name_recursively(f)}"
                except InvalidField:
                    value = f
            else:
                value = f
        return value

    @staticmethod
    def _do_annotate_with_expression(
        annotate: Dict[str, Dict[str, Any]], base_model_fields: Dict[str, Any]
    ) -> Tuple[Dict[str, Dict[str, Any]], List[str]]:
        """
        Processes the annotation with an expression, updating the fields and annotation dictionary.

        Args:
            annotate (Dict[str, Dict[str, Any]]): A dictionary containing field names and their corresponding F expressions.
            base_model_fields (Dict[str, Any]): A dictionary representing the base model fields.

        Returns: Tuple[Dict[str, Dict[str, Any]], List[str]]: A tuple containing the updated annotations and a list
        of field names.

        Raises:
            InvalidAnnotateExpression: If the F expression is not a dictionary.
        """
        # Check if all elements in kwargs were valid
        for item in annotate.values():
            if not isinstance(item, dict):
                raise InvalidAnnotateExpression()

        # Extract field names
        fields = list(annotate.keys())

        # Process base_model_fields
        for field_name in fields:
            if field_name not in base_model_fields:
                accumulator = next(iter(annotate[field_name])).replace("$", "")
                field_type, _ = Aggify._get_field_type_and_accumulator(accumulator)
                base_model_fields[field_name] = field_type

        return annotate, fields

    def __match(self, matches: Dict[str, Any]):
        """
        Generates a MongoDB match pipeline stage.

        Args:
            matches: The match criteria.

        Returns:
            A MongoDB match pipeline stage.
        """
        return Match(matches, self.base_model).compile(self.pipelines)  # noqa

    @staticmethod
    def __lookup(
        from_collection: str, local_field: str, as_name: str, foreign_field: str = "_id"
    ) -> Dict[str, Dict[str, str]]:
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

    def __combine_sequential_matches(self) -> List[Dict[str, Union[dict, Any]]]:
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

    # check_fields_exist(self.base_model, let)  # noqa
    def get_field_name_recursively(
        self, field: str, base: Union[CollectionType, None] = None
    ) -> str:
        """
        Recursively fetch the field name by following the hierarchy indicated by the field parameter.

        The function traverses the field hierarchy indicated by double underscores in the field parameter.
        At each level, it checks if the field exists and then fetches the database field name for it.
        The entire hierarchy is then joined using a dot (.) separator.

        Parameters:
        - field (str): A string indicating the hierarchy of fields, separated by double underscores.

        Returns:
        - str: A dot-separated string representing the full path to the field in the database.
        """

        field_name = []
        base = self.base_model if not base else base

        # Split the field based on double underscores and process each item
        for index, item in enumerate(field.split("__")):
            # Ensure the field exists at the current level of hierarchy
            validate_field_existence(base, [item])  # noqa

            # Append the database field name to the field_name list
            field_name.append(get_db_field(base, item))

            # Move to the next level in the model hierarchy
            base = self.get_model_field(base, item)
            base = base.__dict__.get("document_type_obj", base)

        # Join the entire hierarchy using dots and return
        return ".".join(field_name)

    @last_out_stage_check
    def lookup(
        self,
        from_collection: CollectionType,
        as_name: str,
        query: Union[List[Q], Union[Q, None], List["Aggify"]] = None,
        let: Union[List[str], None] = None,
        local_field: Union[str, None] = None,
        foreign_field: Union[str, None] = None,
        raw_let: Union[Dict, None] = None,
    ) -> "Aggify":
        """
        Generates a MongoDB lookup pipeline stage.

        Args:
            from_collection (Document): The document representing the collection to perform the lookup on.
            as_name (str): The name of the new field to create.
            query (list[Q] | Union[Q, None], optional): List of desired queries with Q function or a single query.
            let (Union[List[str], None], optional): The local field(s) to join on. If provided,
            localField and foreignField are not used.
            local_field (Union[str, None], optional): The local field to join on when `let` is not provided.
            foreign_field (Union[str, None], optional): The foreign field to join on when `let` is not provided.
            let (Union[List[str], None], optional): The local field(s) to join on. If provided,
            localField and foreignField are not used.
            local_field (Union[str, None], optional): The local field to join on when let not provided.
            foreign_field (Union[str, None], optional): The foreign field to join on when let not provided.
            raw_let (Union[Dict, None]): raw let

        Returns:
            Aggify: An instance of the Aggify class representing a MongoDB lookup pipeline stage.
        """

        lookup_stages = []
        check_field_already_exists(self.base_model, as_name)  # noqa
        from_collection_name = from_collection._meta.get("collection")  # noqa

        if not (let or raw_let) and not (local_field and foreign_field):
            raise InvalidArgument(
                expected_list=[["local_field", "foreign_field"], ["let", "raw_let"]]
            )
        elif not (let or raw_let):
            lookup_stage = {
                "$lookup": {
                    "from": from_collection_name,
                    "localField": self.get_field_name_recursively(local_field),  # noqa
                    "foreignField": self.get_field_name_recursively(
                        base=from_collection, field=foreign_field  # noqa
                    ),
                    "as": as_name,
                }
            }
        else:
            if not query:
                raise InvalidArgument(expected_list=["query"])

            if let is None:
                let = []

            let_dict = {
                field: f"${get_db_field(self.base_model, self.get_field_name_recursively(field))}"  # noqa
                for field in let
            }

            let = list(raw_let.keys()) if let is [] else let

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
            if raw_let:
                let_dict.update(raw_let)
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
        self.base_model._fields[as_name] = copy_class(from_collection)  # noqa

        return self

    @staticmethod
    def get_model_field(model: Type[Document], field: str) -> mongoengine_fields:
        """
        Get the field definition of a specified field in a MongoDB model.

        Args:
            model (CollectionType): The MongoDB model.
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
        field_name = get_db_field(self.base_model, embedded_field)
        if "__module__" in model_field.__dict__:
            self.base_model._fields = (
                model_field._fields
            )  # load new fields into old model
            return f"${field_name}"
        if not hasattr(model_field, "document_type") or not issubclass(
            model_field.document_type, EmbeddedDocument
        ):
            raise InvalidEmbeddedField(field=embedded_field)
        return f"${field_name}"

    @last_out_stage_check
    def replace_root(
        self, *, embedded_field: str, merge: Union[Dict, None] = None
    ) -> "Aggify":
        """
        Replace the root document in the aggregation pipeline with a specified embedded field or a merged result.

        Args:
            embedded_field (str): The name of the embedded field to use as the new root.
            merge (Union[Dict, None], optional): A dictionary for merging with the new root. Default is None.

        Returns:
            Aggify: The modified Aggify instance.

        Usage:
            Aggify().replace_root(embedded_field="myEmbeddedField")
            Aggify().replace_root(embedded_field="myEmbeddedField", merge={"child_field": default_value_if_not_exists})
        """
        name = self._replace_base(embedded_field)

        if merge:
            new_root = {"$replaceRoot": {"newRoot": {"$mergeObjects": [merge, name]}}}
            self.base_model._fields.update(  # noqa
                {key: mongoengine_fields.IntField() for key, value in merge.items()}
            )
        else:
            new_root = {"$replaceRoot": {"newRoot": name}}
        self.pipelines.append(new_root)

        return self

    @last_out_stage_check
    def replace_with(
        self, *, embedded_field: str, merge: Union[Dict, None] = None
    ) -> "Aggify":
        """
        Replace the root document in the aggregation pipeline with a specified embedded field or a merged result.

        Args:
            embedded_field (str): The name of the embedded field to use as the new root.
            merge (Union[Dict, None], optional): A dictionary for merging with the new root. Default is None.

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
            self.base_model._fields.update(  # noqa
                {key: mongoengine_fields.IntField() for key, value in merge.items()}
            )
        self.pipelines.append(new_root)

        return self

    @last_out_stage_check
    def redact(self, value1, condition, value2, then_value, else_value):
        """
        Add a $redact stage to the pipeline based on the provided conditions.

        The $redact stage restricts the contents of the returned documents
        based on the condition specified. This method assists in building
        the $redact stage for a MongoDB aggregation pipeline.

        Parameters:
        - value1 (str): The first value to compare in the condition.
        - condition (str): The MongoDB comparison operator (e.g., "$eq", "$lt").
        - value2 (str): The second value to compare in the condition.
        - then_value (str): The action to take if the condition is True.
                            Expected values: "$DESCEND", "$PRUNE", "$KEEP".
        - else_value (str): The action to take if the condition is False.
                            Expected values: "$DESCEND", "$PRUNE", "$KEEP".

        Returns:
        - self: Returns the instance of the class to allow chaining.

        Raises:
        - InvalidArgument: If then_value or else_value are not in the expected list.
        """

        # List of valid redaction values
        redact_values = ["DESCEND", "PRUNE", "KEEP"]

        def clean_then_else(_then_value, _else_value):
            """
            Helper function to sanitize then_value and else_value.

            Strips the dollar sign and converts values to uppercase.
            """
            return (
                _then_value.replace("$", "").upper(),
                _else_value.replace("$", "").upper(),
            )

        # Clean the provided then_value and else_value
        then_value, else_value = clean_then_else(then_value, else_value)

        # Check if the cleaned values are in the valid list
        if then_value not in redact_values or else_value not in redact_values:
            raise InvalidArgument(expected_list=redact_values)

        # Construct the $redact stage with the provided condition
        stage = {
            "$redact": dict(
                Cond(value1, condition, value2, f"$${then_value}", f"$${else_value}")
            )
        }

        # Append the constructed stage to the pipelines list
        self.pipelines.append(stage)

        return self
