from typing import Any, Type, Union, List, Dict

from mongoengine import Document

from aggify.exceptions import MongoIndexError, InvalidField, AlreadyExistsField


def int_to_slice(final_index: int) -> slice:
    """
    Converts an integer to a slice, assuming that the start index is 0.

    Examples:
        >>> int_to_slice(3)
        slice(0, 2)
    """
    return slice(0, final_index)


def to_mongo_positive_index(index: Union[int, slice]) -> slice:
    if isinstance(index, int):
        if index < 0:
            raise MongoIndexError
        return slice(0, index, None)

    if index.step is not None:
        raise MongoIndexError

    if int(index.start) >= index.stop:
        raise MongoIndexError

    if int(index.start) < 0:
        raise MongoIndexError
    return index


def check_fields_exist(model: Document, fields_to_check: List[str]) -> None:
    """
    Check if the specified fields exist in a model's fields.

    Args:
        model: The model containing the fields to check.
        fields_to_check (list): A list of field names to check for existence in the model.

    Raises:
        InvalidField: If any of the specified fields are missing in the model's fields.
    """
    missing_fields = [
        field for field in fields_to_check if not model._fields.get(field)
    ]  # noqa
    if missing_fields:
        raise InvalidField(field=missing_fields[0])


def replace_values_recursive(obj, replacements):
    """
    Replaces let values in a list of match stages.

    Args:
        obj: A list of match stages.
        replacements: Key, values to be replaced.

    Returns:
        A list of updated match stages.
    """
    if isinstance(obj, list):
        updated_stages = []
        for item in obj:
            updated_stages.append(replace_values_recursive(item, replacements))
        return updated_stages
    elif isinstance(obj, dict):
        updated_stage = {}
        for key, value in obj.items():
            updated_stage[key] = replace_values_recursive(value, replacements)
        return updated_stage
    elif str(obj).replace("$", "") in replacements:
        return replacements[obj.replace("$", "")]
    else:
        return obj


def convert_match_query(
    d: Dict,
) -> Union[Dict[Any, Union[List[Union[str, Any]], Dict]], List[Dict], Dict]:
    pass

    """
    Recursively transform a dictionary to modify the structure of '$eq' and '$ne' operators.

    Args:
    d (dict or any): The input dictionary to be transformed.

    Returns:
    dict or any: The transformed dictionary with '$eq' and '$ne' operators modified.

    This function recursively processes the input dictionary, looking for '$eq' and '$ne' operators
    within sub-dictionaries. When found, it restructures the data into the format {'$eq' or '$ne': [field, value]}.
    For other fields, it processes them recursively to maintain the dictionary structure.

    Example:
    original_dict = {'_id': {'$eq': 123456}, 'other_field': {'$ne': 789}, 'dynamic_field': {'$eq': 'dynamic_value'}}
    transformed_dict = transform_dict(original_dict)
    print(transformed_dict)
    # Output: {'$eq': ['_id', 123456], 'other_field': {'$ne': 789}, 'dynamic_field': {'$eq': 'dynamic_value'}}
    """
    if isinstance(d, dict):
        new_dict = {}
        for key, value in d.items():
            if isinstance(value, dict) and ("$eq" in value or "$ne" in value):
                for operator, operand in value.items():
                    new_dict[operator] = [f"${key}", operand]
            else:
                new_dict[key] = convert_match_query(value)
        return new_dict
    elif isinstance(d, list):
        return [convert_match_query(item) for item in d]
    else:
        return d


def check_field_exists(model: Type[Document], field: str) -> None:
    """
    Check if a field exists in the given model.

    Args:
        model (Document): The model to check for the field.
        field (str): The name of the field to check.

    Raises:
        AlreadyExistsField: If the field already exists in the model.
    """
    if model._fields.get(field):  # noqa
        raise AlreadyExistsField(field=field)


def get_db_field(model: Type[Document], field: str, add_dollar_sign=False) -> str:
    """
    Get the database field name for a given field in the model.

    Args:
        add_dollar_sign: Add a "$" at the start of the field or not
        model (Document): The model containing the field.
        field (str): The name of the field.

    Returns:
        str: The database field name if available, otherwise the original field name.
    """
    try:
        db_field = model._fields.get(field).db_field  # noqa
        db_field = field if db_field is None else db_field
        return f"${db_field}" if add_dollar_sign else db_field
    except AttributeError:
        return field
