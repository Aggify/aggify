from typing import Union, Dict, TypeVar, Callable
from mongoengine import Document
from bson import ObjectId

QueryParams = Union[int, None, str, bool, float, Dict, ObjectId]

CollectionType = TypeVar("CollectionType", bound=Callable[..., Document])
