from typing import Union, Dict, TypeVar, Callable
from bson import ObjectId
from mongoengine import Document

QueryParams = Union[int, None, str, bool, float, Dict, ObjectId]
CollectionType = TypeVar("CollectionType", bound=Callable[..., Document])
