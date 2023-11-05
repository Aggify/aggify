from typing import Union, Dict, TypeVar, Callable

from bson import ObjectId

QueryParams = Union[int, None, str, bool, float, Dict, ObjectId]
AggifyType = TypeVar('AggifyType', bound=Callable[..., "Aggify"])
CollectionType = TypeVar('CollectionType', bound=Callable[..., "Document"])
