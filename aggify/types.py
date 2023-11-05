from typing import Union, Dict

from bson import ObjectId

QueryParams = Union[int, None, str, bool, float, Dict, ObjectId]
