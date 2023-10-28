from typing import TypedDict

QueryParams = int | None | str | bool | float


class UnwindDict(TypedDict):
    path: str
    preserveNullAndEmptyArrays: bool
