from typing import TypedDict

QueryParams = int | None | str | bool | float | dict


class UnwindDict(TypedDict):
    path: str
    preserveNullAndEmptyArrays: bool
