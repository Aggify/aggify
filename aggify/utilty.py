from aggify.exceptions import MongoIndexError


def int_to_slice(final_index: int) -> slice:
    """
    Converts an integer to a slice, assuming that the start index is 0.

    Examples:
        >>> int_to_slice(3)
        slice(0, 2)
    """
    return slice(0, final_index)


def to_mongo_positive_index(index: int | slice) -> slice:
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
