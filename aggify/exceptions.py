from typing import Type, List


class AggifyBaseException(Exception):
    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)


class MongoIndexError(AggifyBaseException):
    def __init__(self):
        super().__init__("Index error is invalid, please use int or slice without step!")


class InvalidPipelineStageError(AggifyBaseException):
    """General parent exception for all `pipeline stage` methods

    Subclass and customise for the raised exception in the methods
    """


class AnnotationError(InvalidPipelineStageError):
    pass


class OutStageError(InvalidPipelineStageError):
    def __init__(self, stage):
        super().__init__(f"You cannot add a {stage!r} pipeline after $out stage!")


class AggifyValueError(AggifyBaseException):
    def __init__(self, expected_list: List[Type], result: Type):
        super().__init__(
            f"Input is not correctly passed, expected either of {expected_list}, but got {result}"
        )
        self.expecteds = expected_list
        self.result = result


class InvalidOperator(AggifyBaseException):
    def __init__(self, operator: str):
        super().__init__(
            f"Operator {operator} does not exist, please refer to documentation to see all supported operators."
        )


class InvalidField(AggifyBaseException):
    def __init__(self, field: str):
        super().__init__(f"Field {field} does not exist.")


class InvalidEmbeddedField(AggifyBaseException):
    def __init__(self, field: str):
        super().__init__(f"Field {field} is not embedded.")


class AlreadyExistsField(AggifyBaseException):
    def __init__(self, field: str):
        super().__init__(f"Field {field} already exists.")


class InvalidArgument(AggifyBaseException):
    def __init__(self, expected_list: list):
        super().__init__(f"Input is not correctly passed, expected {expected_list}")
        self.expecteds = expected_list
