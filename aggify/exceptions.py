from typing import Type


class AggifyBaseException(Exception):
    message: str


class MongoIndexError(AggifyBaseException):
    def __init__(self):
        self.message = "Index error is invalid, please use int or slice without step!"
        super().__init__(self.message)


class InvalidPipelineStageError(AggifyBaseException):
    """General parent exception for all `pipeline stage` methods

    Subclass and customise for the raised exception in the methods
    """

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class AnnotationError(InvalidPipelineStageError):
    pass


class OutStageError(InvalidPipelineStageError):
    def __init__(self, stage):
        self.message = f"You cannot add a {self!r} pipeline after $out stage!"
        super().__init__(self.message)


class AggifyValueError(AggifyBaseException):

    def __init__(self, expected_list: list[Type], result: Type):
        self.message = (
            f"Input is not correctly passed, expected either of {[expected for expected in expected_list]}"
            f"but got {result}"
        )
        self.expecteds = expected_list
        self.result = result
        super().__init__(self.message)


class InvalidOperator(AggifyBaseException):
    def __init__(self, operator: str):
        self.message = f"Operator {operator} does not exists, please refer to documentation to see all supported operators."
        super().__init__(self.message)


class InvalidField(AggifyBaseException):
    def __init__(self, field: str):
        self.message = f"Field {field} does not exists."
        super().__init__(self.message)

class InvalidEmbeddedField(AggifyBaseException):
    def __init__(self, field: str):
        self.message = f"Field {field} is not embedded."
        super().__init__(self.message)
