from typing import Type


class AggifyBaseException(Exception):
    message: str


class MongoIndexError(AggifyBaseException):
    def __init__(self):
        self.message = "Index error is invalid, please use int or slice without step!"

        super().__init__(self.message)


class AnnotationError(AggifyBaseException):
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class AggifyValueError(AggifyBaseException):
    def __init__(self, expects: list[Type], result: Type):
        self.message = (
            f"Input is not correctly passed, expected either of {[expected for expected in expects]}"
            f"but got {result}"
        )
        self.expects = expects
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