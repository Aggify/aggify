import pytest
from aggify import Cond


class TestCond:

    # Test condition with greater than operator
    def test_greater_than_operator(self):
        cond = Cond(25, '>', 20, 'High', 'Low')
        assert dict(cond) == {"$cond": {"if": {"$gt": [25, 20]}, "then": "High", "else": "Low"}}

    # Test condition with less than operator
    def test_less_than_operator(self):
        cond = Cond(15, '<', 20, 'Low', 'High')
        assert dict(cond) == {"$cond": {"if": {"$lt": [15, 20]}, "then": "Low", "else": "High"}}

    # Test condition with equal to operator
    def test_equal_to_operator(self):
        cond = Cond(30, '==', 30, 'Equal', 'Not Equal')
        assert dict(cond) == {"$cond": {"if": {"$eq": [30, 30]}, "then": "Equal", "else": "Not Equal"}}

    # Test condition with not equal to operator
    def test_not_equal_to_operator(self):
        cond = Cond(40, '!=', 35, 'Not Equal', 'Equal')
        assert dict(cond) == {"$cond": {"if": {"$ne": [40, 35]}, "then": "Not Equal", "else": "Equal"}}

    # Test condition with greater than or equal to operator
    def test_greater_than_or_equal_to_operator(self):
        cond = Cond(20, '>=', 20, 'Greater or Equal', 'Less')
        assert dict(cond) == {"$cond": {"if": {"$gte": [20, 20]}, "then": "Greater or Equal", "else": "Less"}}

    # Test condition with less than or equal to operator
    def test_less_than_or_equal_to_operator(self):
        cond = Cond(18, '<=', 20, 'Less or Equal', 'Greater')
        assert dict(cond) == {"$cond": {"if": {"$lte": [18, 20]}, "then": "Less or Equal", "else": "Greater"}}

    # Test condition with complex expressions
    def test_complex_expression(self):
        cond = Cond(15, '>', 10, dict(Cond(20, '<', 25, 'Within Range', 'Out of Range')), 'Invalid')
        assert dict(cond) == {"$cond": {"if": {"$gt": [15, 10]}, "then": {"$cond": {"if": {"$lt": [20, 25]}, "then": "Within Range", "else": "Out of Range"}}, "else": "Invalid"}}

    # Test invalid operator
    def test_invalid_operator(self):
        with pytest.raises(ValueError):
            cond = Cond(25, 'invalid_operator', 20, 'High', 'Low')
            dict(cond)
