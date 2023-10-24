from aggify import F  # Import from your actual module


class TestF:

    # Test subtraction using F class
    def test_subtraction(self):
        f1 = F("age")
        f2 = F("income")
        f_combined = f1 - f2
        assert f_combined.to_dict() == {"$subtract": ["$age", "$income"]}

    # Test division using F class
    def test_division(self):
        f1 = F("income")
        f2 = F("expenses")
        f_combined = f1 / f2
        assert f_combined.to_dict() == {"$divide": ["$income", "$expenses"]}

    # Test multiplication using F class
    def test_multiplication(self):
        f1 = F("quantity")
        f2 = F("price")
        f_combined = f1 * f2
        assert f_combined.to_dict() == {"$multiply": ["$quantity", "$price"]}

    # Test addition using F class with a constant
    def test_addition_with_constant(self):
        f1 = F("age")
        constant = 10
        f_combined = f1 + constant
        assert f_combined.to_dict() == {"$add": ["$age", 10]}

    # Test subtraction using F class with a constant
    def test_subtraction_with_constant(self):
        f1 = F("income")
        constant = 5000
        f_combined = f1 - constant
        assert f_combined.to_dict() == {"$subtract": ["$income", 5000]}

    # Test division using F class with a constant
    def test_division_with_constant(self):
        f1 = F("price")
        constant = 2
        f_combined = f1 / constant
        assert f_combined.to_dict() == {"$divide": ["$price", 2]}

    # Test multiplication using F class with a constant
    def test_multiplication_with_constant(self):
        f1 = F("quantity")
        constant = 5
        f_combined = f1 * constant
        assert f_combined.to_dict() == {"$multiply": ["$quantity", 5]}

    # Test addition using F class with multiple fields and constants
    def test_addition_with_multiple_fields(self):
        f1 = F("age")
        f2 = F("income")
        f_combined = f1 + f2
        assert f_combined.to_dict() == {"$add": ["$age", "$income"]}

    # Test subtraction using F class with multiple fields and constants
    def test_subtraction_with_multiple_fields(self):
        f1 = F("income")
        f2 = F("expenses")
        f_combined = f1 - f2
        assert f_combined.to_dict() == {"$subtract": ["$income", "$expenses"]}

    # Test multiplication using F class with multiple fields and constants
    def test_multiplication_with_multiple_fields(self):
        f1 = F("quantity")
        f2 = F("price")
        f3 = F("nano")
        f_combined = f1 * f2 * f3
        assert f_combined.to_dict() == {"$multiply": ["$quantity", "$price", "$nano"]}
