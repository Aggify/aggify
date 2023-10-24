from aggify import Q


class TestQ:

    # Test OR operator with multiple conditions
    def test_or_operator_with_multiple_conditions(self):
        q1 = Q(name="John")
        q2 = Q(name="Alice")
        q_combined = q1 | q2
        assert q_combined.to_dict() == {
            "$match": {"$or": [q1.to_dict()["$match"], q2.to_dict()["$match"]]}}

    # Test combining NOT operators with AND
    def test_combine_not_operators_with_and(self):
        q1 = Q(name="John")
        q2 = Q(age__lt=30)
        q_combined = ~q1 & ~q2
        assert q_combined.to_dict() == {
            "$match": {"$and": [{"$not": [q1.to_dict()["$match"]]}, {"$not": [q2.to_dict()["$match"]]}]}}

    # Test combining NOT operators with OR
    def test_combine_not_operators_with_or(self):
        q1 = Q(name="John")
        q2 = Q(age__lt=30)
        q_combined = ~q1 | ~q2  # Changed | to combine OR
        assert q_combined.to_dict() == {
            "$match": {"$or": [{"$not": [q1.to_dict()["$match"]]}, {"$not": [q2.to_dict()["$match"]]}]}}
