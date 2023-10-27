from aggify import Q


class TestQ:

    # Test OR operator with multiple conditions
    def test_or_operator_with_multiple_conditions(self):
        q1 = Q(name="John")
        q2 = Q(name="Alice")
        q_combined = q1 | q2
        assert dict(q_combined) == {
            "$match": {"$or": [dict(q1)["$match"], dict(q2)["$match"]]}}

    def test_or_operator_with_multiple_conditions_more_than_rwo(self):
        q1 = Q(name="John")
        q2 = Q(name="Alice")
        q3 = Q(name="Bob")
        q_combined = q1 | q2 | q3
        assert dict(q_combined) == {
            "$match": {"$or": [dict(q1)["$match"], dict(q2)["$match"], dict(q3)["$match"]]}}

    # Test combining NOT operators with AND
    def test_combine_not_operators_with_and(self):
        q1 = Q(name="John")
        q2 = Q(age__lt=30)
        q_combined = ~q1 & ~q2
        assert dict(q_combined) == {
            "$match": {"$and": [{"$not": [dict(q1)["$match"]]}, {"$not": [dict(q2)["$match"]]}]}}

    # Test combining NOT operators with OR
    def test_combine_not_operators_with_or(self):
        q1 = Q(name="John")
        q2 = Q(age__lt=30)
        q_combined = ~q1 | ~q2  # Changed | to combine OR
        assert dict(q_combined) == {
            "$match": {"$or": [{"$not": [dict(q1)["$match"]]}, {"$not": [dict(q2)["$match"]]}]}}
