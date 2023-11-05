import pytest

from aggify.compiler import Match
from aggify.exceptions import InvalidOperator


def test_validate_operator_fail():
    with pytest.raises(InvalidOperator):
        Match.validate_operator("key_raise")


def test_validate_operator_fail_not_in_operators():
    with pytest.raises(InvalidOperator):
        Match.validate_operator("key__ge")
