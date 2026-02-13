"""Tests for CLI filter parser."""

import pytest

from ontologia.cli._filters import parse_cli_filters
from ontologia.filters import ComparisonExpression, LogicalExpression


def test_parse_empty():
    assert parse_cli_filters([]) is None


def test_parse_single_eq():
    result = parse_cli_filters([("$.name", "eq", '"Alice"')])
    assert isinstance(result, ComparisonExpression)
    assert result.field_path == "$.name"
    assert result.op == "=="
    assert result.value == "Alice"


def test_parse_numeric():
    result = parse_cli_filters([("$.age", "gt", "25")])
    assert isinstance(result, ComparisonExpression)
    assert result.op == ">"
    assert result.value == 25


def test_parse_in():
    result = parse_cli_filters([("$.tier", "in", '["Gold","Silver"]')])
    assert isinstance(result, ComparisonExpression)
    assert result.op == "IN"
    assert result.value == ["Gold", "Silver"]


def test_parse_is_null():
    result = parse_cli_filters([("$.email", "is_null", "null")])
    assert isinstance(result, ComparisonExpression)
    assert result.op == "IS_NULL"


def test_parse_multiple_and():
    result = parse_cli_filters(
        [
            ("$.tier", "eq", '"Gold"'),
            ("$.age", "gte", "18"),
        ]
    )
    assert isinstance(result, LogicalExpression)
    assert result.op == "AND"
    assert len(result.children) == 2


def test_parse_all_ops():
    for op_token, expected_op in [
        ("eq", "=="),
        ("ne", "!="),
        ("gt", ">"),
        ("gte", ">="),
        ("lt", "<"),
        ("lte", "<="),
        ("in", "IN"),
        ("is_null", "IS_NULL"),
    ]:
        value = '["a"]' if op_token == "in" else '"x"'
        result = parse_cli_filters([("$.f", op_token, value)])
        assert isinstance(result, ComparisonExpression)
        assert result.op == expected_op


def test_parse_unknown_op():
    with pytest.raises(ValueError, match="Unknown filter operator"):
        parse_cli_filters([("$.f", "badop", "1")])


def test_parse_left_right_paths():
    result = parse_cli_filters([("left.$.tier", "eq", '"Gold"')])
    assert isinstance(result, ComparisonExpression)
    assert result.field_path == "left.$.tier"

    result = parse_cli_filters([("right.$.price", "gt", "10")])
    assert isinstance(result, ComparisonExpression)
    assert result.field_path == "right.$.price"
