"""Tests for filter expressions and SQL compilation."""

from __future__ import annotations

import re
from typing import Any

import pytest

from ontologia.filters import (
    NULL_EQ_ERROR,
    NULL_NE_ERROR,
    ComparisonExpression,
    EndpointProxy,
    FieldProxy,
    LogicalExpression,
    left,
    right,
)
from ontologia.storage import _compile_filter


class TestFilterExpression:
    def test_comparison_creation(self):
        expr = ComparisonExpression("$.name", "==", "Alice")
        assert expr.field_path == "$.name"
        assert expr.op == "=="
        assert expr.value == "Alice"

    def test_logical_and(self):
        a = ComparisonExpression("$.age", ">", 18)
        b = ComparisonExpression("$.age", "<", 65)
        combined = a & b
        assert isinstance(combined, LogicalExpression)
        assert combined.op == "AND"
        assert len(combined.children) == 2

    def test_logical_or(self):
        a = ComparisonExpression("$.tier", "==", "Gold")
        b = ComparisonExpression("$.tier", "==", "Platinum")
        combined = a | b
        assert isinstance(combined, LogicalExpression)
        assert combined.op == "OR"

    def test_logical_not(self):
        a = ComparisonExpression("$.active", "==", False)
        negated = ~a
        assert isinstance(negated, LogicalExpression)
        assert negated.op == "NOT"
        assert len(negated.children) == 1

    def test_complex_composition(self):
        expr = (
            ComparisonExpression("$.age", ">=", 21) & ComparisonExpression("$.age", "<=", 65)
        ) & ComparisonExpression("$.email", "IS_NOT_NULL")
        assert isinstance(expr, LogicalExpression)
        assert expr.op == "AND"


class TestFieldProxy:
    def test_eq(self):
        proxy = FieldProxy("$.name")
        expr = proxy == "Alice"
        assert isinstance(expr, ComparisonExpression)
        assert expr.op == "=="

    def test_ne(self):
        proxy = FieldProxy("$.name")
        expr = proxy != "Bob"
        assert isinstance(expr, ComparisonExpression)
        assert expr.op == "!="

    def test_gt(self):
        proxy = FieldProxy("$.age")
        expr = proxy > 30
        assert expr.op == ">"

    def test_ge(self):
        proxy = FieldProxy("$.age")
        expr = proxy >= 30
        assert expr.op == ">="

    def test_lt(self):
        proxy = FieldProxy("$.age")
        expr = proxy < 30
        assert expr.op == "<"

    def test_le(self):
        proxy = FieldProxy("$.age")
        expr = proxy <= 30
        assert expr.op == "<="

    def test_startswith(self):
        proxy = FieldProxy("$.name")
        expr = proxy.startswith("A")
        assert expr.op == "LIKE"
        assert expr.value == "A%"

    def test_endswith(self):
        proxy = FieldProxy("$.email")
        expr = proxy.endswith("@test.com")
        assert expr.op == "LIKE"
        assert expr.value == "%@test.com"

    def test_contains(self):
        proxy = FieldProxy("$.email")
        expr = proxy.contains("@")
        assert expr.op == "LIKE"
        assert expr.value == "%@%"

    def test_in_(self):
        proxy = FieldProxy("$.tier")
        expr = proxy.in_(["Gold", "Platinum"])
        assert expr.op == "IN"
        assert expr.value == ["Gold", "Platinum"]

    def test_is_null(self):
        proxy = FieldProxy("$.email")
        expr = proxy.is_null()
        assert expr.op == "IS_NULL"

    def test_is_not_null(self):
        proxy = FieldProxy("$.email")
        expr = proxy.is_not_null()
        assert expr.op == "IS_NOT_NULL"

    def test_none_eq_raises(self):
        proxy = FieldProxy("$.email")
        with pytest.raises(TypeError, match=re.escape(NULL_EQ_ERROR)):
            proxy.__eq__(None)

    def test_none_ne_raises(self):
        proxy = FieldProxy("$.email")
        with pytest.raises(TypeError, match=re.escape(NULL_NE_ERROR)):
            proxy.__ne__(None)


class TestEndpointProxy:
    def test_left_proxy(self):
        from tests.conftest import Subscription

        proxy = left(Subscription)
        assert isinstance(proxy, EndpointProxy)
        expr = proxy.tier == "Gold"
        assert isinstance(expr, ComparisonExpression)
        assert expr.field_path == "left.$.tier"

    def test_right_proxy(self):
        from tests.conftest import Subscription

        proxy = right(Subscription)
        expr = proxy.price > 100
        assert isinstance(expr, ComparisonExpression)
        assert expr.field_path == "right.$.price"


class TestSQLCompilation:
    def test_compile_eq(self):
        expr = ComparisonExpression("$.name", "==", "Alice")
        params: list[Any] = []
        sql = _compile_filter(expr, params)
        assert "json_extract(fields_json, '$.name') =" in sql
        assert params == ["Alice"]

    def test_compile_ne(self):
        expr = ComparisonExpression("$.tier", "!=", "VIP")
        params: list[Any] = []
        sql = _compile_filter(expr, params)
        assert "!=" in sql
        assert params == ["VIP"]

    def test_compile_gt(self):
        expr = ComparisonExpression("$.age", ">", 30)
        params: list[Any] = []
        sql = _compile_filter(expr, params)
        assert ">" in sql
        assert params == [30]

    def test_compile_like(self):
        expr = ComparisonExpression("$.name", "LIKE", "A%")
        params: list[Any] = []
        sql = _compile_filter(expr, params)
        assert "LIKE" in sql
        assert params == ["A%"]

    def test_compile_in(self):
        expr = ComparisonExpression("$.tier", "IN", ["Gold", "Platinum"])
        params: list[Any] = []
        sql = _compile_filter(expr, params)
        assert "IN" in sql
        assert params == ["Gold", "Platinum"]

    def test_compile_is_null(self):
        expr = ComparisonExpression("$.email", "IS_NULL")
        params: list[Any] = []
        sql = _compile_filter(expr, params)
        assert "IS NULL" in sql
        assert params == []

    def test_compile_is_not_null(self):
        expr = ComparisonExpression("$.email", "IS_NOT_NULL")
        params: list[Any] = []
        sql = _compile_filter(expr, params)
        assert "IS NOT NULL" in sql

    def test_compile_and(self):
        a = ComparisonExpression("$.age", ">", 18)
        b = ComparisonExpression("$.age", "<", 65)
        expr = a & b
        params: list[Any] = []
        sql = _compile_filter(expr, params)
        assert "AND" in sql
        assert params == [18, 65]

    def test_compile_or(self):
        a = ComparisonExpression("$.tier", "==", "Gold")
        b = ComparisonExpression("$.tier", "==", "Platinum")
        expr = a | b
        params: list[Any] = []
        sql = _compile_filter(expr, params)
        assert "OR" in sql

    def test_compile_not(self):
        expr = ~ComparisonExpression("$.active", "==", False)
        params: list[Any] = []
        sql = _compile_filter(expr, params)
        assert "NOT" in sql

    def test_compile_with_table_alias(self):
        expr = ComparisonExpression("$.name", "==", "Alice")
        params: list[Any] = []
        sql = _compile_filter(expr, params, table_alias="eh")
        assert "eh.fields_json" in sql

    def test_compile_left_endpoint(self):
        expr = ComparisonExpression("left.$.tier", "==", "Gold")
        params: list[Any] = []
        sql = _compile_filter(expr, params)
        assert "le.fields_json" in sql

    def test_compile_right_endpoint(self):
        expr = ComparisonExpression("right.$.price", ">", 100)
        params: list[Any] = []
        sql = _compile_filter(expr, params)
        assert "re.fields_json" in sql
