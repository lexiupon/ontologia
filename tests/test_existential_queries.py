"""Tests for RFC 0006 Phase 2: existential predicates, count_where, avg_len."""

from __future__ import annotations

import pytest

from ontologia import Entity, Field
from ontologia.filters import (
    AnyPathProxy,
    ComparisonExpression,
    ExistsComparisonExpression,
    FieldProxy,
    LogicalExpression,
)
from ontologia.runtime import Ontology, _matches_filter
from ontologia.storage import (
    _compile_exists,
    _compile_filter,
    _extract_direct_filter,
    _needs_endpoint_join,
)


# --- AnyPathProxy operator coverage ---


class TestAnyPathProxy:
    def setup_method(self):
        self.proxy = AnyPathProxy("$.events", "kind")

    def test_eq(self):
        expr = self.proxy == "click"
        assert isinstance(expr, ExistsComparisonExpression)
        assert expr.list_field_path == "$.events"
        assert expr.item_path == "kind"
        assert expr.op == "=="
        assert expr.value == "click"

    def test_ne(self):
        expr = self.proxy != "click"
        assert expr.op == "!="

    def test_gt(self):
        expr = self.proxy > 5
        assert expr.op == ">"

    def test_ge(self):
        expr = self.proxy >= 5
        assert expr.op == ">="

    def test_lt(self):
        expr = self.proxy < 5
        assert expr.op == "<"

    def test_le(self):
        expr = self.proxy <= 5
        assert expr.op == "<="

    def test_in(self):
        expr = self.proxy.in_(["a", "b"])
        assert expr.op == "IN"
        assert expr.value == ["a", "b"]

    def test_is_null(self):
        expr = self.proxy.is_null()
        assert expr.op == "IS_NULL"

    def test_is_not_null(self):
        expr = self.proxy.is_not_null()
        assert expr.op == "IS_NOT_NULL"

    def test_eq_none_raises(self):
        with pytest.raises(TypeError):
            _ = self.proxy == None  # noqa: E711

    def test_ne_none_raises(self):
        with pytest.raises(TypeError):
            _ = self.proxy != None  # noqa: E711


# --- ExistsComparisonExpression AST composition ---


class TestExistsExpressionComposition:
    def test_and(self):
        a = ExistsComparisonExpression("$.events", "kind", "==", "click")
        b = ExistsComparisonExpression("$.events", "ts", ">", 100)
        result = a & b
        assert isinstance(result, LogicalExpression)
        assert result.op == "AND"

    def test_or(self):
        a = ExistsComparisonExpression("$.events", "kind", "==", "click")
        b = ExistsComparisonExpression("$.events", "kind", "==", "view")
        result = a | b
        assert isinstance(result, LogicalExpression)
        assert result.op == "OR"

    def test_invert(self):
        a = ExistsComparisonExpression("$.events", "kind", "==", "click")
        result = ~a
        assert isinstance(result, LogicalExpression)
        assert result.op == "NOT"

    def test_hash_and_eq(self):
        a = ExistsComparisonExpression("$.events", "kind", "==", "click")
        b = ExistsComparisonExpression("$.events", "kind", "==", "click")
        assert a == b
        assert hash(a) == hash(b)

    def test_ne(self):
        a = ExistsComparisonExpression("$.events", "kind", "==", "click")
        b = ExistsComparisonExpression("$.events", "kind", "==", "view")
        assert a != b


# --- SQL compilation ---


class TestCompileExists:
    def test_basic_exists(self):
        expr = ExistsComparisonExpression("$.events", "kind", "==", "click")
        params: list[object] = []
        sql = _compile_exists(expr, params, table_alias="eh")
        assert "EXISTS" in sql
        assert "json_each" in sql
        assert "json_extract(je.value, '$.kind')" in sql
        assert params == ["click"]

    def test_exists_in_filter(self):
        expr = ExistsComparisonExpression("$.events", "kind", "IN", ["click", "view"])
        params: list[object] = []
        sql = _compile_filter(expr, params, table_alias="eh")
        assert "EXISTS" in sql
        assert params == ["click", "view"]

    def test_exists_is_null(self):
        expr = ExistsComparisonExpression("$.events", "kind", "IS_NULL")
        params: list[object] = []
        sql = _compile_exists(expr, params, table_alias="eh")
        assert "IS NULL" in sql
        assert params == []

    def test_exists_combined_with_comparison(self):
        a = ComparisonExpression("$.name", "==", "Alice")
        b = ExistsComparisonExpression("$.events", "kind", "==", "click")
        expr = a & b
        params: list[object] = []
        sql = _compile_filter(expr, params, table_alias="eh")
        assert "json_extract(eh.fields_json, '$.name')" in sql
        assert "EXISTS" in sql

    def test_needs_endpoint_join_exists(self):
        expr = ExistsComparisonExpression("left.$.events", "kind", "==", "click")
        assert _needs_endpoint_join(expr, "left") is True
        assert _needs_endpoint_join(expr, "right") is False

    def test_extract_direct_filter_exists(self):
        expr = ExistsComparisonExpression("$.events", "kind", "==", "click")
        result = _extract_direct_filter(expr)
        assert result is expr

    def test_extract_direct_filter_excludes_endpoint(self):
        expr = ExistsComparisonExpression("left.$.events", "kind", "==", "click")
        result = _extract_direct_filter(expr)
        assert result is None


# --- In-process evaluation ---


class TestMatchesFilterExists:
    def test_exists_true(self):
        data = {"events": [{"kind": "click", "ts": 1}, {"kind": "view", "ts": 2}]}
        expr = ExistsComparisonExpression("$.events", "kind", "==", "click")
        assert _matches_filter(data, expr) is True

    def test_exists_false(self):
        data = {"events": [{"kind": "view", "ts": 1}]}
        expr = ExistsComparisonExpression("$.events", "kind", "==", "click")
        assert _matches_filter(data, expr) is False

    def test_exists_empty_list(self):
        data = {"events": []}
        expr = ExistsComparisonExpression("$.events", "kind", "==", "click")
        assert _matches_filter(data, expr) is False

    def test_exists_null_field(self):
        data = {"events": None}
        expr = ExistsComparisonExpression("$.events", "kind", "==", "click")
        assert _matches_filter(data, expr) is False

    def test_exists_missing_field(self):
        data = {"name": "Alice"}
        expr = ExistsComparisonExpression("$.events", "kind", "==", "click")
        assert _matches_filter(data, expr) is False

    def test_exists_gt(self):
        data = {"events": [{"kind": "click", "ts": 50}, {"kind": "view", "ts": 150}]}
        expr = ExistsComparisonExpression("$.events", "ts", ">", 100)
        assert _matches_filter(data, expr) is True

    def test_exists_nested_item_path(self):
        data = {"events": [{"meta": {"source": "web"}}]}
        expr = ExistsComparisonExpression("$.events", "meta.source", "==", "web")
        assert _matches_filter(data, expr) is True


# --- FieldProxy.any_path() ---


class TestFieldProxyAnyPath:
    def test_basic_any_path(self):
        proxy = FieldProxy("$.events")
        ap = proxy.any_path("kind")
        assert isinstance(ap, AnyPathProxy)
        assert ap._list_field_path == "$.events"
        assert ap._item_path == "kind"

    def test_any_path_on_endpoint_raises(self):
        proxy = FieldProxy("left.$.events")
        with pytest.raises(ValueError, match="endpoint"):
            proxy.any_path("kind")

        proxy2 = FieldProxy("right.$.events")
        with pytest.raises(ValueError, match="endpoint"):
            proxy2.any_path("kind")

    def test_any_path_produces_expression(self):
        proxy = FieldProxy("$.events")
        expr = proxy.any_path("kind") == "click"
        assert isinstance(expr, ExistsComparisonExpression)
        assert expr.list_field_path == "$.events"
        assert expr.item_path == "kind"


# --- End-to-end integration ---


class UserWithEvents(Entity):
    uid: Field[str] = Field(primary_key=True)
    name: Field[str]
    events: Field[list[object]] = Field(default_factory=list)
    tags: Field[list[object]] = Field(default_factory=list)


@pytest.fixture
def events_repo(tmp_path):
    db_path = str(tmp_path / "events.db")
    ont = Ontology(db_path=db_path, entity_types=[UserWithEvents])
    sess = ont.session()
    sess.ensure(
        UserWithEvents(
            uid="u1",
            name="Alice",
            events=[{"kind": "click", "ts": 1}, {"kind": "view", "ts": 2}],
            tags=["admin", "active"],
        )
    )
    sess.ensure(
        UserWithEvents(
            uid="u2",
            name="Bob",
            events=[{"kind": "view", "ts": 3}],
            tags=["active"],
        )
    )
    sess.ensure(
        UserWithEvents(
            uid="u3",
            name="Carol",
            events=[],
            tags=[],
        )
    )
    sess.commit()
    yield ont
    ont.close()


class TestExistentialEndToEnd:
    def test_query_with_exists_predicate(self, events_repo):
        results = (
            events_repo.query()
            .entities(UserWithEvents)
            .where(UserWithEvents.events.any_path("kind") == "click")
            .collect()
        )
        assert len(results) == 1
        assert results[0].name == "Alice"

    def test_query_combined_filter(self, events_repo):
        results = (
            events_repo.query()
            .entities(UserWithEvents)
            .where(
                (UserWithEvents.name == "Alice")
                & (UserWithEvents.events.any_path("kind") == "view")
            )
            .collect()
        )
        assert len(results) == 1
        assert results[0].name == "Alice"

    def test_count_where(self, events_repo):
        predicate = UserWithEvents.events.any_path("kind") == "click"
        n = events_repo.query().entities(UserWithEvents).count_where(predicate)
        assert n == 1

    def test_count_where_no_match(self, events_repo):
        predicate = UserWithEvents.events.any_path("kind") == "purchase"
        n = events_repo.query().entities(UserWithEvents).count_where(predicate)
        assert n == 0


class TestAvgLenEndToEnd:
    def test_avg_len_events(self, events_repo):
        result = events_repo.query().entities(UserWithEvents).avg_len(UserWithEvents.events)
        # u1=2, u2=1, u3=0 → avg = 1.0
        assert result == pytest.approx(1.0)

    def test_avg_len_tags(self, events_repo):
        result = events_repo.query().entities(UserWithEvents).avg_len(UserWithEvents.tags)
        # u1=2, u2=1, u3=0 → avg = 1.0
        assert result == pytest.approx(1.0)

    def test_avg_len_with_filter(self, events_repo):
        result = (
            events_repo.query()
            .entities(UserWithEvents)
            .where(UserWithEvents.name == "Alice")
            .avg_len(UserWithEvents.events)
        )
        assert result == pytest.approx(2.0)
