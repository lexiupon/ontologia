"""Tests for RFC 0006 Phase 1: nested path queries and Phase 2: existential predicates."""

from __future__ import annotations

from typing import TypedDict

import pytest

from ontologia import Entity, Field
from ontologia.filters import (
    ComparisonExpression,
    FieldProxy,
    resolve_nested_path,
    _validate_path,
    _validate_segment,
)
from ontologia.runtime import Ontology, _compare_value, _matches_filter
from ontologia.storage import _compile_filter


# --- Path validation ---


class TestValidateSegment:
    def test_valid_segments(self):
        _validate_segment("city")
        _validate_segment("_private")
        _validate_segment("CamelCase")
        _validate_segment("a1")

    def test_invalid_segments(self):
        with pytest.raises(ValueError, match="Invalid path segment"):
            _validate_segment("")
        with pytest.raises(ValueError, match="Invalid path segment"):
            _validate_segment("1abc")
        with pytest.raises(ValueError, match="Invalid path segment"):
            _validate_segment("foo-bar")
        with pytest.raises(ValueError, match="Invalid path segment"):
            _validate_segment("foo.bar")
        with pytest.raises(ValueError, match="Invalid path segment"):
            _validate_segment("*")


class TestValidatePath:
    def test_valid_paths(self):
        _validate_path("city")
        _validate_path("address.city")
        _validate_path("profile.address.zip_code")

    def test_empty_path(self):
        with pytest.raises(ValueError, match="must not be empty"):
            _validate_path("")

    def test_invalid_path_segment(self):
        with pytest.raises(ValueError, match="Invalid path segment"):
            _validate_path("address.1city")
        with pytest.raises(ValueError, match="Invalid path segment"):
            _validate_path("address.*")


# --- FieldProxy.path() / __getitem__() ---


class TestFieldProxyPath:
    def test_path_composition(self):
        proxy = FieldProxy("$.profile")
        nested = proxy.path("address.city")
        assert nested._field_path == "$.profile.address.city"

    def test_getitem_composition(self):
        proxy = FieldProxy("$.profile")
        nested = proxy["address"]
        assert nested._field_path == "$.profile.address"

    def test_chaining(self):
        proxy = FieldProxy("$.profile")
        nested = proxy["address"]["city"]
        assert nested._field_path == "$.profile.address.city"

    def test_path_then_getitem(self):
        proxy = FieldProxy("$.profile")
        nested = proxy.path("address")["city"]
        assert nested._field_path == "$.profile.address.city"

    def test_comparison_from_nested(self):
        proxy = FieldProxy("$.profile")
        expr = proxy.path("address.city") == "SF"
        assert isinstance(expr, ComparisonExpression)
        assert expr.field_path == "$.profile.address.city"
        assert expr.op == "=="
        assert expr.value == "SF"

    def test_invalid_path(self):
        proxy = FieldProxy("$.profile")
        with pytest.raises(ValueError):
            proxy.path("address.*")

    def test_invalid_getitem(self):
        proxy = FieldProxy("$.profile")
        with pytest.raises(ValueError):
            proxy["1invalid"]


# --- resolve_nested_path ---


class TestResolveNestedPath:
    def test_simple_key(self):
        assert resolve_nested_path({"name": "Alice"}, "name") == "Alice"

    def test_nested_path(self):
        data = {"profile": {"address": {"city": "SF"}}}
        assert resolve_nested_path(data, "profile.address.city") == "SF"

    def test_missing_key(self):
        assert resolve_nested_path({"a": 1}, "b") is None

    def test_missing_intermediate(self):
        assert resolve_nested_path({"profile": {"x": 1}}, "profile.address.city") is None

    def test_none_intermediate(self):
        assert resolve_nested_path({"profile": None}, "profile.city") is None

    def test_non_dict_intermediate(self):
        assert resolve_nested_path({"profile": "string"}, "profile.city") is None


# --- SQL compilation with nested paths ---


class TestNestedPathSQLCompilation:
    def test_nested_path_compiles_to_json_extract(self):
        expr = ComparisonExpression("$.profile.address.city", "==", "SF")
        params: list[object] = []
        sql = _compile_filter(expr, params, table_alias="eh")
        assert "json_extract(eh.fields_json, '$.profile.address.city')" in sql
        assert params == ["SF"]

    def test_endpoint_nested_path(self):
        expr = ComparisonExpression("left.$.profile.city", "==", "SF")
        params: list[object] = []
        sql = _compile_filter(expr, params, table_alias="rh")
        assert "json_extract(le.fields_json, '$.profile.city')" in sql

    def test_right_endpoint_nested(self):
        expr = ComparisonExpression("right.$.config.tier", ">=", 3)
        params: list[object] = []
        sql = _compile_filter(expr, params, table_alias="rh")
        assert "json_extract(re.fields_json, '$.config.tier')" in sql


# --- In-process filter with nested data ---


class TestMatchesFilterNested:
    def test_nested_equality(self):
        data = {"profile": {"address": {"city": "SF"}}}
        expr = ComparisonExpression("$.profile.address.city", "==", "SF")
        assert _matches_filter(data, expr) is True

    def test_nested_inequality(self):
        data = {"profile": {"address": {"city": "LA"}}}
        expr = ComparisonExpression("$.profile.address.city", "==", "SF")
        assert _matches_filter(data, expr) is False

    def test_nested_missing_path(self):
        data = {"profile": {"x": 1}}
        expr = ComparisonExpression("$.profile.address.city", "==", "SF")
        assert _matches_filter(data, expr) is False

    def test_nested_gt(self):
        data = {"metrics": {"spend": 150.0}}
        expr = ComparisonExpression("$.metrics.spend", ">", 100.0)
        assert _matches_filter(data, expr) is True

    def test_nested_is_null(self):
        data = {"profile": {"city": None}}
        expr = ComparisonExpression("$.profile.city", "IS_NULL")
        assert _matches_filter(data, expr) is True


# --- End-to-end with SQLite ---


class Address(TypedDict):
    city: str
    zip_code: str


class Profile(TypedDict):
    address: Address
    score: int


class UserWithProfile(Entity):
    uid: Field[str] = Field(primary_key=True)
    name: Field[str]
    profile: Field[dict[str, object]]  # Actually stores Profile-shaped dicts


@pytest.fixture
def profile_repo(tmp_path):
    db_path = str(tmp_path / "profile.db")
    ont = Ontology(db_path=db_path, entity_types=[UserWithProfile])
    sess = ont.session()
    sess.ensure(
        UserWithProfile(
            uid="u1",
            name="Alice",
            profile={"address": {"city": "SF", "zip_code": "94102"}, "score": 95},
        )
    )
    sess.ensure(
        UserWithProfile(
            uid="u2",
            name="Bob",
            profile={"address": {"city": "LA", "zip_code": "90001"}, "score": 80},
        )
    )
    sess.ensure(
        UserWithProfile(
            uid="u3",
            name="Carol",
            profile={"address": {"city": "SF", "zip_code": "94103"}, "score": 70},
        )
    )
    sess.commit()
    yield ont
    ont.close()


class TestNestedPathEndToEnd:
    def test_filter_by_nested_city(self, profile_repo):
        results = (
            profile_repo.query()
            .entities(UserWithProfile)
            .where(UserWithProfile.profile.path("address.city") == "SF")
            .collect()
        )
        assert len(results) == 2
        names = {r.name for r in results}
        assert names == {"Alice", "Carol"}

    def test_filter_by_nested_score_gt(self, profile_repo):
        results = (
            profile_repo.query()
            .entities(UserWithProfile)
            .where(UserWithProfile.profile.path("score") > 85)
            .collect()
        )
        assert len(results) == 1
        assert results[0].name == "Alice"

    def test_count_with_nested_filter(self, profile_repo):
        n = (
            profile_repo.query()
            .entities(UserWithProfile)
            .where(UserWithProfile.profile.path("address.city") == "LA")
            .count()
        )
        assert n == 1

    def test_getitem_chaining(self, profile_repo):
        results = (
            profile_repo.query()
            .entities(UserWithProfile)
            .where(UserWithProfile.profile["address"]["city"] == "SF")
            .collect()
        )
        assert len(results) == 2


# --- _compare_value helper ---


class TestCompareValue:
    def test_eq(self):
        assert _compare_value(5, "==", 5) is True
        assert _compare_value(5, "==", 6) is False

    def test_ne(self):
        assert _compare_value(5, "!=", 6) is True

    def test_gt(self):
        assert _compare_value(5, ">", 3) is True
        assert _compare_value(None, ">", 3) is False

    def test_in(self):
        assert _compare_value("a", "IN", ["a", "b"]) is True
        assert _compare_value("c", "IN", ["a", "b"]) is False

    def test_is_null(self):
        assert _compare_value(None, "IS_NULL", None) is True
        assert _compare_value(5, "IS_NULL", None) is False

    def test_like(self):
        assert _compare_value("hello", "LIKE", "%llo") is True
        assert _compare_value("hello", "LIKE", "hel%") is True
        assert _compare_value("hello", "LIKE", "%ell%") is True
        assert _compare_value(None, "LIKE", "%x%") is False
