"""Tests for temporal query schema version boundary (RFC 0008).

Temporal queries (as_of, with_history, history_since) must only return rows
from the current schema version. Rows written under prior schema versions are
excluded to prevent Pydantic hydration failures.
"""

from __future__ import annotations

import pytest

from ontologia import Entity, Field, Relation, Session
from ontologia.query import QueryBuilder
from ontologia.storage import Repository


# --- Test entity/relation types (v2 schema – has age field) ---


class Customer(Entity):
    id: Field[str] = Field(primary_key=True)
    name: Field[str]
    age: Field[int]


class Product(Entity):
    sku: Field[str] = Field(primary_key=True)
    name: Field[str]


class Subscription(Relation[Customer, Product]):
    active: Field[bool] = Field(default=True)


# --- Fixtures ---


@pytest.fixture
def tmp_db(tmp_path):
    return str(tmp_path / "test.db")


@pytest.fixture
def repo(tmp_db):
    r = Repository(tmp_db)
    yield r
    r.close()


def _setup_two_versions(repo: Repository) -> tuple[int, int, int, int, int]:
    """Create a two-version scenario:

    - Schema version 1: Customer has only {id, name}
    - Schema version 2: Customer has {id, name, age}

    Commit 1: insert Customer "c1" under schema v1 (no age)
    Commit 2: insert Customer "c1" under schema v2 (with age) – simulates
              post-migration rewrite.
    Commit 3: insert Customer "c2" under schema v2.

    Returns (v1_id, v2_id, commit1, commit2, commit3).
    """
    # Create schema version 1
    v1 = repo.create_schema_version(
        "entity", "Customer", '{"fields":{"id":"str","name":"str"}}', "hash_v1", reason="init"
    )
    # Commit 1: insert under v1
    c1 = repo.create_commit({"source": "seed"})
    repo.insert_entity("Customer", "c1", {"id": "c1", "name": "Alice"}, c1, schema_version_id=v1)
    repo.commit_transaction()

    # Create schema version 2 (adds age)
    v2 = repo.create_schema_version(
        "entity",
        "Customer",
        '{"fields":{"id":"str","name":"str","age":"int"}}',
        "hash_v2",
        reason="add age",
    )
    # Commit 2: rewrite c1 under v2 (post-migration)
    c2 = repo.create_commit({"source": "migration"})
    repo.insert_entity(
        "Customer", "c1", {"id": "c1", "name": "Alice", "age": 30}, c2, schema_version_id=v2
    )
    repo.commit_transaction()

    # Commit 3: insert new entity under v2
    c3 = repo.create_commit({"source": "app"})
    repo.insert_entity(
        "Customer", "c2", {"id": "c2", "name": "Bob", "age": 25}, c3, schema_version_id=v2
    )
    repo.commit_transaction()

    return v1, v2, c1, c2, c3


# --- Storage layer tests ---


class TestEntityQuerySchemaVersionBoundary:
    """Test schema_version_id filtering on query_entities()."""

    def test_as_of_before_schema_change_returns_empty(self, repo):
        """as_of(commit_id=1) with schema_version_id=v2 should return empty
        because commit 1 only has v1 rows."""
        _v1, v2, c1, _c2, _c3 = _setup_two_versions(repo)

        results = repo.query_entities("Customer", as_of=c1, schema_version_id=v2)
        assert results == []

    def test_as_of_after_schema_change_returns_data(self, repo):
        """as_of(commit_id=c2) with schema_version_id=v2 should return the
        v2 rewrite of c1."""
        _v1, v2, _c1, c2, _c3 = _setup_two_versions(repo)

        results = repo.query_entities("Customer", as_of=c2, schema_version_id=v2)
        assert len(results) == 1
        assert results[0]["key"] == "c1"
        assert results[0]["fields"]["age"] == 30

    def test_as_of_latest_returns_all_v2_entities(self, repo):
        """as_of(commit_id=c3) with schema_version_id=v2 should return both
        c1 (rewritten) and c2."""
        _v1, v2, _c1, _c2, c3 = _setup_two_versions(repo)

        results = repo.query_entities("Customer", as_of=c3, schema_version_id=v2)
        assert len(results) == 2
        keys = {r["key"] for r in results}
        assert keys == {"c1", "c2"}

    def test_with_history_only_returns_current_version_rows(self, repo):
        """with_history() with schema_version_id=v2 should only return rows
        written under v2, skipping the v1 row."""
        _v1, v2, _c1, _c2, _c3 = _setup_two_versions(repo)

        results = repo.query_entities("Customer", with_history=True, schema_version_id=v2)
        # Should have 2 rows: c1 rewrite (commit 2) and c2 (commit 3)
        assert len(results) == 2
        for r in results:
            assert "age" in r["fields"]

    def test_history_since_spanning_schema_change(self, repo):
        """history_since(commit_id=c1) with schema_version_id=v2 should only
        return rows from v2, even though v1 rows exist after c1."""
        _v1, v2, c1, c2, c3 = _setup_two_versions(repo)

        results = repo.query_entities("Customer", history_since=c1, schema_version_id=v2)
        # Should return commit 2 (c1 rewrite) and commit 3 (c2 insert)
        assert len(results) == 2
        commit_ids = [r["commit_id"] for r in results]
        assert c2 in commit_ids
        assert c3 in commit_ids

    def test_latest_query_unaffected_by_schema_version_id(self, repo):
        """Non-temporal (latest) queries should return results regardless of
        schema_version_id — the filter only applies to temporal queries."""
        _v1, v2, _c1, _c2, _c3 = _setup_two_versions(repo)

        # Latest query with schema_version_id should not filter
        results = repo.query_entities("Customer", schema_version_id=v2)
        assert len(results) == 2
        keys = {r["key"] for r in results}
        assert keys == {"c1", "c2"}

    def test_no_schema_version_id_returns_all_history(self, repo):
        """When schema_version_id is None, temporal queries return all rows
        (backwards compatible)."""
        _v1, _v2, _c1, _c2, _c3 = _setup_two_versions(repo)

        results = repo.query_entities("Customer", with_history=True)
        # Should return all 3 rows (v1 + v2)
        assert len(results) == 3

    def test_pre_versioning_data_excluded(self, repo):
        """Rows with NULL schema_version_id should not match a specific
        schema_version_id filter."""
        # Insert without schema_version_id
        cid = repo.create_commit({"source": "old"})
        repo.insert_entity("Customer", "c0", {"id": "c0", "name": "Old"}, cid)
        repo.commit_transaction()

        v1 = repo.create_schema_version(
            "entity", "Customer", '{"fields":{}}', "hash_v1", reason="init"
        )

        results = repo.query_entities("Customer", with_history=True, schema_version_id=v1)
        assert results == []


class TestRelationQuerySchemaVersionBoundary:
    """Test schema_version_id filtering on query_relations()."""

    def test_with_history_filters_relation_versions(self, repo):
        """Relation with_history() should only return current-version rows."""
        v1 = repo.create_schema_version(
            "relation", "Subscription", '{"fields":{"active":"bool"}}', "r_hash_v1", reason="init"
        )

        c1 = repo.create_commit()
        repo.insert_relation("Subscription", "c1", "p1", {"active": True}, c1, schema_version_id=v1)
        repo.commit_transaction()

        v2 = repo.create_schema_version(
            "relation",
            "Subscription",
            '{"fields":{"active":"bool","tier":"str"}}',
            "r_hash_v2",
            reason="add tier",
        )

        c2 = repo.create_commit()
        repo.insert_relation(
            "Subscription", "c1", "p1", {"active": True, "tier": "Gold"}, c2, schema_version_id=v2
        )
        repo.commit_transaction()

        # with_history filtered to v2 should only return 1 row
        results = repo.query_relations("Subscription", with_history=True, schema_version_id=v2)
        assert len(results) == 1
        assert results[0]["fields"].get("tier") == "Gold"

    def test_as_of_relation_filters_schema_version(self, repo):
        """Relation as_of() should filter by schema version."""
        v1 = repo.create_schema_version(
            "relation", "Subscription", '{"fields":{}}', "r_hash_v1", reason="init"
        )
        c1 = repo.create_commit()
        repo.insert_relation("Subscription", "c1", "p1", {"active": True}, c1, schema_version_id=v1)
        repo.commit_transaction()

        v2 = repo.create_schema_version(
            "relation", "Subscription", '{"fields":{"tier":"str"}}', "r_hash_v2", reason="v2"
        )

        # as_of(c1) with v2 should return empty (c1 only has v1 data)
        results = repo.query_relations("Subscription", as_of=c1, schema_version_id=v2)
        assert results == []


class TestEdgeCasesWithFilters:
    """Verify schema_version_id + filter_expr parameter ordering is correct."""

    def test_as_of_with_filter_and_schema_version(self, repo):
        """Combining as_of, filter_expr, and schema_version_id must produce
        correct SQL parameter ordering."""
        _v1, v2, _c1, _c2, c3 = _setup_two_versions(repo)

        from ontologia.filters import ComparisonExpression

        # Filter for name == "Alice"
        name_filter = ComparisonExpression("$.name", "==", "Alice")

        results = repo.query_entities(
            "Customer",
            filter_expr=name_filter,
            as_of=c3,
            schema_version_id=v2,
        )
        assert len(results) == 1
        assert results[0]["key"] == "c1"
        assert results[0]["fields"]["name"] == "Alice"

    def test_with_history_filter_and_schema_version(self, repo):
        """Combining with_history, filter_expr, and schema_version_id."""
        _v1, v2, _c1, _c2, _c3 = _setup_two_versions(repo)

        from ontologia.filters import ComparisonExpression

        name_filter = ComparisonExpression("$.name", "==", "Bob")

        results = repo.query_entities(
            "Customer",
            filter_expr=name_filter,
            with_history=True,
            schema_version_id=v2,
        )
        assert len(results) == 1
        assert results[0]["key"] == "c2"

    def test_history_since_with_filter_and_schema_version(self, repo):
        """Combining history_since, filter_expr, and schema_version_id."""
        _v1, v2, c1, _c2, _c3 = _setup_two_versions(repo)

        from ontologia.filters import ComparisonExpression

        age_filter = ComparisonExpression("$.age", ">", 20)

        results = repo.query_entities(
            "Customer",
            filter_expr=age_filter,
            history_since=c1,
            schema_version_id=v2,
        )
        # Both v2 rows (c1 rewrite with age=30, c2 with age=25) should match
        assert len(results) == 2

    def test_as_of_with_order_and_schema_version(self, repo):
        """Combining as_of, order_by, and schema_version_id."""
        _v1, v2, _c1, _c2, c3 = _setup_two_versions(repo)

        results = repo.query_entities(
            "Customer",
            as_of=c3,
            schema_version_id=v2,
            order_by="$.name",
            order_desc=True,
        )
        assert len(results) == 2
        assert results[0]["key"] == "c2"  # Bob
        assert results[1]["key"] == "c1"  # Alice


# --- Query DSL integration tests ---


class TestQueryDSLSchemaVersionThreading:
    """Verify schema_version_id is threaded from QueryBuilder to storage."""

    def test_entity_query_threads_schema_version(self, repo):
        """EntityQuery should pass schema_version_id through to storage."""
        _v1, v2, _c1, _c2, _c3 = _setup_two_versions(repo)

        qb = QueryBuilder(repo, schema_version_ids={"Customer": v2})
        results = qb.entities(Customer).with_history().collect()

        # Should only get v2 rows (2 of 3 total)
        assert len(results) == 2
        for c in results:
            assert hasattr(c, "age")

    def test_entity_query_as_of_pre_version(self, repo):
        """EntityQuery.as_of() before current schema version returns empty."""
        _v1, v2, c1, _c2, _c3 = _setup_two_versions(repo)

        qb = QueryBuilder(repo, schema_version_ids={"Customer": v2})
        results = qb.entities(Customer).as_of(c1).collect()
        assert results == []

    def test_entity_query_no_schema_version_ids_can_cause_hydration_error(self, repo):
        """QueryBuilder without schema_version_ids allows cross-version rows,
        which can cause hydration failures — this is the bug that RFC 0008 fixes."""
        _v1, _v2, c1, _c2, _c3 = _setup_two_versions(repo)

        qb = QueryBuilder(repo)
        # Without schema_version_ids filtering, as_of(c1) returns v1 data
        # which lacks the 'age' field required by the current Customer schema.
        # This demonstrates the exact bug that schema version filtering prevents.
        with pytest.raises(Exception):
            qb.entities(Customer).as_of(c1).collect()

    def test_session_threads_schema_version(self, tmp_db):
        """Session.query() should thread schema_version_ids from Ontology."""
        session = Session(
            tmp_db,
            entity_types=[Customer, Product],
            relation_types=[Subscription],
        )
        # After validation, schema_version_ids should be populated
        session._ensure_schema_validated()
        svids = session._ontology._schema_version_ids
        assert "Customer" in svids

        # Insert data under the current schema version
        repo = session._repo
        sv_customer = svids["Customer"]
        c1 = repo.create_commit({"source": "test"})
        repo.insert_entity(
            "Customer",
            "c1",
            {"id": "c1", "name": "Alice", "age": 30},
            c1,
            schema_version_id=sv_customer,
        )
        repo.commit_transaction()

        # Query via session should work
        results = session.query().entities(Customer).with_history().collect()
        assert len(results) == 1
        assert results[0].name == "Alice"
