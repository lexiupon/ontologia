"""Tests for the storage layer: Repository, CRUD, locking."""

from __future__ import annotations

import pytest

from ontologia.filters import ComparisonExpression


class TestCommits:
    def test_create_commit(self, repo):
        cid = repo.create_commit({"source": "test"})
        assert cid == 1

    def test_get_head_commit_id_empty(self, repo):
        assert repo.get_head_commit_id() is None

    def test_get_head_commit_id(self, repo):
        repo.create_commit()
        repo.create_commit()
        assert repo.get_head_commit_id() == 2

    def test_get_commit(self, repo):
        cid = repo.create_commit({"source": "test"})
        commit = repo.get_commit(cid)
        assert commit is not None
        assert commit["id"] == cid
        assert commit["metadata"] == {"source": "test"}
        assert commit["created_at"] is not None

    def test_get_commit_not_found(self, repo):
        assert repo.get_commit(999) is None

    def test_list_commits(self, repo):
        for i in range(5):
            repo.create_commit({"n": str(i)})
        commits = repo.list_commits(limit=3)
        assert len(commits) == 3
        # Most recent first
        assert commits[0]["id"] == 5
        assert commits[2]["id"] == 3

    def test_list_commits_since(self, repo):
        for _ in range(5):
            repo.create_commit()
        commits = repo.list_commits(since_commit_id=3)
        assert len(commits) == 2
        assert all(c["id"] > 3 for c in commits)


class TestEntityOperations:
    def test_insert_and_get_entity(self, repo):
        cid = repo.create_commit()
        repo.insert_entity("Customer", "c1", {"id": "c1", "name": "Alice"}, cid)
        repo.commit_transaction()

        result = repo.get_latest_entity("Customer", "c1")
        assert result is not None
        assert result["fields"]["name"] == "Alice"
        assert result["commit_id"] == cid

    def test_get_entity_not_found(self, repo):
        assert repo.get_latest_entity("Customer", "nonexistent") is None

    def test_entity_append_history(self, repo):
        c1 = repo.create_commit()
        repo.insert_entity("Customer", "c1", {"id": "c1", "name": "Alice"}, c1)
        repo.commit_transaction()

        c2 = repo.create_commit()
        repo.insert_entity("Customer", "c1", {"id": "c1", "name": "Alice Updated"}, c2)
        repo.commit_transaction()

        # Latest should be updated
        result = repo.get_latest_entity("Customer", "c1")
        assert result["fields"]["name"] == "Alice Updated"
        assert result["commit_id"] == c2

    def test_query_entities_latest(self, repo):
        c1 = repo.create_commit()
        repo.insert_entity("Customer", "c1", {"id": "c1", "name": "Alice", "age": 30}, c1)
        repo.insert_entity("Customer", "c2", {"id": "c2", "name": "Bob", "age": 25}, c1)
        repo.commit_transaction()

        rows = repo.query_entities("Customer")
        assert len(rows) == 2

    def test_query_entities_with_filter(self, repo):
        c1 = repo.create_commit()
        repo.insert_entity("Customer", "c1", {"id": "c1", "name": "Alice", "age": 30}, c1)
        repo.insert_entity("Customer", "c2", {"id": "c2", "name": "Bob", "age": 25}, c1)
        repo.commit_transaction()

        expr = ComparisonExpression("$.age", ">", 27)
        rows = repo.query_entities("Customer", filter_expr=expr)
        assert len(rows) == 1
        assert rows[0]["fields"]["name"] == "Alice"

    def test_query_entities_pagination(self, repo):
        c1 = repo.create_commit()
        for i in range(10):
            repo.insert_entity("Customer", f"c{i}", {"id": f"c{i}", "name": f"N{i}", "age": i}, c1)
        repo.commit_transaction()

        rows = repo.query_entities("Customer", order_by="$.id", limit=3, offset=0)
        assert len(rows) == 3

        rows2 = repo.query_entities("Customer", order_by="$.id", limit=3, offset=3)
        assert len(rows2) == 3
        assert rows[0]["key"] != rows2[0]["key"]

    def test_query_entities_with_history(self, repo):
        c1 = repo.create_commit()
        repo.insert_entity("Customer", "c1", {"id": "c1", "name": "V1"}, c1)
        repo.commit_transaction()

        c2 = repo.create_commit()
        repo.insert_entity("Customer", "c1", {"id": "c1", "name": "V2"}, c2)
        repo.commit_transaction()

        rows = repo.query_entities("Customer", with_history=True)
        assert len(rows) == 2

    def test_query_entities_history_since(self, repo):
        c1 = repo.create_commit()
        repo.insert_entity("Customer", "c1", {"id": "c1", "name": "V1"}, c1)
        repo.commit_transaction()

        c2 = repo.create_commit()
        repo.insert_entity("Customer", "c1", {"id": "c1", "name": "V2"}, c2)
        repo.commit_transaction()

        rows = repo.query_entities("Customer", history_since=c1)
        assert len(rows) == 1
        assert rows[0]["commit_id"] == c2

    def test_query_entities_as_of(self, repo):
        c1 = repo.create_commit()
        repo.insert_entity("Customer", "c1", {"id": "c1", "name": "V1"}, c1)
        repo.commit_transaction()

        c2 = repo.create_commit()
        repo.insert_entity("Customer", "c1", {"id": "c1", "name": "V2"}, c2)
        repo.commit_transaction()

        rows = repo.query_entities("Customer", as_of=c1)
        assert len(rows) == 1
        assert rows[0]["fields"]["name"] == "V1"

    def test_count_entities(self, repo):
        c1 = repo.create_commit()
        repo.insert_entity("Customer", "c1", {"id": "c1", "age": 30}, c1)
        repo.insert_entity("Customer", "c2", {"id": "c2", "age": 25}, c1)
        repo.commit_transaction()

        assert repo.count_entities("Customer") == 2

        expr = ComparisonExpression("$.age", ">", 27)
        assert repo.count_entities("Customer", filter_expr=expr) == 1

    def test_aggregate_entities(self, repo):
        c1 = repo.create_commit()
        repo.insert_entity("Order", "o1", {"total": 100}, c1)
        repo.insert_entity("Order", "o2", {"total": 200}, c1)
        repo.insert_entity("Order", "o3", {"total": 300}, c1)
        repo.commit_transaction()

        assert repo.aggregate_entities("Order", "SUM", "total") == 600
        assert repo.aggregate_entities("Order", "AVG", "total") == 200
        assert repo.aggregate_entities("Order", "MIN", "total") == 100
        assert repo.aggregate_entities("Order", "MAX", "total") == 300

    def test_group_by_entities(self, repo):
        c1 = repo.create_commit()
        repo.insert_entity("Order", "o1", {"country": "US", "total": 100}, c1)
        repo.insert_entity("Order", "o2", {"country": "US", "total": 200}, c1)
        repo.insert_entity("Order", "o3", {"country": "UK", "total": 300}, c1)
        repo.commit_transaction()

        results = repo.group_by_entities(
            "Order",
            "country",
            {"order_count": ("COUNT", None), "total_amount": ("SUM", "total")},
        )
        assert len(results) == 2
        us = next(r for r in results if r["country"] == "US")
        assert us["order_count"] == 2
        assert us["total_amount"] == 300


class TestRelationOperations:
    def test_insert_and_get_relation(self, repo):
        cid = repo.create_commit()
        repo.insert_relation("Subscription", "c1", "p1", {"seat_count": 5, "active": True}, cid)
        repo.commit_transaction()

        result = repo.get_latest_relation("Subscription", "c1", "p1")
        assert result is not None
        assert result["fields"]["seat_count"] == 5

    def test_relation_not_found(self, repo):
        assert repo.get_latest_relation("Subscription", "c1", "p1") is None

    def test_relation_append_history(self, repo):
        c1 = repo.create_commit()
        repo.insert_relation("Subscription", "c1", "p1", {"seats": 5}, c1)
        repo.commit_transaction()

        c2 = repo.create_commit()
        repo.insert_relation("Subscription", "c1", "p1", {"seats": 10}, c2)
        repo.commit_transaction()

        result = repo.get_latest_relation("Subscription", "c1", "p1")
        assert result["fields"]["seats"] == 10
        assert result["commit_id"] == c2

    def test_query_relations(self, repo):
        c1 = repo.create_commit()
        repo.insert_relation("Sub", "c1", "p1", {"seats": 5}, c1)
        repo.insert_relation("Sub", "c1", "p2", {"seats": 10}, c1)
        repo.commit_transaction()

        rows = repo.query_relations("Sub")
        assert len(rows) == 2

    def test_query_relations_with_filter(self, repo):
        c1 = repo.create_commit()
        repo.insert_relation("Sub", "c1", "p1", {"seats": 5}, c1)
        repo.insert_relation("Sub", "c1", "p2", {"seats": 10}, c1)
        repo.commit_transaction()

        expr = ComparisonExpression("$.seats", ">", 7)
        rows = repo.query_relations("Sub", filter_expr=expr)
        assert len(rows) == 1
        assert rows[0]["fields"]["seats"] == 10

    def test_query_relations_left_endpoint_filter_honors_as_of(self, repo):
        c1 = repo.create_commit()
        repo.insert_entity("Customer", "c1", {"id": "c1", "tier": "Gold"}, c1)
        repo.insert_relation("Sub", "c1", "p1", {"seats": 5}, c1)
        repo.commit_transaction()

        c2 = repo.create_commit()
        repo.insert_entity("Customer", "c1", {"id": "c1", "tier": "Silver"}, c2)
        repo.commit_transaction()

        expr = ComparisonExpression("left.$.tier", "==", "Gold")
        latest_rows = repo.query_relations("Sub", left_entity_type="Customer", filter_expr=expr)
        assert latest_rows == []

        as_of_rows = repo.query_relations(
            "Sub",
            left_entity_type="Customer",
            filter_expr=expr,
            as_of=c1,
        )
        assert len(as_of_rows) == 1
        assert as_of_rows[0]["left_key"] == "c1"

    def test_query_relations_endpoint_filter_requires_endpoint_type(self, repo):
        c1 = repo.create_commit()
        repo.insert_relation("Sub", "c1", "p1", {"seats": 5}, c1)
        repo.commit_transaction()

        expr = ComparisonExpression("left.$.tier", "==", "Gold")
        with pytest.raises(ValueError):
            repo.query_relations("Sub", filter_expr=expr)

    def test_count_relations(self, repo):
        c1 = repo.create_commit()
        repo.insert_relation("Sub", "c1", "p1", {"seats": 5}, c1)
        repo.insert_relation("Sub", "c1", "p2", {"seats": 10}, c1)
        repo.commit_transaction()

        assert repo.count_relations("Sub") == 2

    def test_get_relations_for_entity(self, repo):
        c1 = repo.create_commit()
        repo.insert_relation("Sub", "c1", "p1", {"seats": 5}, c1)
        repo.insert_relation("Sub", "c1", "p2", {"seats": 10}, c1)
        repo.insert_relation("Sub", "c2", "p1", {"seats": 3}, c1)
        repo.commit_transaction()

        rows = repo.get_relations_for_entity("Sub", "Customer", "c1", direction="left")
        assert len(rows) == 2


class TestLocking:
    def test_acquire_release_lock(self, repo):
        assert repo.acquire_lock("owner-1") is True
        repo.release_lock("owner-1")

    def test_lock_contention(self, repo):
        assert repo.acquire_lock("owner-1") is True
        # Second acquire should fail with short timeout
        assert repo.acquire_lock("owner-2", timeout_ms=100) is False
        repo.release_lock("owner-1")

    def test_lock_after_release(self, repo):
        assert repo.acquire_lock("owner-1") is True
        repo.release_lock("owner-1")
        assert repo.acquire_lock("owner-2") is True
        repo.release_lock("owner-2")

    def test_renew_lock(self, repo):
        assert repo.acquire_lock("owner-1") is True
        assert repo.renew_lock("owner-1") is True
        repo.release_lock("owner-1")

    def test_renew_wrong_owner(self, repo):
        assert repo.acquire_lock("owner-1") is True
        assert repo.renew_lock("owner-2") is False
        repo.release_lock("owner-1")


class TestSchemaRegistry:
    def test_store_and_get_schema(self, repo):
        schema = {"entity_name": "Customer", "fields": {"id": {}, "name": {}}}
        repo.store_schema("entity", "Customer", schema)
        result = repo.get_schema("entity", "Customer")
        assert result == schema

    def test_get_schema_not_found(self, repo):
        assert repo.get_schema("entity", "Nonexistent") is None

    def test_list_schemas(self, repo):
        repo.store_schema("entity", "Customer", {"fields": {}})
        repo.store_schema("entity", "Product", {"fields": {}})
        schemas = repo.list_schemas("entity")
        assert len(schemas) == 2

    def test_store_schema_upsert(self, repo):
        repo.store_schema("entity", "Customer", {"fields": {"id": {}}})
        repo.store_schema("entity", "Customer", {"fields": {"id": {}, "name": {}}})
        result = repo.get_schema("entity", "Customer")
        assert "name" in result["fields"]
