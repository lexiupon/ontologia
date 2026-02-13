"""Tests for the query DSL: EntityQuery, RelationQuery, TraversalQuery, aggregations."""

from __future__ import annotations

import pytest

from ontologia import Session
from ontologia.filters import left, right
from ontologia.query import Path, count, sum
from tests.conftest import Customer, Follows, Order, Product, Subscription, Wishlisted


@pytest.fixture
def onto(tmp_db):
    """Create an Ontology with test types, no handlers."""
    o = Session(
        tmp_db,
        entity_types=[Customer, Product, Order],
        relation_types=[Subscription, Follows, Wishlisted],
    )
    return o


@pytest.fixture
def seeded_onto(onto):
    """Ontology with some initial data."""
    repo = onto._repo

    # Insert some entities
    cid = repo.create_commit({"source": "seed"})
    repo.insert_entity(
        "Customer",
        "c1",
        {
            "id": "c1",
            "name": "Alice",
            "age": 30,
            "email": "alice@test.com",
            "tier": "Gold",
            "active": True,
        },
        cid,
    )
    repo.insert_entity(
        "Customer",
        "c2",
        {
            "id": "c2",
            "name": "Bob",
            "age": 25,
            "email": "bob@test.com",
            "tier": "Standard",
            "active": True,
        },
        cid,
    )
    repo.insert_entity(
        "Customer",
        "c3",
        {"id": "c3", "name": "Charlie", "age": 40, "email": None, "tier": "Gold", "active": False},
        cid,
    )
    repo.insert_entity(
        "Product", "p1", {"sku": "p1", "name": "Widget", "price": 9.99, "category": "Tools"}, cid
    )
    repo.insert_entity(
        "Product",
        "p2",
        {"sku": "p2", "name": "Gadget", "price": 49.99, "category": "Electronics"},
        cid,
    )

    # Insert relations
    repo.insert_relation(
        "Subscription",
        "c1",
        "p1",
        {"seat_count": 5, "started_at": "2024-01-15", "active": True},
        cid,
    )
    repo.insert_relation(
        "Subscription",
        "c1",
        "p2",
        {"seat_count": 10, "started_at": "2024-02-01", "active": True},
        cid,
    )
    repo.insert_relation(
        "Subscription",
        "c2",
        "p1",
        {"seat_count": 2, "started_at": "2024-03-01", "active": False},
        cid,
    )
    repo.commit_transaction()

    # Insert orders for aggregation tests
    c2 = repo.create_commit()
    repo.insert_entity(
        "Order",
        "o1",
        {
            "id": "o1",
            "customer_id": "c1",
            "total_amount": 100.0,
            "status": "Completed",
            "country": "US",
        },
        c2,
    )
    repo.insert_entity(
        "Order",
        "o2",
        {
            "id": "o2",
            "customer_id": "c1",
            "total_amount": 200.0,
            "status": "Completed",
            "country": "US",
        },
        c2,
    )
    repo.insert_entity(
        "Order",
        "o3",
        {
            "id": "o3",
            "customer_id": "c2",
            "total_amount": 150.0,
            "status": "Pending",
            "country": "UK",
        },
        c2,
    )
    repo.commit_transaction()

    return onto


class TestEntityQuery:
    def test_collect_all(self, seeded_onto):
        customers = seeded_onto.query().entities(Customer).collect()
        assert len(customers) == 3
        assert all(isinstance(c, Customer) for c in customers)

    def test_where_eq(self, seeded_onto):
        results = seeded_onto.query().entities(Customer).where(Customer.name == "Alice").collect()
        assert len(results) == 1
        assert results[0].name == "Alice"

    def test_where_gt(self, seeded_onto):
        results = seeded_onto.query().entities(Customer).where(Customer.age > 27).collect()
        assert len(results) == 2
        names = {c.name for c in results}
        assert names == {"Alice", "Charlie"}

    def test_where_combined(self, seeded_onto):
        results = (
            seeded_onto.query()
            .entities(Customer)
            .where((Customer.tier == "Gold") & Customer.active.is_true())
            .collect()
        )
        assert len(results) == 1
        assert results[0].name == "Alice"

    def test_where_or(self, seeded_onto):
        results = (
            seeded_onto.query()
            .entities(Customer)
            .where((Customer.name == "Alice") | (Customer.name == "Bob"))
            .collect()
        )
        assert len(results) == 2

    def test_where_startswith(self, seeded_onto):
        results = (
            seeded_onto.query().entities(Customer).where(Customer.name.startswith("A")).collect()
        )
        assert len(results) == 1
        assert results[0].name == "Alice"

    def test_where_in(self, seeded_onto):
        results = (
            seeded_onto.query().entities(Customer).where(Customer.tier.in_(["Gold"])).collect()
        )
        assert len(results) == 2

    def test_where_is_null(self, seeded_onto):
        results = seeded_onto.query().entities(Customer).where(Customer.email.is_null()).collect()
        assert len(results) == 1
        assert results[0].name == "Charlie"

    def test_where_is_not_null(self, seeded_onto):
        results = (
            seeded_onto.query().entities(Customer).where(Customer.email.is_not_null()).collect()
        )
        assert len(results) == 2

    def test_first(self, seeded_onto):
        result = seeded_onto.query().entities(Customer).where(Customer.name == "Alice").first()
        assert result is not None
        assert result.name == "Alice"

    def test_first_not_found(self, seeded_onto):
        result = (
            seeded_onto.query().entities(Customer).where(Customer.name == "Nonexistent").first()
        )
        assert result is None

    def test_pagination(self, seeded_onto):
        page1 = (
            seeded_onto.query()
            .entities(Customer)
            .order_by(Customer.id)
            .limit(2)
            .offset(0)
            .collect()
        )
        assert len(page1) == 2

        page2 = (
            seeded_onto.query()
            .entities(Customer)
            .order_by(Customer.id)
            .limit(2)
            .offset(2)
            .collect()
        )
        assert len(page2) == 1

    def test_metadata_on_hydrated(self, seeded_onto):
        customers = seeded_onto.query().entities(Customer).collect()
        for c in customers:
            m = c.meta()
            assert m.commit_id > 0
            assert m.type_name == "Customer"
            assert m.key == c.id


class TestEntityHistory:
    def test_with_history(self, seeded_onto):
        # Update Alice
        repo = seeded_onto._repo
        c2 = repo.create_commit()
        repo.insert_entity(
            "Customer",
            "c1",
            {
                "id": "c1",
                "name": "Alice Updated",
                "age": 31,
                "email": "alice@test.com",
                "tier": "Platinum",
                "active": True,
            },
            c2,
        )
        repo.commit_transaction()

        history = (
            seeded_onto.query()
            .entities(Customer)
            .where(Customer.id == "c1")
            .with_history()
            .collect()
        )
        assert len(history) == 2
        assert history[0].name == "Alice"
        assert history[1].name == "Alice Updated"

    def test_history_since(self, seeded_onto):
        first_commit = seeded_onto._repo.get_head_commit_id()

        repo = seeded_onto._repo
        c2 = repo.create_commit()
        repo.insert_entity(
            "Customer",
            "c1",
            {
                "id": "c1",
                "name": "Alice V2",
                "age": 31,
                "email": "alice@test.com",
                "tier": "Gold",
                "active": True,
            },
            c2,
        )
        repo.commit_transaction()

        changes = (
            seeded_onto.query().entities(Customer).history_since(commit_id=first_commit).collect()
        )
        assert len(changes) == 1
        assert changes[0].name == "Alice V2"

    def test_as_of(self, seeded_onto):
        first_commit = 1  # First seed commit

        repo = seeded_onto._repo
        c2 = repo.create_commit()
        repo.insert_entity(
            "Customer",
            "c1",
            {
                "id": "c1",
                "name": "Alice V2",
                "age": 31,
                "email": "alice@test.com",
                "tier": "Gold",
                "active": True,
            },
            c2,
        )
        repo.commit_transaction()

        snapshot = (
            seeded_onto.query()
            .entities(Customer)
            .where(Customer.id == "c1")
            .as_of(commit_id=first_commit)
            .collect()
        )
        assert len(snapshot) == 1
        assert snapshot[0].name == "Alice"


class TestRelationQuery:
    def test_collect_all(self, seeded_onto):
        subs = seeded_onto.query().relations(Subscription).collect()
        assert len(subs) == 3
        assert all(isinstance(s, Subscription) for s in subs)

    def test_where_attribute(self, seeded_onto):
        results = (
            seeded_onto.query().relations(Subscription).where(Subscription.seat_count > 5).collect()
        )
        assert len(results) == 1
        assert results[0].seat_count == 10

    def test_endpoint_hydration(self, seeded_onto):
        subs = seeded_onto.query().relations(Subscription).collect()
        for sub in subs:
            assert sub.left is not None, f"left not hydrated for {sub}"
            assert isinstance(sub.left, Customer)
            assert sub.right is not None, f"right not hydrated for {sub}"
            assert isinstance(sub.right, Product)

    def test_endpoint_filter_left(self, seeded_onto):
        results = (
            seeded_onto.query()
            .relations(Subscription)
            .where(left(Subscription).tier == "Gold")
            .collect()
        )
        assert len(results) == 2
        for r in results:
            assert r.left.tier == "Gold"

    def test_endpoint_filter_right(self, seeded_onto):
        results = (
            seeded_onto.query()
            .relations(Subscription)
            .where(right(Subscription).category == "Electronics")
            .collect()
        )
        assert len(results) == 1
        assert results[0].right.name == "Gadget"

    def test_combined_filters(self, seeded_onto):
        results = (
            seeded_onto.query()
            .relations(Subscription)
            .where((Subscription.seat_count > 3) & (left(Subscription).tier == "Gold"))
            .collect()
        )
        assert len(results) == 2

    def test_first(self, seeded_onto):
        result = (
            seeded_onto.query().relations(Subscription).where(Subscription.seat_count == 10).first()
        )
        assert result is not None
        assert result.seat_count == 10

    def test_relation_metadata(self, seeded_onto):
        subs = seeded_onto.query().relations(Subscription).collect()
        for sub in subs:
            m = sub.meta()
            assert m.commit_id > 0
            assert m.type_name == "Subscription"
            assert m.left_key is not None
            assert m.right_key is not None


class TestTraversal:
    def test_basic_traversal(self, seeded_onto):
        results = seeded_onto.query().entities(Customer).via(Subscription).collect()
        assert isinstance(results, list)
        for r in results:
            assert isinstance(r, Path)
            assert isinstance(r.source, Customer)

    def test_traversal_with_filter(self, seeded_onto):
        results = (
            seeded_onto.query()
            .entities(Customer)
            .where(Customer.name == "Alice")
            .via(Subscription)
            .collect()
        )
        assert len(results) == 1
        assert results[0].source.name == "Alice"
        assert len(results[0].relations) == 2  # Alice has 2 subscriptions

    def test_traversal_relations_have_endpoints(self, seeded_onto):
        results = (
            seeded_onto.query()
            .entities(Customer)
            .where(Customer.name == "Alice")
            .via(Subscription)
            .collect()
        )
        for rel in results[0].relations:
            assert rel.left is not None
            assert rel.right is not None

    def test_without_relations(self, seeded_onto):
        products = (
            seeded_onto.query()
            .entities(Customer)
            .where(Customer.name == "Alice")
            .via(Subscription)
            .without_relations()
        )
        assert len(products) == 2
        assert all(isinstance(p, Product) for p in products)


class TestEntityAggregation:
    def test_count(self, seeded_onto):
        c = seeded_onto.query().entities(Customer).count()
        assert c == 3

    def test_count_with_filter(self, seeded_onto):
        c = seeded_onto.query().entities(Customer).where(Customer.active.is_true()).count()
        assert c == 2

    def test_sum(self, seeded_onto):
        total = seeded_onto.query().entities(Order).sum(Order.total_amount)
        assert total == 450.0

    def test_avg(self, seeded_onto):
        a = seeded_onto.query().entities(Order).avg(Order.total_amount)
        assert a == 150.0

    def test_min(self, seeded_onto):
        m = seeded_onto.query().entities(Order).min(Order.total_amount)
        assert m == 100.0

    def test_max(self, seeded_onto):
        m = seeded_onto.query().entities(Order).max(Order.total_amount)
        assert m == 200.0

    def test_group_by_agg(self, seeded_onto):
        results = (
            seeded_onto.query()
            .entities(Order)
            .group_by(Order.country)
            .agg(
                order_count=count(),
                total_amount=sum(Order.total_amount),
            )
        )
        assert len(results) == 2
        us = next(r for r in results if r["country"] == "US")
        assert us["order_count"] == 2
        assert us["total_amount"] == 300.0

        uk = next(r for r in results if r["country"] == "UK")
        assert uk["order_count"] == 1
        assert uk["total_amount"] == 150.0

    def test_group_by_having(self, seeded_onto):
        total_amount = sum(Order.total_amount)
        results = (
            seeded_onto.query()
            .entities(Order)
            .group_by(Order.country)
            .having(total_amount > 200)
            .agg(
                total_amount=total_amount,
                order_count=count(),
            )
        )
        assert len(results) == 1
        assert results[0]["country"] == "US"


class TestRelationAggregation:
    def test_count(self, seeded_onto):
        c = seeded_onto.query().relations(Subscription).count()
        assert c == 3

    def test_avg(self, seeded_onto):
        a = seeded_onto.query().relations(Subscription).avg(Subscription.seat_count)
        # (5 + 10 + 2) / 3 ≈ 5.67
        assert abs(a - 5.67) < 0.1

    def test_sum(self, seeded_onto):
        s = seeded_onto.query().relations(Subscription).sum(Subscription.seat_count)
        assert s == 17
