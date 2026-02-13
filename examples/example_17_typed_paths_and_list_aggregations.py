"""Example 17: Structured Paths and List Aggregations.

This example demonstrates nested path queries and list-aware aggregations:
- Field.path("a.b.c") and bracket sugar Field["a"]["b"]["c"]
- any_path(...) existential predicates on list fields
- count_where(...) and avg_len(...) aggregation helpers
- path-aware scalar/grouped aggregations
- endpoint nested-path filtering with left(...)
"""

from typing import TypedDict

from ontologia import Entity, Field, Relation, Session, left
from ontologia.query import count


class Profile(TypedDict):
    city: str
    metrics: dict[str, float]


class EventPayload(TypedDict):
    kind: str
    score: float


class Customer(Entity):
    id: Field[str] = Field(primary_key=True)
    name: Field[str]
    active: Field[bool] = Field(default=True)
    profile: Field[Profile]
    events: Field[list[EventPayload]] = Field(default_factory=list)


class Product(Entity):
    sku: Field[str] = Field(primary_key=True)
    name: Field[str]


class Subscription(Relation[Customer, Product]):
    status: Field[str]


def setup_data(onto: Session) -> None:
    """Populate sample customers, products, and subscriptions."""
    with onto.session() as session:
        session.ensure(
            [
                Customer(
                    id="c1",
                    name="Alice",
                    active=True,
                    profile={"city": "SF", "metrics": {"score": 91.0, "spend": 1200.0}},
                    events=[
                        {"kind": "click", "score": 95.0},
                        {"kind": "view", "score": 42.0},
                    ],
                ),
                Customer(
                    id="c2",
                    name="Bob",
                    active=True,
                    profile={"city": "SF", "metrics": {"score": 78.0, "spend": 800.0}},
                    events=[],
                ),
                Customer(
                    id="c3",
                    name="Carol",
                    active=False,
                    profile={"city": "NYC", "metrics": {"score": 88.0, "spend": 950.0}},
                    events=[{"kind": "click", "score": 60.0}],
                ),
                Customer(
                    id="c4",
                    name="Dave",
                    active=True,
                    profile={"city": "SEA", "metrics": {"score": 73.0, "spend": 400.0}},
                    events=[{"kind": "purchase", "score": 99.0}],
                ),
                Product(sku="p1", name="Analytics"),
                Product(sku="p2", name="Warehouse"),
                Subscription(left_key="c1", right_key="p1", status="active"),
                Subscription(left_key="c2", right_key="p1", status="active"),
                Subscription(left_key="c3", right_key="p2", status="paused"),
            ]
        )


def main() -> None:
    """Run structured path and list aggregation queries."""
    print("=" * 80)
    print("ONTOLOGIA STRUCTURED PATHS AND LIST AGGREGATIONS EXAMPLE")
    print("=" * 80)

    onto = Session(datastore_uri="tmp/typed_paths_and_list_aggregations.db")
    print("\n✓ Ontology initialized")

    setup_data(onto)
    print("✓ Sample data loaded")

    print("\n" + "=" * 80)
    print("1. NESTED PATH FILTERS")
    print("=" * 80)

    sf_customers = (
        onto.query().entities(Customer).where(Customer.profile.path("city") == "SF").collect()
    )
    print(f"Customers in SF (path): {[c.name for c in sf_customers]}")

    high_score = (
        onto.query()
        .entities(Customer)
        .where(Customer.profile["metrics"]["score"] >= 85.0)
        .collect()
    )
    print(f"Customers with score >= 85 (bracket sugar): {[c.name for c in high_score]}")

    print("\n" + "=" * 80)
    print("2. EXISTENTIAL LIST FILTERS")
    print("=" * 80)

    click_customers = (
        onto.query().entities(Customer).where(Customer.events.any_path("kind") == "click").collect()
    )
    print(f"Customers with at least one click event: {[c.name for c in click_customers]}")

    high_event_score = (
        onto.query().entities(Customer).where(Customer.events.any_path("score") > 90).collect()
    )
    print(f"Customers with any event score > 90: {[c.name for c in high_event_score]}")

    print("\n" + "=" * 80)
    print("3. PATH/LIST AGGREGATIONS")
    print("=" * 80)

    avg_profile_score = onto.query().entities(Customer).avg(Customer.profile.path("metrics.score"))
    print(f"Average profile score: {avg_profile_score}")

    click_count = (
        onto.query().entities(Customer).count_where(Customer.events.any_path("kind") == "click")
    )
    print(f"Count of customers with click events (count_where): {click_count}")

    avg_event_len = onto.query().entities(Customer).avg_len(Customer.events)
    print(f"Average number of events per customer (avg_len): {avg_event_len}")

    by_city = (
        onto.query()
        .entities(Customer)
        .group_by(Customer.profile.path("city"))
        .agg(customers=count())
    )
    print("Customers by city:")
    for row in by_city:
        print(f"  - {row['profile.city']}: {row['customers']}")

    print("\n" + "=" * 80)
    print("4. ENDPOINT NESTED PATH FILTERS")
    print("=" * 80)

    sf_subscriptions = (
        onto.query()
        .relations(Subscription)
        .where(left(Subscription).profile.path("city") == "SF")
        .collect()
    )
    print(f"Subscriptions with left endpoint in SF: {len(sf_subscriptions)}")

    print("\n" + "=" * 80)
    print("EXAMPLE COMPLETE")
    print("=" * 80)
    print("Database file: tmp/typed_paths_and_list_aggregations.db")


if __name__ == "__main__":
    main()
