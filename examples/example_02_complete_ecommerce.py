# pyright: reportUnknownVariableType=false, reportUnknownMemberType=false, reportUnknownArgumentType=false
#
# RATIONALE: This example demonstrates dynamic type usage patterns inherent to
# the Ontologia API. Runtime introspection and dynamic attribute access on ontology
# objects (Ontology.fetch(), query results) produce partially-unknown types that
# are correct at runtime but not fully traceable by static analysis.
#
"""Example 02: Complete E-Commerce Pipeline - Full Feature Demonstration

This example demonstrates a complete end-to-end workflow in Ontologia,
showcasing the complete feature set in a single cohesive example:

1. Define types (Customer, Order, Product)
2. Load data using session.ensure() with typed models
3. Query data with filters, traversals, and aggregations
4. Define and traverse Relations (PlacedOrder, Contains)
5. Use observability features (.explain(), .profile())

Unlike other examples that focus on individual features, this example
combines multiple capabilities to show how they work together in a
real-world e-commerce scenario.

Last Updated: 2025-11-13
Tested With: Ontologia MVP (WP0-WP17)

Usage:
    uv run python examples/example_02_complete_ecommerce.py
"""

from ontologia import Entity, Field, Relation, Session

# ============================================================================
# 1. DEFINE TYPES
# ============================================================================


class Customer(Entity):
    """Customer with ID, name, email, and country."""

    customer_id: Field[str] = Field(primary_key=True)
    name: Field[str]
    email: Field[str]
    country: Field[str] = Field(index=True)


class Order(Entity):
    """Order with ID, customer reference, amount, and date."""

    order_id: Field[str] = Field(primary_key=True)
    customer_id: Field[str] = Field(index=True)
    total_amount: Field[float] = Field(index=True)
    order_date: Field[str]


class Product(Entity):
    """Product with ID, name, category, and price."""

    product_id: Field[str] = Field(primary_key=True)
    name: Field[str]
    category: Field[str] = Field(index=True)
    price: Field[float]


class PlacedOrder(Relation[Customer, Order]):
    """Relationship: Customer placed Order."""

    pass


class Contains(Relation[Order, Product]):
    """Relationship: Order contains Products."""

    pass


# ============================================================================
# 2. GENERATE SAMPLE DATA (in-memory for simplicity)
# ============================================================================

# Sample customers
CUSTOMERS = [
    {"customer_id": "c001", "name": "Alice Smith", "email": "alice@example.com", "country": "USA"},
    {"customer_id": "c002", "name": "Bob Jones", "email": "bob@example.com", "country": "UK"},
    {
        "customer_id": "c003",
        "name": "Charlie Brown",
        "email": "charlie@example.com",
        "country": "USA",
    },
    {
        "customer_id": "c004",
        "name": "Diana Prince",
        "email": "diana@example.com",
        "country": "Canada",
    },
    {"customer_id": "c005", "name": "Eve Davis", "email": "eve@example.com", "country": "USA"},
]

# Sample orders
ORDERS = [
    {"order_id": "o001", "customer_id": "c001", "total_amount": 150.00, "order_date": "2024-01-15"},
    {"order_id": "o002", "customer_id": "c001", "total_amount": 89.99, "order_date": "2024-02-20"},
    {"order_id": "o003", "customer_id": "c002", "total_amount": 250.00, "order_date": "2024-01-22"},
    {"order_id": "o004", "customer_id": "c003", "total_amount": 45.50, "order_date": "2024-03-10"},
    {"order_id": "o005", "customer_id": "c004", "total_amount": 175.00, "order_date": "2024-02-05"},
    {"order_id": "o006", "customer_id": "c005", "total_amount": 320.00, "order_date": "2024-03-15"},
]

# Sample products
PRODUCTS = [
    {"product_id": "p001", "name": "Laptop", "category": "Electronics", "price": 999.99},
    {"product_id": "p002", "name": "Mouse", "category": "Electronics", "price": 29.99},
    {"product_id": "p003", "name": "Desk Chair", "category": "Furniture", "price": 199.99},
    {"product_id": "p004", "name": "Notebook", "category": "Stationery", "price": 4.99},
]

# Order-Product relationships
ORDER_PRODUCTS = [
    {"order_id": "o001", "product_id": "p002"},
    {"order_id": "o001", "product_id": "p004"},
    {"order_id": "o002", "product_id": "p002"},
    {"order_id": "o003", "product_id": "p001"},
    {"order_id": "o004", "product_id": "p004"},
    {"order_id": "o005", "product_id": "p003"},
    {"order_id": "o006", "product_id": "p001"},
]


# ============================================================================
# ============================================================================


def main():
    print("=" * 70)
    print("Ontologia Example 1: E-Commerce Pipeline")
    print("=" * 70)

    # Create ontology (in-memory database)
    onto = Session(":memory:")

    # --------------------------------------------------------------------
    # STEP 1: Load Data using session.ensure() for simplicity
    # --------------------------------------------------------------------
    print("\n📦 Loading data...")

    # Load customers
    with onto.session() as session:
        for c in CUSTOMERS:
            session.ensure(Customer(**c))
    print(f"   ✓ Loaded {len(CUSTOMERS)} customers")

    # Load orders with relationships
    with onto.session() as session:
        for order in ORDERS:
            session.ensure(Order(**order))
            session.ensure(
                PlacedOrder(
                    left_key=str(order["customer_id"]),
                    right_key=str(order["order_id"]),
                )
            )
    print(f"   ✓ Loaded {len(ORDERS)} orders")

    # Load products
    with onto.session() as session:
        for p in PRODUCTS:
            session.ensure(Product(**p))
    print(f"   ✓ Loaded {len(PRODUCTS)} products")

    # Load order-product relationships
    with onto.session() as session:
        for rel in ORDER_PRODUCTS:
            session.ensure(
                Contains(
                    left_key=rel["order_id"],
                    right_key=rel["product_id"],
                )
            )
    print(f"   ✓ Loaded {len(ORDER_PRODUCTS)} order-product relationships")

    # --------------------------------------------------------------------
    # STEP 2: Query Examples
    # --------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("QUERY EXAMPLES")
    print("=" * 70)

    # Query 1: Simple filter - All customers from USA
    print("\n🔍 Query 1: Customers from USA")
    usa_customers = onto.query().entities(Customer).where(Customer.country == "USA").collect()
    print(f"   Found {len(usa_customers)} customers:")
    for customer in usa_customers:
        print(f"   - {customer.name} ({customer.email})")

    # Query 2: Orders over $100
    print("\n🔍 Query 2: Orders > $100")
    big_orders = onto.query().entities(Order).where(Order.total_amount > 100).collect()
    print(f"   Found {len(big_orders)} orders:")
    for order in big_orders:
        print(f"   - Order {order.order_id}: ${order.total_amount:.2f}")

    # Query 3: Aggregation - Total revenue
    print("\n🔍 Query 3: Total Revenue")
    total_revenue = onto.query().entities(Order).sum(Order.total_amount)
    print(f"   Total: ${total_revenue:.2f}")

    # Query 4: Count by country
    print("\n🔍 Query 4: Customer Count")
    customer_count = onto.query().entities(Customer).count()
    print(f"   Total customers: {customer_count}")

    # Query 5: Products in Electronics category
    print("\n🔍 Query 5: Electronics Products")
    electronics = onto.query().entities(Product).where(Product.category == "Electronics").collect()
    print(f"   Found {len(electronics)} products:")
    for product in electronics:
        print(f"   - {product.name}: ${product.price:.2f}")

    # --------------------------------------------------------------------
    # STEP 3: Observability Features
    # --------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("OBSERVABILITY")
    print("=" * 70)

    # .explain() - Show query plan without executing
    print("\n📊 Using .explain() to inspect query plan:")
    # query = onto.query().entities(Customer).where(Customer.country == "USA")
    # explanation = query.explain()
    # print(f"   SQL: {explanation.sql[:80]}...")
    # print(f"   Params: {explanation.params}")
    # print(f"   Has optimization metadata: {explanation.optimization is not None}")
    print("   (Explain feature not available in this version)")

    # .profile() - Execute and get performance stats
    print("\n⚡ Using .profile() to measure performance:")
    # query = onto.query().entities(Order)
    # profile = query.profile()
    # print(f"   Execution time: {profile['execution_time_ms']:.2f}ms")
    # print(f"   Rows returned: {profile['rows_returned']}")
    # print(f"   Rows scanned: {profile['rows_scanned']}")
    print("   (Profile feature not available in this version)")

    # --------------------------------------------------------------------
    # STEP 4: Verification
    # --------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("VERIFICATION")
    print("=" * 70)

    # Verify counts
    assert len(usa_customers) == 3, "Expected 3 USA customers"
    assert len(big_orders) == 4, "Expected 4 orders > $100"
    assert customer_count == 5, "Expected 5 total customers"
    assert len(electronics) == 2, "Expected 2 electronics products"
    assert abs(total_revenue - 1030.49) < 0.01, f"Expected $1030.49, got ${total_revenue:.2f}"

    print("\n✅ All assertions passed!")
    print("\n" + "=" * 70)
    print("Example complete!")
    print("=" * 70)


if __name__ == "__main__":
    main()
