"""Example 11: Introspection - Schema Inspection and Drift Detection.

This example demonstrates introspection capabilities:
- Query introspection: Using .explain() for query plan analysis (RFC-0041)
- Schema introspection: Using schema_hash() for schema fingerprinting
- Detecting registry drift with diff_registry_vs_db()
- Comparing schemas across environments
- Use cases: Query debugging, CI/CD validation, environment verification

NOTE: Many introspection features are planned but not yet available:
  - query.explain() method (RFC-0041)
  - onto.schema_hash() method
  - onto.diff_registry_vs_db() method

References: RFC-0041 (Fluent .explain() API), WRK-0053 (Introspection & Export), SPEC §12
"""

from ontologia import Entity, Field, Relation, Session


# Define schema for a customer relationship management system
class Customer(Entity):
    """A customer in the CRM system."""

    email: Field[str] = Field(primary_key=True)
    name: Field[str]
    tier: Field[str]  # "bronze", "silver", "gold", "platinum"
    signup_date: Field[str]


class Product(Entity):
    """A product in the catalog."""

    sku: Field[str] = Field(primary_key=True)
    name: Field[str]
    category: Field[str]
    price: Field[float]


class Purchased(Relation[Customer, Product]):
    """Purchase relation."""

    purchase_date: Field[str]
    quantity: Field[int]
    total_amount: Field[float]


def setup_environment(db_path: str) -> Session:
    """Set up environment with sample data."""
    onto = Session(datastore_uri=db_path)

    with onto.session() as session:
        session.ensure(
            [
                Customer(
                    email="alice@example.com",
                    name="Alice Smith",
                    tier="gold",
                    signup_date="2023-01-15",
                ),
                Product(
                    sku="SKU-001",
                    name="Widget Pro",
                    category="widgets",
                    price=99.99,
                ),
            ]
        )

    return onto


def main():
    """Run the introspection example."""
    print("=" * 80)
    print("ONTOLOGIA INTROSPECTION EXAMPLE")
    print("=" * 80)

    print("\nNOTE: This example demonstrates planned introspection APIs that are")
    print("not yet available in the current version. These features are specified")
    print("in RFC-0041 and WRK-0053.")

    # Part 1: Query Introspection (RFC-0041)
    print("\n" + "=" * 80)
    print("1. QUERY INTROSPECTION (RFC-0041)")
    print("=" * 80)

    print("\nPlanned API: query.explain()")
    print("  - Returns ExplainResult with .sql, .params, .plan")
    print("  - Use cases: debugging, testing, performance analysis")
    print("  - Works for EntityQuery, RelationQuery, and traversals")
    print("\n  Example usage (not yet available):")
    print('    query = onto.query().entities(Customer).where(Customer.tier == "gold")')
    print("    result = query.explain()")
    print("    print(result.sql)      # Generated SQL")
    print("    print(result.params)   # Bound parameters")
    print("    print(result.plan)     # Query execution plan")

    # Part 2: Schema Fingerprinting
    print("\n" + "=" * 80)
    print("2. SCHEMA FINGERPRINTING")
    print("=" * 80)

    print("\nSetting up environment with sample data...")
    _ = setup_environment("tmp/introspection_env1.db")
    print("  ✓ Environment initialized")

    print("\nPlanned API: onto.schema_hash()")
    print("  - Returns deterministic hash of the current schema")
    print("  - Use cases: CI/CD validation, environment comparison")
    print("\n  Example usage (not yet available):")
    print("    hash1 = env1.schema_hash()")
    print("    hash2 = env2.schema_hash()")
    print("    if hash1 == hash2:")
    print('        print("Schemas match - safe to deploy")')

    # Part 3: Registry Drift Detection
    print("\n" + "=" * 80)
    print("3. REGISTRY DRIFT DETECTION")
    print("=" * 80)

    print("\nPlanned API: onto.diff_registry_vs_db()")
    print("  - Compares code-defined schema with database schema")
    print("  - Reports: missing_in_db, missing_in_registry, version_mismatches")
    print("  - Use cases: Detect schema drift, pre-deployment validation")
    print("\n  Example usage (not yet available):")
    print("    diff = env1.diff_registry_vs_db()")
    print("    print(f\"Missing in DB: {len(diff['missing_in_db'])}\")")
    print("    print(f\"Version mismatches: {len(diff['version_mismatches'])}\")")

    # Part 4: CI/CD Validation Use Case
    print("\n" + "=" * 80)
    print("4. USE CASE: CI/CD PIPELINE VALIDATION")
    print("=" * 80)

    print("\nScenario: Validate schema before deployment")
    print("  1. Compute schema_hash() of current environment")
    print("  2. Compare with hash from target environment")
    print("  3. Block deployment if hashes don't match")
    print("  4. Check diff_registry_vs_db() for detailed issues")
    print("\n  Benefits:")
    print("    - Prevent schema mismatches in production")
    print("    - Catch drift early in the pipeline")
    print("    - Automated validation without manual checks")

    # Part 5: Schema Version Control
    print("\n" + "=" * 80)
    print("5. USE CASE: SCHEMA VERSION CONTROL")
    print("=" * 80)

    print("\nScenario: Track schema changes in version control")
    print("  1. Generate schema_hash() after schema changes")
    print("  2. Save fingerprint to JSON file")
    print("  3. Commit to git for audit trail")
    print("\n  Benefits:")
    print("    - Track schema evolution over time")
    print("    - Rollback to known schema versions")
    print("    - Correlate schema changes with code changes")

    # Part 6: Multi-Environment Comparison
    print("\n" + "=" * 80)
    print("6. USE CASE: MULTI-ENVIRONMENT COMPARISON")
    print("=" * 80)

    print("\nScenario: Compare schemas across dev/staging/production")
    print("  1. Compute schema_hash() for each environment")
    print("  2. Compare hashes to detect drift")
    print("  3. Alert if environments are out of sync")
    print("\n  Benefits:")
    print("    - Ensure consistency across environments")
    print("    - Catch deployment issues early")
    print("    - Simplify troubleshooting")

    print("\n" + "=" * 80)
    print("EXAMPLE COMPLETE")
    print("=" * 80)
    print("\nKey concepts (planned features):")
    print("\n  Query Introspection (RFC-0041):")
    print("  - query.explain() for SQL and execution plan inspection")
    print("  - Debugging complex queries")
    print("  - Testing query generation")
    print("\n  Schema Introspection (WRK-0053):")
    print("  - schema_hash() for deterministic fingerprinting")
    print("  - diff_registry_vs_db() for drift detection")
    print("  - CI/CD integration for deployment safety")
    print("  - Multi-environment schema comparison")
    print("\nDatabase file:")
    print("  - tmp/introspection_env1.db")
    print("=" * 80)


if __name__ == "__main__":
    main()
