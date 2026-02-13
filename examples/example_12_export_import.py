"""Example 12: Export/Import - Data Portability and Migration.

This example demonstrates export/import capabilities:
- Exporting ontology to portable format using export()
- Importing ontology data using import_data()
- Understanding manifest format and JSONL structure
- Use cases: backups, migrations, test fixtures, data sharing

NOTE: Export/import features are planned but not yet available in this version.
These features are specified in WRK-0053 (Introspection & Export).

References: WRK-0053 (Introspection & Export), SPEC §13
"""

from ontologia import Entity, Field, Relation, Session


class Customer(Entity):
    """A customer in our e-commerce system."""

    email: Field[str] = Field(primary_key=True)
    name: Field[str]
    tier: Field[str]  # "bronze", "silver", "gold"
    signup_date: Field[str]


class Product(Entity):
    """A product in our catalog."""

    sku: Field[str] = Field(primary_key=True)
    name: Field[str]
    category: Field[str]
    price: Field[float]
    in_stock: Field[bool]


class Purchased(Relation[Customer, Product]):
    """Customer purchase relation."""

    purchase_date: Field[str]
    quantity: Field[int]
    total_amount: Field[float]


def setup_sample_data(onto: Session) -> None:
    """Populate the ontology with sample e-commerce data."""
    with onto.session() as session:
        # Add customers
        session.ensure(
            [
                Customer(
                    email="alice@example.com",
                    name="Alice Cooper",
                    tier="gold",
                    signup_date="2023-01-15",
                ),
                Customer(
                    email="bob@example.com",
                    name="Bob Dylan",
                    tier="silver",
                    signup_date="2023-03-20",
                ),
            ]
        )

        # Add products
        session.ensure(
            [
                Product(
                    sku="SKU-001",
                    name="Premium Widget",
                    category="widgets",
                    price=149.99,
                    in_stock=True,
                ),
                Product(
                    sku="SKU-002",
                    name="Basic Gadget",
                    category="gadgets",
                    price=49.99,
                    in_stock=True,
                ),
            ]
        )

        # Add purchase relations
        session.ensure(
            [
                Purchased(
                    left_key="alice@example.com",
                    right_key="SKU-001",
                    purchase_date="2024-01-15",
                    quantity=2,
                    total_amount=299.98,
                ),
            ]
        )


def main():
    """Run the export/import example."""
    print("=" * 80)
    print("ONTOLOGIA EXPORT/IMPORT EXAMPLE")
    print("=" * 80)

    print("\nNOTE: Export/import features are planned but not yet available.")
    print("These features are specified in WRK-0053 (Introspection & Export).")

    # Step 1: Create and populate source database
    print("\n" + "=" * 80)
    print("1. CREATING SOURCE DATABASE")
    print("=" * 80)

    source_db = "tmp/source_ecommerce.db"
    onto_source = Session(datastore_uri=source_db)

    print(f"\n✓ Created source database: {source_db}")
    print("\nPopulating with sample data...")
    setup_sample_data(onto_source)
    print("✓ Data loaded")

    # Verify source data
    customers = list(onto_source.query().entities(Customer).collect())
    products = list(onto_source.query().entities(Product).collect())
    purchases = list(onto_source.query().relations(Purchased).collect())

    print("\nSource database contents:")
    print(f"  Customers: {len(customers)}")
    print(f"  Products: {len(products)}")
    print(f"  Purchases: {len(purchases)}")

    # Step 2: Export (planned feature)
    print("\n" + "=" * 80)
    print("2. EXPORTING ONTOLOGY (PLANNED)")
    print("=" * 80)

    print("\nPlanned API: onto.export(export_dir)")
    print("  - Exports entities to entities.jsonl")
    print("  - Exports relations to relations.jsonl")
    print("  - Creates manifest.json with metadata")
    print("\n  Example usage (not yet available):")
    print('    export_dir = Path("tmp/ecommerce_export")')
    print("    onto_source.export(export_dir)")

    print("\nExport format:")
    print("  manifest.json - Metadata and table listing")
    print("  entities.jsonl - One JSON object per entity")
    print("  relations.jsonl - One JSON object per relation")

    # Step 3: Import (planned feature)
    print("\n" + "=" * 80)
    print("3. IMPORTING ONTOLOGY (PLANNED)")
    print("=" * 80)

    print("\nPlanned API: onto.import_data(export_dir)")
    print("  - Reads manifest.json for metadata")
    print("  - Imports entities from entities.jsonl")
    print("  - Imports relations from relations.jsonl")
    print("  - Validates schema compatibility")
    print("\n  Example usage (not yet available):")
    print('    target_db = "tmp/target_ecommerce.db"')
    print("    onto_target = Session(datastore_uri=target_db)")
    print("    onto_target.import_data(export_dir)")

    # Use Case 1: Automated Backups
    print("\n" + "=" * 80)
    print("4. USE CASE: AUTOMATED BACKUPS")
    print("=" * 80)

    print("\nScenario: Daily backup automation")
    print("  1. Export ontology with timestamp")
    print("  2. Store in backup directory")
    print("  3. Compress and upload to cloud storage")
    print("\n  Example:")
    print("    from datetime import datetime")
    print('    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")')
    print('    backup_dir = Path(f"backups/backup_{timestamp}")')
    print("    onto.export(backup_dir)")

    # Use Case 2: Test Fixtures
    print("\n" + "=" * 80)
    print("5. USE CASE: TEST FIXTURES")
    print("=" * 80)

    print("\nScenario: Create test fixtures from production data")
    print("  1. Export production database subset")
    print("  2. Store as test fixture")
    print("  3. Import in test suite for realistic testing")
    print("\n  Benefits:")
    print("    - Consistent test data across environments")
    print("    - Realistic data patterns")
    print("    - Easy to reset test state")

    # Use Case 3: Cross-Environment Migration
    print("\n" + "=" * 80)
    print("6. USE CASE: CROSS-ENVIRONMENT MIGRATION")
    print("=" * 80)

    print("\nScenario: Migrate data from dev to staging to production")
    print("  1. Export from source environment")
    print("  2. Validate schema compatibility")
    print("  3. Import to target environment")
    print("\n  Validation steps:")
    print("    - Check schema hashes match")
    print("    - Verify no data loss")
    print("    - Confirm relations intact")

    # Use Case 4: Data Sharing
    print("\n" + "=" * 80)
    print("7. USE CASE: DATA SHARING")
    print("=" * 80)

    print("\nScenario: Share ontology data with team members")
    print("  - Export specific entity/relation types")
    print("  - Share via file transfer or version control")
    print("  - Others can import and explore")

    print("\n" + "=" * 80)
    print("EXAMPLE COMPLETE")
    print("=" * 80)
    print("\nKey concepts (planned features):")
    print("  - onto.export() - Export to JSONL format")
    print("  - onto.import_data() - Import from export directory")
    print("  - Manifest format with metadata")
    print("  - JSONL format (newline-delimited JSON)")
    print("  - Use case: Automated backups")
    print("  - Use case: Test fixtures")
    print("  - Use case: Cross-environment migration")
    print("  - Use case: Data sharing")
    print("\nDatabase file:")
    print(f"  {source_db}")
    print("=" * 80)


if __name__ == "__main__":
    main()
