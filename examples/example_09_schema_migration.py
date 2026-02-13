"""Example 09: Schema Migration - Evolution and Upgrades.

This example demonstrates schema migration capabilities:
- onto.migrate(dry_run=True) for previewing changes
- @upgrader decorator for data transformation
- Token-based apply workflow for safety
- Multi-version jumps with chained upgraders
- Schema versioning and drift detection

Scenario: Customer schema evolution from v1 to v2
  v1: Customer with name and email
  v2: Customer with name, email, AND phone_number

NOTE: Schema migration API is planned but not yet available.
These features are specified in SPEC §Ontology Runtime.

References: SPEC §Schema Migration, WRK-00XX (Migrations)
"""

from ontologia import Entity, Field, Session


# Version 1: Original Customer schema
class CustomerV1(Entity):
    """Customer schema version 1 (legacy)."""

    id: Field[str] = Field(primary_key=True)
    name: Field[str]
    email: Field[str]


# Version 2: Updated Customer schema with phone_number
class CustomerV2(Entity):
    """Customer schema version 2 (current)."""

    id: Field[str] = Field(primary_key=True)
    name: Field[str]
    email: Field[str]
    phone_number: Field[str | None] = Field(default=None)


def setup_legacy_data(onto: Session) -> None:
    """Create sample data using v1 schema."""
    with onto.session() as session:
        session.ensure(
            [
                CustomerV1(
                    id="c1",
                    name="Alice Johnson",
                    email="alice@example.com",
                ),
                CustomerV1(
                    id="c2",
                    name="Bob Smith",
                    email="bob@example.com",
                ),
                CustomerV1(
                    id="c3",
                    name="Carol White",
                    email="carol@example.com",
                ),
            ]
        )

    print("  ✓ Created 3 customers with v1 schema")


def main():
    """Run the schema migration example."""
    print("=" * 80)
    print("ONTOLOGIA SCHEMA MIGRATION EXAMPLE")
    print("=" * 80)
    print("\nNOTE: Schema migration API is planned but not yet available.")
    print("These features are specified in SPEC §Schema Migration.")

    print("\nScenario: Upgrade Customer schema from v1 to v2")
    print("  v1: id, name, email")
    print("  v2: id, name, email, phone_number (NEW)")

    # Create v1 database
    print("\n" + "=" * 80)
    print("1. CREATE LEGACY DATABASE (v1 Schema)")
    print("=" * 80)

    onto_v1 = Session(datastore_uri="tmp/migration_v1.db")
    print("\n✓ Ontology initialized with v1 schema")

    print("\nPopulating with legacy data...")
    setup_legacy_data(onto_v1)

    # Check current state
    customers = list(onto_v1.query().entities(CustomerV1).collect())
    print(f"\nCurrent customers: {len(customers)}")
    for cust in customers:
        print(f"  - {cust.name} ({cust.email})")

    # Part 1: Migration Preview
    print("\n" + "=" * 80)
    print("2. MIGRATION PREVIEW (dry_run=True)")
    print("=" * 80)

    print("\nPlanned API: onto.migrate(dry_run=True)")
    print("  - Preview changes without applying")
    print("  - Generate deterministic token")
    print("  - Identify required upgraders")

    print("\n  Example (not yet available):")
    print("    onto_v2 = Session(")
    print("        db_path='tmp/migration_v1.db',")
    print("        entity_types=[CustomerV2]  # New schema")
    print("    )")
    print("")
    print("    preview = onto_v2.migrate(dry_run=True)")
    print("    print(f'Has changes: {preview.has_changes}')")
    print("    print(f'Token: {preview.token}')")
    print("    print(f'Rows to migrate: {preview.estimated_rows}')")
    print("    print(f'Required upgraders: {preview.missing_upgraders}')")

    # Expected output
    print("\n  Expected output:")
    print("    Has changes: True")
    print("    Token: <deterministic-hash>")
    print("    Rows to migrate: {'Customer': 3}")
    print("    Required upgraders: [('Customer', 1)]")

    # Part 2: Upgrader Functions
    print("\n" + "=" * 80)
    print("3. UPGRADER FUNCTIONS (@upgrader)")
    print("=" * 80)

    print("\nPlanned API: @upgrader(type_name, from_version)")
    print("  - Transform data from old schema to new")
    print("  - Chained for multi-version jumps")
    print("  - Validated against target schema")

    print("\n  Example (not yet available):")
    print("    from ontologia import upgrader")
    print("")
    print("    @upgrader('Customer', from_version=1)")
    print("    def upgrade_customer_v1(fields: dict) -> dict:")
    print("        # Transform v1 fields to v2")
    print("        return {")
    print("            'id': fields['id'],")
    print("            'name': fields['name'],")
    print("            'email': fields['email'],")
    print("            'phone_number': None  # New field, default to None")
    print("        }")

    print("\n  Multi-version jumps (automatic chaining):")
    print("    @upgrader('Customer', from_version=1)")
    print("    def v1_to_v2(fields): ...")
    print("")
    print("    @upgrader('Customer', from_version=2)")
    print("    def v2_to_v3(fields): ...")
    print("")
    print("    # Migration from v1 to v3 automatically chains both upgraders")

    # Part 3: Apply Migration
    print("\n" + "=" * 80)
    print("4. APPLY MIGRATION (dry_run=False)")
    print("=" * 80)

    print("\nPlanned API: onto.migrate(dry_run=False, token=..., upgraders=...)")
    print("  - Apply changes with token verification")
    print("  - Execute upgraders under write lock")
    print("  - Atomic: all or nothing")

    print("\n  Example (not yet available):")
    print("    result = onto_v2.migrate(")
    print("        dry_run=False,")
    print("        token=preview.token,  # From dry_run preview")
    print("        upgraders={")
    print("            ('Customer', 1): upgrade_customer_v1")
    print("        }")
    print("    )")
    print("")
    print("    print(f'Success: {result.success}')")
    print("    print(f'Rows migrated: {result.rows_migrated}')")
    print("    print(f'New versions: {result.new_schema_versions}')")

    print("\n  Safety features:")
    print("    ✓ Token verification ensures plan hasn't changed")
    print("    ✓ Write lock prevents concurrent modifications")
    print("    ✓ Upgrader output validated before commit")
    print("    ✓ Atomic: all types succeed or none")

    # Part 4: Force Migration
    print("\n" + "=" * 80)
    print("5. FORCE MIGRATION (skip token verification)")
    print("=" * 80)

    print("\nPlanned API: onto.migrate(force=True)")
    print("  - Skip token verification (emergencies only)")
    print("  - Still validates under write lock")
    print("  - Use with caution!")

    print("\n  Example (not yet available):")
    print("    # Emergency: skip token, still recompute and validate")
    print("    result = onto.migrate(")
    print("        dry_run=False,")
    print("        force=True,  # Skip token check")
    print("        upgraders={...}")
    print("    )")

    # Part 5: Load Upgraders from Module
    print("\n" + "=" * 80)
    print("6. LOADING UPGRADERS FROM MODULE")
    print("=" * 80)

    print("\nPlanned API: load_upgraders(module_path)")
    print("  - Scan module for @upgrader decorators")
    print("  - Auto-collect upgraders by (type, version)")
    print("  - Convenient for organizing migrations")

    print("\n  Example (not yet available):")
    print("    from ontologia import load_upgraders")
    print("")
    print("    # In myapp/migrations.py:")
    print("    @upgrader('Customer', from_version=1)")
    print("    def upgrade_customer(fields): ...")
    print("")
    print("    @upgrader('Order', from_version=1)")
    print("    def upgrade_order(fields): ...")
    print("")
    print("    # Load all upgraders:")
    print("    upgraders = load_upgraders('myapp.migrations')")
    print("    result = onto.migrate(")
    print("        dry_run=False,")
    print("        token=preview.token,")
    print("        upgraders=upgraders")
    print("    )")

    # Part 6: Migration Workflow
    print("\n" + "=" * 80)
    print("7. COMPLETE MIGRATION WORKFLOW")
    print("=" * 80)

    print("\nBest practice workflow:")

    print("\n  Step 1: Deploy new code with updated schema")
    print("    - Update Customer entity to v2")
    print("    - Deploy application code")
    print("    - Database still has v1 schema")

    print("\n  Step 2: Preview migration")
    print("    onto = Session(datastore_uri='...', entity_types=[CustomerV2])")
    print("    preview = onto.migrate(dry_run=True)")
    print("    # Review changes, check row counts")

    print("\n  Step 3: Write upgraders")
    print("    @upgrader('Customer', from_version=1)")
    print("    def upgrade_customer(fields): ...")

    print("\n  Step 4: Test migration")
    print("    # On staging environment")
    print("    result = onto.migrate(dry_run=False, token=preview.token, ...)")
    print("    # Verify data integrity")

    print("\n  Step 5: Apply to production")
    print("    # During maintenance window")
    print("    result = onto.migrate(dry_run=False, token=preview.token, ...)")
    print("    # Monitor for errors")

    print("\n  Step 6: Verify")
    print("    onto.validate()  # Should pass without errors")
    print("    # Application now uses v2 schema")

    # Part 7: Error Handling
    print("\n" + "=" * 80)
    print("8. ERROR HANDLING")
    print("=" * 80)

    print("\nPlanned error types:")

    print("\n  SchemaOutdatedError:")
    print("    - Code schema doesn't match stored schema")
    print("    - Call onto.migrate() to resolve")

    print("\n  MigrationTokenError:")
    print("    - Token is stale (data changed since preview)")
    print("    - Re-run dry_run to get new token")

    print("\n  MissingUpgraderError:")
    print("    - Data exists but no upgrader provided")
    print("    - Write @upgrader for the version gap")

    print("\n  MigrationError:")
    print("    - Upgrader failed or validation error")
    print("    - Check error details for failing row")

    print("\n" + "=" * 80)
    print("EXAMPLE COMPLETE")
    print("=" * 80)
    print("\nKey concepts (planned features):")
    print("  - onto.migrate(dry_run=True) for preview")
    print("  - @upgrader decorator for data transformation")
    print("  - Token-based safety verification")
    print("  - Multi-version jump with chained upgraders")
    print("  - load_upgraders() for module-based migrations")
    print("  - Atomic migrations (all or nothing)")
    print("  - Comprehensive error handling")
    print("\nDatabase files:")
    print("  - tmp/migration_v1.db (legacy v1 data)")
    print("=" * 80)


if __name__ == "__main__":
    main()
