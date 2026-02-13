"""Example 05: Keyed Relations - Multiple Instances Per Endpoint Pair.

This example demonstrates keyed relations using Field(instance_key=True):
- Defining relations with instance keys for multiple concurrent instances
- Employment stints scenario (same person, same company, different time periods)
- Creating and querying keyed relations
- Accessing instance_key in relation metadata

Unlike unkeyed relations where identity is (type, left, right), keyed relations
have identity (type, left, right, instance_key), enabling multiple concurrent
instances between the same endpoint pair.

References: SPEC §2.2 (Relation Type Rules), WRK-0074 (Entity/Relation API)
"""

from ontologia import Entity, Field, Relation, Session


# Define entity types
class Person(Entity):
    """A person who can have multiple employment stints."""

    person_id: Field[str] = Field(primary_key=True)
    name: Field[str]
    email: Field[str]


class Company(Entity):
    """A company that employs people."""

    company_id: Field[str] = Field(primary_key=True)
    name: Field[str]
    industry: Field[str]


# Define a keyed relation - multiple employment stints per person/company pair
class Employment(Relation[Person, Company]):
    """Employment stint with instance key.

    Keyed relation allows multiple concurrent instances between same person
    and company (e.g., rehires, internships converted to full-time).

    Identity: (Employment, person_id, company_id, stint_id)
    """

    # Instance key - distinguishes multiple stints between same endpoints
    stint_id: Field[str] = Field(instance_key=True)

    # Relation attributes
    role: Field[str]
    department: Field[str]
    start_date: Field[str]
    end_date: Field[str | None] = Field(default=None)
    salary: Field[int]
    is_active: Field[bool] = Field(default=True)


def setup_data(onto: Session) -> None:
    """Create sample data demonstrating keyed relations."""
    # Add people
    with onto.session() as session:
        session.ensure(
            [
                Person(
                    person_id="p1",
                    name="Alice Johnson",
                    email="alice@example.com",
                ),
                Person(
                    person_id="p2",
                    name="Bob Smith",
                    email="bob@example.com",
                ),
            ]
        )

        # Add companies
        session.ensure(
            [
                Company(
                    company_id="c1",
                    name="TechCorp",
                    industry="Technology",
                ),
                Company(
                    company_id="c2",
                    name="StartupXYZ",
                    industry="Technology",
                ),
            ]
        )

    print("  ✓ Added 2 people and 2 companies")

    # Create keyed relations - multiple stints for same person at same company
    with onto.session() as session:
        # Alice's first stint at TechCorp (Engineer)
        session.ensure(
            Employment(
                left_key="p1",
                right_key="c1",
                stint_id="stint-1",  # Instance key distinguishes this stint
                role="Software Engineer",
                department="Engineering",
                start_date="2020-01-15",
                end_date="2022-06-30",
                salary=80000,
                is_active=False,
            )
        )

        # Alice's second stint at TechCorp (Senior Engineer) - same company, different role
        session.ensure(
            Employment(
                left_key="p1",
                right_key="c1",
                stint_id="stint-2",  # Different instance key = different relation instance
                role="Senior Engineer",
                department="Engineering",
                start_date="2022-07-01",
                end_date=None,
                salary=120000,
                is_active=True,
            )
        )

        # Alice's stint at StartupXYZ
        session.ensure(
            Employment(
                left_key="p1",
                right_key="c2",
                stint_id="stint-3",
                role="Consultant",
                department="Product",
                start_date="2021-03-01",
                end_date="2021-08-31",
                salary=15000,  # Part-time contract
                is_active=False,
            )
        )

        # Bob's stint at TechCorp
        session.ensure(
            Employment(
                left_key="p2",
                right_key="c1",
                stint_id="stint-4",
                role="Product Manager",
                department="Product",
                start_date="2019-05-01",
                end_date=None,
                salary=110000,
                is_active=True,
            )
        )

    print("  ✓ Created 4 employment stints (keyed relations)")


def main():
    """Run the keyed relations example."""
    print("=" * 80)
    print("ONTOLOGIA KEYED RELATIONS EXAMPLE")
    print("=" * 80)
    print("\nDemonstrating: Field(instance_key=True) for multiple instances")

    onto = Session(datastore_uri="tmp/keyed_relations.db")
    print("\n✓ Ontology initialized")

    print("\nLoading sample data...")
    setup_data(onto)

    # Example 1: Query all employment relations
    print("\n" + "=" * 80)
    print("1. ALL EMPLOYMENT STINTS")
    print("=" * 80)

    all_employments = list(onto.query().relations(Employment).collect())
    print(f"\nTotal employment stints: {len(all_employments)}")

    for emp in all_employments:
        person = emp.left
        company = emp.right
        status = "Active" if emp.is_active else "Ended"
        print(f"\n  {person.name} at {company.name}")
        print(f"    Stint ID: {emp.stint_id}")
        print(f"    Role: {emp.role} ({emp.department})")
        print(f"    Period: {emp.start_date} to {emp.end_date or 'Present'}")
        print(f"    Salary: ${emp.salary:,}")
        print(f"    Status: {status}")

    # Example 2: Multiple stints for same person at same company
    print("\n" + "=" * 80)
    print("2. MULTIPLE STINTS: SAME PERSON, SAME COMPANY")
    print("=" * 80)

    # Query all employments and filter by accessing hydrated endpoints
    all_employments = list(onto.query().relations(Employment).collect())
    alice_techcorp = [
        emp
        for emp in all_employments
        if emp.left.person_id == "p1" and emp.right.company_id == "c1"
    ]

    print("\nAlice's employment history at TechCorp:")
    print(f"  Total stints: {len(alice_techcorp)}")

    for emp in alice_techcorp:
        print(f"\n  Stint: {emp.stint_id}")
        print(f"    Role: {emp.role}")
        print(f"    Duration: {emp.start_date} to {emp.end_date or 'Present'}")
        print(f"    Progression: ${emp.salary:,}")

    # Example 3: Career progression at one company
    print("\n" + "=" * 80)
    print("3. CAREER PROGRESSION ANALYSIS")
    print("=" * 80)

    # Query all and filter in Python
    all_employments = list(onto.query().relations(Employment).collect())
    alice_stints = [emp for emp in all_employments if emp.left.person_id == "p1"]

    print(f"\nAlice's complete career history ({len(list(alice_stints))} stints):")
    print("\n  Career Path:")

    total_earnings = 0
    for emp in alice_stints:
        company = emp.right
        duration = f"{emp.start_date} to {emp.end_date or 'Present'}"
        print(f"    • {company.name}: {emp.role}")
        print(f"      ({duration}) - ${emp.salary:,}")
        if emp.is_active:
            total_earnings += emp.salary

    print(f"\n  Current total compensation: ${total_earnings:,}")

    # Example 4: Active employments only
    print("\n" + "=" * 80)
    print("4. ACTIVE EMPLOYMENTS")
    print("=" * 80)

    active_employments = (
        onto.query().relations(Employment).where(Employment.is_active.is_true()).collect()
    )

    print(f"\nCurrently active employments: {len(list(active_employments))}")
    for emp in active_employments:
        person = emp.left
        company = emp.right
        print(f"\n  {person.name} - {emp.role} at {company.name}")
        print(f"    Started: {emp.start_date}")
        print(f"    Department: {emp.department}")
        print(f"    Annual Salary: ${emp.salary:,}")

    # Example 5: Department analysis
    print("\n" + "=" * 80)
    print("5. DEPARTMENT COMPOSITION (TechCorp)")
    print("=" * 80)

    # Query active and filter by company in Python
    all_active = list(
        onto.query().relations(Employment).where(Employment.is_active.is_true()).collect()
    )
    techcorp_employments = [emp for emp in all_active if emp.right.company_id == "c1"]

    print("\nActive employees at TechCorp:")
    dept_counts: dict[str, int] = {}
    for emp in techcorp_employments:
        dept = emp.department
        dept_counts[dept] = dept_counts.get(dept, 0) + 1

    for dept, count in sorted(dept_counts.items()):
        print(f"  {dept}: {count} employee(s)")

    # Summary
    print("\n" + "=" * 80)
    print("KEY CONCEPTS DEMONSTRATED")
    print("=" * 80)
    print("\n✓ Keyed Relations (Field(instance_key=True)):")
    print("  - Identity: (type, left_key, right_key, instance_key)")
    print("  - Enables multiple concurrent instances per endpoint pair")
    print("  - Perfect for: employment stints, course enrollments, repeated events")
    print("\n✓ Comparison with Unkeyed Relations:")
    print("  - Unkeyed: (type, left, right) → One current state per pair")
    print("  - Keyed: (type, left, right, instance) → Multiple concurrent states")
    print("\n✓ Query Patterns:")
    print("  - Filter by instance attributes (stint_id, role, etc.)")
    print("  - Filter by endpoint fields via .left and .right")
    print("  - Aggregate across multiple instances")
    print("\nDatabase file: tmp/keyed_relations.db")
    print("=" * 80)


if __name__ == "__main__":
    main()
