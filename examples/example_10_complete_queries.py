"""Example 10: Complete Queries - All Query Operators and Patterns.

This example demonstrates all query operators and patterns:
- Comparison operators: ==, !=, >, <, >=, <=
- String operations: startswith, endswith, contains
- Collection operations: in_
- Null checks: is_null, is_not_null
- Boolean checks: is_true, is_false
- Logical operators: &, |, ~ (AND, OR, NOT)
- Metadata access: obj.meta()
- Full aggregation suite: avg, min, max

References: SPEC §Query API, WRK-0017 (Filtering), WRK-0027 (Aggregations)
"""

from ontologia import Entity, Field, Session

# Note: We import aggregation functions for reference, but use Python built-ins
# for manual calculations.
# from ontologia.query import avg, count, max, min, sum


class Employee(Entity):
    """Employee entity with various field types for query demonstrations."""

    employee_id: Field[str] = Field(primary_key=True)
    name: Field[str]
    email: Field[str]
    department: Field[str]
    salary: Field[int]
    is_active: Field[bool] = Field(default=True)
    manager_id: Field[str | None] = Field(default=None)  # Null for top-level
    skills: Field[list[str]] = Field(default_factory=list)
    start_date: Field[str]


def setup_sample_data(onto: Session) -> None:
    """Create diverse employee data for query demonstrations."""
    with onto.session() as session:
        session.ensure(
            [
                # Engineering team
                Employee(
                    employee_id="e1",
                    name="Alice Johnson",
                    email="alice.johnson@company.com",
                    department="Engineering",
                    salary=120000,
                    is_active=True,
                    manager_id=None,  # CEO
                    skills=["python", "leadership", "architecture"],
                    start_date="2020-01-15",
                ),
                Employee(
                    employee_id="e2",
                    name="Bob Smith",
                    email="bob.smith@company.com",
                    department="Engineering",
                    salary=95000,
                    is_active=True,
                    manager_id="e1",
                    skills=["python", "javascript", "react"],
                    start_date="2021-03-20",
                ),
                Employee(
                    employee_id="e3",
                    name="Carol White",
                    email="carol.white@company.com",
                    department="Engineering",
                    salary=110000,
                    is_active=True,
                    manager_id="e1",
                    skills=["python", "rust", "database"],
                    start_date="2020-08-10",
                ),
                # Sales team
                Employee(
                    employee_id="e4",
                    name="David Brown",
                    email="david.brown@company.com",
                    department="Sales",
                    salary=85000,
                    is_active=True,
                    manager_id=None,  # Sales VP
                    skills=["negotiation", "crm", "presentation"],
                    start_date="2019-05-01",
                ),
                Employee(
                    employee_id="e5",
                    name="Eve Davis",
                    email="eve.davis@company.com",
                    department="Sales",
                    salary=75000,
                    is_active=False,  # Inactive
                    manager_id="e4",
                    skills=["cold-calling", "crm"],
                    start_date="2022-02-14",
                ),
                # Marketing team
                Employee(
                    employee_id="e6",
                    name="Frank Miller",
                    email="frank.miller@company.com",
                    department="Marketing",
                    salary=90000,
                    is_active=True,
                    manager_id=None,
                    skills=["seo", "content", "analytics"],
                    start_date="2021-01-10",
                ),
            ]
        )

    print("  ✓ Created 6 employees across 3 departments")


def main():
    """Run the complete queries example."""
    print("=" * 80)
    print("ONTOLOGIA COMPLETE QUERIES EXAMPLE")
    print("=" * 80)
    print("\nDemonstrating all query operators and patterns")

    onto = Session(datastore_uri="tmp/complete_queries.db")
    print("\n✓ Ontology initialized")

    print("\nLoading sample data...")
    setup_sample_data(onto)

    # Section 1: Comparison Operators
    print("\n" + "=" * 80)
    print("1. COMPARISON OPERATORS")
    print("=" * 80)

    # Equality (==)
    print("\n1.1 Equality (==)")
    engineers = list(
        onto.query().entities(Employee).where(Employee.department == "Engineering").collect()
    )
    print(f"  Engineering employees: {len(engineers)}")

    # Not equal (!=)
    print("\n1.2 Not Equal (!=)")
    non_sales = list(
        onto.query().entities(Employee).where(Employee.department != "Sales").collect()
    )
    print(f"  Non-Sales employees: {len(non_sales)}")

    # Greater than (>)
    print("\n1.3 Greater Than (>)")
    high_earners = list(onto.query().entities(Employee).where(Employee.salary > 100000).collect())
    print(f"  Employees earning > $100k: {len(high_earners)}")
    for emp in high_earners:
        print(f"    - {emp.name}: ${emp.salary:,}")

    # Less than (<)
    print("\n1.4 Less Than (<)")
    lower_earners = list(onto.query().entities(Employee).where(Employee.salary < 90000).collect())
    print(f"  Employees earning < $90k: {len(lower_earners)}")

    # Greater than or equal (>=)
    print("\n1.5 Greater Than or Equal (>=)")
    senior = list(onto.query().entities(Employee).where(Employee.salary >= 100000).collect())
    print(f"  Employees earning >= $100k: {len(senior)}")

    # Less than or equal (<=)
    print("\n1.6 Less Than or Equal (<=)")
    junior = list(onto.query().entities(Employee).where(Employee.salary <= 80000).collect())
    print(f"  Employees earning <= $80k: {len(junior)}")

    # Section 2: String Operations
    print("\n" + "=" * 80)
    print("2. STRING OPERATIONS")
    print("=" * 80)

    # startswith
    print("\n2.1 startswith()")
    j_names = list(onto.query().entities(Employee).where(Employee.name.startswith("J")).collect())
    print(f"  Names starting with 'J': {len(j_names)}")
    for emp in j_names:
        print(f"    - {emp.name}")

    # endswith
    print("\n2.2 endswith()")
    smiths = list(onto.query().entities(Employee).where(Employee.name.endswith("Smith")).collect())
    print(f"  Names ending with 'Smith': {len(smiths)}")

    # contains
    print("\n2.3 contains()")
    company_emails = list(
        onto.query().entities(Employee).where(Employee.email.contains("@company.com")).collect()
    )
    print(f"  Employees with @company.com email: {len(company_emails)}")

    # Section 3: Collection Operations
    print("\n" + "=" * 80)
    print("3. COLLECTION OPERATIONS")
    print("=" * 80)

    # in_ - membership test
    print("\n3.1 in_() - Department membership")
    tech_depts = list(
        onto.query()
        .entities(Employee)
        .where(Employee.department.in_(["Engineering", "Marketing"]))
        .collect()
    )
    print(f"  Engineering or Marketing: {len(tech_depts)}")
    for emp in tech_depts:
        print(f"    - {emp.name} ({emp.department})")

    # Section 4: Null Checks
    print("\n" + "=" * 80)
    print("4. NULL CHECKS")
    print("=" * 80)

    # is_null
    print("\n4.1 is_null()")
    top_level = list(onto.query().entities(Employee).where(Employee.manager_id.is_null()).collect())
    print(f"  Top-level employees (no manager): {len(top_level)}")
    for emp in top_level:
        print(f"    - {emp.name} ({emp.department} head)")

    # is_not_null
    print("\n4.2 is_not_null()")
    has_manager = list(
        onto.query().entities(Employee).where(Employee.manager_id.is_not_null()).collect()
    )
    print(f"  Employees with managers: {len(has_manager)}")

    # Section 5: Boolean Checks
    print("\n" + "=" * 80)
    print("5. BOOLEAN CHECKS")
    print("=" * 80)

    # is_true
    print("\n5.1 is_true()")
    active = list(onto.query().entities(Employee).where(Employee.is_active.is_true()).collect())
    print(f"  Active employees: {len(active)}")

    # is_false
    print("\n5.2 is_false()")
    inactive = list(onto.query().entities(Employee).where(Employee.is_active.is_false()).collect())
    print(f"  Inactive employees: {len(inactive)}")
    for emp in inactive:
        print(f"    - {emp.name}")

    # Section 6: Logical Operators
    print("\n" + "=" * 80)
    print("6. LOGICAL OPERATORS")
    print("=" * 80)

    # AND (&)
    print("\n6.1 AND (&)")
    senior_eng = list(
        onto.query()
        .entities(Employee)
        .where((Employee.department == "Engineering") & (Employee.salary >= 100000))
        .collect()
    )
    print(f"  Senior Engineers (>=$100k): {len(senior_eng)}")
    for emp in senior_eng:
        print(f"    - {emp.name}: ${emp.salary:,}")

    # OR (|)
    print("\n6.2 OR (|)")
    sales_or_high = list(
        onto.query()
        .entities(Employee)
        .where((Employee.department == "Sales") | (Employee.salary > 100000))
        .collect()
    )
    print(f"  Sales OR High Earners: {len(sales_or_high)}")

    # NOT (~)
    print("\n6.3 NOT (~)")
    not_engineering = list(
        onto.query().entities(Employee).where(~(Employee.department == "Engineering")).collect()
    )
    print(f"  Not in Engineering: {len(not_engineering)}")

    # Complex combination
    print("\n6.4 Complex Combination")
    complex_filter = list(
        onto.query()
        .entities(Employee)
        .where(
            (Employee.is_active.is_true())
            & ((Employee.department == "Engineering") | (Employee.department == "Sales"))
            & (Employee.salary >= 80000)
        )
        .collect()
    )
    print(f"  Active Eng/Sales earning >=$80k: {len(complex_filter)}")

    # Section 7: Complete Aggregation Suite
    print("\n" + "=" * 80)
    print("7. COMPLETE AGGREGATION SUITE")
    print("=" * 80)

    all_employees = list(onto.query().entities(Employee).collect())

    # count
    total = len(all_employees)
    print(f"\n7.1 count(): {total} total employees")

    # sum
    total_payroll = sum(emp.salary for emp in all_employees)
    print(f"7.2 sum(): ${total_payroll:,} total payroll")

    # avg
    avg_salary = total_payroll // total if total > 0 else 0
    print(f"7.3 avg(): ${avg_salary:,} average salary")

    # min
    min_salary = min(emp.salary for emp in all_employees) if all_employees else 0
    print(f"7.4 min(): ${min_salary:,} minimum salary")

    # max
    max_salary = max(emp.salary for emp in all_employees) if all_employees else 0
    print(f"7.5 max(): ${max_salary:,} maximum salary")

    # Section 8: Grouped Aggregations
    print("\n" + "=" * 80)
    print("8. GROUPED AGGREGATIONS")
    print("=" * 80)

    # Department stats using Python grouping
    from collections import defaultdict

    dept_stats = defaultdict(lambda: {"count": 0, "total": 0, "min": float("inf"), "max": 0})

    for emp in all_employees:
        dept = emp.department
        dept_stats[dept]["count"] += 1
        dept_stats[dept]["total"] += emp.salary
        dept_stats[dept]["min"] = min(dept_stats[dept]["min"], emp.salary)
        dept_stats[dept]["max"] = max(dept_stats[dept]["max"], emp.salary)

    print("\nDepartment Statistics:")
    for dept, stats in sorted(dept_stats.items()):
        avg_sal = stats["total"] // stats["count"] if stats["count"] > 0 else 0
        print(f"\n  {dept}:")
        print(f"    Count: {stats['count']}")
        print(f"    Total: ${stats['total']:,}")
        print(f"    Average: ${avg_sal:,}")
        print(f"    Range: ${stats['min']:,} - ${stats['max']:,}")

    # Section 9: Metadata Access
    print("\n" + "=" * 80)
    print("9. METADATA ACCESS (obj.meta())")
    print("=" * 80)

    print("\n9.1 Entity Metadata")
    sample_emp = onto.query().entities(Employee).first()
    if sample_emp:
        meta = sample_emp.meta()
        print(f"  Employee: {sample_emp.name}")
        print("  Metadata:")
        print(f"    - commit_id: {meta.commit_id}")
        print(f"    - type_name: {meta.type_name}")
        print(f"    - key: {meta.key}")

    print("\n9.2 Query All with Metadata")
    print("  Recent hires (showing commit IDs):")
    recent = list(onto.query().entities(Employee).collect())[:3]
    for emp in recent:
        meta = emp.meta()
        print(f"    - {emp.name}: created in commit {meta.commit_id}")

    # Summary
    print("\n" + "=" * 80)
    print("QUERY OPERATORS SUMMARY")
    print("=" * 80)
    print("\n✓ Comparison Operators:")
    print("  ==  (equality)")
    print("  !=  (not equal)")
    print("  >   (greater than)")
    print("  <   (less than)")
    print("  >=  (greater than or equal)")
    print("  <=  (less than or equal)")
    print("\n✓ String Operations:")
    print("  .startswith(str)")
    print("  .endswith(str)")
    print("  .contains(str)")
    print("\n✓ Collection Operations:")
    print("  .in_(list) - membership test")
    print("\n✓ Null Checks:")
    print("  .is_null()")
    print("  .is_not_null()")
    print("\n✓ Boolean Checks:")
    print("  .is_true()")
    print("  .is_false()")
    print("\n✓ Logical Operators:")
    print("  &  (AND)")
    print("  |  (OR)")
    print("  ~  (NOT)")
    print("\n✓ Aggregations:")
    print("  .count(), sum(), avg(), min(), max()")
    print("\n✓ Metadata:")
    print("  obj.meta() - access commit_id, type_name, key")
    print("\nDatabase file: tmp/complete_queries.db")
    print("=" * 80)


if __name__ == "__main__":
    main()
