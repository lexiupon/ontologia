"""Example 07: Publishers - Data Export and Integration.

This example demonstrates exporting query results:
- Manual CSV export
- Manual JSON export
- Use cases: data extraction, reporting, integration

Note: This example shows manual export methods. For production use,
consider using the csv_publisher and ndjson_publisher functions from
ontologia.publishers.csv and ontologia.publishers.ndjson modules.

References: WRK-0028 (Publishers)
"""

import csv
import json
from pathlib import Path

from ontologia import Entity, Field, Session


class Employee(Entity):
    """An employee in the organization."""

    employee_id: Field[str] = Field(primary_key=True)
    name: Field[str]
    department: Field[str]
    salary: Field[int]
    hire_date: Field[str]


def setup_data(onto: Session) -> None:
    """Populate the ontology with sample employee data."""
    with onto.session() as session:
        session.ensure(
            [
                Employee(
                    employee_id="EMP-001",
                    name="Alice Johnson",
                    department="Engineering",
                    salary=120000,
                    hire_date="2020-01-15",
                ),
                Employee(
                    employee_id="EMP-002",
                    name="Bob Smith",
                    department="Engineering",
                    salary=110000,
                    hire_date="2020-06-01",
                ),
                Employee(
                    employee_id="EMP-003",
                    name="Carol Williams",
                    department="Sales",
                    salary=95000,
                    hire_date="2019-03-20",
                ),
                Employee(
                    employee_id="EMP-004",
                    name="Dave Brown",
                    department="Marketing",
                    salary=85000,
                    hire_date="2021-08-10",
                ),
                Employee(
                    employee_id="EMP-005",
                    name="Eve Davis",
                    department="Engineering",
                    salary=105000,
                    hire_date="2022-02-14",
                ),
            ]
        )


def main():
    """Run the publishers example."""
    print("=" * 80)
    print("ONTOLOGIA EXPORT EXAMPLE")
    print("=" * 80)

    onto = Session(datastore_uri="tmp/publishers.db")
    print("\n✓ Ontology initialized")

    print("\nLoading sample data...")
    setup_data(onto)
    print("✓ Data loaded")

    # Create output directory
    output_dir = Path("tmp/exports")
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"\n✓ Output directory created: {output_dir}")

    # Example 1: Export to CSV
    print("\n" + "=" * 80)
    print("1. EXPORTING TO CSV")
    print("=" * 80)

    print("\nExporting all employees to CSV...")
    employees = list(onto.query().entities(Employee).collect())
    csv_path = output_dir / "employees.csv"

    # Write CSV manually
    fieldnames = ["employee_id", "name", "department", "salary", "hire_date"]
    if employees:
        with open(csv_path, "w", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for emp in employees:
                writer.writerow(
                    {
                        "employee_id": emp.employee_id,
                        "name": emp.name,
                        "department": emp.department,
                        "salary": emp.salary,
                        "hire_date": emp.hire_date,
                    }
                )

        print(f"✓ Exported to: {csv_path}")
        print(f"  File size: {csv_path.stat().st_size} bytes")
        print(f"  Records: {len(employees)}")

    # Example 2: Export to JSON Lines
    print("\n" + "=" * 80)
    print("2. EXPORTING TO JSON LINES (NDJSON)")
    print("=" * 80)

    print("\nExporting all employees to NDJSON...")
    ndjson_path = output_dir / "employees.ndjson"

    with open(ndjson_path, "w") as f:
        for emp in employees:
            json.dump(
                {
                    "employee_id": emp.employee_id,
                    "name": emp.name,
                    "department": emp.department,
                    "salary": emp.salary,
                    "hire_date": emp.hire_date,
                },
                f,
            )
            f.write("\n")

    print(f"✓ Exported to: {ndjson_path}")
    print(f"  File size: {ndjson_path.stat().st_size} bytes")

    # Example 3: Export Filtered Results
    print("\n" + "=" * 80)
    print("3. EXPORTING FILTERED RESULTS")
    print("=" * 80)

    print("\nExporting high-salary employees (>$100k) to CSV...")
    high_earners = list(onto.query().entities(Employee).where(Employee.salary > 100000).collect())
    high_earners_path = output_dir / "high_earners.csv"

    with open(high_earners_path, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for emp in high_earners:
            writer.writerow(
                {
                    "employee_id": emp.employee_id,
                    "name": emp.name,
                    "department": emp.department,
                    "salary": emp.salary,
                    "hire_date": emp.hire_date,
                }
            )

    print(f"✓ Exported to: {high_earners_path}")
    print(f"  Records exported: {len(high_earners)}")

    # Summary
    print("\n" + "=" * 80)
    print("EXPORT SUMMARY")
    print("=" * 80)

    print("\nGenerated files:")
    for export_file in sorted(output_dir.glob("*")):
        size = export_file.stat().st_size
        print(f"  - {export_file.name} ({size} bytes)")

    print("\n" + "=" * 80)
    print("EXAMPLE COMPLETE")
    print("=" * 80)
    print("\nKey concepts demonstrated:")
    print("  ✓ Manual CSV export using Python csv module")
    print("  ✓ Manual NDJSON export using Python json module")
    print("  ✓ Exporting filtered query results")
    print("  ✓ Use case: Data extraction for external systems")
    print("\nDatabase file: tmp/publishers.db")
    print(f"Export directory: {output_dir}")
    print("=" * 80)


if __name__ == "__main__":
    main()
