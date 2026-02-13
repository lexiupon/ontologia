"""Example 01: Basic Usage - Ontologia Fundamentals.

This example demonstrates the fundamental operations:
- Defining entities using Entity base class with Field[T] annotations
- Defining relations using Relation[L, R] base class
- Loading data using session.ensure() with typed entities
- Basic queries using onto.query().entities()
- Simple filtering and result iteration

References: WRK-0074 (Entity/Relation API), WRK-0013 (Query DSL)
"""

from ontologia import Entity, Field, Relation, Session


# Step 1: Define Entity Types
# Entities represent first-class domain models in your ontology.
# Use the Entity base class with version and key metadata.
class Person(Entity):
    """A person in our system."""

    email: Field[str] = Field(primary_key=True)
    name: Field[str]
    age: Field[int]
    city: Field[str | None] = Field(default=None)


class Company(Entity):
    """A company in our system."""

    name: Field[str] = Field(primary_key=True)
    industry: Field[str]
    founded: Field[int | None] = Field(default=None)


# Step 2: Define Relation Types
# Relations represent connections between entities.
# Use Relation[L, R] base class with typed left and right endpoints.
class WorksAt(Relation[Person, Company]):
    """Employment relation between a person and a company."""

    role: Field[str]
    start_date: Field[str]
    end_date: Field[str | None] = Field(default=None)


class FoundedBy(Relation[Company, Person]):
    """Founder relation from company to person."""

    founding_date: Field[str]


def main():
    """Run the basic usage example."""
    print("=" * 80)
    print("ONTOLOGIA BASIC USAGE EXAMPLE")
    print("=" * 80)

    # Step 3: Initialize the Ontology
    # Create an ontology instance connected to a SQLite database.
    # Use a local path (not /tmp) for the database file.
    onto = Session(datastore_uri="tmp/basic_usage.db")
    print("\n✓ Ontology initialized: tmp/basic_usage.db")

    # Step 4: Load Data
    # Use session.ensure() with typed entity instances to add entities.
    print("\n" + "=" * 80)
    print("LOADING DATA")
    print("=" * 80)

    print("\nAdding people...")
    with onto.session() as session:
        session.ensure(
            [
                Person(
                    email="alice@example.com",
                    name="Alice Smith",
                    age=32,
                    city="San Francisco",
                ),
                Person(
                    email="bob@example.com",
                    name="Bob Johnson",
                    age=28,
                    city="New York",
                ),
                Person(
                    email="carol@example.com",
                    name="Carol Williams",
                    age=35,
                    city="Austin",
                ),
            ]
        )
    print("✓ Added 3 people")

    print("\nAdding companies...")
    with onto.session() as session:
        session.ensure(
            [
                Company(
                    name="Acme Corp",
                    industry="Technology",
                    founded=2010,
                ),
                Company(
                    name="TechStart Inc",
                    industry="Software",
                    founded=2018,
                ),
            ]
        )
    print("✓ Added 2 companies")

    print("\nAdding relations...")
    with onto.session() as session:
        session.ensure(
            [
                WorksAt(
                    left_key="alice@example.com",
                    right_key="Acme Corp",
                    role="Senior Engineer",
                    start_date="2020-01-15",
                ),
                WorksAt(
                    left_key="bob@example.com",
                    right_key="TechStart Inc",
                    role="Product Manager",
                    start_date="2021-06-01",
                ),
                FoundedBy(
                    left_key="TechStart Inc",
                    right_key="carol@example.com",
                    founding_date="2018-03-15",
                ),
            ]
        )
    print("✓ Added 3 relations")

    # Step 5: Basic Queries
    # Use onto.query() to retrieve data from the ontology.
    print("\n" + "=" * 80)
    print("QUERYING DATA")
    print("=" * 80)

    # Query all people - type-safe with class parameter!
    print("\n1. All people in the ontology:")
    people_query = onto.query().entities(Person)
    people = list(people_query.collect())
    print(f"   Found {len(people)} people:")
    for person in people:
        print(f"   - {person.name} ({person.email}), age {person.age}")

    # Query all companies - type-safe with class parameter!
    print("\n2. All companies in the ontology:")
    companies_query = onto.query().entities(Company)
    companies = list(companies_query.collect())
    print(f"   Found {len(companies)} companies:")
    for company in companies:
        industry = company.industry
        founded = company.founded if company.founded else "Unknown"
        print(f"   - {company.name} ({industry}), founded {founded}")

    # Simple filtering with typed expressions - IDE autocomplete!
    print("\n3. People older than 30:")
    older_people_query = onto.query().entities(Person).where(Person.age > 30)
    older_people = list(older_people_query.collect())
    print(f"   Found {len(older_people)} people:")
    for person in older_people:
        print(f"   - {person.name}, age {person.age}")

    # Filter by city - type-safe!
    print("\n4. People in San Francisco:")
    sf_people_query = onto.query().entities(Person).where(Person.city == "San Francisco")
    sf_people = list(sf_people_query.collect())
    print(f"   Found {len(sf_people)} people:")
    for person in sf_people:
        print(f"   - {person.name} ({person.city})")

    print("\n" + "=" * 80)
    print("EXAMPLE COMPLETE")
    print("=" * 80)
    print("\nKey concepts demonstrated:")
    print("  ✓ Defining entities with Entity base class")
    print("  ✓ Using Field[T] for type-safe field annotations")
    print("  ✓ Defining relations with Relation[L, R] base class")
    print("  ✓ Loading data with session.ensure()")
    print("  ✓ Creating relations with Ensure")
    print("  ✓ Querying entities with onto.query().entities()")
    print("  ✓ Filtering results with .where()")
    print("\nDatabase file: tmp/basic_usage.db")
    print("=" * 80)


if __name__ == "__main__":
    main()
