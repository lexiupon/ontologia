"""Example 03: Advanced Querying - Complex Queries and Aggregations.

This example demonstrates advanced query operations:
- Multi-hop traversals using .via()
- Complex filtering with .where() conditions
- Aggregations with .group_by() and .having()
- Combining multiple query operations

References: WRK-0015 (Traversals), WRK-0017 (Filtering), WRK-0027 (Aggregations)
"""

from ontologia import Entity, Field, Relation, Session
from ontologia.query import count


# Define schema for a social network with skills
class Person(Entity):
    """A person in the social network."""

    email: Field[str] = Field(primary_key=True)
    name: Field[str]
    age: Field[int]
    city: Field[str]


class Skill(Entity):
    """A professional skill."""

    name: Field[str] = Field(primary_key=True)
    category: Field[str]  # e.g., "programming", "design", "management"


class Project(Entity):
    """A project in the system."""

    id: Field[str] = Field(primary_key=True)
    name: Field[str]
    status: Field[str]  # "active", "completed", "archived"
    budget: Field[int]


class KnowsPerson(Relation[Person, Person]):
    """Social connection between people."""

    since: Field[str]
    connection_strength: Field[int]  # 1-10


class HasSkill(Relation[Person, Skill]):
    """Person's skill proficiency."""

    proficiency: Field[int]  # 1-10
    years_experience: Field[int]


class WorksOn(Relation[Person, Project]):
    """Person working on a project."""

    role: Field[str]
    hours_per_week: Field[int]


def setup_data(onto: Session) -> None:
    """Populate the ontology with sample data."""
    # Add people
    with onto.session() as session:
        session.ensure(
            [
                Person(
                    email="alice@example.com",
                    name="Alice",
                    age=32,
                    city="San Francisco",
                ),
                Person(
                    email="bob@example.com",
                    name="Bob",
                    age=28,
                    city="New York",
                ),
                Person(
                    email="carol@example.com",
                    name="Carol",
                    age=35,
                    city="Austin",
                ),
                Person(
                    email="dave@example.com",
                    name="Dave",
                    age=29,
                    city="Seattle",
                ),
            ]
        )

    # Add skills
    with onto.session() as session:
        session.ensure(
            [
                Skill(name="Python", category="programming"),
                Skill(name="JavaScript", category="programming"),
                Skill(name="UI Design", category="design"),
                Skill(name="Project Management", category="management"),
            ]
        )

    # Add projects
    with onto.session() as session:
        session.ensure(
            [
                Project(id="P1", name="Mobile App", status="active", budget=100000),
                Project(id="P2", name="Data Platform", status="active", budget=200000),
                Project(id="P3", name="Legacy Migration", status="completed", budget=150000),
            ]
        )

    # Add social connections
    with onto.session() as session:
        session.ensure(
            [
                KnowsPerson(
                    left_key="alice@example.com",
                    right_key="bob@example.com",
                    since="2020-01-15",
                    connection_strength=8,
                ),
                KnowsPerson(
                    left_key="alice@example.com",
                    right_key="carol@example.com",
                    since="2019-06-20",
                    connection_strength=9,
                ),
                KnowsPerson(
                    left_key="bob@example.com",
                    right_key="dave@example.com",
                    since="2021-03-10",
                    connection_strength=7,
                ),
            ]
        )

    # Add skill relations
    with onto.session() as session:
        session.ensure(
            [
                HasSkill(
                    left_key="alice@example.com",
                    right_key="Python",
                    proficiency=9,
                    years_experience=8,
                ),
                HasSkill(
                    left_key="alice@example.com",
                    right_key="JavaScript",
                    proficiency=7,
                    years_experience=5,
                ),
                HasSkill(
                    left_key="bob@example.com",
                    right_key="Python",
                    proficiency=6,
                    years_experience=3,
                ),
                HasSkill(
                    left_key="carol@example.com",
                    right_key="UI Design",
                    proficiency=9,
                    years_experience=10,
                ),
                HasSkill(
                    left_key="dave@example.com",
                    right_key="Project Management",
                    proficiency=8,
                    years_experience=6,
                ),
            ]
        )

    # Add project assignments
    with onto.session() as session:
        session.ensure(
            [
                WorksOn(
                    left_key="alice@example.com",
                    right_key="P1",
                    role="Tech Lead",
                    hours_per_week=30,
                ),
                WorksOn(
                    left_key="alice@example.com",
                    right_key="P2",
                    role="Contributor",
                    hours_per_week=10,
                ),
                WorksOn(
                    left_key="bob@example.com",
                    right_key="P1",
                    role="Developer",
                    hours_per_week=40,
                ),
                WorksOn(
                    left_key="carol@example.com",
                    right_key="P1",
                    role="Designer",
                    hours_per_week=20,
                ),
                WorksOn(
                    left_key="dave@example.com",
                    right_key="P2",
                    role="Project Manager",
                    hours_per_week=40,
                ),
            ]
        )


def main():
    """Run the advanced querying example."""
    print("=" * 80)
    print("ONTOLOGIA ADVANCED QUERYING EXAMPLE")
    print("=" * 80)

    onto = Session(datastore_uri="tmp/advanced_querying.db")
    print("\n✓ Ontology initialized")

    print("\nLoading sample data...")
    setup_data(onto)
    print("✓ Data loaded")

    # Example 1: Multi-hop Traversals
    print("\n" + "=" * 80)
    print("1. MULTI-HOP TRAVERSALS (.via)")
    print("=" * 80)

    print("\nFind friends of Alice:")
    alice_friends = (
        onto.query()
        .entities(Person)
        .where(Person.email == "alice@example.com")
        .via(KnowsPerson)
        .collect()
    )
    for path in alice_friends:
        friend = path.entities[-1]
        print(f"  - {friend.name} ({friend.email})")

    print("\nFind skills of Alice's friends (2-hop traversal):")
    friends_skills = (
        onto.query()
        .entities(Person)
        .where(Person.email == "alice@example.com")
        .via(KnowsPerson)
        .via(HasSkill)
        .collect()
    )
    for path in friends_skills:
        skill = path.entities[-1]
        friend = path.entities[1]
        print(f"  - {friend.name} has skill: {skill.name} ({skill.category})")

    # Example 2: Complex Filtering
    print("\n" + "=" * 80)
    print("2. COMPLEX FILTERING (.where)")
    print("=" * 80)

    print("\nPeople over 30 in West Coast cities:")
    # Note: For complex filters, chain multiple .where() or filter in Python
    all_people_over_30 = onto.query().entities(Person).where(Person.age > 30).collect()
    west_coast_seniors = [
        p for p in all_people_over_30 if p.city in ["San Francisco", "Seattle", "Los Angeles"]
    ]
    print(f"  Found {len(west_coast_seniors)} people:")
    for person in west_coast_seniors:
        print(f"  - {person.name}, age {person.age}, {person.city}")

    print("\nActive projects with budget over $150k:")
    # Note: For multiple conditions, use compound expressions with &
    all_active_projects = (
        onto.query()
        .entities(Project)
        .where((Project.status == "active") & (Project.budget > 150000))
        .collect()
    )
    results = list(all_active_projects)
    print(f"  Found {len(results)} projects:")
    for project in results:
        print(f"  - {project.name}: ${project.budget:,} ({project.status})")

    # Example 3: Aggregations
    print("\n" + "=" * 80)
    print("3. AGGREGATIONS (.group_by, .having)")
    print("=" * 80)

    print("\nPeople grouped by city:")
    by_city = onto.query().entities(Person).group_by(Person.city).agg(person_count=count())
    for group in by_city:
        city = group["city"]
        person_count = group["person_count"]
        print(f"  {city}: {person_count} people")

    print("\nSkills grouped by category:")
    by_category = onto.query().entities(Skill).group_by(Skill.category).agg(skill_count=count())
    for group in by_category:
        category = group["category"]
        skill_count = group["skill_count"]
        print(f"  {category}: {skill_count} skills")

    print("\nCities with more than 1 person (.having):")
    # Note: .having() filters aggregated results using @alias syntax
    populous_cities = (
        onto.query()
        .entities(Person)
        .group_by(Person.city)
        .having(count() > 1)
        .agg(person_count=count())
    )
    for group in populous_cities:
        city = group["city"]
        person_count = group["person_count"]
        print(f"  {city}: {person_count} people")

    # Example 4: Combining Operations
    print("\n" + "=" * 80)
    print("4. COMBINING OPERATIONS")
    print("=" * 80)

    print("\nPeople under 35 who work on active projects:")
    young_workers_on_active = (
        onto.query()
        .entities(Person)
        .where(Person.age < 35)
        .via(WorksOn)
        .where(Project.status == "active")
        .collect()
    )
    for path in young_workers_on_active:
        # TraversalPath contains raw dict rows, so access fields via dict keys.
        person = path.entities[0]
        project = path.entities[-1]
        print(f"  - {person.name} (age {person.age}) works on {project.name}")

    print("\nSkills in 'programming' category with high proficiency (>7):")
    # Note: To filter by relation attributes, we need to query edges
    edges = onto.query().relations(HasSkill).collect()
    high_proficiency_skills: list[tuple[str, int, int]] = []
    for edge in edges:
        if edge.proficiency > 7:
            skill = edge.right
            skill_name = skill.name
            proficiency = edge.proficiency
            years = edge.years_experience
            high_proficiency_skills.append((skill_name, proficiency, years))

    high_proficiency_skills.sort(key=lambda row: -row[1])
    for skill_name, proficiency, years in high_proficiency_skills:
        print(f"  - {skill_name}: proficiency {proficiency}/10, {years} years exp")

    print("\n" + "=" * 80)
    print("EXAMPLE COMPLETE")
    print("=" * 80)
    print("\nKey concepts demonstrated:")
    print("  ✓ Multi-hop traversals with .via()")
    print("  ✓ Complex filtering with .where() and compound conditions")
    print("  ✓ Grouping with .group_by()")
    print("  ✓ Filtering groups with .having()")
    print("  ✓ Combining multiple query operations")
    print("  ✓ Edge queries for relation attributes")
    print("\nDatabase file: tmp/advanced_querying.db")
    print("=" * 80)


if __name__ == "__main__":
    main()
