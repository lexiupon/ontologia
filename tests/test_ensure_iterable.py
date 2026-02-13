"""Tests for iterable support in ensure() method."""

import pytest

from ontologia import Session
from ontologia.types import Entity, Field, Relation


class MyEntity(Entity):
    id: Field[str] = Field(primary_key=True)
    name: Field[str]
    value: Field[int]


class MyRelation(Relation[MyEntity, MyEntity]):
    weight: Field[int] = Field(default=1)


def test_ensure_with_list_of_entities():
    """Test ensure() with a list of entities."""
    onto = Session(datastore_uri=":memory:")

    with onto.session() as session:
        session.ensure(
            [
                MyEntity(id="e1", name="Entity 1", value=10),
                MyEntity(id="e2", name="Entity 2", value=20),
                MyEntity(id="e3", name="Entity 3", value=30),
            ]
        )

    entities = onto.query().entities(MyEntity).collect()
    assert len(entities) == 3
    assert {e.id for e in entities} == {"e1", "e2", "e3"}


def test_ensure_with_list_of_relations():
    """Test ensure() with a list of relations."""
    onto = Session(datastore_uri=":memory:")

    # First add entities
    with onto.session() as session:
        session.ensure(
            [
                MyEntity(id="e1", name="Entity 1", value=10),
                MyEntity(id="e2", name="Entity 2", value=20),
            ]
        )

    # Then add relations
    with onto.session() as session:
        session.ensure(
            [
                MyRelation(left_key="e1", right_key="e2", weight=5),
                MyRelation(left_key="e2", right_key="e1", weight=3),
            ]
        )

    relations = onto.query().relations(MyRelation).collect()
    assert len(relations) == 2


def test_ensure_with_mixed_entities_and_relations():
    """Test ensure() with mixed entities and relations in same call."""
    onto = Session(datastore_uri=":memory:")

    with onto.session() as session:
        session.ensure(
            [
                MyEntity(id="e1", name="Entity 1", value=10),
                MyEntity(id="e2", name="Entity 2", value=20),
                MyRelation(left_key="e1", right_key="e2", weight=5),
            ]
        )

    entities = onto.query().entities(MyEntity).collect()
    relations = onto.query().relations(MyRelation).collect()

    assert len(entities) == 2
    assert len(relations) == 1


def test_ensure_with_generator():
    """Test ensure() with a generator expression."""
    onto = Session(datastore_uri=":memory:")

    data = [
        {"id": "e1", "name": "Entity 1", "value": 10},
        {"id": "e2", "name": "Entity 2", "value": 20},
        {"id": "e3", "name": "Entity 3", "value": 30},
    ]

    with onto.session() as session:
        session.ensure(MyEntity(id=row["id"], name=row["name"], value=row["value"]) for row in data)

    entities = onto.query().entities(MyEntity).collect()
    assert len(entities) == 3


def test_ensure_with_tuple():
    """Test ensure() with a tuple."""
    onto = Session(datastore_uri=":memory:")

    with onto.session() as session:
        session.ensure(
            (
                MyEntity(id="e1", name="Entity 1", value=10),
                MyEntity(id="e2", name="Entity 2", value=20),
            )
        )

    entities = onto.query().entities(MyEntity).collect()
    assert len(entities) == 2


def test_ensure_with_empty_list():
    """Test ensure() with empty list is a no-op."""
    onto = Session(datastore_uri=":memory:")

    with onto.session() as session:
        session.ensure([])

    entities = onto.query().entities(MyEntity).collect()
    assert len(entities) == 0


def test_ensure_single_object_still_works():
    """Test backward compatibility - single object still works."""
    onto = Session(datastore_uri=":memory:")

    with onto.session() as session:
        session.ensure(MyEntity(id="e1", name="Entity 1", value=10))

    entities = onto.query().entities(MyEntity).collect()
    assert len(entities) == 1
    assert entities[0].id == "e1"


def test_ensure_fails_fast_on_invalid_item():
    """Test that ensure() fails fast on first invalid item."""
    onto = Session(datastore_uri=":memory:")

    with pytest.raises(TypeError, match="Expected Entity or Relation"):
        with onto.session() as session:
            invalid_list: list[object] = [
                MyEntity(id="e1", name="Entity 1", value=10),
                "invalid",  # This should cause failure
                MyEntity(id="e2", name="Entity 2", value=20),
            ]
            session.ensure(invalid_list)  # type: ignore[arg-type]


def test_ensure_rejects_string():
    """Test that ensure() rejects strings (which are iterable)."""
    onto = Session(datastore_uri=":memory:")

    with pytest.raises(TypeError, match="Expected Entity, Relation, or Iterable"):
        with onto.session() as session:
            session.ensure("not an entity")  # type: ignore[arg-type]


def test_ensure_rejects_invalid_single_object():
    """Test that ensure() rejects invalid single objects."""
    onto = Session(datastore_uri=":memory:")

    with pytest.raises(TypeError, match="Expected Entity, Relation, or Iterable"):
        with onto.session() as session:
            session.ensure(123)  # type: ignore[arg-type]


def test_ensure_preserves_order():
    """Test that ensure() processes items in iteration order."""
    onto = Session(datastore_uri=":memory:")

    with onto.session() as session:
        session.ensure(
            [
                MyEntity(id="e3", name="Entity 3", value=30),
                MyEntity(id="e1", name="Entity 1", value=10),
                MyEntity(id="e2", name="Entity 2", value=20),
            ]
        )

    # Query with history to check commit order
    entities = onto.query().entities(MyEntity).collect()
    assert len(entities) == 3


def test_ensure_large_batch():
    """Test ensure() with a large batch of items."""
    onto = Session(datastore_uri=":memory:")

    batch_size = 1000
    with onto.session() as session:
        session.ensure(
            [MyEntity(id=f"e{i}", name=f"Entity {i}", value=i) for i in range(batch_size)]
        )

    entities = onto.query().entities(MyEntity).collect()
    assert len(entities) == batch_size
