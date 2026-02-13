"""Shared test fixtures for Ontologia tests."""

from __future__ import annotations

import pytest

from ontologia import Entity, Field, Relation
from ontologia.storage import Repository

# --- Test Entity/Relation types ---


class Customer(Entity):
    id: Field[str] = Field(primary_key=True)
    name: Field[str]
    age: Field[int]
    email: Field[str | None] = Field(default=None, index=True)
    tier: Field[str] = Field(default="Standard", index=True)
    active: Field[bool] = Field(default=True)


class Product(Entity):
    sku: Field[str] = Field(primary_key=True)
    name: Field[str]
    price: Field[float]
    category: Field[str] = Field(default="General", index=True)


class Order(Entity):
    id: Field[str] = Field(primary_key=True)
    customer_id: Field[str] = Field(index=True)
    total_amount: Field[float]
    status: Field[str] = Field(default="Pending")
    country: Field[str] = Field(default="US")


class Subscription(Relation[Customer, Product]):
    seat_count: Field[int]
    started_at: Field[str]
    active: Field[bool] = Field(default=True)


class Follows(Relation[Customer, Customer]):
    pass


class Wishlisted(Relation[Customer, Product]):
    added_at: Field[str]


class Person(Entity):
    id: Field[str] = Field(primary_key=True)
    name: Field[str]


class Company(Entity):
    id: Field[str] = Field(primary_key=True)
    name: Field[str]


class Employment(Relation[Person, Company]):
    stint_id: Field[str] = Field(instance_key=True)
    role: Field[str]
    started_at: Field[str]


# --- Fixtures ---


@pytest.fixture
def tmp_db(tmp_path):
    """Create a temporary SQLite database path."""
    return str(tmp_path / "test.db")


@pytest.fixture
def repo(tmp_db):
    """Create a Repository instance with a temporary database."""
    r = Repository(tmp_db)
    yield r
    r.close()
