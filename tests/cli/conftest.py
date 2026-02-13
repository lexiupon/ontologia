"""Shared fixtures for CLI tests."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from typer.testing import CliRunner

from ontologia import Session
from ontologia.cli import app

# Reuse the model types from the main conftest
from tests.conftest import Customer, Product, Subscription

if TYPE_CHECKING:
    from click.testing import Result


@pytest.fixture
def runner():
    """Create a CLI test runner."""
    return CliRunner()


@pytest.fixture
def cli_db(tmp_path):
    """Create a temp DB path and set it as the CLI state."""
    db_path = str(tmp_path / "cli_test.db")
    return db_path


@pytest.fixture
def seeded_db(cli_db):
    """Create a DB with some seed data."""
    onto = Session(
        cli_db,
        entity_types=[Customer, Product],
        relation_types=[Subscription],
    )
    session = onto.session()
    session.ensure(Customer(id="c1", name="Alice", age=30, tier="Gold"))
    session.ensure(Customer(id="c2", name="Bob", age=25, tier="Silver"))
    session.ensure(Product(sku="p1", name="Widget", price=9.99))
    session.ensure(Product(sku="p2", name="Gadget", price=19.99, category="Tech"))
    session.commit()

    session.ensure(
        Subscription(left_key="c1", right_key="p1", seat_count=5, started_at="2024-01-01")
    )
    session.commit()

    onto.close()
    return cli_db


def invoke(runner: CliRunner, args: list[str], db_path: str | None = None) -> "Result":
    """Invoke CLI with proper state setup."""
    if db_path:
        # Inject --db before subcommand
        args = ["--db", db_path] + args
    result = runner.invoke(app, args, catch_exceptions=False)
    return result
