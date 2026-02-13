"""Tests for onto schema commands."""

import json

from tests.cli.conftest import invoke


def test_schema_export_code(runner, seeded_db):
    result = invoke(runner, ["schema", "export", "--models", "tests.conftest"], seeded_db)
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert "entities" in data
    assert "Customer" in data["entities"]


def test_schema_export_stored(runner, seeded_db):
    result = invoke(
        runner, ["schema", "export", "--kind", "entity", "--type", "Customer"], seeded_db
    )
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert "entity_name" in data or "fields" in data


def test_schema_export_stored_version(runner, seeded_db):
    result = invoke(
        runner,
        ["schema", "export", "--kind", "entity", "--type", "Customer", "--version", "1"],
        seeded_db,
    )
    assert result.exit_code == 0


def test_schema_export_not_found(runner, seeded_db):
    result = invoke(
        runner,
        ["schema", "export", "--kind", "entity", "--type", "Nonexistent"],
        seeded_db,
    )
    assert result.exit_code != 0


def test_schema_export_exclusive_modes(runner, seeded_db):
    result = invoke(
        runner,
        [
            "schema",
            "export",
            "--models",
            "tests.conftest",
            "--kind",
            "entity",
            "--type",
            "Customer",
        ],
        seeded_db,
    )
    assert result.exit_code != 0


def test_schema_export_with_hash(runner, seeded_db):
    result = invoke(
        runner,
        ["schema", "export", "--models", "tests.conftest", "--with-hash"],
        seeded_db,
    )
    assert result.exit_code == 0
    data = json.loads(result.output)
    for schemas in data["entities"].values():
        assert "schema_hash" in schemas


def test_schema_history(runner, seeded_db):
    result = invoke(
        runner,
        ["schema", "history", "--kind", "entity", "--type", "Customer"],
        seeded_db,
    )
    assert result.exit_code == 0
    assert "version" in result.output.lower() or "1" in result.output


def test_schema_history_json(runner, seeded_db):
    result = invoke(
        runner,
        ["--json", "schema", "history", "--kind", "entity", "--type", "Customer"],
        seeded_db,
    )
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert isinstance(data, list)
    assert len(data) >= 1


def test_schema_history_detail(runner, seeded_db):
    result = invoke(
        runner,
        ["--json", "schema", "history", "--kind", "entity", "--type", "Customer", "--version", "1"],
        seeded_db,
    )
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert "schema_json" in data


def test_schema_history_not_found(runner, seeded_db):
    result = invoke(
        runner,
        ["schema", "history", "--kind", "entity", "--type", "Customer", "--version", "999"],
        seeded_db,
    )
    assert result.exit_code != 0


def test_schema_drop_dry_run(runner, seeded_db):
    """Dry-run drops relation (no dependents)."""
    result = invoke(
        runner,
        ["--json", "schema", "drop", "relation", "Subscription", "--purge-history"],
        seeded_db,
    )
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert "token" in data


def test_schema_drop_entity_needs_cascade(runner, seeded_db):
    """Dropping entity with dependents should fail without cascade."""
    result = invoke(
        runner,
        ["schema", "drop", "entity", "Customer"],
        seeded_db,
    )
    assert result.exit_code != 0


def test_schema_drop_entity_cascade_dry_run(runner, seeded_db):
    result = invoke(
        runner,
        [
            "--json",
            "schema",
            "drop",
            "entity",
            "Customer",
            "--cascade-relations",
            "--purge-history",
        ],
        seeded_db,
    )
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert "token" in data
    affected_names = [a["type_name"] for a in data["affected_types"]]
    assert "Customer" in affected_names


def test_schema_drop_apply(runner, seeded_db):
    """Full dry-run then apply cycle for a relation type."""
    # Dry-run first
    result = invoke(
        runner,
        ["--json", "schema", "drop", "relation", "Subscription", "--purge-history"],
        seeded_db,
    )
    assert result.exit_code == 0
    data = json.loads(result.output)
    token = data["token"]

    # Apply
    result = invoke(
        runner,
        [
            "--json",
            "schema",
            "drop",
            "relation",
            "Subscription",
            "--purge-history",
            "--apply",
            "--token",
            token,
        ],
        seeded_db,
    )
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["status"] == "dropped"


def test_schema_drop_apply_no_token(runner, seeded_db):
    result = invoke(
        runner,
        ["schema", "drop", "relation", "Subscription", "--apply"],
        seeded_db,
    )
    assert result.exit_code != 0


def test_schema_drop_token_no_apply(runner, seeded_db):
    result = invoke(
        runner,
        ["schema", "drop", "relation", "Subscription", "--token", "abc"],
        seeded_db,
    )
    assert result.exit_code != 0


def test_schema_drop_relation_no_cascade(runner, seeded_db):
    """--cascade-relations invalid for relation targets."""
    result = invoke(
        runner,
        ["schema", "drop", "relation", "Subscription", "--cascade-relations"],
        seeded_db,
    )
    assert result.exit_code != 0


def test_schema_drop_mutual_exclusive(runner, seeded_db):
    """--drop-relation and --cascade-relations are mutually exclusive."""
    result = invoke(
        runner,
        [
            "schema",
            "drop",
            "entity",
            "Customer",
            "--drop-relation",
            "Subscription",
            "--cascade-relations",
        ],
        seeded_db,
    )
    assert result.exit_code != 0
