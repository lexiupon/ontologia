"""Tests for onto query command."""

import json

from tests.cli.conftest import invoke


def test_query_entities(runner, seeded_db):
    result = invoke(
        runner, ["query", "entities", "Customer", "--models", "tests.conftest"], seeded_db
    )
    assert result.exit_code == 0
    assert "Alice" in result.output


def test_query_entities_json(runner, seeded_db):
    result = invoke(
        runner, ["--json", "query", "entities", "Customer", "--models", "tests.conftest"], seeded_db
    )
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert isinstance(data, list)
    assert len(data) == 2
    keys = {d["key"] for d in data}
    assert keys == {"c1", "c2"}


def test_query_entities_filter(runner, seeded_db):
    result = invoke(
        runner,
        [
            "--json",
            "query",
            "entities",
            "Customer",
            "--models",
            "tests.conftest",
            "--filter",
            "$.tier",
            "--filter",
            "eq",
            "--filter",
            '"Gold"',
        ],
        seeded_db,
    )
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert len(data) == 1
    assert data[0]["tier"] == "Gold"


def test_query_entities_limit(runner, seeded_db):
    result = invoke(
        runner,
        ["--json", "query", "entities", "Customer", "--models", "tests.conftest", "--limit", "1"],
        seeded_db,
    )
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert len(data) == 1


def test_query_entities_no_models(runner, seeded_db):
    result = invoke(runner, ["query", "entities", "Customer"], seeded_db)
    assert result.exit_code != 0


def test_query_entities_unknown_type(runner, seeded_db):
    result = invoke(
        runner, ["query", "entities", "Nonexistent", "--models", "tests.conftest"], seeded_db
    )
    assert result.exit_code != 0


def test_query_relations(runner, seeded_db):
    result = invoke(
        runner,
        ["--json", "query", "relations", "Subscription", "--models", "tests.conftest"],
        seeded_db,
    )
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert len(data) == 1
    assert data[0]["left_key"] == "c1"
    assert data[0]["right_key"] == "p1"


def test_query_traverse(runner, seeded_db):
    result = invoke(
        runner,
        [
            "--json",
            "query",
            "traverse",
            "Customer",
            "--via",
            "Subscription",
            "--models",
            "tests.conftest",
        ],
        seeded_db,
    )
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert isinstance(data, list)


def test_query_entities_as_of(runner, seeded_db):
    result = invoke(
        runner,
        ["--json", "query", "entities", "Customer", "--models", "tests.conftest", "--as-of", "1"],
        seeded_db,
    )
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert len(data) == 2


def test_query_entities_as_of_before_activation_warns(runner, seeded_db):
    result = invoke(
        runner,
        ["query", "entities", "Customer", "--models", "tests.conftest", "--as-of", "0"],
        seeded_db,
    )
    assert result.exit_code == 0
    assert "activation_commit_id" in result.output
    assert "before schema activation boundary" in result.output
