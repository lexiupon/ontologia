"""Tests for onto info command."""

import json

from tests.cli.conftest import invoke


def test_info_basic(runner, seeded_db):
    result = invoke(runner, ["info"], seeded_db)
    assert result.exit_code == 0
    assert "Database:" in result.output
    assert "Head commit:" in result.output


def test_info_json(runner, seeded_db):
    result = invoke(runner, ["--json", "info"], seeded_db)
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert "db_path" in data
    assert "head_commit_id" in data
    assert "engine_version" in data


def test_info_stats(runner, seeded_db):
    result = invoke(runner, ["info", "--stats"], seeded_db)
    assert result.exit_code == 0
    assert "Customer:" in result.output


def test_info_schema(runner, seeded_db):
    result = invoke(runner, ["info", "--schema"], seeded_db)
    assert result.exit_code == 0
    assert "Customer" in result.output
    assert "Product" in result.output


def test_info_missing_db(runner, tmp_path):
    db = str(tmp_path / "nonexistent.db")
    result = invoke(runner, ["info"], db)
    assert result.exit_code != 0


def test_info_stats_json(runner, seeded_db):
    result = invoke(runner, ["--json", "info", "--stats"], seeded_db)
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert "entity_counts" in data
    assert data["entity_counts"]["Customer"] == 2
