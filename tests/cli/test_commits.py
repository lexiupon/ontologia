"""Tests for onto commits command."""

import json

from tests.cli.conftest import invoke


def test_commits_list(runner, seeded_db):
    result = invoke(runner, ["commits"], seeded_db)
    assert result.exit_code == 0
    assert "commit_id" in result.output


def test_commits_last(runner, seeded_db):
    result = invoke(runner, ["commits", "--last", "1"], seeded_db)
    assert result.exit_code == 0


def test_commits_json(runner, seeded_db):
    result = invoke(runner, ["--json", "commits"], seeded_db)
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert isinstance(data, list)
    assert len(data) >= 1
    assert "commit_id" in data[0]


def test_commits_examine(runner, seeded_db):
    result = invoke(runner, ["commits", "examine", "--id", "1"], seeded_db)
    assert result.exit_code == 0
    assert "Commit: 1" in result.output
    assert "Changes" in result.output


def test_commits_examine_json(runner, seeded_db):
    result = invoke(runner, ["--json", "commits", "examine", "--id", "1"], seeded_db)
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["commit_id"] == 1
    assert "changes" in data


def test_commits_examine_legacy_id(runner, seeded_db):
    """Test legacy --id flag on commits (should work like examine)."""
    result = invoke(runner, ["commits", "--id", "1"], seeded_db)
    assert result.exit_code == 0
    assert "Commit: 1" in result.output


def test_commits_examine_not_found(runner, seeded_db):
    result = invoke(runner, ["commits", "examine", "--id", "999"], seeded_db)
    assert result.exit_code != 0


def test_commits_since(runner, seeded_db):
    result = invoke(runner, ["--json", "commits", "--since", "1"], seeded_db)
    assert result.exit_code == 0
    data = json.loads(result.output)
    # Should only include commits after ID 1
    for c in data:
        assert c["commit_id"] > 1
