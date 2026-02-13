"""Tests for onto verify command."""

import json

from tests.cli.conftest import invoke


def test_verify_ok(runner, seeded_db):
    result = invoke(runner, ["verify", "--models", "tests.conftest"], seeded_db)
    assert result.exit_code == 0
    assert "OK" in result.output


def test_verify_json(runner, seeded_db):
    result = invoke(runner, ["--json", "verify", "--models", "tests.conftest"], seeded_db)
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["status"] == "ok"


def test_verify_no_models(runner, seeded_db):
    result = invoke(runner, ["verify"], seeded_db)
    assert result.exit_code != 0


def test_verify_diff_no_mismatch(runner, seeded_db):
    result = invoke(runner, ["verify", "--models", "tests.conftest", "--diff"], seeded_db)
    # No mismatch, diff mode should still succeed
    assert result.exit_code == 0
