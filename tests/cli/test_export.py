"""Tests for onto export command."""

import json
import os

from tests.cli.conftest import invoke


def test_export_basic(runner, seeded_db, tmp_path):
    out_dir = str(tmp_path / "export_out")
    result = invoke(runner, ["export", "--output", out_dir], seeded_db)
    assert result.exit_code == 0
    assert "Exported" in result.output

    # Check files exist
    files = os.listdir(out_dir)
    assert "Customer.jsonl" in files
    assert "Product.jsonl" in files

    # Check content
    with open(os.path.join(out_dir, "Customer.jsonl")) as f:
        lines = [json.loads(line) for line in f]
    assert len(lines) == 2
    keys = {row["key"] for row in lines}
    assert keys == {"c1", "c2"}
    assert lines[0]["type_kind"] == "entity"


def test_export_with_metadata(runner, seeded_db, tmp_path):
    out_dir = str(tmp_path / "export_meta")
    result = invoke(runner, ["export", "--output", out_dir, "--with-metadata"], seeded_db)
    assert result.exit_code == 0

    with open(os.path.join(out_dir, "Customer.jsonl")) as f:
        line = json.loads(f.readline())
    assert "commit_id" in line


def test_export_type_filter(runner, seeded_db, tmp_path):
    out_dir = str(tmp_path / "export_filter")
    result = invoke(runner, ["export", "--output", out_dir, "--type", "Customer"], seeded_db)
    assert result.exit_code == 0

    files = os.listdir(out_dir)
    assert "Customer.jsonl" in files
    # Product shouldn't have data
    if "Product.jsonl" in files:
        with open(os.path.join(out_dir, "Product.jsonl")) as f:
            assert f.read().strip() == ""


def test_export_relations(runner, seeded_db, tmp_path):
    out_dir = str(tmp_path / "export_rels")
    result = invoke(runner, ["export", "--output", out_dir], seeded_db)
    assert result.exit_code == 0

    sub_file = os.path.join(out_dir, "Subscription.jsonl")
    assert os.path.exists(sub_file)
    with open(sub_file) as f:
        lines = [json.loads(line) for line in f]
    assert len(lines) == 1
    assert lines[0]["type_kind"] == "relation"
    assert lines[0]["left_key"] == "c1"


def test_export_as_of_before_activation_warns(runner, seeded_db, tmp_path):
    out_dir = str(tmp_path / "export_warn")
    result = invoke(runner, ["export", "--output", out_dir, "--as-of", "0"], seeded_db)
    assert result.exit_code == 0
    assert "before schema activation boundary" in result.output
