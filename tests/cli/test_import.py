"""Tests for onto import command."""

import json
from typing import Any

from ontologia.storage import Repository
from tests.cli.conftest import invoke


def _write_jsonl(path: str, records: list[dict[str, Any]]) -> None:
    with open(path, "w") as f:
        for rec in records:
            f.write(json.dumps(rec) + "\n")


def test_import_dry_run(runner, seeded_db, tmp_path):
    # Create import data with a new customer
    import_file = str(tmp_path / "import.jsonl")
    _write_jsonl(
        import_file,
        [
            {
                "type_kind": "entity",
                "type_name": "Customer",
                "key": "c3",
                "fields": {
                    "id": "c3",
                    "name": "Charlie",
                    "age": 35,
                    "tier": "Standard",
                    "active": True,
                },
            },
        ],
    )

    result = invoke(
        runner,
        ["import", "--input", import_file, "--models", "tests.conftest", "--dry-run"],
        seeded_db,
    )
    assert result.exit_code == 0
    assert "dry" in result.output.lower() or "Inserts: 1" in result.output


def test_import_dry_run_json(runner, seeded_db, tmp_path):
    import_file = str(tmp_path / "import.jsonl")
    _write_jsonl(
        import_file,
        [
            {
                "type_kind": "entity",
                "type_name": "Customer",
                "key": "c3",
                "fields": {
                    "id": "c3",
                    "name": "Charlie",
                    "age": 35,
                    "tier": "Standard",
                    "active": True,
                },
            },
        ],
    )

    result = invoke(
        runner,
        ["--json", "import", "--input", import_file, "--models", "tests.conftest", "--dry-run"],
        seeded_db,
    )
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["inserts"] == 1


def test_import_apply_upsert(runner, seeded_db, tmp_path):
    import_file = str(tmp_path / "import.jsonl")
    _write_jsonl(
        import_file,
        [
            {
                "type_kind": "entity",
                "type_name": "Customer",
                "key": "c3",
                "fields": {
                    "id": "c3",
                    "name": "Charlie",
                    "age": 35,
                    "tier": "Standard",
                    "active": True,
                },
            },
        ],
    )

    result = invoke(
        runner,
        [
            "--json",
            "import",
            "--input",
            import_file,
            "--models",
            "tests.conftest",
            "--apply",
            "--on-conflict",
            "upsert",
        ],
        seeded_db,
    )
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["status"] == "applied"
    assert data["inserts"] == 1

    # Verify the data is actually in the DB
    repo = Repository(seeded_db)
    entity = repo.get_latest_entity("Customer", "c3")
    assert entity is not None
    assert entity["fields"]["name"] == "Charlie"
    repo.close()


def test_import_apply_abort_conflict(runner, seeded_db, tmp_path):
    """Import existing entity with abort policy should fail."""
    import_file = str(tmp_path / "import.jsonl")
    _write_jsonl(
        import_file,
        [
            {
                "type_kind": "entity",
                "type_name": "Customer",
                "key": "c1",
                "fields": {
                    "id": "c1",
                    "name": "Alice Updated",
                    "age": 31,
                    "tier": "Gold",
                    "active": True,
                },
            },
        ],
    )

    result = invoke(
        runner,
        [
            "import",
            "--input",
            import_file,
            "--models",
            "tests.conftest",
            "--apply",
            "--on-conflict",
            "abort",
        ],
        seeded_db,
    )
    assert result.exit_code != 0


def test_import_apply_skip(runner, seeded_db, tmp_path):
    """Import with skip policy should skip existing entities."""
    import_file = str(tmp_path / "import.jsonl")
    _write_jsonl(
        import_file,
        [
            {
                "type_kind": "entity",
                "type_name": "Customer",
                "key": "c1",
                "fields": {
                    "id": "c1",
                    "name": "Alice Updated",
                    "age": 31,
                    "tier": "Gold",
                    "active": True,
                },
            },
            {
                "type_kind": "entity",
                "type_name": "Customer",
                "key": "c3",
                "fields": {
                    "id": "c3",
                    "name": "Charlie",
                    "age": 35,
                    "tier": "Standard",
                    "active": True,
                },
            },
        ],
    )

    result = invoke(
        runner,
        [
            "--json",
            "import",
            "--input",
            import_file,
            "--models",
            "tests.conftest",
            "--apply",
            "--on-conflict",
            "skip",
        ],
        seeded_db,
    )
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["skipped"] == 1
    assert data["inserts"] == 1


def test_import_from_directory(runner, seeded_db, tmp_path):
    """Import from a directory of JSONL files."""
    import_dir = tmp_path / "import_data"
    import_dir.mkdir()
    _write_jsonl(
        str(import_dir / "Customer.jsonl"),
        [
            {
                "type_kind": "entity",
                "type_name": "Customer",
                "key": "c3",
                "fields": {
                    "id": "c3",
                    "name": "Charlie",
                    "age": 35,
                    "tier": "Standard",
                    "active": True,
                },
            },
        ],
    )

    result = invoke(
        runner,
        [
            "--json",
            "import",
            "--input",
            str(import_dir),
            "--models",
            "tests.conftest",
            "--apply",
            "--on-conflict",
            "upsert",
        ],
        seeded_db,
    )
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["inserts"] == 1


def test_import_precondition_must_exist(runner, seeded_db, tmp_path):
    """must_exist precondition should fail for new entities."""
    import_file = str(tmp_path / "import.jsonl")
    _write_jsonl(
        import_file,
        [
            {
                "type_kind": "entity",
                "type_name": "Customer",
                "key": "c_new",
                "fields": {
                    "id": "c_new",
                    "name": "New",
                    "age": 20,
                    "tier": "Standard",
                    "active": True,
                },
            },
        ],
    )

    result = invoke(
        runner,
        [
            "import",
            "--input",
            import_file,
            "--models",
            "tests.conftest",
            "--apply",
            "--on-conflict",
            "upsert",
            "--precondition",
            "must_exist",
        ],
        seeded_db,
    )
    # Should fail with conflict because c_new doesn't exist
    assert result.exit_code != 0


def test_import_no_models(runner, seeded_db, tmp_path):
    import_file = str(tmp_path / "import.jsonl")
    _write_jsonl(import_file, [])

    result = invoke(
        runner,
        ["import", "--input", import_file],
        seeded_db,
    )
    assert result.exit_code != 0


def test_import_round_trip(runner, seeded_db, tmp_path):
    """Export then import into fresh DB — round trip test."""
    export_dir = str(tmp_path / "roundtrip_export")
    result = invoke(runner, ["export", "--output", export_dir], seeded_db)
    assert result.exit_code == 0

    # Create fresh DB
    fresh_db = str(tmp_path / "fresh.db")
    result = invoke(
        runner,
        [
            "--json",
            "import",
            "--input",
            export_dir,
            "--models",
            "tests.conftest",
            "--apply",
            "--on-conflict",
            "upsert",
        ],
        fresh_db,
    )
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["inserts"] > 0

    # Verify counts match
    from ontologia.storage import Repository

    orig = Repository(seeded_db)
    fresh = Repository(fresh_db)

    assert orig.count_latest_entities("Customer") == fresh.count_latest_entities("Customer")
    assert orig.count_latest_entities("Product") == fresh.count_latest_entities("Product")
    assert orig.count_latest_relations("Subscription") == fresh.count_latest_relations(
        "Subscription"
    )

    orig.close()
    fresh.close()
