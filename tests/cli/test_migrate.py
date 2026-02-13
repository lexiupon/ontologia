"""Tests for onto migrate command."""

import json
import textwrap

from ontologia import Session
from tests.cli.conftest import invoke


def test_migrate_no_changes(runner, seeded_db):
    result = invoke(runner, ["migrate", "--models", "tests.conftest"], seeded_db)
    assert result.exit_code == 0
    assert "No schema changes" in result.output


def test_migrate_no_changes_json(runner, seeded_db):
    result = invoke(runner, ["--json", "migrate", "--models", "tests.conftest"], seeded_db)
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["has_changes"] is False


def test_migrate_no_models(runner, seeded_db):
    result = invoke(runner, ["migrate"], seeded_db)
    assert result.exit_code != 0


def test_migrate_force_token_exclusive(runner, seeded_db):
    result = invoke(
        runner,
        ["migrate", "--models", "tests.conftest", "--apply", "--force", "--token", "abc"],
        seeded_db,
    )
    assert result.exit_code != 0


def test_migrate_apply_no_token(runner, seeded_db):
    result = invoke(
        runner,
        ["migrate", "--models", "tests.conftest", "--apply"],
        seeded_db,
    )
    assert result.exit_code != 0


def test_migrate_with_changes(runner, tmp_path):
    """Create DB with old schema, then migrate with new models."""
    db_path = str(tmp_path / "migrate_test.db")

    # Create a models file with old schema (no email field)
    old_models = tmp_path / "old_models.py"
    old_models.write_text(
        textwrap.dedent("""\
        from ontologia import Entity, Field

        class SimpleEntity(Entity):
            id: Field[str] = Field(primary_key=True)
            name: Field[str]
    """)
    )

    # Initialize DB with old schema
    from ontologia.cli._loader import load_models

    entity_types, _ = load_models(models_path=str(old_models))
    onto = Session(db_path, entity_types=list(entity_types.values()))
    onto.validate()
    onto.close()

    # Create new models with added field
    new_models = tmp_path / "new_models.py"
    new_models.write_text(
        textwrap.dedent("""\
        from ontologia import Entity, Field

        class SimpleEntity(Entity):
            id: Field[str] = Field(primary_key=True)
            name: Field[str]
            email: Field[str | None] = None
    """)
    )

    # Dry-run should show changes
    result = invoke(
        runner,
        ["--json", "migrate", "--models-path", str(new_models)],
        db_path,
    )
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["has_changes"] is True
    assert data["token"]
    assert len(data["diffs"]) == 1
