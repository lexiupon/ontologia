"""onto verify — verify stored schema matches code-defined schema."""

from __future__ import annotations

from typing import Any, Optional

import typer

from ontologia.cli import _exitcodes as ec
from ontologia.cli._loader import load_models
from ontologia.cli._output import print_error, print_object
from ontologia.cli._storage import open_ontology
from ontologia.errors import SchemaOutdatedError


def verify_cmd(
    models: Optional[str] = typer.Option(None, "--models", help="Python import path for models"),
    models_path: Optional[str] = typer.Option(
        None, "--models-path", help="Filesystem path to models"
    ),
    diff: bool = typer.Option(False, "--diff", help="Show mismatches only"),
    strict: bool = typer.Option(False, "--strict", help="Non-zero exit on any mismatch"),
) -> None:
    """Verify stored schema matches code-defined schema."""
    from ontologia.cli import state

    if not models and not models_path:
        print_error("One of --models or --models-path is required")
        raise typer.Exit(ec.USAGE_ERROR)

    try:
        entity_types, relation_types = load_models(models, models_path)
    except Exception as e:
        print_error(f"Failed to load models: {e}")
        raise typer.Exit(ec.GENERAL_ERROR)

    json_mode = state.json_output

    onto = open_ontology(
        entity_types=list(entity_types.values()),
        relation_types=list(relation_types.values()),
    )

    try:
        onto.validate()
        if not diff:
            if json_mode:
                print_object({"status": "ok", "diffs": []}, json_mode=True)
            else:
                print("Schema OK — no mismatches found.")
    except SchemaOutdatedError as e:
        diffs_data = []
        for d in e.diffs:
            diff_info: dict[str, Any] = {
                "type_kind": d.type_kind,
                "type_name": d.type_name,
                "stored_version": d.stored_version,
            }
            if d.added_fields:
                diff_info["added_fields"] = d.added_fields
            if d.removed_fields:
                diff_info["removed_fields"] = d.removed_fields
            if d.changed_fields:
                diff_info["changed_fields"] = d.changed_fields
            diffs_data.append(diff_info)

        if json_mode:
            print_object({"status": "mismatch", "diffs": diffs_data}, json_mode=True)
        else:
            print(f"Schema mismatch — {len(e.diffs)} type(s) differ:")
            for d in e.diffs:
                print(f"\n  {d.type_kind} '{d.type_name}' (stored v{d.stored_version}):")
                if d.added_fields:
                    print(f"    Added fields: {', '.join(d.added_fields)}")
                if d.removed_fields:
                    print(f"    Removed fields: {', '.join(d.removed_fields)}")
                if d.changed_fields:
                    for fname, changes in d.changed_fields.items():
                        print(f"    Changed '{fname}': {changes}")

        if strict:
            raise typer.Exit(ec.SCHEMA_MISMATCH)
    except Exception as e:
        print_error(str(e))
        raise typer.Exit(ec.GENERAL_ERROR)
    finally:
        try:
            onto.close()
        except Exception:
            pass
