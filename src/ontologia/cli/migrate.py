"""onto migrate — plan and apply schema migration."""

from __future__ import annotations

from typing import Any, Optional

import typer

from ontologia.cli import _exitcodes as ec
from ontologia.cli._loader import load_models
from ontologia.cli._output import print_error, print_object
from ontologia.cli._storage import open_ontology


def migrate_cmd(
    models: Optional[str] = typer.Option(None, "--models", help="Python import path for models"),
    models_path: Optional[str] = typer.Option(
        None, "--models-path", help="Filesystem path to models"
    ),
    upgraders_path: Optional[str] = typer.Option(
        None, "--upgraders", help="Python import path for upgraders module"
    ),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show migration plan only (default)"),
    apply: bool = typer.Option(False, "--apply", help="Execute migration"),
    token: Optional[str] = typer.Option(None, "--token", help="Safety token from dry-run"),
    force: bool = typer.Option(False, "--force", help="Skip token verification"),
    meta_opts: Optional[list[str]] = typer.Option(None, "--meta", help="KEY=VALUE metadata"),
) -> None:
    """Plan and optionally apply schema migration from code models to database."""
    from ontologia.cli import state

    json_mode = state.json_output

    if not models and not models_path:
        print_error("One of --models or --models-path is required")
        raise typer.Exit(ec.USAGE_ERROR)

    if force and token:
        print_error("--force and --token are mutually exclusive")
        raise typer.Exit(ec.USAGE_ERROR)

    if apply and not token and not force:
        print_error("--apply requires --token or --force")
        raise typer.Exit(ec.USAGE_ERROR)

    try:
        entity_types, relation_types = load_models(models, models_path)
    except Exception as e:
        print_error(f"Failed to load models: {e}")
        raise typer.Exit(ec.GENERAL_ERROR)

    # Load upgraders if specified
    upgraders = None
    if upgraders_path:
        from ontologia.migration import load_upgraders

        try:
            upgraders = load_upgraders(upgraders_path)
        except Exception as e:
            print_error(f"Failed to load upgraders: {e}")
            raise typer.Exit(ec.GENERAL_ERROR)

    onto = open_ontology(
        entity_types=list(entity_types.values()),
        relation_types=list(relation_types.values()),
    )

    try:
        if not apply:
            # Dry-run (default)
            preview = onto.migrate(dry_run=True, upgraders=upgraders)

            if not preview.has_changes:
                if json_mode:
                    print_object({"has_changes": False}, json_mode=True)
                else:
                    print("No schema changes detected.")
                return

            result: dict[str, Any] = {
                "has_changes": True,
                "token": preview.token,
                "diffs": [],
                "estimated_rows": preview.estimated_rows,
                "types_requiring_upgraders": preview.types_requiring_upgraders,
                "types_schema_only": preview.types_schema_only,
                "missing_upgraders": preview.missing_upgraders,
            }
            for d in preview.diffs:
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
                result["diffs"].append(diff_info)

            if json_mode:
                print_object(result, json_mode=True)
            else:
                print("Migration plan:")
                for d in preview.diffs:
                    print(f"\n  {d.type_kind} '{d.type_name}' (v{d.stored_version}):")
                    if d.added_fields:
                        print(f"    + {', '.join(d.added_fields)}")
                    if d.removed_fields:
                        print(f"    - {', '.join(d.removed_fields)}")
                    if d.changed_fields:
                        for fname, changes in d.changed_fields.items():
                            print(f"    ~ {fname}: {changes}")
                    rows = preview.estimated_rows.get(d.type_name, 0)
                    print(f"    Rows: {rows}")

                if preview.types_requiring_upgraders:
                    needing = ", ".join(preview.types_requiring_upgraders)
                    print(f"\nTypes requiring upgraders: {needing}")
                if preview.missing_upgraders:
                    print(f"Missing upgraders: {', '.join(preview.missing_upgraders)}")

                print(f"\nToken: {preview.token}")
                model_ref = models or models_path
                print(
                    f"\nTo apply: onto migrate --models {model_ref} --apply --token {preview.token}"
                )
        else:
            # Apply
            try:
                migration_result = onto.migrate(
                    dry_run=False,
                    token=token,
                    force=force,
                    upgraders=upgraders,
                )
            except Exception as e:
                print_error(str(e))
                raise typer.Exit(ec.EXECUTION_FAILURE)

            result_data: dict[str, Any] = {
                "success": migration_result.success,
                "types_migrated": migration_result.types_migrated,
                "rows_migrated": migration_result.rows_migrated,
                "new_schema_versions": migration_result.new_schema_versions,
                "duration_s": round(migration_result.duration_s, 3),
            }

            if json_mode:
                print_object(result_data, json_mode=True)
            else:
                print("Migration applied successfully.")
                for name in migration_result.types_migrated:
                    rows = migration_result.rows_migrated.get(name, 0)
                    ver = migration_result.new_schema_versions.get(name)
                    print(f"  {name}: {rows} rows migrated → v{ver}")
                print(f"Duration: {migration_result.duration_s:.3f}s")

    except typer.Exit:
        raise
    except Exception as e:
        print_error(str(e))
        raise typer.Exit(ec.GENERAL_ERROR)
    finally:
        try:
            onto.close()
        except Exception:
            pass
