"""onto import — controlled operational ingest."""

from __future__ import annotations

import json
import os
from typing import Any, Optional

import typer

from ontologia.cli import _exitcodes as ec
from ontologia.cli._loader import load_models
from ontologia.cli._output import print_error, print_object
from ontologia.cli._storage import open_ontology


def import_cmd(
    input_path: str = typer.Option(..., "--input", help="JSONL file or directory"),
    models: Optional[str] = typer.Option(None, "--models", help="Python import path for models"),
    models_path: Optional[str] = typer.Option(
        None, "--models-path", help="Filesystem path to models"
    ),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show planned delta"),
    apply: bool = typer.Option(False, "--apply", help="Execute write"),
    on_conflict: Optional[str] = typer.Option(
        None, "--on-conflict", help="Conflict policy: abort|skip|upsert"
    ),
    precondition: Optional[str] = typer.Option(
        None, "--precondition", help="must_exist|must_not_exist|if_commit_id:ID"
    ),
    meta_opts: Optional[list[str]] = typer.Option(None, "--meta", help="KEY=VALUE metadata"),
) -> None:
    """Controlled operational ingest from JSONL files."""
    from ontologia.cli import state

    json_mode = state.json_output

    if not models and not models_path:
        print_error("One of --models or --models-path is required")
        raise typer.Exit(ec.USAGE_ERROR)

    if apply and not on_conflict:
        print_error("--apply requires --on-conflict")
        raise typer.Exit(ec.USAGE_ERROR)

    if on_conflict and on_conflict not in ("abort", "skip", "upsert"):
        print_error("--on-conflict must be 'abort', 'skip', or 'upsert'")
        raise typer.Exit(ec.USAGE_ERROR)

    try:
        entity_types, relation_types = load_models(models, models_path)
    except Exception as e:
        print_error(f"Failed to load models: {e}")
        raise typer.Exit(ec.GENERAL_ERROR)

    # Load JSONL records
    records = _load_jsonl(input_path)
    if not records:
        print("No records to import.")
        return

    # Validate records against schema
    errors: list[str] = []
    valid_records: list[dict[str, Any]] = []
    for i, rec in enumerate(records):
        try:
            _validate_record(rec, entity_types, relation_types)
            valid_records.append(rec)
        except ValueError as e:
            errors.append(f"Record {i}: {e}")

    if errors:
        for err in errors[:10]:
            print_error(err)
        if len(errors) > 10:
            print_error(f"... and {len(errors) - 10} more errors")
        raise typer.Exit(ec.IMPORT_CONFLICT)

    onto = open_ontology(
        entity_types=list(entity_types.values()),
        relation_types=list(relation_types.values()),
    )

    try:
        # Build delta: check preconditions and conflicts
        inserts: list[dict[str, Any]] = []
        updates: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        conflicts: list[dict[str, Any]] = []

        for rec in valid_records:
            existing = _find_existing(onto, rec, entity_types, relation_types)

            if precondition:
                pre_err = _check_precondition(precondition, rec, existing)
                if pre_err:
                    conflicts.append({"record": rec, "error": pre_err})
                    continue

            if existing is not None:
                if on_conflict == "abort":
                    conflicts.append({"record": rec, "error": "already exists"})
                elif on_conflict == "skip":
                    skipped.append(rec)
                else:  # upsert
                    updates.append(rec)
            else:
                inserts.append(rec)

        if conflicts:
            if json_mode:
                print_object({"status": "conflict", "conflicts": len(conflicts)}, json_mode=True)
            else:
                print_error(f"{len(conflicts)} conflict(s) found:")
                for c in conflicts[:5]:
                    print_error(f"  {c['error']}: {_record_identity(c['record'])}")
            raise typer.Exit(ec.IMPORT_CONFLICT)

        # Summary
        summary: dict[str, Any] = {
            "total_records": len(valid_records),
            "inserts": len(inserts),
            "updates": len(updates),
            "skipped": len(skipped),
            "conflicts": len(conflicts),
        }

        if dry_run or not apply:
            if json_mode:
                print_object({"status": "dry_run", **summary}, json_mode=True)
            else:
                print("Import plan (dry-run):")
                print(f"  Total records: {summary['total_records']}")
                print(f"  Inserts: {summary['inserts']}")
                print(f"  Updates: {summary['updates']}")
                print(f"  Skipped: {summary['skipped']}")
                if conflicts:
                    print(f"  Conflicts: {summary['conflicts']}")
            return

        # Apply: single atomic commit
        _parse_meta_opts(meta_opts)  # Parse for validation but not used yet
        session = onto.session()

        for rec in inserts + updates:
            obj = _build_object(rec, entity_types, relation_types)
            session.ensure(obj)

        commit_id = session.commit()

        result: dict[str, Any] = {
            "status": "applied",
            "commit_id": commit_id,
            **summary,
        }

        if json_mode:
            print_object(result, json_mode=True)
        else:
            print(f"Import applied (commit {commit_id}):")
            print(f"  Inserts: {summary['inserts']}")
            print(f"  Updates: {summary['updates']}")
            print(f"  Skipped: {summary['skipped']}")

    except typer.Exit:
        raise
    except Exception as e:
        print_error(str(e))
        raise typer.Exit(ec.EXECUTION_FAILURE)
    finally:
        try:
            onto.close()
        except Exception:
            pass


def _load_jsonl(path: str) -> list[dict[str, Any]]:
    """Load JSONL records from a file or directory."""
    records: list[dict[str, Any]] = []

    if os.path.isdir(path):
        for fname in sorted(os.listdir(path)):
            if fname.endswith(".jsonl"):
                records.extend(_read_jsonl_file(os.path.join(path, fname)))
    elif os.path.isfile(path):
        records = _read_jsonl_file(path)
    else:
        raise FileNotFoundError(f"Input path not found: {path}")

    return records


def _read_jsonl_file(filepath: str) -> list[dict[str, Any]]:
    """Read a single JSONL file."""
    records: list[dict[str, Any]] = []
    with open(filepath) as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as e:
                raise ValueError(f"{filepath}:{line_num}: Invalid JSON: {e}")
    return records


def _validate_record(
    rec: dict[str, Any],
    entity_types: dict[str, type],
    relation_types: dict[str, type],
) -> None:
    """Validate a JSONL record against known types."""
    kind = rec.get("type_kind")
    tn = rec.get("type_name")

    if kind not in ("entity", "relation"):
        raise ValueError(f"Invalid type_kind: {kind}")
    if not tn:
        raise ValueError("Missing type_name")

    if kind == "entity":
        if tn not in entity_types:
            raise ValueError(f"Unknown entity type: {tn}")
        if "key" not in rec:
            raise ValueError("Entity record missing 'key'")
        if "fields" not in rec:
            raise ValueError("Entity record missing 'fields'")
    else:
        if tn not in relation_types:
            raise ValueError(f"Unknown relation type: {tn}")
        if "left_key" not in rec or "right_key" not in rec:
            raise ValueError("Relation record missing 'left_key' or 'right_key'")
        if "fields" not in rec:
            raise ValueError("Relation record missing 'fields'")


def _find_existing(
    onto: Any,
    rec: dict[str, Any],
    entity_types: dict[str, type],
    relation_types: dict[str, type],
) -> dict[str, Any] | None:
    """Check if a record already exists in the database."""
    kind = rec["type_kind"]
    tn = rec["type_name"]

    if kind == "entity":
        return onto.repo.get_latest_entity(tn, rec["key"])
    else:
        return onto.repo.get_latest_relation(
            tn,
            rec["left_key"],
            rec["right_key"],
            instance_key=rec.get("instance_key", ""),
        )


def _check_precondition(
    precondition: str,
    rec: dict[str, Any],
    existing: dict[str, Any] | None,
) -> str | None:
    """Check import precondition. Returns error string or None."""
    if precondition == "must_exist":
        if existing is None:
            return f"must_exist failed for {_record_identity(rec)}"
    elif precondition == "must_not_exist":
        if existing is not None:
            return f"must_not_exist failed for {_record_identity(rec)}"
    elif precondition.startswith("if_commit_id:"):
        target_cid = int(precondition.split(":", 1)[1])
        if existing is not None and existing.get("commit_id") != target_cid:
            actual = existing.get("commit_id")
            return (
                f"if_commit_id:{target_cid} failed for {_record_identity(rec)} (actual: {actual})"
            )
    return None


def _build_object(
    rec: dict[str, Any], entity_types: dict[str, type], relation_types: dict[str, type]
) -> Any:
    """Build an Entity or Relation instance from a JSONL record."""
    kind = rec["type_kind"]
    tn = rec["type_name"]

    if kind == "entity":
        cls = entity_types[tn]
        return cls(**rec["fields"])
    else:
        cls = relation_types[tn]
        data = {
            **rec["fields"],
            "left_key": rec["left_key"],
            "right_key": rec["right_key"],
        }
        if rec.get("instance_key"):
            ik_field = getattr(cls, "_instance_key_field", None)
            if ik_field:
                data[ik_field] = rec["instance_key"]
        return cls(**data)


def _record_identity(rec: dict[str, Any]) -> str:
    """Format record identity for error messages."""
    kind = rec.get("type_kind", "?")
    tn = rec.get("type_name", "?")
    if kind == "entity":
        return f"{tn}(key={rec.get('key', '?')})"
    return f"{tn}(left={rec.get('left_key', '?')}, right={rec.get('right_key', '?')})"


def _parse_meta_opts(meta_opts: list[str] | None) -> dict[str, str]:
    """Parse KEY=VALUE meta options."""
    if not meta_opts:
        return {}
    result: dict[str, str] = {}
    for item in meta_opts:
        if "=" not in item:
            raise typer.BadParameter(f"Invalid meta (expected KEY=VALUE): {item}")
        k, v = item.split("=", 1)
        result[k] = v
    return result
