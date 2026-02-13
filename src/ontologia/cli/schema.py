"""onto schema — schema management commands (export, history, drop)."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Optional

import typer
import yaml

from ontologia.cli import _exitcodes as ec
from ontologia.cli._loader import load_models
from ontologia.cli._output import print_error, print_object, print_table
from ontologia.cli._storage import open_repo

app = typer.Typer(no_args_is_help=True)


@app.command(name="export")
def schema_export_cmd(
    models: Optional[str] = typer.Option(None, "--models", help="Python import path for models"),
    models_path: Optional[str] = typer.Option(
        None, "--models-path", help="Filesystem path to models"
    ),
    kind: Optional[str] = typer.Option(None, "--kind", help="entity or relation (stored mode)"),
    type_name: Optional[str] = typer.Option(None, "--type", help="Type name (stored mode)"),
    version: Optional[int] = typer.Option(
        None, "--version", help="Schema version ID (stored mode)"
    ),
    output: Optional[str] = typer.Option(None, "--output", help="Output file path"),
    fmt: str = typer.Option("json", "--format", help="Output format: json or yaml"),
    with_hash: bool = typer.Option(False, "--with-hash", help="Include schema fingerprint"),
) -> None:
    """Export schema artifacts for review, diffing, and CI artifacts."""
    from ontologia.cli import state

    # Validate exclusive modes
    has_code_mode = models is not None or models_path is not None
    has_stored_mode = kind is not None or type_name is not None

    if has_code_mode and has_stored_mode:
        print_error(
            "Code mode (--models/--models-path) and stored mode (--kind/--type) are exclusive"
        )
        raise typer.Exit(ec.USAGE_ERROR)

    if not has_code_mode and not has_stored_mode:
        print_error("One of --models/--models-path or --kind/--type is required")
        raise typer.Exit(ec.USAGE_ERROR)

    if has_stored_mode:
        if kind is None or type_name is None:
            print_error("Both --kind and --type are required for stored mode")
            raise typer.Exit(ec.USAGE_ERROR)
        if kind not in ("entity", "relation"):
            print_error("--kind must be 'entity' or 'relation'")
            raise typer.Exit(ec.USAGE_ERROR)
        _export_stored(state.db, kind, type_name, version, output, fmt, with_hash)
    else:
        if version is not None:
            print_error("--version is only valid in stored mode (--kind/--type)")
            raise typer.Exit(ec.USAGE_ERROR)
        _export_code(models, models_path, output, fmt, with_hash)


def _export_code(
    models: str | None,
    models_path: str | None,
    output: str | None,
    fmt: str,
    with_hash: bool,
) -> None:
    """Export code-defined schema."""
    from ontologia.runtime import _entity_schema, _relation_schema

    try:
        entity_types, relation_types = load_models(models, models_path)
    except Exception as e:
        print_error(f"Failed to load models: {e}")
        raise typer.Exit(ec.GENERAL_ERROR)

    schema_data: dict[str, Any] = {"entities": {}, "relations": {}}
    for name, cls in entity_types.items():
        s = _entity_schema(cls)
        if with_hash:
            s["schema_hash"] = _hash_schema(s)
        schema_data["entities"][name] = s
    for name, cls in relation_types.items():
        s = _relation_schema(cls)
        if with_hash:
            s["schema_hash"] = _hash_schema(s)
        schema_data["relations"][name] = s

    _write_output(schema_data, output, fmt)


def _export_stored(
    db_path: str,
    kind: str,
    type_name: str,
    version: int | None,
    output: str | None,
    fmt: str,
    with_hash: bool,
) -> None:
    """Export stored schema version."""
    try:
        repo = open_repo()
    except Exception as e:
        print_error(f"Cannot open storage backend: {e}")
        raise typer.Exit(ec.DATABASE_ERROR)

    try:
        if version is not None:
            sv = repo.get_schema_version(kind, type_name, version)
        else:
            sv = repo.get_current_schema_version(kind, type_name)

        if sv is None:
            print_error(
                f"Schema not found: {kind} '{type_name}'"
                + (f" version {version}" if version else "")
            )
            raise typer.Exit(ec.GENERAL_ERROR)

        schema_data = json.loads(sv["schema_json"])
        schema_data["schema_version_id"] = sv["schema_version_id"]
        schema_data["schema_hash"] = sv["schema_hash"]
        schema_data["created_at"] = sv["created_at"]

        _write_output(schema_data, output, fmt)
    finally:
        repo.close()


@app.command(name="history")
def schema_history_cmd(
    kind: str = typer.Option(..., "--kind", help="entity or relation"),
    type_name: str = typer.Option(..., "--type", help="Type name"),
    last: int = typer.Option(20, "--last", help="Number of recent versions"),
    since_version: Optional[int] = typer.Option(
        None, "--since-version", help="Versions after this ID"
    ),
    version: Optional[int] = typer.Option(None, "--version", help="Single version detail"),
) -> None:
    """Inspect stored schema version lineage for a single type."""
    from ontologia.cli import state

    json_mode = state.json_output

    if kind not in ("entity", "relation"):
        print_error("--kind must be 'entity' or 'relation'")
        raise typer.Exit(ec.USAGE_ERROR)

    # --version is mutually exclusive with --last/--since-version
    if version is not None and since_version is not None:
        print_error("--version is mutually exclusive with --since-version")
        raise typer.Exit(ec.USAGE_ERROR)

    try:
        repo = open_repo()
    except Exception as e:
        print_error(f"Cannot open storage backend: {e}")
        raise typer.Exit(ec.DATABASE_ERROR)

    try:
        if version is not None:
            # Detail mode
            sv = repo.get_schema_version(kind, type_name, version)
            if sv is None:
                print_error(f"Version {version} not found for {kind} '{type_name}'")
                raise typer.Exit(ec.GENERAL_ERROR)

            result = {
                "type_kind": kind,
                "type_name": type_name,
                "schema_version_id": sv["schema_version_id"],
                "schema_hash": sv["schema_hash"],
                "created_at": sv["created_at"],
                "runtime_id": sv["runtime_id"],
                "reason": sv["reason"],
                "schema_json": json.loads(sv["schema_json"]),
            }
            print_object(result, json_mode=json_mode)
        else:
            # List mode
            versions = repo.list_schema_versions(kind, type_name)

            if since_version is not None:
                versions = [v for v in versions if v["schema_version_id"] > since_version]

            # Apply --last limit (from the end)
            versions = versions[-last:]

            rows = []
            for v in versions:
                rows.append(
                    {
                        "type_kind": kind,
                        "type_name": type_name,
                        "schema_version_id": v["schema_version_id"],
                        "schema_hash": v["schema_hash"][:12],
                        "created_at": v["created_at"],
                        "runtime_id": v["runtime_id"] or "",
                        "reason": v["reason"] or "",
                    }
                )

            if json_mode:
                print_object(rows, json_mode=True)
            else:
                headers = ["version", "hash", "created_at", "runtime_id", "reason"]
                table_rows = [
                    [
                        r["schema_version_id"],
                        r["schema_hash"],
                        r["created_at"],
                        r["runtime_id"],
                        r["reason"],
                    ]
                    for r in rows
                ]
                print_table(headers, table_rows)
    finally:
        repo.close()


@app.command(name="drop")
def schema_drop_cmd(
    kind: str = typer.Argument(..., help="entity or relation"),
    type_name: str = typer.Argument(..., help="Type name to drop"),
    drop_relation: Optional[list[str]] = typer.Option(
        None, "--drop-relation", help="Explicit relation types to drop"
    ),
    cascade_relations: bool = typer.Option(
        False, "--cascade-relations", help="Drop all dependent relations"
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Show plan only (default if neither --dry-run nor --apply)"
    ),
    apply: bool = typer.Option(False, "--apply", help="Execute the drop"),
    token: Optional[str] = typer.Option(None, "--token", help="Safety token from dry-run"),
    purge_history: bool = typer.Option(
        False, "--purge-history", help="Purge history rows for affected types"
    ),
    meta_opts: Optional[list[str]] = typer.Option(None, "--meta", help="KEY=VALUE metadata"),
) -> None:
    """Drop one or more schema types (administrative destructive path)."""
    from ontologia.cli import state

    json_mode = state.json_output

    if kind not in ("entity", "relation"):
        print_error("KIND must be 'entity' or 'relation'")
        raise typer.Exit(ec.USAGE_ERROR)

    # Validate option combinations
    if apply and not token:
        print_error("--apply requires --token")
        raise typer.Exit(ec.USAGE_ERROR)
    if token and not apply:
        print_error("--token requires --apply")
        raise typer.Exit(ec.USAGE_ERROR)
    if kind == "relation" and (drop_relation or cascade_relations):
        print_error("--drop-relation and --cascade-relations are invalid for relation targets")
        raise typer.Exit(ec.USAGE_ERROR)
    if drop_relation and cascade_relations:
        print_error("--drop-relation and --cascade-relations are mutually exclusive")
        raise typer.Exit(ec.USAGE_ERROR)

    meta = _parse_meta_opts(meta_opts)

    try:
        repo = open_repo()
    except Exception as e:
        print_error(f"Cannot open storage backend: {e}")
        raise typer.Exit(ec.DATABASE_ERROR)

    try:
        # Resolve affected types
        affected_types: list[tuple[str, str]] = [(kind, type_name)]

        if kind == "entity":
            dependent_rels = _find_dependent_relations(repo, type_name)
            if cascade_relations:
                for rname in dependent_rels:
                    affected_types.append(("relation", rname))
            elif drop_relation:
                for rname in drop_relation:
                    affected_types.append(("relation", rname))
                # Check for undeclared dependents
                undeclared = set(dependent_rels) - set(drop_relation or [])
                if undeclared:
                    print_error(
                        f"Entity '{type_name}' has dependent relations not included in drop: "
                        f"{', '.join(sorted(undeclared))}. "
                        f"Use --drop-relation or --cascade-relations."
                    )
                    raise typer.Exit(ec.SCHEMA_DROP_SAFETY)
            elif dependent_rels:
                print_error(
                    f"Entity '{type_name}' has dependent relations: "
                    f"{', '.join(sorted(dependent_rels))}. "
                    f"Use --drop-relation or --cascade-relations."
                )
                raise typer.Exit(ec.SCHEMA_DROP_SAFETY)

        # Check row counts
        row_counts: dict[str, int] = {}
        for tk, tn in affected_types:
            if tk == "entity":
                row_counts[tn] = repo.count_latest_entities(tn)
            else:
                row_counts[tn] = repo.count_latest_relations(tn)

        has_rows = any(c > 0 for c in row_counts.values())
        if has_rows and not purge_history:
            types_with_rows = [n for n, c in row_counts.items() if c > 0]
            print_error(
                f"Types with existing rows: {', '.join(types_with_rows)}. "
                f"Use --purge-history to drop types with data."
            )
            raise typer.Exit(ec.SCHEMA_DROP_SAFETY)

        # Compute token
        head = repo.get_head_commit_id()
        schema_heads: dict[str, int] = {}
        for tk, tn in affected_types:
            sv = repo.get_current_schema_version(tk, tn)
            if sv:
                schema_heads[tn] = sv["schema_version_id"]

        computed_token = _compute_drop_token(affected_types, purge_history, head, schema_heads)

        if not apply:
            # Dry-run output
            plan: dict[str, Any] = {
                "target": {"kind": kind, "type_name": type_name},
                "affected_types": [{"kind": tk, "type_name": tn} for tk, tn in affected_types],
                "row_counts": row_counts,
                "purge_history_required": has_rows,
                "purge_history": purge_history,
                "token": computed_token,
            }
            if json_mode:
                print_object(plan, json_mode=True)
            else:
                print("Schema drop plan (dry-run):")
                print(f"  Target: {kind} '{type_name}'")
                print("  Affected types:")
                for tk, tn in affected_types:
                    print(f"    {tk} '{tn}' ({row_counts.get(tn, 0)} rows)")
                if has_rows:
                    print("  Purge history required: yes")
                print(f"\n  Token: {computed_token}")
                apply_cmd = (
                    f"\n  To apply: onto schema drop {kind} {type_name} "
                    f"--apply --token {computed_token}"
                )
                print(apply_cmd + (" --purge-history" if has_rows else ""))
            return

        # Apply mode — verify token
        if token != computed_token:
            print_error(
                "Token mismatch — plan may have changed since dry-run. Re-run without --apply."
            )
            raise typer.Exit(ec.SCHEMA_DROP_SAFETY)

        commit_meta = {
            "operation": "schema_drop",
            "target_kind": kind,
            "target_type": type_name,
            "affected_types": json.dumps(
                [{"kind": tk, "type_name": tn} for tk, tn in affected_types]
            ),
            **meta,
        }
        commit_id = repo.apply_schema_drop(
            affected_types=affected_types,
            purge_history=purge_history,
            commit_meta=commit_meta,
        )

        result = {
            "status": "dropped",
            "commit_id": commit_id,
            "affected_types": [{"kind": tk, "type_name": tn} for tk, tn in affected_types],
        }
        if json_mode:
            print_object(result, json_mode=True)
        else:
            print(f"Schema drop applied (commit {commit_id}):")
            for tk, tn in affected_types:
                print(f"  Dropped {tk} '{tn}'")
    except typer.Exit:
        raise
    except Exception as e:
        print_error(str(e))
        raise typer.Exit(ec.EXECUTION_FAILURE)
    finally:
        repo.close()


def _find_dependent_relations(repo: Any, entity_name: str) -> list[str]:
    """Find relation types whose left or right endpoint references the given entity."""
    relation_schemas = repo.list_schemas("relation")
    dependents: list[str] = []
    for rs in relation_schemas:
        schema = rs["schema"]
        if schema.get("left_type") == entity_name or schema.get("right_type") == entity_name:
            dependents.append(rs["type_name"])
    return dependents


def _compute_drop_token(
    affected_types: list[tuple[str, str]],
    purge_history: bool,
    head_commit_id: int | None,
    schema_heads: dict[str, int],
) -> str:
    """Compute a deterministic safety token for schema drop."""
    import base64

    data = json.dumps(
        {
            "affected": [(k, n) for k, n in sorted(affected_types)],
            "purge": purge_history,
            "head": head_commit_id,
            "schema_heads": dict(sorted(schema_heads.items())),
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    h = hashlib.sha256(data.encode()).hexdigest()
    return base64.urlsafe_b64encode(f"{h}:{head_commit_id}".encode()).decode()


def _write_output(data: dict[str, Any], output: str | None, fmt: str) -> None:
    """Write schema data to file or stdout."""
    if fmt == "yaml":
        content = yaml.dump(data, default_flow_style=False)
    else:
        content = json.dumps(data, indent=2, default=str)

    if output:
        with open(output, "w") as f:
            f.write(content)
        print(f"Written to {output}")
    else:
        print(content)


def _hash_schema(schema: dict[str, Any]) -> str:
    """Compute stable hash of a schema dict."""
    canonical = json.dumps(schema, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


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
