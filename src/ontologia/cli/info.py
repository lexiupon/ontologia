"""onto info — show DB status and high-level metadata."""

from __future__ import annotations

import os
from typing import Any

import typer

from ontologia.cli import _exitcodes as ec
from ontologia.cli._output import print_error, print_object
from ontologia.cli._storage import open_repo, resolve_storage_binding
from ontologia.storage import parse_storage_target


def info_cmd(
    stats: bool = typer.Option(False, "--stats", help="Show counts and storage stats"),
    schema: bool = typer.Option(False, "--schema", help="Show entity/relation schema summary"),
) -> None:
    """Show DB status and high-level metadata."""
    from ontologia.cli import state

    json_mode = state.json_output
    db_path, storage_uri = resolve_storage_binding()
    sqlite_path_to_check: str | None = None
    if storage_uri is not None:
        try:
            target = parse_storage_target(storage_uri=storage_uri)
        except Exception as e:
            print_error(f"Invalid storage URI: {e}")
            raise typer.Exit(ec.DATABASE_ERROR)
        if target.backend == "sqlite":
            sqlite_path_to_check = target.db_path
    else:
        sqlite_path_to_check = db_path

    if (
        sqlite_path_to_check
        and sqlite_path_to_check != ":memory:"
        and not os.path.exists(sqlite_path_to_check)
    ):
        print_error(f"Database not found: {sqlite_path_to_check}")
        raise typer.Exit(ec.DATABASE_ERROR)

    try:
        repo = open_repo()
    except Exception as e:
        print_error(f"Cannot open storage backend: {e}")
        raise typer.Exit(ec.DATABASE_ERROR)

    try:
        head = repo.get_head_commit_id()
        storage = repo.storage_info()

        data: dict[str, Any] = {"head_commit_id": head, **storage}

        if storage.get("backend") == "sqlite":
            db_path = str(storage.get("db_path"))
            if os.path.exists(db_path):
                data["file_size_bytes"] = os.path.getsize(db_path)

        if stats:
            entity_schemas = repo.list_schemas("entity")
            relation_schemas = repo.list_schemas("relation")
            entity_counts = {
                s["type_name"]: repo.count_latest_entities(s["type_name"]) for s in entity_schemas
            }
            relation_counts = {
                s["type_name"]: repo.count_latest_relations(s["type_name"])
                for s in relation_schemas
            }
            data["entity_counts"] = entity_counts
            data["relation_counts"] = relation_counts

        if schema:
            entity_schemas = repo.list_schemas("entity")
            relation_schemas = repo.list_schemas("relation")
            data["entity_schemas"] = [s["type_name"] for s in entity_schemas]
            data["relation_schemas"] = [s["type_name"] for s in relation_schemas]

        if json_mode:
            print_object(data, json_mode=True)
        else:
            backend = str(storage.get("backend", "unknown"))
            engine_version = str(storage.get("engine_version", "v1"))
            print(f"Backend: {backend}")
            print(f"Engine version: {engine_version}")
            if backend == "sqlite":
                db_path = str(storage.get("db_path"))
                print(f"Database: {db_path}")
                if "file_size_bytes" in data:
                    print(f"File size: {int(data['file_size_bytes']):,} bytes")
            elif backend == "s3":
                print(f"Storage URI: {storage.get('storage_uri')}")
                print(f"Bucket: {storage.get('bucket')}")
                print(f"Prefix: {storage.get('prefix')}")
            print(f"Head commit: {head or '(none)'}")

            if stats:
                print("\nEntity counts:")
                for name, cnt in data.get("entity_counts", {}).items():
                    print(f"  {name}: {cnt}")
                print("Relation counts:")
                for name, cnt in data.get("relation_counts", {}).items():
                    print(f"  {name}: {cnt}")

            if schema:
                print("\nEntity types:")
                for name in data.get("entity_schemas", []):
                    print(f"  {name}")
                print("Relation types:")
                for name in data.get("relation_schemas", []):
                    print(f"  {name}")
    finally:
        repo.close()
