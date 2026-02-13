"""onto export — export ontology data as JSONL."""

from __future__ import annotations

import json
import os
from typing import Any, Optional

import typer

from ontologia.cli import _exitcodes as ec
from ontologia.cli._output import print_error
from ontologia.cli._storage import open_repo


def export_cmd(
    output: str = typer.Option(..., "--output", help="Output directory path"),
    type_filter: Optional[str] = typer.Option(None, "--type", help="Export only this type"),
    as_of: Optional[int] = typer.Option(None, "--as-of", help="Snapshot at commit ID"),
    history_since: Optional[int] = typer.Option(
        None, "--history-since", help="Changes since commit ID"
    ),
    with_metadata: bool = typer.Option(False, "--with-metadata", help="Include commit_id per row"),
) -> None:
    """Export ontology data as JSONL."""
    try:
        repo = open_repo()
    except Exception as e:
        print_error(f"Cannot open storage backend: {e}")
        raise typer.Exit(ec.DATABASE_ERROR)

    try:
        os.makedirs(output, exist_ok=True)
        warnings: list[str] = []

        entity_schemas = repo.list_schemas("entity")
        relation_schemas = repo.list_schemas("relation")

        total_rows = 0

        # Export entities
        for es in entity_schemas:
            tn = es["type_name"]
            if type_filter and tn != type_filter:
                continue

            filepath = os.path.join(output, f"{tn}.jsonl")
            count = 0

            with open(filepath, "w") as f:
                if as_of is not None:
                    rows = repo.query_entities(tn, as_of=as_of)
                    _collect_query_warning(repo, f"entity:{tn}", warnings)
                    for row in rows:
                        line: dict[str, Any] = {
                            "type_kind": "entity",
                            "type_name": tn,
                            "key": row["key"],
                            "fields": row["fields"],
                        }
                        if with_metadata:
                            line["commit_id"] = row["commit_id"]
                        f.write(json.dumps(line, default=str) + "\n")
                        count += 1
                elif history_since is not None:
                    rows = repo.query_entities(tn, history_since=history_since)
                    _collect_query_warning(repo, f"entity:{tn}", warnings)
                    for row in rows:
                        line = {
                            "type_kind": "entity",
                            "type_name": tn,
                            "key": row["key"],
                            "fields": row["fields"],
                        }
                        if with_metadata:
                            line["commit_id"] = row["commit_id"]
                        f.write(json.dumps(line, default=str) + "\n")
                        count += 1
                else:
                    for batch in repo.iter_latest_entities(tn):
                        for key, fields, commit_id, _svid in batch:
                            line = {
                                "type_kind": "entity",
                                "type_name": tn,
                                "key": key,
                                "fields": fields,
                            }
                            if with_metadata:
                                line["commit_id"] = commit_id
                            f.write(json.dumps(line, default=str) + "\n")
                            count += 1

            total_rows += count

        # Export relations
        for rs in relation_schemas:
            tn = rs["type_name"]
            if type_filter and tn != type_filter:
                continue

            filepath = os.path.join(output, f"{tn}.jsonl")
            count = 0

            with open(filepath, "w") as f:
                if as_of is not None:
                    rows = repo.query_relations(tn, as_of=as_of)
                    _collect_query_warning(repo, f"relation:{tn}", warnings)
                    for row in rows:
                        line = {
                            "type_kind": "relation",
                            "type_name": tn,
                            "left_key": row["left_key"],
                            "right_key": row["right_key"],
                            "fields": row["fields"],
                        }
                        if row.get("instance_key"):
                            line["instance_key"] = row["instance_key"]
                        if with_metadata:
                            line["commit_id"] = row["commit_id"]
                        f.write(json.dumps(line, default=str) + "\n")
                        count += 1
                elif history_since is not None:
                    rows = repo.query_relations(tn, history_since=history_since)
                    _collect_query_warning(repo, f"relation:{tn}", warnings)
                    for row in rows:
                        line = {
                            "type_kind": "relation",
                            "type_name": tn,
                            "left_key": row["left_key"],
                            "right_key": row["right_key"],
                            "fields": row["fields"],
                        }
                        if row.get("instance_key"):
                            line["instance_key"] = row["instance_key"]
                        if with_metadata:
                            line["commit_id"] = row["commit_id"]
                        f.write(json.dumps(line, default=str) + "\n")
                        count += 1
                else:
                    for batch in repo.iter_latest_relations(tn):
                        for left_key, right_key, instance_key, fields, commit_id, _svid in batch:
                            line = {
                                "type_kind": "relation",
                                "type_name": tn,
                                "left_key": left_key,
                                "right_key": right_key,
                                "fields": fields,
                            }
                            if instance_key:
                                line["instance_key"] = instance_key
                            if with_metadata:
                                line["commit_id"] = commit_id
                            f.write(json.dumps(line, default=str) + "\n")
                            count += 1

            total_rows += count

        print(f"Exported {total_rows} rows to {output}/")
        for warning in warnings:
            print(warning)

    except typer.Exit:
        raise
    except Exception as e:
        print_error(str(e))
        raise typer.Exit(ec.EXECUTION_FAILURE)
    finally:
        repo.close()


def _collect_query_warning(repo: Any, scope: str, warnings: list[str]) -> None:
    if not hasattr(repo, "get_last_query_diagnostics"):
        return
    diag = repo.get_last_query_diagnostics()
    if not isinstance(diag, dict):
        return
    if diag.get("reason") == "commit_before_activation":
        activation = diag.get("activation_commit_id")
        warnings.append(
            f"Warning [{scope}]: requested commit is before schema activation boundary "
            f"(activation_commit_id={activation}); exported rows are empty."
        )
