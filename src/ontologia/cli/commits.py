"""onto commits — inspect commit history."""

from __future__ import annotations

import json
from typing import Any, Optional

import typer

from ontologia.cli import _exitcodes as ec
from ontologia.cli._output import print_error, print_object, print_table
from ontologia.cli._storage import open_repo

app = typer.Typer(no_args_is_help=False, invoke_without_command=True)


@app.callback(invoke_without_command=True)
def commits_cmd(
    ctx: typer.Context,
    last: int = typer.Option(10, "--last", help="Number of recent commits"),
    since: Optional[int] = typer.Option(None, "--since", help="Show commits after this ID"),
    meta_filter: Optional[list[str]] = typer.Option(
        None, "--meta", help="Filter by metadata KEY=VALUE"
    ),
    commit_id: Optional[int] = typer.Option(
        None, "--id", help="Examine a single commit (legacy alias)"
    ),
) -> None:
    """Inspect commit history (summary mode)."""
    if ctx.invoked_subcommand is not None:
        return

    from ontologia.cli import state

    json_mode = state.json_output

    # Legacy --id switches to examine mode
    if commit_id is not None:
        if since is not None or meta_filter:
            print_error("--id is mutually exclusive with --since and --meta")
            raise typer.Exit(ec.USAGE_ERROR)
        _examine(commit_id, json_mode)
        return

    try:
        repo = open_repo()
    except Exception as e:
        print_error(f"Cannot open storage backend: {e}")
        raise typer.Exit(ec.DATABASE_ERROR)

    try:
        commits = repo.list_commits(limit=last, since_commit_id=since)

        # Apply --meta filter in Python
        if meta_filter:
            parsed_meta = _parse_meta_filters(meta_filter)
            commits = [
                c
                for c in commits
                if c.get("metadata")
                and all(c["metadata"].get(k) == v for k, v in parsed_meta.items())
            ]

        # Count operations per commit
        rows = []
        for c in commits:
            op_count = repo.count_commit_operations(c["id"])
            rows.append(
                {
                    "commit_id": c["id"],
                    "timestamp": c["created_at"],
                    "operations": op_count,
                    "meta": c.get("metadata") or {},
                }
            )

        if json_mode:
            print_object(rows, json_mode=True)
        else:
            headers = ["commit_id", "timestamp", "operations", "meta"]
            table_rows = [
                [
                    r["commit_id"],
                    r["timestamp"],
                    r["operations"],
                    json.dumps(r["meta"]) if r["meta"] else "",
                ]
                for r in rows
            ]
            print_table(headers, table_rows)
    finally:
        repo.close()


@app.command(name="examine")
def examine_cmd(
    commit_id: int = typer.Option(..., "--id", help="Commit ID to examine"),
) -> None:
    """Inspect one commit in detail."""
    from ontologia.cli import state

    _examine(commit_id, state.json_output)


def _examine(commit_id: int, json_mode: bool) -> None:
    """Examine a single commit."""
    try:
        repo = open_repo()
    except Exception as e:
        print_error(f"Cannot open storage backend: {e}")
        raise typer.Exit(ec.DATABASE_ERROR)

    try:
        commit = repo.get_commit(commit_id)
        if commit is None:
            print_error(f"Commit {commit_id} not found")
            raise typer.Exit(ec.GENERAL_ERROR)

        changes = repo.list_commit_changes(commit_id)
        op_count = repo.count_commit_operations(commit_id)

        result: dict[str, Any] = {
            "commit_id": commit["id"],
            "timestamp": commit["created_at"],
            "operations": op_count,
            "meta": commit.get("metadata") or {},
            "changes": changes,
        }

        if json_mode:
            print_object(result, json_mode=True)
        else:
            print(f"Commit: {commit['id']}")
            print(f"Timestamp: {commit['created_at']}")
            print(f"Operations: {op_count}")
            meta = commit.get("metadata")
            if meta:
                print(f"Metadata: {json.dumps(meta)}")
            print(f"\nChanges ({len(changes)}):")
            for ch in changes:
                if ch["kind"] == "entity":
                    print(f"  {ch['operation']} entity {ch['type_name']} key={ch['key']}")
                else:
                    print(
                        f"  {ch['operation']} relation {ch['type_name']} "
                        f"left_key={ch['left_key']} right_key={ch['right_key']}"
                    )
    finally:
        repo.close()


def _parse_meta_filters(meta_filter: list[str]) -> dict[str, str]:
    """Parse KEY=VALUE meta filter options."""
    result: dict[str, str] = {}
    for item in meta_filter:
        if "=" not in item:
            raise typer.BadParameter(f"Invalid meta filter (expected KEY=VALUE): {item}")
        k, v = item.split("=", 1)
        result[k] = v
    return result
