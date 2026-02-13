"""onto events — event bus inspection and maintenance commands."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import typer

from ontologia.cli import _exitcodes as ec
from ontologia.cli._output import print_error, print_object, print_table
from ontologia.cli._storage import open_event_store

app = typer.Typer(no_args_is_help=True)


def _parse_before(value: str) -> datetime:
    """Parse duration shorthand like 7d/12h/30m into an absolute UTC timestamp."""
    if not value:
        raise ValueError("--before must be non-empty")
    suffix = value[-1]
    amount_str = value[:-1]
    if not amount_str.isdigit():
        raise ValueError(f"Invalid duration '{value}'")
    amount = int(amount_str)

    if suffix == "d":
        delta = timedelta(days=amount)
    elif suffix == "h":
        delta = timedelta(hours=amount)
    elif suffix == "m":
        delta = timedelta(minutes=amount)
    else:
        raise ValueError("Duration must end with d, h, or m (e.g., 7d, 12h, 30m)")

    return datetime.now(timezone.utc) - delta


@app.command(name="list-namespaces")
def list_namespaces_cmd() -> None:
    """List event namespaces with session/pending/dead-letter counts."""
    from ontologia.cli import state

    json_mode = state.json_output

    repo = None
    try:
        repo, store = open_event_store()
        rows = store.list_namespaces(session_ttl_ms=state_session_ttl_ms())
        if json_mode:
            print_object(rows, json_mode=True)
            return

        headers = ["namespace", "sessions", "pending_events", "dead_letters"]
        table_rows = [
            [r["namespace"], r["sessions"], r["pending_events"], r["dead_letters"]] for r in rows
        ]
        print_table(headers, table_rows)
    except Exception as e:
        print_error(str(e))
        raise typer.Exit(ec.EXECUTION_FAILURE)
    finally:
        if repo is not None:
            repo.close()


@app.command(name="sessions")
def sessions_cmd(
    namespace: str = typer.Option(..., "--namespace", help="Namespace to inspect"),
) -> None:
    """Show session heartbeats for one namespace."""
    from ontologia.cli import state

    json_mode = state.json_output

    repo = None
    try:
        repo, store = open_event_store()
        rows = store.list_sessions(namespace, session_ttl_ms=state_session_ttl_ms())
        if json_mode:
            print_object(rows, json_mode=True)
            return

        headers = ["session_id", "started_at", "last_heartbeat", "is_dead"]
        table_rows = [
            [r["session_id"], r["started_at"], r["last_heartbeat"], r["is_dead"]] for r in rows
        ]
        print_table(headers, table_rows)
    except Exception as e:
        print_error(str(e))
        raise typer.Exit(ec.EXECUTION_FAILURE)
    finally:
        if repo is not None:
            repo.close()


@app.command(name="show")
def show_cmd(
    namespace: str = typer.Option(..., "--namespace", help="Namespace to inspect"),
    limit: int = typer.Option(10, "--limit", help="Number of events to show"),
) -> None:
    """Show events in a namespace."""
    from ontologia.cli import state

    json_mode = state.json_output

    repo = None
    try:
        repo, store = open_event_store()
        rows = store.list_events(namespace, limit=limit)
        if json_mode:
            print_object(rows, json_mode=True)
            return

        headers = ["id", "type", "created_at", "priority", "status", "handler"]
        table_rows = [
            [r["id"], r["type"], r["created_at"], r["priority"], r["status"], r["handler"] or "-"]
            for r in rows
        ]
        print_table(headers, table_rows)
    except Exception as e:
        print_error(str(e))
        raise typer.Exit(ec.EXECUTION_FAILURE)
    finally:
        if repo is not None:
            repo.close()


@app.command(name="dead-letters")
def dead_letters_cmd(
    namespace: str = typer.Option(..., "--namespace", help="Namespace to inspect"),
    limit: int = typer.Option(100, "--limit", help="Number of rows"),
) -> None:
    """Show dead-letter rows for one namespace."""
    from ontologia.cli import state

    json_mode = state.json_output

    repo = None
    try:
        repo, store = open_event_store()
        rows = store.list_dead_letters(namespace, limit=limit)
        if json_mode:
            print_object(rows, json_mode=True)
            return

        headers = ["event_id", "type", "handler_id", "attempts", "last_error", "failed_at"]
        table_rows = [
            [
                r["event_id"],
                r["type"],
                r["handler_id"],
                r["attempts"],
                r["last_error"],
                r["failed_at"],
            ]
            for r in rows
        ]
        print_table(headers, table_rows)
    except Exception as e:
        print_error(str(e))
        raise typer.Exit(ec.EXECUTION_FAILURE)
    finally:
        if repo is not None:
            repo.close()


@app.command(name="cleanup")
def cleanup_cmd(
    namespace: str = typer.Option(..., "--namespace", help="Namespace to clean"),
    before: str = typer.Option(..., "--before", help="Retention window like 7d, 24h, 30m"),
) -> None:
    """Delete events older than the specified duration."""
    from ontologia.cli import state

    json_mode = state.json_output

    repo = None
    try:
        cutoff = _parse_before(before)
        repo, store = open_event_store()
        deleted = store.cleanup_events(namespace, before=cutoff)
        result: dict[str, Any] = {
            "namespace": namespace,
            "before": cutoff.isoformat(),
            "deleted": deleted,
        }
        print_object(result, json_mode=json_mode)
    except Exception as e:
        print_error(str(e))
        raise typer.Exit(ec.EXECUTION_FAILURE)
    finally:
        if repo is not None:
            repo.close()


@app.command(name="replay")
def replay_cmd(
    namespace: str = typer.Option(..., "--namespace", help="Namespace"),
    event_id: str = typer.Option(..., "--event-id", help="Event ID to replay"),
) -> None:
    """Re-enqueue an existing event by ID."""
    from ontologia.cli import state

    json_mode = state.json_output

    repo = None
    try:
        repo, store = open_event_store()
        new_id = store.replay_event(namespace, event_id)
        print_object(
            {
                "replayed_event_id": event_id,
                "new_event_id": new_id,
                "namespace": namespace,
            },
            json_mode=json_mode,
        )
    except Exception as e:
        print_error(str(e))
        raise typer.Exit(ec.EXECUTION_FAILURE)
    finally:
        if repo is not None:
            repo.close()


@app.command(name="inspect")
def inspect_cmd(
    event_id: str = typer.Option(..., "--event-id", help="Event ID"),
    namespace: str | None = typer.Option(None, "--namespace", help="Optional namespace filter"),
) -> None:
    """Inspect one event and claim history."""
    from ontologia.cli import state

    json_mode = state.json_output

    repo = None
    try:
        repo, store = open_event_store()
        data = store.inspect_event(event_id, namespace=namespace)
        if data is None:
            print_error(f"Event '{event_id}' not found")
            raise typer.Exit(ec.GENERAL_ERROR)
        print_object(data, json_mode=json_mode)
    except typer.Exit:
        raise
    except Exception as e:
        print_error(str(e))
        raise typer.Exit(ec.EXECUTION_FAILURE)
    finally:
        if repo is not None:
            repo.close()


def state_session_ttl_ms() -> int:
    # Keep CLI isolated from session runtime; value may later come from config file.
    from ontologia.config import OntologiaConfig

    return OntologiaConfig().session_ttl_ms
