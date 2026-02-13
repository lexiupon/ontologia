"""Ontologia CLI: operator console for inspecting and managing data stores."""

from __future__ import annotations

from typing import Optional

import typer
from click.core import ParameterSource

from ontologia.cli import (
    commits,
    compact,
    events,
    export_cmd,
    import_cmd,
    index,
    info,
    init_cmd,
    migrate,
    query,
    schema,
    verify,
)

app = typer.Typer(
    name="onto",
    help="Ontologia CLI — operator console for inspecting and managing data stores.",
    no_args_is_help=True,
)


class _State:
    """Global CLI state shared across subcommands."""

    db: str = "onto.db"
    storage_uri: str | None = None
    config: str | None = None
    json_output: bool = False


state = _State()


def _version_callback(value: bool) -> None:
    if value:
        try:
            from importlib.metadata import version

            v = version("ontologia")
        except Exception:
            v = "unknown"
        print(f"onto {v}")
        raise typer.Exit()


@app.callback(invoke_without_command=True)
def main_callback(
    ctx: typer.Context,
    db: Optional[str] = typer.Option(
        None,
        "--db",
        envvar="ONTOLOGIA_DB",
        help="SQLite database file path (default: onto.db)",
    ),
    storage_uri: Optional[str] = typer.Option(
        None,
        "--storage-uri",
        envvar="ONTOLOGIA_STORAGE_URI",
        help="Backend storage URI (e.g. sqlite:///onto.db or s3://bucket/prefix)",
    ),
    config: Optional[str] = typer.Option(
        None,
        "--config",
        "-c",
        envvar="ONTOLOGIA_CONFIG",
        help="Config file path",
    ),
    json_output: bool = typer.Option(False, "--json", help="JSON output when supported"),
    version: bool = typer.Option(
        False, "--version", help="Show version", is_eager=True, callback=_version_callback
    ),
) -> None:
    """Global options for all onto commands."""
    from ontologia.storage import parse_storage_target

    db_source = ctx.get_parameter_source("db")
    uri_source = ctx.get_parameter_source("storage_uri")

    resolved_db = db or "onto.db"
    resolved_uri = storage_uri
    # Explicit --db should override ONTOLOGIA_STORAGE_URI when --storage-uri is not explicitly set.
    if db_source == ParameterSource.COMMANDLINE and uri_source == ParameterSource.ENVIRONMENT:
        resolved_uri = None

    db_for_validation: str | None = None
    if uri_source == ParameterSource.COMMANDLINE:
        # Explicit --storage-uri owns backend selection; only validate against explicit --db.
        if db_source == ParameterSource.COMMANDLINE:
            db_for_validation = resolved_db
    elif db_source == ParameterSource.COMMANDLINE:
        db_for_validation = resolved_db
    elif (
        db_source == ParameterSource.ENVIRONMENT
        and resolved_uri is not None
        and resolved_uri.startswith("sqlite:")
    ):
        db_for_validation = resolved_db
    if resolved_uri:
        try:
            parse_storage_target(
                db_path=db_for_validation,
                storage_uri=resolved_uri,
            )
        except Exception as e:
            raise typer.BadParameter(str(e))

    state.db = resolved_db
    state.storage_uri = resolved_uri
    state.config = config
    state.json_output = json_output
    if ctx.invoked_subcommand is None and not version:
        print(ctx.get_help())
        raise typer.Exit()


# Register subcommand groups
app.add_typer(schema.app, name="schema", help="Schema management commands")
app.add_typer(query.app, name="query", help="Query entities, relations, and traversals")
app.add_typer(commits.app, name="commits", help="Inspect commit history")
app.add_typer(index.app, name="index", help="S3 index health and repair commands")
app.add_typer(events.app, name="events", help="Inspect and manage event bus state")

# Register top-level commands
app.command(name="info")(info.info_cmd)
app.command(name="verify")(verify.verify_cmd)
app.command(name="migrate")(migrate.migrate_cmd)
app.command(name="export")(export_cmd.export_cmd)
app.command(name="import")(import_cmd.import_cmd)
app.command(name="init")(init_cmd.init_cmd)
app.command(name="compact")(compact.compact_cmd)


def main() -> None:
    """Entry point for the onto CLI."""
    app()
