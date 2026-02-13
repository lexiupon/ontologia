"""CLI tests for onto events subcommands."""

from __future__ import annotations

from ontologia import Event, Field, Session
from tests.cli.conftest import invoke


class Ping(Event):
    value: Field[str]


def test_events_show_and_inspect(runner, cli_db) -> None:
    with Session(datastore_uri=f"sqlite:///{cli_db}") as session:
        session.commit(event=Ping(value="hello"))

    show = invoke(runner, ["events", "show", "--namespace", "default", "--limit", "5"], cli_db)
    assert show.exit_code == 0
    assert "ping" in show.stdout

    # Fetch one ID from JSON output for inspect.
    show_json = invoke(
        runner,
        ["--json", "events", "show", "--namespace", "default", "--limit", "1"],
        cli_db,
    )
    assert show_json.exit_code == 0
    assert '"id"' in show_json.stdout


def test_events_cleanup(runner, cli_db) -> None:
    with Session(datastore_uri=f"sqlite:///{cli_db}") as session:
        session.commit(event=Ping(value="cleanup"))

    result = invoke(
        runner,
        ["events", "cleanup", "--namespace", "default", "--before", "0m"],
        cli_db,
    )
    assert result.exit_code == 0
    assert "deleted" in result.stdout
