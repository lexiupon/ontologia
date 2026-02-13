"""onto compact — S3 compaction command."""

from __future__ import annotations

import typer

from ontologia.cli import _exitcodes as ec
from ontologia.cli._output import print_error, print_object
from ontologia.cli._storage import open_repo


def compact_cmd(
    type_name: str | None = typer.Option(None, "--type", help="Compact only this type"),
    apply: bool = typer.Option(False, "--apply", help="Execute compaction (default: dry-run)"),
) -> None:
    """Compact per-commit files into snapshot entries."""
    from ontologia.cli import state

    json_mode = state.json_output

    try:
        repo = open_repo()
    except Exception as e:
        print_error(str(e))
        raise typer.Exit(ec.DATABASE_ERROR)

    try:
        info = repo.storage_info()
        if info.get("backend") != "s3":
            print_error("compact is available only for S3 backends")
            raise typer.Exit(ec.USAGE_ERROR)
        if not hasattr(repo, "compact"):
            raise RuntimeError("Backend does not support compaction")

        data = repo.compact(type_name=type_name, apply=apply)  # type: ignore[attr-defined]
        print_object(data, json_mode=json_mode)
    except typer.Exit:
        raise
    except Exception as e:
        print_error(str(e))
        raise typer.Exit(ec.EXECUTION_FAILURE)
    finally:
        repo.close()
