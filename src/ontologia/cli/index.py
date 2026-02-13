"""onto index — S3 index maintenance commands."""

from __future__ import annotations

import typer

from ontologia.cli import _exitcodes as ec
from ontologia.cli._output import print_error, print_object
from ontologia.cli._storage import open_repo

app = typer.Typer(no_args_is_help=True)


def _require_s3(repo: object) -> object:
    info = repo.storage_info()  # type: ignore[attr-defined]
    if info.get("backend") != "s3":
        raise RuntimeError("index commands are available only for S3 backends")
    return repo


@app.command(name="verify")
def index_verify_cmd() -> None:
    """Verify S3 index health."""
    from ontologia.cli import state

    json_mode = state.json_output

    try:
        repo = _require_s3(open_repo())
    except Exception as e:
        print_error(str(e))
        raise typer.Exit(ec.USAGE_ERROR)

    try:
        if not hasattr(repo, "index_verify"):
            raise RuntimeError("Backend does not support index verification")
        data = repo.index_verify()  # type: ignore[attr-defined]
        print_object(data, json_mode=json_mode)
        if not data.get("ok", False):
            raise typer.Exit(ec.EXECUTION_FAILURE)
    except typer.Exit:
        raise
    except Exception as e:
        print_error(str(e))
        raise typer.Exit(ec.EXECUTION_FAILURE)
    finally:
        repo.close()  # type: ignore[attr-defined]


@app.command(name="repair")
def index_repair_cmd(
    apply: bool = typer.Option(False, "--apply", help="Apply repair (default: dry-run)"),
) -> None:
    """Repair stale/missing S3 index coverage."""
    from ontologia.cli import state

    json_mode = state.json_output

    try:
        repo = _require_s3(open_repo())
    except Exception as e:
        print_error(str(e))
        raise typer.Exit(ec.USAGE_ERROR)

    try:
        if not hasattr(repo, "index_repair"):
            raise RuntimeError("Backend does not support index repair")
        data = repo.index_repair(apply=apply)  # type: ignore[attr-defined]
        print_object(data, json_mode=json_mode)
    except typer.Exit:
        raise
    except Exception as e:
        print_error(str(e))
        raise typer.Exit(ec.EXECUTION_FAILURE)
    finally:
        repo.close()  # type: ignore[attr-defined]
