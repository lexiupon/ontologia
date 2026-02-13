"""onto init — initialize storage backends."""

from __future__ import annotations

import sqlite3

import typer

from ontologia.cli import _exitcodes as ec
from ontologia.cli._output import print_error, print_object
from ontologia.cli._storage import _config_from_env, resolve_storage_binding
from ontologia.storage import open_repository, parse_storage_target


def init_cmd(
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview initialization only"),
    force: bool = typer.Option(False, "--force", help="Force re-initialization"),
    token: str | None = typer.Option(None, "--token", help="Confirmation token for --force"),
    engine_version: str | None = typer.Option(
        None, "--engine-version", help="Storage engine version (v1 or v2)"
    ),
) -> None:
    """Initialize the selected storage backend."""
    from ontologia.cli import state

    json_mode = state.json_output
    db_path, storage_uri = resolve_storage_binding()

    try:
        target = parse_storage_target(db_path=db_path, storage_uri=storage_uri)
    except Exception as e:
        print_error(str(e))
        raise typer.Exit(ec.USAGE_ERROR)

    try:
        selected_engine = engine_version or "v2"
        if selected_engine not in {"v1", "v2"}:
            raise ValueError(f"Unsupported engine version '{selected_engine}'")

        if target.backend == "sqlite":
            if dry_run:
                data = {
                    "backend": "sqlite",
                    "db_path": target.db_path,
                    "engine_version": selected_engine,
                    "status": "dry_run",
                    "message": "SQLite initializes lazily on first open.",
                }
                print_object(data, json_mode=json_mode)
                return

            cfg = _config_from_env()
            repo = open_repository(
                target.db_path,
                storage_uri=target.uri,
                config=cfg,
                engine_version=selected_engine,
            )
            try:
                if selected_engine == "v1":
                    assert target.db_path is not None
                    conn = sqlite3.connect(target.db_path)
                    try:
                        conn.executescript("""
                            CREATE TABLE IF NOT EXISTS storage_meta (
                                key   TEXT PRIMARY KEY,
                                value TEXT NOT NULL
                            );
                        """)
                        conn.execute(
                            "INSERT OR REPLACE INTO storage_meta (key, value) VALUES ('engine_version', 'v1')"
                        )
                        conn.execute(
                            "INSERT OR REPLACE INTO storage_meta (key, value) VALUES ('backend', 'sqlite')"
                        )
                        conn.commit()
                    finally:
                        conn.close()
                info = repo.storage_info()
            finally:
                repo.close()

            data = {
                "backend": "sqlite",
                "db_path": info.get("db_path", target.db_path),
                "engine_version": info.get("engine_version", selected_engine),
                "status": "initialized",
            }
            print_object(data, json_mode=json_mode)
            return

        from ontologia.storage_s3 import S3RepositoryV1, S3RepositoryV2

        assert target.bucket is not None
        repo_cls = S3RepositoryV2 if selected_engine == "v2" else S3RepositoryV1
        repo = repo_cls(
            bucket=target.bucket,
            prefix=target.prefix or "",
            storage_uri=target.uri,
            config=_config_from_env(),
            allow_uninitialized=True,
        )
        try:
            data = repo.initialize_storage(
                force=force,
                token=token,
                dry_run=dry_run,
                engine_version=selected_engine,
            )
        finally:
            repo.close()

        if json_mode:
            print_object(data, json_mode=True)
        else:
            if dry_run:
                print("Init plan:")
                print(f"  Storage URI: {data['storage_uri']}")
                print(f"  Already initialized: {data['already_initialized']}")
                print(f"  Force token: {data['force_token']}")
                print("  Planned objects:")
                for obj in data["planned_objects"]:
                    print(f"    {obj}")
            else:
                print(f"Initialized: {target.uri}")
    except typer.Exit:
        raise
    except Exception as e:
        print_error(str(e))
        raise typer.Exit(ec.EXECUTION_FAILURE)
