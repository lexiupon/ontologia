"""CLI tests for --storage-uri and new maintenance commands."""

from __future__ import annotations

import json
import os

from ontologia.cli import app


def test_info_with_storage_uri_sqlite(runner, seeded_db):
    result = runner.invoke(
        app,
        ["--storage-uri", f"sqlite:///{seeded_db}", "--json", "info"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["backend"] == "sqlite"


def test_init_sqlite_dry_run(runner, cli_db):
    result = runner.invoke(
        app,
        ["--db", cli_db, "--json", "init", "--dry-run"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["backend"] == "sqlite"
    assert payload["status"] == "dry_run"
    assert payload["engine_version"] == "v2"


def test_init_sqlite_dry_run_with_explicit_engine_v1(runner, cli_db):
    result = runner.invoke(
        app,
        ["--db", cli_db, "--json", "init", "--dry-run", "--engine-version", "v1"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["backend"] == "sqlite"
    assert payload["engine_version"] == "v1"


def test_compact_on_sqlite_fails(runner, seeded_db):
    result = runner.invoke(app, ["--db", seeded_db, "compact"], catch_exceptions=False)
    assert result.exit_code != 0


def test_index_verify_on_sqlite_fails(runner, seeded_db):
    result = runner.invoke(app, ["--db", seeded_db, "index", "verify"], catch_exceptions=False)
    assert result.exit_code != 0


def test_db_flag_overrides_env_storage_uri(runner, seeded_db):
    result = runner.invoke(
        app,
        ["--db", seeded_db, "--json", "info"],
        env={"ONTOLOGIA_STORAGE_URI": "s3://example-bucket/should-not-be-used"},
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["backend"] == "sqlite"
    assert payload["db_path"] == seeded_db


def test_info_sqlite_uri_missing_file_fails_without_creating_db(runner, tmp_path):
    missing = tmp_path / "missing-info.db"
    assert not missing.exists()

    result = runner.invoke(
        app,
        ["--storage-uri", f"sqlite:///{missing}", "info"],
        catch_exceptions=False,
    )
    assert result.exit_code != 0
    assert not os.path.exists(missing)


def test_storage_uri_s3_ignores_env_db_for_validation(runner):
    result = runner.invoke(
        app,
        ["--storage-uri", "s3://example-bucket/example-prefix"],
        env={"ONTOLOGIA_DB": "local.db"},
        catch_exceptions=False,
    )
    assert result.exit_code == 0


def test_explicit_storage_uri_sqlite_overrides_env_db(runner, tmp_path):
    explicit_db = tmp_path / "explicit.db"
    result = runner.invoke(
        app,
        ["--storage-uri", f"sqlite:///{explicit_db}", "--json", "init", "--dry-run"],
        env={"ONTOLOGIA_DB": str(tmp_path / "env.db")},
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["backend"] == "sqlite"
    assert payload["db_path"] == str(explicit_db)


def test_info_sqlite_memory_uri_succeeds(runner):
    result = runner.invoke(
        app,
        ["--storage-uri", "sqlite:///:memory:", "--json", "info"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["backend"] == "sqlite"
    assert payload["db_path"] == ":memory:"


def test_init_s3_uses_env_config(runner, monkeypatch):
    captured: dict[str, object] = {}

    class _FakeS3Repo:
        def __init__(
            self,
            *,
            bucket: str,
            prefix: str,
            storage_uri: str,
            config,
            allow_uninitialized: bool = False,
        ) -> None:
            captured["bucket"] = bucket
            captured["prefix"] = prefix
            captured["region"] = config.s3_region
            captured["endpoint"] = config.s3_endpoint_url
            captured["allow_uninitialized"] = allow_uninitialized

        def initialize_storage(
            self,
            *,
            force: bool = False,
            token: str | None = None,
            dry_run: bool = True,
            engine_version: str | None = None,
        ) -> dict[str, object]:
            return {
                "storage_uri": "s3://example-bucket/example-prefix",
                "already_initialized": False,
                "planned_objects": ["meta/head.json"],
                "force_token": "abc",
                "engine_version": engine_version or "v2",
            }

        def close(self) -> None:
            return None

    monkeypatch.setattr("ontologia.storage_s3.S3RepositoryV1", _FakeS3Repo)
    monkeypatch.setattr("ontologia.storage_s3.S3RepositoryV2", _FakeS3Repo)
    result = runner.invoke(
        app,
        ["--storage-uri", "s3://example-bucket/example-prefix", "--json", "init", "--dry-run"],
        env={
            "ONTOLOGIA_S3_REGION": "us-east-1",
            "ONTOLOGIA_S3_ENDPOINT_URL": "http://127.0.0.1:9000",
        },
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    assert captured["region"] == "us-east-1"
    assert captured["endpoint"] == "http://127.0.0.1:9000"
    assert captured["allow_uninitialized"] is True
