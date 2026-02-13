"""Tests for sqlite v2 engine dispatch and semantics."""

from __future__ import annotations

from ontologia.errors import StorageBackendError
from ontologia.storage import open_repository


def test_open_repository_sqlite_v2_explicit(tmp_path) -> None:
    db_path = str(tmp_path / "onto-v2.db")
    repo = open_repository(db_path, engine_version="v2")
    try:
        info = repo.storage_info()
        assert info["backend"] == "sqlite"
        assert info["engine_version"] == "v2"
    finally:
        repo.close()


def test_open_repository_sqlite_detects_v2_metadata(tmp_path) -> None:
    db_path = str(tmp_path / "onto-v2-detect.db")
    repo = open_repository(db_path, engine_version="v2")
    repo.close()

    reopened = open_repository(db_path)
    try:
        assert reopened.storage_info()["engine_version"] == "v2"
    finally:
        reopened.close()


def test_sqlite_v2_as_of_before_activation_returns_empty_with_diagnostic(tmp_path) -> None:
    db_path = str(tmp_path / "onto-v2-boundary.db")
    repo = open_repository(db_path, engine_version="v2")
    try:
        v1 = repo.create_schema_version(
            "entity",
            "Customer",
            '{"fields":{"id":"str","name":"str"}}',
            "h1",
            reason="init",
        )
        c1 = repo.create_commit({"kind": "seed"})
        repo.insert_entity(
            "Customer", "c1", {"id": "c1", "name": "Alice"}, c1, schema_version_id=v1
        )
        repo.commit_transaction()

        v2 = repo.create_schema_version(
            "entity",
            "Customer",
            '{"fields":{"id":"str","name":"str","age":"int"}}',
            "h2",
            reason="migrate",
        )
        c2 = repo.create_commit({"kind": "migration"})
        activate = getattr(repo, "activate_schema_version")
        activate(
            type_kind="entity",
            type_name="Customer",
            schema_version_id=v2,
            activation_commit_id=c2,
        )
        repo.insert_entity(
            "Customer",
            "c1",
            {"id": "c1", "name": "Alice", "age": 30},
            c2,
            schema_version_id=v2,
        )
        repo.commit_transaction()

        before = repo.query_entities("Customer", as_of=c1)
        assert before == []
        diag = repo.get_last_query_diagnostics()
        assert diag is not None
        assert diag["reason"] == "commit_before_activation"
        assert diag["activation_commit_id"] == c2

        latest = repo.query_entities("Customer")
        assert len(latest) == 1
        assert latest[0]["fields"]["age"] == 30
    finally:
        repo.close()


def test_sqlite_v2_insert_requires_current_schema_version(tmp_path) -> None:
    db_path = str(tmp_path / "onto-v2-write.db")
    repo = open_repository(db_path, engine_version="v2")
    try:
        v1 = repo.create_schema_version("entity", "User", '{"fields":{"id":"str"}}', "h1")
        v2 = repo.create_schema_version(
            "entity",
            "User",
            '{"fields":{"id":"str","email":"str"}}',
            "h2",
        )
        cid = repo.create_commit()

        # Omitted schema_version_id infers current schema version.
        repo.insert_entity("User", "u1", {"id": "u1", "email": "u1@example.com"}, cid)

        try:
            repo.insert_entity("User", "u2", {"id": "u2"}, cid, schema_version_id=v1)
            raise AssertionError("expected schema_version mismatch error")
        except StorageBackendError:
            pass

        repo.insert_entity(
            "User", "u3", {"id": "u3", "email": "u3@example.com"}, cid, schema_version_id=v2
        )
        repo.commit_transaction()
    finally:
        repo.close()
