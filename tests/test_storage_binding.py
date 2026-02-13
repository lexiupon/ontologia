"""Tests for storage binding and backend selection."""

from __future__ import annotations

import pytest

from ontologia import Session
from ontologia.storage import open_repository, parse_storage_target


def test_parse_storage_target_defaults_to_sqlite() -> None:
    target = parse_storage_target()
    assert target.backend == "sqlite"
    assert target.db_path == "onto.db"


def test_parse_storage_target_sqlite_uri() -> None:
    target = parse_storage_target(storage_uri="sqlite:///tmp/example.db")
    assert target.backend == "sqlite"
    assert target.db_path == "/tmp/example.db"


def test_parse_storage_target_sqlite_memory_uri() -> None:
    target = parse_storage_target(storage_uri="sqlite:///:memory:")
    assert target.backend == "sqlite"
    assert target.db_path == ":memory:"


def test_parse_storage_target_conflicting_sqlite_raises() -> None:
    with pytest.raises(Exception):
        parse_storage_target(db_path="a.db", storage_uri="sqlite:///b.db")


def test_open_repository_sqlite(tmp_path) -> None:
    db_path = str(tmp_path / "onto.db")
    repo = open_repository(db_path)
    try:
        assert repo.storage_info()["backend"] == "sqlite"
        assert repo.storage_info()["engine_version"] == "v2"
    finally:
        repo.close()


def test_ontology_accepts_storage_uri_sqlite(tmp_path) -> None:
    db_path = tmp_path / "onto.db"
    onto = Session(datastore_uri=f"sqlite:///{db_path}")
    try:
        assert onto.repo.storage_info()["backend"] == "sqlite"
    finally:
        onto.close()


def test_open_repository_sqlite_v1_fallback_for_legacy_db(tmp_path) -> None:
    import sqlite3

    db_path = str(tmp_path / "legacy.db")
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE commits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            metadata_json TEXT
        );
        """
    )
    conn.commit()
    conn.close()

    repo = open_repository(db_path)
    try:
        assert repo.storage_info()["engine_version"] == "v1"
    finally:
        repo.close()
