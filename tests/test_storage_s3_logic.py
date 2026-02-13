"""Focused unit tests for S3 repository logic that do not require a live S3 endpoint."""

from __future__ import annotations

import pytest

from ontologia.filters import ComparisonExpression
from ontologia.storage_s3 import S3Repository, _IndexDoc


def test_resolve_type_files_falls_back_on_head_path_mismatch() -> None:
    repo = object.__new__(S3Repository)
    repo._is_type_dropped = lambda _kind, _name: False  # type: ignore[method-assign]
    repo._read_index = lambda _kind, _name: _IndexDoc(  # type: ignore[method-assign]
        type_name="Customer",
        max_indexed_commit=5,
        entries=[
            {
                "min_commit_id": 5,
                "max_commit_id": 5,
                "path": "commits/5-stale/entities/Customer.parquet",
            }
        ],
    )
    repo._read_head = lambda required=True: {  # type: ignore[method-assign]
        "commit_id": 5,
        "manifest_path": "commits/5-good/manifest.json",
    }
    repo._read_manifest = lambda _path: {  # type: ignore[method-assign]
        "commit_id": 5,
        "files": [
            {
                "kind": "entity",
                "type_name": "Customer",
                "path": "commits/5-good/entities/Customer.parquet",
            }
        ],
        "parent_manifest_path": "commits/4/manifest.json",
    }
    repo._walk_manifest_chain = lambda *, start_path=None: iter(  # type: ignore[method-assign]
        [
            {
                "commit_id": 5,
                "files": [
                    {
                        "kind": "entity",
                        "type_name": "Customer",
                        "path": "commits/5-good/entities/Customer.parquet",
                    }
                ],
                "parent_manifest_path": "commits/4/manifest.json",
            },
            {"commit_id": 4, "files": [], "parent_manifest_path": "commits/3/manifest.json"},
        ]
    )

    files = repo._resolve_type_files(
        kind="entity",
        type_name="Customer",
        q_head=5,
        lower_exclusive=0,
    )
    assert files == ["commits/5-good/entities/Customer.parquet"]
    assert repo._last_index_warning is not None
    assert "latest coverage mismatch" in repo._last_index_warning


def test_query_relations_endpoint_filters_use_as_of_temporal_window() -> None:
    repo = object.__new__(S3Repository)
    repo._temporal_window = lambda **_kwargs: (1, 0, False)  # type: ignore[method-assign]

    def _resolve_type_files(
        *,
        kind: str,
        type_name: str,
        q_head: int,
        lower_exclusive: int,
    ) -> list[str]:
        return ["data.parquet"]

    repo._resolve_type_files = _resolve_type_files  # type: ignore[method-assign]
    repo._scan_sql_for_files = lambda _files: "read_parquet(['x'])"  # type: ignore[method-assign]

    class _Cursor:
        def __init__(self, sql: str, params: list[object]) -> None:
            self.sql = sql
            self.params = params

        def fetchall(self) -> list[tuple[object, ...]]:
            return [("c1", "p1", "", '{"seat_count":1}', 1)]

    class _Conn:
        def __init__(self) -> None:
            self.sql = ""
            self.params: list[object] = []

        def execute(self, sql: str, params: list[object]) -> _Cursor:
            self.sql = sql
            self.params = params
            return _Cursor(sql, params)

    conn = _Conn()
    repo._duck_conn = lambda: conn  # type: ignore[method-assign]

    rows = repo.query_relations(
        "Subscription",
        left_entity_type="Customer",
        filter_expr=ComparisonExpression("left.$.tier", "==", "Gold"),
        as_of=1,
    )

    assert len(rows) == 1
    assert "EXISTS" in conn.sql
    assert 1 in conn.params


def test_index_repair_plan_includes_missing_latest_types() -> None:
    repo = object.__new__(S3Repository)
    repo.index_verify = lambda: {  # type: ignore[method-assign]
        "head_commit_id": 7,
        "lagged_types": [],
        "missing_latest": ["entity:Customer"],
        "ok": False,
    }
    repo._read_types_catalog = lambda *, required: {  # type: ignore[method-assign]
        "entities": ["Customer"],
        "relations": [],
    }
    repo._read_index = lambda _kind, _name: _IndexDoc(  # type: ignore[method-assign]
        type_name="Customer",
        max_indexed_commit=7,
        entries=[],
    )

    result = repo.index_repair(apply=False)
    assert result["planned_types"] == ["entity:Customer"]


def test_query_relations_endpoint_filter_requires_endpoint_type() -> None:
    repo = object.__new__(S3Repository)
    repo._relation_rows_raw = lambda *_args, **_kwargs: []  # type: ignore[method-assign]
    with pytest.raises(ValueError):
        repo.query_relations(
            "Subscription",
            filter_expr=ComparisonExpression("left.$.tier", "==", "Gold"),
        )


def test_update_indices_after_commit_continues_after_per_type_failure() -> None:
    repo = object.__new__(S3Repository)
    repo._last_index_warning = None
    repo._ensure_lease_safe = lambda: None  # type: ignore[method-assign]
    repo._read_types_catalog = lambda *, required: {  # type: ignore[method-assign]
        "entities": ["Customer"],
        "relations": ["Subscription"],
    }
    repo._read_index = lambda kind, type_name: _IndexDoc(  # type: ignore[method-assign]
        type_name=type_name,
        max_indexed_commit=1,
        entries=[],
    )

    def _repair_index_gap(
        *,
        kind: str,
        type_name: str,
        index: _IndexDoc,
        previous_head: int,
        previous_manifest_path: str | None,
    ) -> _IndexDoc:
        return index

    repo._repair_index_gap = _repair_index_gap  # type: ignore[method-assign]

    written: list[tuple[str, str]] = []

    def _write_index(kind: str, doc: _IndexDoc) -> None:
        written.append((kind, doc.type_name))
        if kind == "entity":
            raise RuntimeError("boom")

    repo._write_index = _write_index  # type: ignore[method-assign]

    repo._update_indices_after_commit(
        previous_head=1,
        previous_manifest_path=None,
        commit_id=2,
        files=[],
    )

    assert ("entity", "Customer") in written
    assert ("relation", "Subscription") in written
    assert repo._last_index_warning is not None
    assert "Customer" in repo._last_index_warning
