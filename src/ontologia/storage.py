"""Storage backends and shared query/filter helpers."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterator, Protocol, runtime_checkable
from urllib.parse import urlparse

from ontologia.config import OntologiaConfig
from ontologia.errors import StorageBackendError

# from ontologia.errors import LockContentionError
from ontologia.filters import (
    ComparisonExpression,
    ExistsComparisonExpression,
    FilterExpression,
    LogicalExpression,
)


def _compile_filter(
    expr: FilterExpression,
    params: list[Any],
    *,
    table_alias: str = "",
    left_entity_type: str | None = None,
    right_entity_type: str | None = None,
) -> str:
    """Compile a FilterExpression tree into a SQL WHERE clause fragment."""
    if isinstance(expr, ExistsComparisonExpression):
        return _compile_exists(expr, params, table_alias=table_alias)
    elif isinstance(expr, ComparisonExpression):
        return _compile_comparison(
            expr,
            params,
            table_alias=table_alias,
            left_entity_type=left_entity_type,
            right_entity_type=right_entity_type,
        )
    elif isinstance(expr, LogicalExpression):
        if expr.op == "NOT":
            child_sql = _compile_filter(
                expr.children[0],
                params,
                table_alias=table_alias,
                left_entity_type=left_entity_type,
                right_entity_type=right_entity_type,
            )
            return f"NOT ({child_sql})"
        elif expr.op in ("AND", "OR"):
            parts = [
                _compile_filter(
                    c,
                    params,
                    table_alias=table_alias,
                    left_entity_type=left_entity_type,
                    right_entity_type=right_entity_type,
                )
                for c in expr.children
            ]
            joiner = f" {expr.op} "
            return f"({joiner.join(parts)})"
    raise ValueError(f"Unknown filter expression type: {type(expr)}")


def _compile_comparison(
    expr: ComparisonExpression,
    params: list[Any],
    *,
    table_alias: str = "",
    left_entity_type: str | None = None,
    right_entity_type: str | None = None,
) -> str:
    """Compile a single comparison expression to SQL."""
    field_path = expr.field_path

    # Determine the JSON extract expression based on field path prefix
    if field_path.startswith("left.$."):
        field_name = field_path[7:]  # strip "left.$."
        # Subquery against entity_history for left endpoint
        json_col = f"json_extract(le.fields_json, '$.{field_name}')"
    elif field_path.startswith("right.$."):
        field_name = field_path[8:]  # strip "right.$."
        json_col = f"json_extract(re.fields_json, '$.{field_name}')"
    elif field_path.startswith("$."):
        field_name = field_path[2:]
        prefix = f"{table_alias}." if table_alias else ""
        json_col = f"json_extract({prefix}fields_json, '$.{field_name}')"
    else:
        raise ValueError(f"Invalid field path: {field_path}")

    op = expr.op
    if op == "IS_NULL":
        return f"{json_col} IS NULL"
    elif op == "IS_NOT_NULL":
        return f"{json_col} IS NOT NULL"
    elif op == "IN":
        placeholders = ", ".join("?" for _ in expr.value)
        params.extend(expr.value)
        return f"{json_col} IN ({placeholders})"
    elif op == "LIKE":
        params.append(expr.value)
        return f"{json_col} LIKE ?"
    else:
        sql_op = {"==": "=", "!=": "!=", ">": ">", ">=": ">=", "<": "<", "<=": "<="}[op]
        params.append(expr.value)
        return f"{json_col} {sql_op} ?"


def _compile_exists(
    expr: ExistsComparisonExpression,
    params: list[Any],
    *,
    table_alias: str = "",
) -> str:
    """Compile an existential predicate to EXISTS (SELECT 1 FROM json_each(...))."""
    list_path = expr.list_field_path
    if list_path.startswith("$."):
        field_name = list_path[2:]
        prefix = f"{table_alias}." if table_alias else ""
        json_col = f"{prefix}fields_json"
    else:
        raise ValueError(
            f"ExistsComparisonExpression: unsupported list_field_path prefix: {list_path}"
        )

    # json_each extracts each element of the JSON array
    item_path = expr.item_path
    item_col = f"json_extract(je.value, '$.{item_path}')"
    op = expr.op

    if op == "IS_NULL":
        condition = f"{item_col} IS NULL"
    elif op == "IS_NOT_NULL":
        condition = f"{item_col} IS NOT NULL"
    elif op == "IN":
        placeholders = ", ".join("?" for _ in expr.value)
        params.extend(expr.value)
        condition = f"{item_col} IN ({placeholders})"
    elif op == "LIKE":
        params.append(expr.value)
        condition = f"{item_col} LIKE ?"
    else:
        sql_op = {"==": "=", "!=": "!=", ">": ">", ">=": ">=", "<": "<", "<=": "<="}[op]
        params.append(expr.value)
        condition = f"{item_col} {sql_op} ?"

    return (
        f"EXISTS (SELECT 1 FROM json_each(json_extract({json_col}, '$.{field_name}')) AS je "
        f"WHERE {condition})"
    )


def _needs_endpoint_join(expr: FilterExpression | None, prefix: str) -> bool:
    """Check if a filter expression references endpoint fields."""
    if expr is None:
        return False
    if isinstance(expr, ComparisonExpression):
        return expr.field_path.startswith(f"{prefix}.")
    if isinstance(expr, ExistsComparisonExpression):
        return expr.list_field_path.startswith(f"{prefix}.")
    if isinstance(expr, LogicalExpression):
        return any(_needs_endpoint_join(c, prefix) for c in expr.children)
    return False


def _schema_hash(schema_json: str) -> str:
    """Compute deterministic SHA-256 hash of schema JSON."""
    canonical = json.dumps(json.loads(schema_json), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


@dataclass(frozen=True)
class StorageTarget:
    """Resolved storage target from legacy db_path and URI forms."""

    backend: str
    uri: str
    db_path: str | None = None
    bucket: str | None = None
    prefix: str | None = None


def parse_storage_target(
    db_path: str | None = None,
    storage_uri: str | None = None,
) -> StorageTarget:
    """Resolve backend target from legacy db_path and URI forms."""
    if storage_uri is None and db_path is None:
        db_path = "onto.db"

    if storage_uri is None and db_path is not None:
        return StorageTarget(backend="sqlite", uri=f"sqlite:///{db_path}", db_path=db_path)

    assert storage_uri is not None
    parsed = urlparse(storage_uri)

    if parsed.scheme == "sqlite":
        sqlite_path = parsed.path
        if parsed.netloc:
            sqlite_path = f"{parsed.netloc}{sqlite_path}"
        elif sqlite_path.startswith("//"):
            # sqlite:////abs/path -> /abs/path
            sqlite_path = sqlite_path[1:]
        if sqlite_path == "/:memory:":
            sqlite_path = ":memory:"
        if not sqlite_path:
            raise StorageBackendError("parse_storage_uri", f"Invalid sqlite URI: {storage_uri}")
        if db_path is not None and os.path.abspath(db_path) != os.path.abspath(sqlite_path):
            raise StorageBackendError(
                "parse_storage_uri",
                f"Conflicting db_path '{db_path}' and storage_uri '{storage_uri}'",
            )
        return StorageTarget(backend="sqlite", uri=storage_uri, db_path=sqlite_path)

    if parsed.scheme == "s3":
        bucket = parsed.netloc
        prefix = parsed.path.lstrip("/").rstrip("/")
        if not bucket:
            raise StorageBackendError("parse_storage_uri", f"Invalid s3 URI: {storage_uri}")
        if db_path is not None:
            raise StorageBackendError(
                "parse_storage_uri",
                "db_path cannot be provided for s3 storage targets",
            )
        return StorageTarget(backend="s3", uri=storage_uri, bucket=bucket, prefix=prefix)

    raise StorageBackendError(
        "parse_storage_uri",
        f"Unsupported storage URI scheme '{parsed.scheme}' for '{storage_uri}'",
    )


@runtime_checkable
class RepositoryProtocol(Protocol):
    """Backend-agnostic repository contract used by runtime/query/CLI."""

    def close(self) -> None: ...

    def create_commit(self, metadata: dict[str, Any] | None = None) -> int: ...

    def get_head_commit_id(self) -> int | None: ...

    def get_commit(self, commit_id: int) -> dict[str, Any] | None: ...

    def list_commits(
        self,
        *,
        limit: int = 10,
        since_commit_id: int | None = None,
    ) -> list[dict[str, Any]]: ...

    def get_latest_entity(self, type_name: str, key: str) -> dict[str, Any] | None: ...

    def insert_entity(
        self,
        type_name: str,
        key: str,
        fields: dict[str, Any],
        commit_id: int,
        schema_version_id: int | None = None,
    ) -> None: ...

    def query_entities(
        self,
        type_name: str,
        *,
        filter_expr: FilterExpression | None = None,
        order_by: str | None = None,
        order_desc: bool = False,
        limit: int | None = None,
        offset: int | None = None,
        with_history: bool = False,
        history_since: int | None = None,
        as_of: int | None = None,
        schema_version_id: int | None = None,
    ) -> list[dict[str, Any]]: ...

    def count_entities(
        self,
        type_name: str,
        *,
        filter_expr: FilterExpression | None = None,
    ) -> int: ...

    def aggregate_entities(
        self,
        type_name: str,
        agg_func: str,
        field_name: str,
        *,
        filter_expr: FilterExpression | None = None,
    ) -> Any: ...

    def group_by_entities(
        self,
        type_name: str,
        group_field: str,
        agg_specs: dict[str, tuple[str, str | None]],
        *,
        filter_expr: FilterExpression | None = None,
        having_sql_fragment: str | None = None,
        having_params: list[Any] | None = None,
    ) -> list[dict[str, Any]]: ...

    def get_latest_relation(
        self, type_name: str, left_key: str, right_key: str, instance_key: str = ""
    ) -> dict[str, Any] | None: ...

    def insert_relation(
        self,
        type_name: str,
        left_key: str,
        right_key: str,
        fields: dict[str, Any],
        commit_id: int,
        schema_version_id: int | None = None,
        instance_key: str = "",
    ) -> None: ...

    def query_relations(
        self,
        type_name: str,
        *,
        left_entity_type: str | None = None,
        right_entity_type: str | None = None,
        filter_expr: FilterExpression | None = None,
        order_by: str | None = None,
        order_desc: bool = False,
        limit: int | None = None,
        offset: int | None = None,
        with_history: bool = False,
        history_since: int | None = None,
        as_of: int | None = None,
        schema_version_id: int | None = None,
    ) -> list[dict[str, Any]]: ...

    def count_relations(
        self,
        type_name: str,
        *,
        left_entity_type: str | None = None,
        right_entity_type: str | None = None,
        filter_expr: FilterExpression | None = None,
    ) -> int: ...

    def aggregate_relations(
        self,
        type_name: str,
        agg_func: str,
        field_name: str,
        *,
        filter_expr: FilterExpression | None = None,
    ) -> Any: ...

    def group_by_relations(
        self,
        type_name: str,
        group_field: str,
        agg_specs: dict[str, tuple[str, str | None]],
        *,
        left_entity_type: str | None = None,
        right_entity_type: str | None = None,
        filter_expr: FilterExpression | None = None,
        having_sql_fragment: str | None = None,
        having_params: list[Any] | None = None,
    ) -> list[dict[str, Any]]: ...

    def get_relations_for_entity(
        self,
        relation_type: str,
        left_entity_type: str,
        entity_key: str,
        *,
        direction: str = "left",
    ) -> list[dict[str, Any]]: ...

    def get_schema(self, type_kind: str, type_name: str) -> dict[str, Any] | None: ...

    def store_schema(self, type_kind: str, type_name: str, schema: dict[str, Any]) -> None: ...

    def list_schemas(self, type_kind: str) -> list[dict[str, Any]]: ...

    def create_schema_version(
        self,
        type_kind: str,
        type_name: str,
        schema_json: str,
        schema_hash: str,
        runtime_id: str | None = None,
        reason: str | None = None,
    ) -> int: ...

    def get_current_schema_version(
        self, type_kind: str, type_name: str
    ) -> dict[str, Any] | None: ...

    def get_schema_version(
        self, type_kind: str, type_name: str, version_id: int
    ) -> dict[str, Any] | None: ...

    def list_schema_versions(self, type_kind: str, type_name: str) -> list[dict[str, Any]]: ...

    def count_latest_entities(self, type_name: str) -> int: ...

    def count_latest_relations(self, type_name: str) -> int: ...

    def iter_latest_entities(
        self, type_name: str, batch_size: int = 1000
    ) -> Iterator[list[tuple[str, dict[str, Any], int, int | None]]]: ...

    def iter_latest_relations(
        self, type_name: str, batch_size: int = 1000
    ) -> Iterator[list[tuple[str, str, str, dict[str, Any], int, int | None]]]: ...

    def acquire_lock(
        self, owner_id: str, timeout_ms: int = 5000, lease_ms: int = 30000
    ) -> bool: ...

    def renew_lock(self, owner_id: str, lease_ms: int = 30000) -> bool: ...

    def release_lock(self, owner_id: str) -> None: ...

    def begin_transaction(self) -> None: ...

    def commit_transaction(self) -> None: ...

    def rollback_transaction(self) -> None: ...

    def list_commit_changes(self, commit_id: int) -> list[dict[str, Any]]: ...

    def count_commit_operations(self, commit_id: int) -> int: ...

    def storage_info(self) -> dict[str, Any]: ...

    def get_last_query_diagnostics(self) -> dict[str, Any] | None: ...

    def apply_schema_drop(
        self,
        *,
        affected_types: list[tuple[str, str]],
        purge_history: bool,
        commit_meta: dict[str, str] | None = None,
    ) -> int: ...


class Repository:
    """SQLite-backed repository for entity and relation history."""

    def __init__(self, db_path: str) -> None:
        self.engine_version = "v1"
        self.db_path = db_path
        self._conn = sqlite3.connect(db_path)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._last_query_diagnostics: dict[str, Any] | None = None
        self._create_tables()

    def _create_tables(self) -> None:
        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS commits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                metadata_json TEXT
            );

            CREATE TABLE IF NOT EXISTS entity_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entity_type TEXT NOT NULL,
                entity_key TEXT NOT NULL,
                fields_json TEXT NOT NULL,
                commit_id INTEGER NOT NULL,
                FOREIGN KEY (commit_id) REFERENCES commits(id)
            );

            CREATE INDEX IF NOT EXISTS idx_entity_history_lookup
                ON entity_history(entity_type, entity_key, commit_id DESC);

            CREATE TABLE IF NOT EXISTS relation_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                relation_type TEXT NOT NULL,
                left_key TEXT NOT NULL,
                right_key TEXT NOT NULL,
                fields_json TEXT NOT NULL,
                commit_id INTEGER NOT NULL,
                FOREIGN KEY (commit_id) REFERENCES commits(id)
            );

            CREATE INDEX IF NOT EXISTS idx_relation_history_lookup
                ON relation_history(relation_type, left_key, right_key, commit_id DESC);

            CREATE TABLE IF NOT EXISTS schema_registry (
                type_kind TEXT NOT NULL,
                type_name TEXT NOT NULL,
                schema_json TEXT NOT NULL,
                PRIMARY KEY (type_kind, type_name)
            );

            CREATE TABLE IF NOT EXISTS schema_versions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                type_kind TEXT NOT NULL,
                type_name TEXT NOT NULL,
                schema_version_id INTEGER NOT NULL,
                schema_json TEXT NOT NULL,
                schema_hash TEXT NOT NULL,
                created_at TEXT NOT NULL,
                runtime_id TEXT,
                reason TEXT,
                UNIQUE(type_kind, type_name, schema_version_id)
            );

            CREATE TABLE IF NOT EXISTS locks (
                lock_name TEXT PRIMARY KEY,
                owner_id TEXT NOT NULL,
                acquired_at TEXT NOT NULL,
                expires_at TEXT NOT NULL
            );
        """)
        self._conn.commit()
        self._migrate_history_columns()
        self._migrate_instance_key_column()
        self._bootstrap_schema_versions()

    def _migrate_history_columns(self) -> None:
        """Add schema_version_id column to history tables if missing."""
        for table in ("entity_history", "relation_history"):
            cols = {row[1] for row in self._conn.execute(f"PRAGMA table_info({table})").fetchall()}
            if "schema_version_id" not in cols:
                self._conn.execute(f"ALTER TABLE {table} ADD COLUMN schema_version_id INTEGER")
                self._conn.commit()

    def _migrate_instance_key_column(self) -> None:
        """Add instance_key column to relation_history if missing."""
        cols = {
            row[1] for row in self._conn.execute("PRAGMA table_info(relation_history)").fetchall()
        }
        if "instance_key" not in cols:
            self._conn.execute(
                "ALTER TABLE relation_history ADD COLUMN instance_key TEXT NOT NULL DEFAULT ''"
            )
            # Recreate index to include instance_key
            self._conn.execute("DROP INDEX IF EXISTS idx_relation_history_lookup")
            self._conn.execute(
                "CREATE INDEX idx_relation_history_lookup "
                "ON relation_history(relation_type, left_key, right_key, "
                "instance_key, commit_id DESC)"
            )
            self._conn.commit()

    def _bootstrap_schema_versions(self) -> None:
        """Seed schema_versions from schema_registry if empty."""
        has_versions = self._conn.execute("SELECT COUNT(*) FROM schema_versions").fetchone()[0]
        if has_versions:
            return
        rows = self._conn.execute(
            "SELECT type_kind, type_name, schema_json FROM schema_registry"
        ).fetchall()
        if not rows:
            return
        now = datetime.now(timezone.utc).isoformat()
        for kind, name, schema_json in rows:
            schema_hash = _schema_hash(schema_json)
            self._conn.execute(
                "INSERT INTO schema_versions "
                "(type_kind, type_name, schema_version_id, schema_json, "
                "schema_hash, created_at, reason) "
                "VALUES (?, ?, 1, ?, ?, ?, ?)",
                (kind, name, schema_json, schema_hash, now, "bootstrap"),
            )
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    # --- Commit operations ---

    def create_commit(self, metadata: dict[str, Any] | None = None) -> int:
        now = datetime.now(timezone.utc).isoformat()
        meta_json = json.dumps(metadata) if metadata else None
        cursor = self._conn.execute(
            "INSERT INTO commits (created_at, metadata_json) VALUES (?, ?)",
            (now, meta_json),
        )
        return cursor.lastrowid  # type: ignore[return-value]

    def get_head_commit_id(self) -> int | None:
        row = self._conn.execute("SELECT MAX(id) FROM commits").fetchone()
        return row[0] if row and row[0] is not None else None

    def get_commit(self, commit_id: int) -> dict[str, Any] | None:
        row = self._conn.execute(
            "SELECT id, created_at, metadata_json FROM commits WHERE id = ?",
            (commit_id,),
        ).fetchone()
        if row is None:
            return None
        return {
            "id": row[0],
            "created_at": row[1],
            "metadata": json.loads(row[2]) if row[2] else None,
        }

    def list_commits(
        self,
        *,
        limit: int = 10,
        since_commit_id: int | None = None,
    ) -> list[dict[str, Any]]:
        if since_commit_id is not None:
            rows = self._conn.execute(
                "SELECT id, created_at, metadata_json FROM commits "
                "WHERE id > ? ORDER BY id DESC LIMIT ?",
                (since_commit_id, limit),
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT id, created_at, metadata_json FROM commits ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [
            {
                "id": r[0],
                "created_at": r[1],
                "metadata": json.loads(r[2]) if r[2] else None,
            }
            for r in rows
        ]

    # --- Entity operations ---

    def get_latest_entity(self, type_name: str, key: str) -> dict[str, Any] | None:
        row = self._conn.execute(
            "SELECT fields_json, commit_id FROM entity_history "
            "WHERE entity_type = ? AND entity_key = ? "
            "ORDER BY commit_id DESC LIMIT 1",
            (type_name, key),
        ).fetchone()
        if row is None:
            return None
        return {"fields": json.loads(row[0]), "commit_id": row[1]}

    def insert_entity(
        self,
        type_name: str,
        key: str,
        fields: dict[str, Any],
        commit_id: int,
        schema_version_id: int | None = None,
    ) -> None:
        self._conn.execute(
            "INSERT INTO entity_history "
            "(entity_type, entity_key, fields_json, commit_id, schema_version_id) "
            "VALUES (?, ?, ?, ?, ?)",
            (type_name, key, json.dumps(fields), commit_id, schema_version_id),
        )

    def query_entities(
        self,
        type_name: str,
        *,
        filter_expr: FilterExpression | None = None,
        order_by: str | None = None,
        order_desc: bool = False,
        limit: int | None = None,
        offset: int | None = None,
        with_history: bool = False,
        history_since: int | None = None,
        as_of: int | None = None,
        schema_version_id: int | None = None,
    ) -> list[dict[str, Any]]:
        """Query entity history rows, returning latest version per key by default."""
        self._last_query_diagnostics = None
        params: list[Any] = []
        # Determine whether to apply schema version filtering (temporal queries only)
        _apply_sv = schema_version_id is not None and (
            with_history or history_since is not None or as_of is not None
        )

        if with_history or history_since is not None:
            # Return all version rows
            sql = (
                "SELECT eh.entity_key, eh.fields_json, eh.commit_id "
                "FROM entity_history eh "
                "WHERE eh.entity_type = ?"
            )
            params.append(type_name)
            if history_since is not None:
                sql += " AND eh.commit_id > ?"
                params.append(history_since)
            if _apply_sv:
                sql += " AND eh.schema_version_id = ?"
                params.append(schema_version_id)
        elif as_of is not None:
            # Return latest version as of a specific commit
            sv_filter_sub = ""
            sv_filter_outer = ""
            if _apply_sv:
                sv_filter_sub = " AND schema_version_id = ?"
                sv_filter_outer = " AND eh.schema_version_id = ?"
            sql = (
                "SELECT eh.entity_key, eh.fields_json, eh.commit_id "
                "FROM entity_history eh "
                "INNER JOIN ("
                "  SELECT entity_key, MAX(commit_id) as max_cid "
                "  FROM entity_history "
                "  WHERE entity_type = ? AND commit_id <= ?"
                + sv_filter_sub
                + "  GROUP BY entity_key"
                ") latest ON eh.entity_key = latest.entity_key AND eh.commit_id = latest.max_cid "
                "WHERE eh.entity_type = ?" + sv_filter_outer
            )
            params.extend([type_name, as_of])
            if _apply_sv:
                params.append(schema_version_id)
            params.append(type_name)
            if _apply_sv:
                params.append(schema_version_id)
        else:
            # Return latest version per key
            sql = (
                "SELECT eh.entity_key, eh.fields_json, eh.commit_id "
                "FROM entity_history eh "
                "INNER JOIN ("
                "  SELECT entity_key, MAX(commit_id) as max_cid "
                "  FROM entity_history "
                "  WHERE entity_type = ? "
                "  GROUP BY entity_key"
                ") latest ON eh.entity_key = latest.entity_key AND eh.commit_id = latest.max_cid "
                "WHERE eh.entity_type = ?"
            )
            params.extend([type_name, type_name])

        if filter_expr is not None:
            where_sql = _compile_filter(filter_expr, params, table_alias="eh")
            sql += f" AND {where_sql}"

        if order_by:
            field_name = order_by.removeprefix("$.")
            direction = "DESC" if order_desc else "ASC"
            sql += f" ORDER BY json_extract(eh.fields_json, '$.{field_name}') {direction}"
        elif with_history or history_since is not None:
            sql += " ORDER BY eh.commit_id ASC"

        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        if offset is not None:
            sql += " OFFSET ?"
            params.append(offset)

        rows = self._conn.execute(sql, params).fetchall()
        return [
            {
                "key": r[0],
                "fields": json.loads(r[1]),
                "commit_id": r[2],
            }
            for r in rows
        ]

    def count_entities(
        self,
        type_name: str,
        *,
        filter_expr: FilterExpression | None = None,
    ) -> int:
        params: list[Any] = []
        sql = (
            "SELECT COUNT(*) FROM entity_history eh "
            "INNER JOIN ("
            "  SELECT entity_key, MAX(commit_id) as max_cid "
            "  FROM entity_history WHERE entity_type = ? GROUP BY entity_key"
            ") latest ON eh.entity_key = latest.entity_key AND eh.commit_id = latest.max_cid "
            "WHERE eh.entity_type = ?"
        )
        params.extend([type_name, type_name])
        if filter_expr is not None:
            where_sql = _compile_filter(filter_expr, params, table_alias="eh")
            sql += f" AND {where_sql}"
        row = self._conn.execute(sql, params).fetchone()
        return row[0] if row else 0

    def aggregate_entities(
        self,
        type_name: str,
        agg_func: str,
        field_name: str,
        *,
        filter_expr: FilterExpression | None = None,
    ) -> Any:
        params: list[Any] = []
        json_path = f"json_extract(eh.fields_json, '$.{field_name}')"

        if agg_func.upper() == "AVG_LEN":
            expr = f"json_array_length(json_extract(eh.fields_json, '$.{field_name}'))"
            agg_func = "AVG"
        elif agg_func.upper() in ("SUM", "AVG"):
            # Cast to number for arithmetic aggregations
            expr = f"CAST({json_path} AS REAL)"
        else:
            expr = json_path

        sql = (
            f"SELECT {agg_func}({expr}) FROM entity_history eh "
            "INNER JOIN ("
            "  SELECT entity_key, MAX(commit_id) as max_cid "
            "  FROM entity_history WHERE entity_type = ? GROUP BY entity_key"
            ") latest ON eh.entity_key = latest.entity_key AND eh.commit_id = latest.max_cid "
            "WHERE eh.entity_type = ?"
        )
        params.extend([type_name, type_name])

        # print(f"DEBUG SQL: {sql}")
        # print(f"DEBUG PARAMS: {params}")

        if filter_expr is not None:
            where_sql = _compile_filter(filter_expr, params, table_alias="eh")
            sql += f" AND {where_sql}"
        row = self._conn.execute(sql, params).fetchone()
        return row[0] if row else None

    def group_by_entities(
        self,
        type_name: str,
        group_field: str,
        agg_specs: dict[str, tuple[str, str | None]],
        *,
        filter_expr: FilterExpression | None = None,
        having_sql_fragment: str | None = None,
        having_params: list[Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Group entities by a field and compute aggregates.

        agg_specs: {"alias": ("FUNC", "field_name" or None for COUNT)}
        """
        params: list[Any] = []
        group_json = f"json_extract(eh.fields_json, '$.{group_field}')"

        select_parts = [f"{group_json} as group_key"]
        for alias, (func, field_name) in agg_specs.items():
            if func.upper() == "COUNT":
                select_parts.append(f"COUNT(*) as [{alias}]")
            else:
                fp = f"json_extract(eh.fields_json, '$.{field_name}')"
                select_parts.append(f"{func}({fp}) as [{alias}]")

        select_clause = ", ".join(select_parts)

        sql = (
            f"SELECT {select_clause} FROM entity_history eh "
            "INNER JOIN ("
            "  SELECT entity_key, MAX(commit_id) as max_cid "
            "  FROM entity_history WHERE entity_type = ? GROUP BY entity_key"
            ") latest ON eh.entity_key = latest.entity_key AND eh.commit_id = latest.max_cid "
            "WHERE eh.entity_type = ?"
        )
        params.extend([type_name, type_name])

        if filter_expr is not None:
            where_sql = _compile_filter(filter_expr, params, table_alias="eh")
            sql += f" AND {where_sql}"

        sql += f" GROUP BY {group_json}"

        if having_sql_fragment:
            sql += f" HAVING {having_sql_fragment}"
            if having_params:
                params.extend(having_params)

        rows = self._conn.execute(sql, params).fetchall()
        result = []
        for row in rows:
            d: dict[str, Any] = {group_field: row[0]}
            for i, alias in enumerate(agg_specs.keys()):
                d[alias] = row[i + 1]
            result.append(d)
        return result

    # --- Relation operations ---

    def get_latest_relation(
        self, type_name: str, left_key: str, right_key: str, instance_key: str = ""
    ) -> dict[str, Any] | None:
        row = self._conn.execute(
            "SELECT fields_json, commit_id FROM relation_history "
            "WHERE relation_type = ? AND left_key = ? AND right_key = ? AND instance_key = ? "
            "ORDER BY commit_id DESC LIMIT 1",
            (type_name, left_key, right_key, instance_key),
        ).fetchone()
        if row is None:
            return None
        return {"fields": json.loads(row[0]), "commit_id": row[1]}

    def insert_relation(
        self,
        type_name: str,
        left_key: str,
        right_key: str,
        fields: dict[str, Any],
        commit_id: int,
        schema_version_id: int | None = None,
        instance_key: str = "",
    ) -> None:
        self._conn.execute(
            "INSERT INTO relation_history "
            "(relation_type, left_key, right_key, instance_key, fields_json, commit_id, "
            "schema_version_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                type_name,
                left_key,
                right_key,
                instance_key,
                json.dumps(fields),
                commit_id,
                schema_version_id,
            ),
        )

    def query_relations(
        self,
        type_name: str,
        *,
        left_entity_type: str | None = None,
        right_entity_type: str | None = None,
        filter_expr: FilterExpression | None = None,
        order_by: str | None = None,
        order_desc: bool = False,
        limit: int | None = None,
        offset: int | None = None,
        with_history: bool = False,
        history_since: int | None = None,
        as_of: int | None = None,
        schema_version_id: int | None = None,
    ) -> list[dict[str, Any]]:
        self._last_query_diagnostics = None
        params: list[Any] = []
        needs_left = _needs_endpoint_join(filter_expr, "left")
        needs_right = _needs_endpoint_join(filter_expr, "right")
        if needs_left and left_entity_type is None:
            raise ValueError("left_entity_type is required for left endpoint filters")
        if needs_right and right_entity_type is None:
            raise ValueError("right_entity_type is required for right endpoint filters")

        # Determine whether to apply schema version filtering (temporal queries only)
        _apply_sv = schema_version_id is not None and (
            with_history or history_since is not None or as_of is not None
        )

        if with_history or history_since is not None:
            sql = (
                "SELECT rh.left_key, rh.right_key, rh.instance_key, rh.fields_json, rh.commit_id "
                "FROM relation_history rh "
                "WHERE rh.relation_type = ?"
            )
            params.append(type_name)
            if history_since is not None:
                sql += " AND rh.commit_id > ?"
                params.append(history_since)
            if _apply_sv:
                sql += " AND rh.schema_version_id = ?"
                params.append(schema_version_id)
        elif as_of is not None:
            sv_filter_sub = ""
            sv_filter_outer = ""
            if _apply_sv:
                sv_filter_sub = " AND schema_version_id = ?"
                sv_filter_outer = " AND rh.schema_version_id = ?"
            sql = (
                "SELECT rh.left_key, rh.right_key, rh.instance_key, rh.fields_json, rh.commit_id "
                "FROM relation_history rh "
                "INNER JOIN ("
                "  SELECT left_key, right_key, instance_key, MAX(commit_id) as max_cid "
                "  FROM relation_history "
                "  WHERE relation_type = ? AND commit_id <= ?"
                + sv_filter_sub
                + "  GROUP BY left_key, right_key, instance_key"
                ") latest ON rh.left_key = latest.left_key AND rh.right_key = latest.right_key "
                "AND rh.instance_key = latest.instance_key "
                "AND rh.commit_id = latest.max_cid "
                "WHERE rh.relation_type = ?" + sv_filter_outer
            )
            params.extend([type_name, as_of])
            if _apply_sv:
                params.append(schema_version_id)
            params.append(type_name)
            if _apply_sv:
                params.append(schema_version_id)
        else:
            sql = (
                "SELECT rh.left_key, rh.right_key, rh.instance_key, rh.fields_json, rh.commit_id "
                "FROM relation_history rh "
                "INNER JOIN ("
                "  SELECT left_key, right_key, instance_key, MAX(commit_id) as max_cid "
                "  FROM relation_history "
                "  WHERE relation_type = ? "
                "  GROUP BY left_key, right_key, instance_key"
                ") latest ON rh.left_key = latest.left_key AND rh.right_key = latest.right_key "
                "AND rh.instance_key = latest.instance_key "
                "AND rh.commit_id = latest.max_cid "
                "WHERE rh.relation_type = ?"
            )
            params.extend([type_name, type_name])

        # Join left endpoint entity for filtering
        if needs_left and left_entity_type:
            sql += " AND EXISTS ( SELECT 1 FROM entity_history le "
            if with_history or history_since is not None:
                sql += " WHERE le.entity_type = ? AND le.entity_key = rh.left_key"
                params.append(left_entity_type)
                if history_since is not None:
                    sql += " AND le.commit_id > ?"
                    params.append(history_since)
            elif as_of is not None:
                sql += (
                    " INNER JOIN ("
                    "   SELECT entity_key, MAX(commit_id) as max_cid "
                    "   FROM entity_history WHERE entity_type = ? AND commit_id <= ? "
                    "   GROUP BY entity_key"
                    " ) le_latest ON le.entity_key = le_latest.entity_key "
                    " AND le.commit_id = le_latest.max_cid "
                    " WHERE le.entity_type = ? AND le.entity_key = rh.left_key"
                )
                params.extend([left_entity_type, as_of, left_entity_type])
            else:
                sql += (
                    " INNER JOIN ("
                    "   SELECT entity_key, MAX(commit_id) as max_cid "
                    "   FROM entity_history WHERE entity_type = ? GROUP BY entity_key"
                    " ) le_latest ON le.entity_key = le_latest.entity_key "
                    " AND le.commit_id = le_latest.max_cid "
                    " WHERE le.entity_type = ? AND le.entity_key = rh.left_key"
                )
                params.extend([left_entity_type, left_entity_type])
            left_filter = _extract_prefix_filter(filter_expr, "left")
            if left_filter:
                left_where = _compile_filter(left_filter, params, table_alias="le")
                sql += f" AND {left_where}"
            sql += ")"

        if needs_right and right_entity_type:
            sql += " AND EXISTS ( SELECT 1 FROM entity_history re "
            if with_history or history_since is not None:
                sql += " WHERE re.entity_type = ? AND re.entity_key = rh.right_key"
                params.append(right_entity_type)
                if history_since is not None:
                    sql += " AND re.commit_id > ?"
                    params.append(history_since)
            elif as_of is not None:
                sql += (
                    " INNER JOIN ("
                    "   SELECT entity_key, MAX(commit_id) as max_cid "
                    "   FROM entity_history WHERE entity_type = ? AND commit_id <= ? "
                    "   GROUP BY entity_key"
                    " ) re_latest ON re.entity_key = re_latest.entity_key "
                    " AND re.commit_id = re_latest.max_cid "
                    " WHERE re.entity_type = ? AND re.entity_key = rh.right_key"
                )
                params.extend([right_entity_type, as_of, right_entity_type])
            else:
                sql += (
                    " INNER JOIN ("
                    "   SELECT entity_key, MAX(commit_id) as max_cid "
                    "   FROM entity_history WHERE entity_type = ? GROUP BY entity_key"
                    " ) re_latest ON re.entity_key = re_latest.entity_key "
                    " AND re.commit_id = re_latest.max_cid "
                    " WHERE re.entity_type = ? AND re.entity_key = rh.right_key"
                )
                params.extend([right_entity_type, right_entity_type])
            right_filter = _extract_prefix_filter(filter_expr, "right")
            if right_filter:
                right_where = _compile_filter(right_filter, params, table_alias="re")
                sql += f" AND {right_where}"
            sql += ")"

        # Apply direct relation field filters
        direct_filter = _extract_direct_filter(filter_expr)
        if direct_filter:
            where_sql = _compile_filter(direct_filter, params, table_alias="rh")
            sql += f" AND {where_sql}"

        if order_by:
            field_name = order_by.removeprefix("$.")
            direction = "DESC" if order_desc else "ASC"
            sql += f" ORDER BY json_extract(rh.fields_json, '$.{field_name}') {direction}"
        elif with_history or history_since is not None:
            sql += " ORDER BY rh.commit_id ASC"

        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        if offset is not None:
            sql += " OFFSET ?"
            params.append(offset)

        rows = self._conn.execute(sql, params).fetchall()
        return [
            {
                "left_key": r[0],
                "right_key": r[1],
                "instance_key": r[2],
                "fields": json.loads(r[3]),
                "commit_id": r[4],
            }
            for r in rows
        ]

    def count_relations(
        self,
        type_name: str,
        *,
        left_entity_type: str | None = None,
        right_entity_type: str | None = None,
        filter_expr: FilterExpression | None = None,
    ) -> int:
        params: list[Any] = []
        sql = (
            "SELECT COUNT(*) FROM relation_history rh "
            "INNER JOIN ("
            "  SELECT left_key, right_key, instance_key, MAX(commit_id) as max_cid "
            "  FROM relation_history WHERE relation_type = ? "
            "  GROUP BY left_key, right_key, instance_key"
            ") latest ON rh.left_key = latest.left_key AND rh.right_key = latest.right_key "
            "AND rh.instance_key = latest.instance_key "
            "AND rh.commit_id = latest.max_cid "
            "WHERE rh.relation_type = ?"
        )
        params.extend([type_name, type_name])

        if filter_expr is not None:
            direct = _extract_direct_filter(filter_expr)
            if direct:
                where_sql = _compile_filter(direct, params, table_alias="rh")
                sql += f" AND {where_sql}"

        row = self._conn.execute(sql, params).fetchone()
        return row[0] if row else 0

    def aggregate_relations(
        self,
        type_name: str,
        agg_func: str,
        field_name: str,
        *,
        filter_expr: FilterExpression | None = None,
    ) -> Any:
        params: list[Any] = []
        json_path = f"json_extract(rh.fields_json, '$.{field_name}')"

        if agg_func.upper() == "AVG_LEN":
            agg_expr = f"AVG(json_array_length(json_extract(rh.fields_json, '$.{field_name}')))"
        else:
            agg_expr = f"{agg_func}({json_path})"

        sql = (
            f"SELECT {agg_expr} FROM relation_history rh "
            "INNER JOIN ("
            "  SELECT left_key, right_key, instance_key, MAX(commit_id) as max_cid "
            "  FROM relation_history WHERE relation_type = ? "
            "  GROUP BY left_key, right_key, instance_key"
            ") latest ON rh.left_key = latest.left_key AND rh.right_key = latest.right_key "
            "AND rh.instance_key = latest.instance_key "
            "AND rh.commit_id = latest.max_cid "
            "WHERE rh.relation_type = ?"
        )
        params.extend([type_name, type_name])
        if filter_expr is not None:
            direct = _extract_direct_filter(filter_expr)
            if direct:
                where_sql = _compile_filter(direct, params, table_alias="rh")
                sql += f" AND {where_sql}"
        row = self._conn.execute(sql, params).fetchone()
        return row[0] if row else None

    def group_by_relations(
        self,
        type_name: str,
        group_field: str,
        agg_specs: dict[str, tuple[str, str | None]],
        *,
        left_entity_type: str | None = None,
        right_entity_type: str | None = None,
        filter_expr: FilterExpression | None = None,
        having_sql_fragment: str | None = None,
        having_params: list[Any] | None = None,
    ) -> list[dict[str, Any]]:
        params: list[Any] = []

        # Determine group by column
        if group_field.startswith("left.$."):
            real_field = group_field[7:]
            # Need join to left entity
            group_col = f"json_extract(le.fields_json, '$.{real_field}')"
            # needs_left_join = True (implicit by logic flow in query builder if implemented)
        elif group_field.startswith("right.$."):
            real_field = group_field[8:]
            group_col = f"json_extract(re.fields_json, '$.{real_field}')"
        else:
            real_field = group_field
            group_col = f"json_extract(rh.fields_json, '$.{real_field}')"

        select_parts = [f"{group_col} as group_key"]
        for alias, (func, fname) in agg_specs.items():
            if func.upper() == "COUNT":
                select_parts.append(f"COUNT(*) as [{alias}]")
            else:
                fp = f"json_extract(rh.fields_json, '$.{fname}')"
                select_parts.append(f"{func}({fp}) as [{alias}]")

        select_clause = ", ".join(select_parts)

        sql = (
            f"SELECT {select_clause} FROM relation_history rh "
            "INNER JOIN ("
            "  SELECT left_key, right_key, instance_key, MAX(commit_id) as max_cid "
            "  FROM relation_history WHERE relation_type = ? "
            "  GROUP BY left_key, right_key, instance_key"
            ") latest ON rh.left_key = latest.left_key AND rh.right_key = latest.right_key "
            "AND rh.instance_key = latest.instance_key "
            "AND rh.commit_id = latest.max_cid "
            "WHERE rh.relation_type = ?"
        )
        params.extend([type_name, type_name])

        # Join left entities if needed for grouping
        if group_field.startswith("left.$.") and left_entity_type:
            sql = (
                f"SELECT {select_clause} FROM relation_history rh "
                "INNER JOIN ("
                "  SELECT left_key, right_key, instance_key, MAX(commit_id) as max_cid "
                "  FROM relation_history WHERE relation_type = ? "
                "  GROUP BY left_key, right_key, instance_key"
                ") latest ON rh.left_key = latest.left_key AND rh.right_key = latest.right_key "
                "AND rh.instance_key = latest.instance_key "
                "AND rh.commit_id = latest.max_cid "
                "INNER JOIN entity_history le ON le.entity_key = rh.left_key "
                "AND le.entity_type = ? "
                "INNER JOIN ("
                "  SELECT entity_key, MAX(commit_id) as max_cid "
                "  FROM entity_history WHERE entity_type = ? GROUP BY entity_key"
                ") le_latest ON le.entity_key = le_latest.entity_key "
                "AND le.commit_id = le_latest.max_cid "
                "WHERE rh.relation_type = ?"
            )
            params = [type_name, left_entity_type, left_entity_type, type_name]

        if filter_expr is not None:
            direct = _extract_direct_filter(filter_expr)
            if direct:
                where_sql = _compile_filter(direct, params, table_alias="rh")
                sql += f" AND {where_sql}"

        sql += f" GROUP BY {group_col}"

        if having_sql_fragment:
            sql += f" HAVING {having_sql_fragment}"
            if having_params:
                params.extend(having_params)

        rows = self._conn.execute(sql, params).fetchall()
        # Determine the group_by result key name
        if group_field.startswith("left.$."):
            result_key = group_field[7:]
        elif group_field.startswith("right.$."):
            result_key = group_field[8:]
        else:
            result_key = group_field

        result = []
        for row in rows:
            d: dict[str, Any] = {result_key: row[0]}
            for i, alias in enumerate(agg_specs.keys()):
                d[alias] = row[i + 1]
            result.append(d)
        return result

    # --- Traversal helpers ---

    def get_relations_for_entity(
        self,
        relation_type: str,
        left_entity_type: str,
        entity_key: str,
        *,
        direction: str = "left",
    ) -> list[dict[str, Any]]:
        """Get latest relations connected to an entity."""
        if direction == "left":
            key_col = "left_key"
        else:
            key_col = "right_key"

        params: list[Any] = [relation_type, relation_type, entity_key]
        sql = (
            "SELECT rh.left_key, rh.right_key, rh.instance_key, rh.fields_json, rh.commit_id "
            "FROM relation_history rh "
            "INNER JOIN ("
            "  SELECT left_key, right_key, instance_key, MAX(commit_id) as max_cid "
            "  FROM relation_history WHERE relation_type = ? "
            "  GROUP BY left_key, right_key, instance_key"
            ") latest ON rh.left_key = latest.left_key AND rh.right_key = latest.right_key "
            "AND rh.instance_key = latest.instance_key "
            "AND rh.commit_id = latest.max_cid "
            f"WHERE rh.relation_type = ? AND rh.{key_col} = ?"
        )
        rows = self._conn.execute(sql, params).fetchall()
        return [
            {
                "left_key": r[0],
                "right_key": r[1],
                "instance_key": r[2],
                "fields": json.loads(r[3]),
                "commit_id": r[4],
            }
            for r in rows
        ]

    # --- Schema registry ---

    def get_schema(self, type_kind: str, type_name: str) -> dict[str, Any] | None:
        row = self._conn.execute(
            "SELECT schema_json FROM schema_registry WHERE type_kind = ? AND type_name = ?",
            (type_kind, type_name),
        ).fetchone()
        return json.loads(row[0]) if row else None

    def store_schema(self, type_kind: str, type_name: str, schema: dict[str, Any]) -> None:
        self._conn.execute(
            "INSERT OR REPLACE INTO schema_registry (type_kind, type_name, schema_json) "
            "VALUES (?, ?, ?)",
            (type_kind, type_name, json.dumps(schema)),
        )
        self._conn.commit()

    def list_schemas(self, type_kind: str) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            "SELECT type_name, schema_json FROM schema_registry WHERE type_kind = ?",
            (type_kind,),
        ).fetchall()
        return [{"type_name": r[0], "schema": json.loads(r[1])} for r in rows]

    # --- Schema versions ---

    def create_schema_version(
        self,
        type_kind: str,
        type_name: str,
        schema_json: str,
        schema_hash: str,
        runtime_id: str | None = None,
        reason: str | None = None,
    ) -> int:
        """Create a new schema version. Returns the new version_id."""
        row = self._conn.execute(
            "SELECT MAX(schema_version_id) FROM schema_versions "
            "WHERE type_kind = ? AND type_name = ?",
            (type_kind, type_name),
        ).fetchone()
        next_id = (row[0] or 0) + 1
        now = datetime.now(timezone.utc).isoformat()
        self._conn.execute(
            "INSERT INTO schema_versions "
            "(type_kind, type_name, schema_version_id, schema_json, schema_hash, "
            "created_at, runtime_id, reason) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (type_kind, type_name, next_id, schema_json, schema_hash, now, runtime_id, reason),
        )
        self._conn.commit()
        return next_id

    def get_current_schema_version(self, type_kind: str, type_name: str) -> dict[str, Any] | None:
        """Get the latest schema version row for a type."""
        row = self._conn.execute(
            "SELECT schema_version_id, schema_json, schema_hash, created_at, runtime_id, reason "
            "FROM schema_versions WHERE type_kind = ? AND type_name = ? "
            "ORDER BY schema_version_id DESC LIMIT 1",
            (type_kind, type_name),
        ).fetchone()
        if row is None:
            return None
        return {
            "schema_version_id": row[0],
            "schema_json": row[1],
            "schema_hash": row[2],
            "created_at": row[3],
            "runtime_id": row[4],
            "reason": row[5],
        }

    def get_schema_version(
        self, type_kind: str, type_name: str, version_id: int
    ) -> dict[str, Any] | None:
        """Get a specific schema version row."""
        row = self._conn.execute(
            "SELECT schema_version_id, schema_json, schema_hash, created_at, runtime_id, reason "
            "FROM schema_versions WHERE type_kind = ? AND type_name = ? AND schema_version_id = ?",
            (type_kind, type_name, version_id),
        ).fetchone()
        if row is None:
            return None
        return {
            "schema_version_id": row[0],
            "schema_json": row[1],
            "schema_hash": row[2],
            "created_at": row[3],
            "runtime_id": row[4],
            "reason": row[5],
        }

    def list_schema_versions(self, type_kind: str, type_name: str) -> list[dict[str, Any]]:
        """List all schema versions for a type, ordered by version_id."""
        rows = self._conn.execute(
            "SELECT schema_version_id, schema_json, schema_hash, created_at, runtime_id, reason "
            "FROM schema_versions WHERE type_kind = ? AND type_name = ? "
            "ORDER BY schema_version_id ASC",
            (type_kind, type_name),
        ).fetchall()
        return [
            {
                "schema_version_id": r[0],
                "schema_json": r[1],
                "schema_hash": r[2],
                "created_at": r[3],
                "runtime_id": r[4],
                "reason": r[5],
            }
            for r in rows
        ]

    def count_latest_entities(self, type_name: str) -> int:
        """Count distinct latest entities of a given type."""
        row = self._conn.execute(
            "SELECT COUNT(DISTINCT entity_key) FROM entity_history WHERE entity_type = ?",
            (type_name,),
        ).fetchone()
        return row[0] if row else 0

    def count_latest_relations(self, type_name: str) -> int:
        """Count distinct latest relations of a given type."""
        row = self._conn.execute(
            "SELECT COUNT(*) FROM ("
            "  SELECT DISTINCT left_key, right_key, instance_key "
            "  FROM relation_history WHERE relation_type = ?"
            ")",
            (type_name,),
        ).fetchone()
        return row[0] if row else 0

    def iter_latest_entities(
        self, type_name: str, batch_size: int = 1000
    ) -> Iterator[list[tuple[str, dict[str, Any], int, int | None]]]:
        """Yield batches of (key, fields_dict, commit_id, schema_version_id) for latest entities."""
        offset = 0
        while True:
            rows = self._conn.execute(
                "SELECT eh.entity_key, eh.fields_json, eh.commit_id, eh.schema_version_id "
                "FROM entity_history eh "
                "INNER JOIN ("
                "  SELECT entity_key, MAX(commit_id) as max_cid "
                "  FROM entity_history WHERE entity_type = ? GROUP BY entity_key"
                ") latest ON eh.entity_key = latest.entity_key AND eh.commit_id = latest.max_cid "
                "WHERE eh.entity_type = ? "
                "ORDER BY eh.entity_key "
                "LIMIT ? OFFSET ?",
                (type_name, type_name, batch_size, offset),
            ).fetchall()
            if not rows:
                break
            batch = [(r[0], json.loads(r[1]), r[2], r[3]) for r in rows]
            yield batch
            if len(rows) < batch_size:
                break
            offset += batch_size

    def iter_latest_relations(
        self, type_name: str, batch_size: int = 1000
    ) -> Iterator[list[tuple[str, str, str, dict[str, Any], int, int | None]]]:
        """Yield batches of latest relation rows with schema version metadata."""
        offset = 0
        while True:
            rows = self._conn.execute(
                "SELECT rh.left_key, rh.right_key, rh.instance_key, rh.fields_json, "
                "rh.commit_id, rh.schema_version_id "
                "FROM relation_history rh "
                "INNER JOIN ("
                "  SELECT left_key, right_key, instance_key, MAX(commit_id) as max_cid "
                "  FROM relation_history WHERE relation_type = ? "
                "  GROUP BY left_key, right_key, instance_key"
                ") latest ON rh.left_key = latest.left_key AND rh.right_key = latest.right_key "
                "AND rh.instance_key = latest.instance_key "
                "AND rh.commit_id = latest.max_cid "
                "WHERE rh.relation_type = ? "
                "ORDER BY rh.left_key, rh.right_key, rh.instance_key "
                "LIMIT ? OFFSET ?",
                (type_name, type_name, batch_size, offset),
            ).fetchall()
            if not rows:
                break
            batch = [(r[0], r[1], r[2], json.loads(r[3]), r[4], r[5]) for r in rows]
            yield batch
            if len(rows) < batch_size:
                break
            offset += batch_size

    def list_commit_changes(self, commit_id: int) -> list[dict[str, Any]]:
        """List commit changes with inferred operation kind."""
        changes: list[dict[str, Any]] = []

        entity_rows = self._conn.execute(
            "SELECT entity_type, entity_key FROM entity_history WHERE commit_id = ?",
            (commit_id,),
        ).fetchall()
        for etype, ekey in entity_rows:
            prev = self._conn.execute(
                "SELECT 1 FROM entity_history "
                "WHERE entity_type = ? AND entity_key = ? AND commit_id < ? LIMIT 1",
                (etype, ekey, commit_id),
            ).fetchone()
            changes.append(
                {
                    "kind": "entity",
                    "type_name": etype,
                    "key": ekey,
                    "operation": "update_version" if prev else "insert",
                }
            )

        relation_rows = self._conn.execute(
            "SELECT relation_type, left_key, right_key, instance_key "
            "FROM relation_history WHERE commit_id = ?",
            (commit_id,),
        ).fetchall()
        for rtype, lkey, rkey, ikey in relation_rows:
            prev = self._conn.execute(
                "SELECT 1 FROM relation_history "
                "WHERE relation_type = ? AND left_key = ? AND right_key = ? "
                "AND instance_key = ? AND commit_id < ? LIMIT 1",
                (rtype, lkey, rkey, ikey, commit_id),
            ).fetchone()
            changes.append(
                {
                    "kind": "relation",
                    "type_name": rtype,
                    "left_key": lkey,
                    "right_key": rkey,
                    "instance_key": ikey,
                    "operation": "update_version" if prev else "insert",
                }
            )

        return changes

    def count_commit_operations(self, commit_id: int) -> int:
        """Count total operations in a commit."""
        e_count = self._conn.execute(
            "SELECT COUNT(*) FROM entity_history WHERE commit_id = ?",
            (commit_id,),
        ).fetchone()[0]
        r_count = self._conn.execute(
            "SELECT COUNT(*) FROM relation_history WHERE commit_id = ?",
            (commit_id,),
        ).fetchone()[0]
        return int(e_count) + int(r_count)

    def storage_info(self) -> dict[str, Any]:
        """Return backend info for operator commands."""
        return {
            "backend": "sqlite",
            "db_path": self.db_path,
            "engine_version": self.engine_version,
        }

    def get_last_query_diagnostics(self) -> dict[str, Any] | None:
        return self._last_query_diagnostics

    def apply_schema_drop(
        self,
        *,
        affected_types: list[tuple[str, str]],
        purge_history: bool,
        commit_meta: dict[str, str] | None = None,
    ) -> int:
        """Apply schema drop atomically and return the admin commit ID."""
        self.begin_transaction()
        try:
            commit_id = self.create_commit(commit_meta)
            for tk, tn in affected_types:
                self._conn.execute(
                    "DELETE FROM schema_registry WHERE type_kind = ? AND type_name = ?",
                    (tk, tn),
                )
                self._conn.execute(
                    "DELETE FROM schema_versions WHERE type_kind = ? AND type_name = ?",
                    (tk, tn),
                )
                if purge_history:
                    if tk == "entity":
                        self._conn.execute(
                            "DELETE FROM entity_history WHERE entity_type = ?",
                            (tn,),
                        )
                    else:
                        self._conn.execute(
                            "DELETE FROM relation_history WHERE relation_type = ?",
                            (tn,),
                        )
            self.commit_transaction()
            return commit_id
        except Exception:
            self.rollback_transaction()
            raise

    # --- Write lock ---

    def acquire_lock(self, owner_id: str, timeout_ms: int = 5000, lease_ms: int = 30000) -> bool:
        lock_name = "ontology_write"
        deadline = time.monotonic() + timeout_ms / 1000.0
        while True:
            now = datetime.now(timezone.utc)
            now_iso = now.isoformat()

            # Try to acquire or take over expired lock
            self._conn.execute(
                "DELETE FROM locks WHERE lock_name = ? AND expires_at < ?",
                (lock_name, now_iso),
            )

            try:
                expires = datetime.fromtimestamp(
                    now.timestamp() + lease_ms / 1000.0, tz=timezone.utc
                ).isoformat()
                self._conn.execute(
                    "INSERT INTO locks (lock_name, owner_id, acquired_at, expires_at) "
                    "VALUES (?, ?, ?, ?)",
                    (lock_name, owner_id, now_iso, expires),
                )
                self._conn.commit()
                return True
            except sqlite3.IntegrityError:
                self._conn.rollback()
                if time.monotonic() >= deadline:
                    return False
                time.sleep(0.01)

    def renew_lock(self, owner_id: str, lease_ms: int = 30000) -> bool:
        lock_name = "ontology_write"
        now = datetime.now(timezone.utc)
        expires = datetime.fromtimestamp(
            now.timestamp() + lease_ms / 1000.0, tz=timezone.utc
        ).isoformat()
        cursor = self._conn.execute(
            "UPDATE locks SET expires_at = ? WHERE lock_name = ? AND owner_id = ?",
            (expires, lock_name, owner_id),
        )
        self._conn.commit()
        return cursor.rowcount > 0

    def release_lock(self, owner_id: str) -> None:
        self._conn.execute(
            "DELETE FROM locks WHERE lock_name = ? AND owner_id = ?",
            ("ontology_write", owner_id),
        )
        self._conn.commit()

    # --- Transaction helpers ---

    def begin_transaction(self) -> None:
        self._conn.execute("BEGIN IMMEDIATE")

    def commit_transaction(self) -> None:
        self._conn.commit()

    def rollback_transaction(self) -> None:
        self._conn.rollback()


def _extract_prefix_filter(expr: FilterExpression | None, prefix: str) -> FilterExpression | None:
    """Extract only the parts of a filter that reference a specific prefix (left/right)."""
    if expr is None:
        return None
    if isinstance(expr, ComparisonExpression):
        if expr.field_path.startswith(f"{prefix}."):
            return expr
        return None
    if isinstance(expr, ExistsComparisonExpression):
        if expr.list_field_path.startswith(f"{prefix}."):
            return expr
        return None
    if isinstance(expr, LogicalExpression):
        if expr.op == "NOT":
            child = _extract_prefix_filter(expr.children[0], prefix)
            return LogicalExpression("NOT", [child]) if child else None
        filtered = [
            c for c in (_extract_prefix_filter(c, prefix) for c in expr.children) if c is not None
        ]
        if not filtered:
            return None
        if len(filtered) == 1:
            return filtered[0]
        return LogicalExpression(expr.op, filtered)
    return None


def _extract_direct_filter(
    expr: FilterExpression | None,
) -> FilterExpression | None:
    """Extract only the parts of a filter that reference direct fields ($.)."""
    if expr is None:
        return None
    if isinstance(expr, ComparisonExpression):
        if expr.field_path.startswith("$."):
            return expr
        return None
    if isinstance(expr, ExistsComparisonExpression):
        if expr.list_field_path.startswith("$."):
            return expr
        return None
    if isinstance(expr, LogicalExpression):
        if expr.op == "NOT":
            child = _extract_direct_filter(expr.children[0])
            return LogicalExpression("NOT", [child]) if child else None
        filtered = [c for c in (_extract_direct_filter(c) for c in expr.children) if c is not None]
        if not filtered:
            return None
        if len(filtered) == 1:
            return filtered[0]
        return LogicalExpression(expr.op, filtered)
    return None


SqliteRepositoryV1 = Repository
SqliteRepository = SqliteRepositoryV1


def _sqlite_detect_engine_version(db_path: str) -> str:
    if db_path == ":memory:" or not os.path.exists(db_path):
        # New sqlite stores default to latest engine.
        return "v2"
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='storage_meta'"
        ).fetchone()
        if row is None:
            return "v1"
        row = conn.execute(
            "SELECT value FROM storage_meta WHERE key = 'engine_version' LIMIT 1"
        ).fetchone()
        if row is None:
            return "v1"
        return str(row[0])
    finally:
        conn.close()


def open_repository(
    db_path: str | None = None,
    *,
    storage_uri: str | None = None,
    config: OntologiaConfig | None = None,
    engine_version: str | None = None,
) -> RepositoryProtocol:
    """Open a backend repository from legacy or URI-style storage binding."""
    target = parse_storage_target(db_path=db_path, storage_uri=storage_uri)
    if target.backend == "sqlite":
        assert target.db_path is not None
        resolved_engine = engine_version or _sqlite_detect_engine_version(target.db_path)
        if resolved_engine == "v1":
            return SqliteRepositoryV1(target.db_path)
        if resolved_engine == "v2":
            from ontologia.storage_sqlite_v2 import SqliteRepositoryV2

            return SqliteRepositoryV2(target.db_path)
        raise StorageBackendError(
            "open_repository",
            f"Unsupported sqlite engine version '{resolved_engine}'",
        )
    if target.backend == "s3":
        from ontologia.storage_s3 import S3RepositoryV1, S3RepositoryV2, detect_s3_engine_version

        assert target.bucket is not None
        cfg = config or OntologiaConfig()
        resolved_engine = engine_version or detect_s3_engine_version(
            bucket=target.bucket,
            prefix=target.prefix or "",
            storage_uri=target.uri,
            config=cfg,
        )
        if resolved_engine == "v1":
            return S3RepositoryV1(
                bucket=target.bucket,
                prefix=target.prefix or "",
                storage_uri=target.uri,
                config=cfg,
            )
        if resolved_engine == "v2":
            return S3RepositoryV2(
                bucket=target.bucket,
                prefix=target.prefix or "",
                storage_uri=target.uri,
                config=cfg,
            )
        raise StorageBackendError(
            "open_repository",
            f"Unsupported s3 engine version '{resolved_engine}'",
        )
    raise StorageBackendError("open_repository", f"Unsupported backend '{target.backend}'")


__all__ = [
    "Repository",
    "SqliteRepository",
    "SqliteRepositoryV1",
    "RepositoryProtocol",
    "StorageTarget",
    "parse_storage_target",
    "open_repository",
    "_schema_hash",
]
