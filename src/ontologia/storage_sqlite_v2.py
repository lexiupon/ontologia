"""SQLite v2 storage engine with current-schema-only typed reads."""

from __future__ import annotations

from typing import Any

from ontologia.errors import StorageBackendError
from ontologia.filters import FilterExpression
from ontologia.storage import Repository


class SqliteRepositoryV2(Repository):
    """SQLite v2 repository.

    The implementation keeps shared history tables for compatibility, while enforcing
    v2 semantics through type layout activation metadata.
    """

    def __init__(self, db_path: str) -> None:
        super().__init__(db_path)
        self.engine_version = "v2"

    def _create_tables(self) -> None:
        super()._create_tables()
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS storage_meta (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS type_layout_catalog (
                type_kind             TEXT NOT NULL,
                type_name             TEXT NOT NULL,
                schema_version_id     INTEGER NOT NULL,
                table_name            TEXT NOT NULL,
                activation_commit_id  INTEGER NOT NULL,
                is_current            INTEGER NOT NULL DEFAULT 0,
                created_at            TEXT NOT NULL DEFAULT (datetime('now')),
                PRIMARY KEY (type_kind, type_name, schema_version_id)
            );
            """
        )
        self._conn.execute(
            "INSERT OR REPLACE INTO storage_meta (key, value) VALUES ('engine_version', 'v2')"
        )
        self._conn.execute(
            "INSERT OR REPLACE INTO storage_meta (key, value) VALUES ('backend', 'sqlite')"
        )
        self._conn.commit()

    def _layout_table_name(self, type_kind: str, type_name: str, schema_version_id: int) -> str:
        prefix = "entity" if type_kind == "entity" else "relation"
        return f"{prefix}_{type_name}_v{schema_version_id}"

    def _get_current_layout(self, type_kind: str, type_name: str) -> dict[str, Any] | None:
        row = self._conn.execute(
            "SELECT schema_version_id, activation_commit_id, table_name "
            "FROM type_layout_catalog "
            "WHERE type_kind = ? AND type_name = ? AND is_current = 1 "
            "LIMIT 1",
            (type_kind, type_name),
        ).fetchone()
        if row is None:
            return None
        return {
            "schema_version_id": int(row[0]),
            "activation_commit_id": int(row[1]),
            "table_name": str(row[2]),
        }

    def activate_schema_version(
        self,
        *,
        type_kind: str,
        type_name: str,
        schema_version_id: int,
        activation_commit_id: int,
    ) -> None:
        self._conn.execute(
            "UPDATE type_layout_catalog SET is_current = 0 WHERE type_kind = ? AND type_name = ?",
            (type_kind, type_name),
        )
        table_name = self._layout_table_name(type_kind, type_name, schema_version_id)
        self._conn.execute(
            "INSERT INTO type_layout_catalog "
            "(type_kind, type_name, schema_version_id, table_name, activation_commit_id, is_current) "
            "VALUES (?, ?, ?, ?, ?, 1) "
            "ON CONFLICT(type_kind, type_name, schema_version_id) DO UPDATE SET "
            "table_name = excluded.table_name, "
            "activation_commit_id = excluded.activation_commit_id, "
            "is_current = 1",
            (type_kind, type_name, schema_version_id, table_name, activation_commit_id),
        )

    def _resolve_active_version(
        self,
        *,
        type_kind: str,
        type_name: str,
    ) -> tuple[int, int] | None:
        layout = self._get_current_layout(type_kind, type_name)
        if layout is None:
            return None
        return int(layout["schema_version_id"]), int(layout["activation_commit_id"])

    def _set_boundary_diag(self, activation_commit_id: int) -> None:
        self._last_query_diagnostics = {
            "reason": "commit_before_activation",
            "activation_commit_id": int(activation_commit_id),
        }

    def insert_entity(
        self,
        type_name: str,
        key: str,
        fields: dict[str, Any],
        commit_id: int,
        schema_version_id: int | None = None,
    ) -> None:
        current = self.get_current_schema_version("entity", type_name)
        if current is None:
            # Compatibility fallback for low-level repo usage that bypasses schema registration.
            super().insert_entity(
                type_name, key, fields, commit_id, schema_version_id=schema_version_id
            )
            return

        expected = int(current["schema_version_id"])
        if schema_version_id is None:
            schema_version_id = expected
        if int(schema_version_id) != expected:
            raise StorageBackendError(
                "insert_entity",
                f"schema_version_id mismatch for entity '{type_name}': expected {expected}, got {schema_version_id}",
            )

        layout = self._get_current_layout("entity", type_name)
        if layout is None or int(layout["schema_version_id"]) != expected:
            self.activate_schema_version(
                type_kind="entity",
                type_name=type_name,
                schema_version_id=expected,
                activation_commit_id=commit_id,
            )

        super().insert_entity(
            type_name,
            key,
            fields,
            commit_id,
            schema_version_id=schema_version_id,
        )

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
        current = self.get_current_schema_version("relation", type_name)
        if current is None:
            # Compatibility fallback for low-level repo usage that bypasses schema registration.
            super().insert_relation(
                type_name,
                left_key,
                right_key,
                fields,
                commit_id,
                schema_version_id=schema_version_id,
                instance_key=instance_key,
            )
            return

        expected = int(current["schema_version_id"])
        if schema_version_id is None:
            schema_version_id = expected
        if int(schema_version_id) != expected:
            raise StorageBackendError(
                "insert_relation",
                f"schema_version_id mismatch for relation '{type_name}': expected {expected}, got {schema_version_id}",
            )

        layout = self._get_current_layout("relation", type_name)
        if layout is None or int(layout["schema_version_id"]) != expected:
            self.activate_schema_version(
                type_kind="relation",
                type_name=type_name,
                schema_version_id=expected,
                activation_commit_id=commit_id,
            )

        super().insert_relation(
            type_name,
            left_key,
            right_key,
            fields,
            commit_id,
            schema_version_id=schema_version_id,
            instance_key=instance_key,
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
        self._last_query_diagnostics = None
        active = self._resolve_active_version(type_kind="entity", type_name=type_name)
        if active is None:
            return super().query_entities(
                type_name,
                filter_expr=filter_expr,
                order_by=order_by,
                order_desc=order_desc,
                limit=limit,
                offset=offset,
                with_history=with_history,
                history_since=history_since,
                as_of=as_of,
                schema_version_id=schema_version_id,
            )
        current_schema_version_id, activation_commit_id = active

        if as_of is not None:
            if as_of < activation_commit_id:
                self._set_boundary_diag(activation_commit_id)
                return []
            return super().query_entities(
                type_name,
                filter_expr=filter_expr,
                order_by=order_by,
                order_desc=order_desc,
                limit=limit,
                offset=offset,
                as_of=as_of,
                schema_version_id=current_schema_version_id,
            )

        if with_history or history_since is not None:
            effective_since = max(history_since or 0, activation_commit_id - 1)
            return super().query_entities(
                type_name,
                filter_expr=filter_expr,
                order_by=order_by,
                order_desc=order_desc,
                limit=limit,
                offset=offset,
                history_since=effective_since,
                schema_version_id=current_schema_version_id,
            )

        head = self.get_head_commit_id()
        if head is None or head < activation_commit_id:
            return []
        return super().query_entities(
            type_name,
            filter_expr=filter_expr,
            order_by=order_by,
            order_desc=order_desc,
            limit=limit,
            offset=offset,
            as_of=head,
            schema_version_id=current_schema_version_id,
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
        active = self._resolve_active_version(type_kind="relation", type_name=type_name)
        if active is None:
            return super().query_relations(
                type_name,
                left_entity_type=left_entity_type,
                right_entity_type=right_entity_type,
                filter_expr=filter_expr,
                order_by=order_by,
                order_desc=order_desc,
                limit=limit,
                offset=offset,
                with_history=with_history,
                history_since=history_since,
                as_of=as_of,
                schema_version_id=schema_version_id,
            )
        current_schema_version_id, activation_commit_id = active

        if as_of is not None:
            if as_of < activation_commit_id:
                self._set_boundary_diag(activation_commit_id)
                return []
            return super().query_relations(
                type_name,
                left_entity_type=left_entity_type,
                right_entity_type=right_entity_type,
                filter_expr=filter_expr,
                order_by=order_by,
                order_desc=order_desc,
                limit=limit,
                offset=offset,
                as_of=as_of,
                schema_version_id=current_schema_version_id,
            )

        if with_history or history_since is not None:
            effective_since = max(history_since or 0, activation_commit_id - 1)
            return super().query_relations(
                type_name,
                left_entity_type=left_entity_type,
                right_entity_type=right_entity_type,
                filter_expr=filter_expr,
                order_by=order_by,
                order_desc=order_desc,
                limit=limit,
                offset=offset,
                history_since=effective_since,
                schema_version_id=current_schema_version_id,
            )

        head = self.get_head_commit_id()
        if head is None or head < activation_commit_id:
            return []
        return super().query_relations(
            type_name,
            left_entity_type=left_entity_type,
            right_entity_type=right_entity_type,
            filter_expr=filter_expr,
            order_by=order_by,
            order_desc=order_desc,
            limit=limit,
            offset=offset,
            as_of=head,
            schema_version_id=current_schema_version_id,
        )

    def storage_info(self) -> dict[str, Any]:
        out = super().storage_info()
        out["engine_version"] = "v2"

        rows = self._conn.execute(
            "SELECT type_kind, type_name, schema_version_id, activation_commit_id, is_current "
            "FROM type_layout_catalog"
        ).fetchall()

        grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for r in rows:
            key = (str(r[0]), str(r[1]))
            grouped.setdefault(key, []).append(
                {
                    "schema_version_id": int(r[2]),
                    "activation_commit_id": int(r[3]),
                    "is_current": bool(int(r[4])),
                }
            )

        type_layouts: dict[str, Any] = {}
        for (type_kind, type_name), entries in grouped.items():
            current = next((e for e in entries if e["is_current"]), None)
            if current is None:
                continue
            historical = sorted(
                e["schema_version_id"]
                for e in entries
                if e["schema_version_id"] != current["schema_version_id"]
            )
            layout_key = type_name
            if layout_key in type_layouts:
                layout_key = f"{type_kind}:{type_name}"
            type_layouts[layout_key] = {
                "type_kind": type_kind,
                "current_schema_version_id": current["schema_version_id"],
                "activation_commit_id": current["activation_commit_id"],
                "historical_versions": historical,
            }

        out["type_layouts"] = type_layouts
        return out

    def apply_schema_drop(
        self,
        *,
        affected_types: list[tuple[str, str]],
        purge_history: bool,
        commit_meta: dict[str, str] | None = None,
    ) -> int:
        commit_id = super().apply_schema_drop(
            affected_types=affected_types,
            purge_history=purge_history,
            commit_meta=commit_meta,
        )
        for kind, name in affected_types:
            self._conn.execute(
                "UPDATE type_layout_catalog SET is_current = 0 WHERE type_kind = ? AND type_name = ?",
                (kind, name),
            )
        self._conn.commit()
        return commit_id
