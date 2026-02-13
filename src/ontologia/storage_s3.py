"""S3 storage backend with Parquet commit files and DuckDB query execution."""

from __future__ import annotations

import hashlib
import json
import random
import tempfile
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator
from urllib.parse import urlparse

import boto3
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError, ParamValidationError

from ontologia.config import OntologiaConfig
from ontologia.errors import (
    HeadMismatchError,
    LeaseExpiredError,
    StorageBackendError,
    UninitializedStorageError,
)
from ontologia.filters import (
    ComparisonExpression,
    ExistsComparisonExpression,
    FilterExpression,
    LogicalExpression,
    resolve_nested_path,
)
from ontologia.storage import (
    _compile_filter,
    _extract_direct_filter,
    _extract_prefix_filter,
    _needs_endpoint_join,
)


@dataclass
class _StagedEntityRow:
    type_name: str
    key: str
    fields: dict[str, Any]
    schema_version_id: int | None


@dataclass
class _StagedRelationRow:
    type_name: str
    left_key: str
    right_key: str
    instance_key: str
    fields: dict[str, Any]
    schema_version_id: int | None


@dataclass
class _StagedCommit:
    commit_id: int
    metadata: dict[str, Any] | None
    entities: dict[str, list[_StagedEntityRow]] = field(default_factory=dict)
    relations: dict[str, list[_StagedRelationRow]] = field(default_factory=dict)


@dataclass
class _IndexDoc:
    type_name: str
    max_indexed_commit: int
    entries: list[dict[str, Any]]


class _PreconditionFailed(Exception):
    pass


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _numeric_values(values: list[Any]) -> list[float]:
    out: list[float] = []
    for value in values:
        parsed = _safe_float(value)
        if parsed is not None:
            out.append(parsed)
    return out


def _entry_covers(entry: dict[str, Any], commit_id: int) -> bool:
    return int(entry["min_commit_id"]) <= commit_id <= int(entry["max_commit_id"])


def _entry_intersects(entry: dict[str, Any], lower_exclusive: int, upper_inclusive: int) -> bool:
    return (
        int(entry["max_commit_id"]) > lower_exclusive
        and int(entry["min_commit_id"]) <= upper_inclusive
    )


def detect_s3_engine_version(
    *,
    bucket: str,
    prefix: str,
    storage_uri: str,
    config: OntologiaConfig,
) -> str:
    """Detect engine version from meta/engine.json, with v1 fallback."""
    clean_prefix = prefix.strip("/")
    key = f"{clean_prefix}/meta/engine.json" if clean_prefix else "meta/engine.json"
    session = boto3.Session(region_name=config.s3_region)
    client = session.client(
        "s3",
        region_name=config.s3_region,
        endpoint_url=config.s3_endpoint_url,
        config=BotoConfig(
            connect_timeout=config.s3_request_timeout_s,
            read_timeout=config.s3_request_timeout_s,
            retries={"max_attempts": 5, "mode": "standard"},
        ),
    )
    try:
        resp = client.get_object(Bucket=bucket, Key=key)
        body = resp["Body"].read()
        obj = json.loads(body.decode("utf-8"))
        version = str(obj.get("engine_version", "v1"))
        return version or "v1"
    except Exception as e:
        if isinstance(e, ClientError):
            code = e.response.get("Error", {}).get("Code", "")
            if code in {"NoSuchKey", "404", "NotFound"}:
                return "v1"
        raise StorageBackendError(
            "open_repository",
            f"Failed to detect S3 engine metadata for '{storage_uri}': {e}",
        ) from e


class S3Repository:
    """S3-backed append-only repository."""

    def __init__(
        self,
        *,
        bucket: str,
        prefix: str,
        storage_uri: str,
        config: OntologiaConfig,
        allow_uninitialized: bool = False,
        engine_version: str = "v1",
    ) -> None:
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.storage_uri = storage_uri
        self._config = config
        self.engine_version = engine_version

        self._session = boto3.Session(
            region_name=config.s3_region,
        )
        self._s3 = self._session.client(
            "s3",
            region_name=config.s3_region,
            endpoint_url=config.s3_endpoint_url,
            config=BotoConfig(
                connect_timeout=config.s3_request_timeout_s,
                read_timeout=config.s3_request_timeout_s,
                retries={"max_attempts": 5, "mode": "standard"},
            ),
        )

        self._duck = None
        self._duck_httpfs_configured = False
        self._tmpdir = tempfile.TemporaryDirectory(prefix="ontologia-s3-")
        self._download_cache: dict[str, str] = {}

        self._tx_active = False
        self._implicit_tx = False
        self._next_commit_id: int | None = None
        self._staged_commits: dict[int, _StagedCommit] = {}
        self._staged_order: list[int] = []
        self._staged_schema_registry: dict[tuple[str, str], dict[str, Any]] = {}
        self._staged_schema_deletes: set[tuple[str, str]] = set()
        self._staged_schema_versions: dict[tuple[str, str], list[dict[str, Any]]] = {}
        self._staged_dropped_updates: dict[tuple[str, str], dict[str, Any] | None] = {}
        self._pending_layout_activations: dict[tuple[str, str], tuple[int, int]] = {}

        self._lock_owner_id: str | None = None
        self._lock_etag: str | None = None
        self._lease_expires_at: datetime | None = None
        self._lease_ttl_ms: int = config.s3_lease_ttl_ms
        self._lease_unsafe = False

        self._last_index_warning: str | None = None
        self._last_query_diagnostics: dict[str, Any] | None = None

        if not allow_uninitialized:
            head = self._read_head(required=False)
            if head is None:
                raise UninitializedStorageError(storage_uri)

    # --- Key/object helpers ---

    def _k(self, rel_path: str) -> str:
        return f"{self.prefix}/{rel_path}" if self.prefix else rel_path

    def _head_key(self) -> str:
        return self._k("meta/head.json")

    def _lock_key(self) -> str:
        return self._k("meta/locks/ontology_write.json")

    def _types_key(self) -> str:
        return self._k("meta/schema/types.json")

    def _registry_key(self) -> str:
        return self._k("meta/schema/registry.json")

    def _index_key(self, kind: str, type_name: str) -> str:
        kind_dir = "entities" if kind == "entity" else "relations"
        return self._k(f"meta/indices/{kind_dir}/{type_name}.json")

    def _schema_versions_key(self, kind: str, type_name: str) -> str:
        return self._k(f"meta/schema/versions/{kind}/{type_name}.json")

    def _dropped_key(self) -> str:
        return self._k("meta/schema/dropped.json")

    def _engine_key(self) -> str:
        return self._k("meta/engine.json")

    def _type_layout_catalog_key(self) -> str:
        return self._k("meta/type_layout_catalog.json")

    def _is_not_found(self, err: Exception) -> bool:
        if isinstance(err, ClientError):
            code = err.response.get("Error", {}).get("Code", "")
            return code in {"NoSuchKey", "404", "NotFound"}
        return False

    def _is_precondition_failed(self, err: Exception) -> bool:
        if isinstance(err, ClientError):
            code = err.response.get("Error", {}).get("Code", "")
            return code in {"PreconditionFailed", "412"}
        return False

    def _head_etag(self, key: str) -> str | None:
        try:
            resp = self._s3.head_object(Bucket=self.bucket, Key=key)
            etag = resp.get("ETag")
            return etag if isinstance(etag, str) else None
        except Exception as e:
            if self._is_not_found(e):
                return None
            raise

    def _put_bytes(
        self,
        *,
        key: str,
        body: bytes,
        if_none_match: str | None = None,
        if_match: str | None = None,
        content_type: str | None = None,
    ) -> str:
        kwargs: dict[str, Any] = {
            "Bucket": self.bucket,
            "Key": key,
            "Body": body,
        }
        if content_type is not None:
            kwargs["ContentType"] = content_type
        if if_none_match is not None:
            kwargs["IfNoneMatch"] = if_none_match
        if if_match is not None:
            kwargs["IfMatch"] = if_match

        try:
            resp = self._s3.put_object(**kwargs)
            etag = resp.get("ETag")
            return etag if isinstance(etag, str) else ""
        except ParamValidationError as e:
            if if_none_match is not None or if_match is not None:
                raise StorageBackendError(
                    "conditional_write",
                    "S3 endpoint does not support conditional write preconditions",
                ) from e
            resp = self._s3.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=body,
                **({"ContentType": content_type} if content_type else {}),
            )
            etag = resp.get("ETag")
            return etag if isinstance(etag, str) else ""
        except Exception as e:
            if self._is_precondition_failed(e):
                raise _PreconditionFailed() from e
            raise

    def _get_bytes(self, key: str, *, required: bool = True) -> tuple[bytes | None, str | None]:
        try:
            resp = self._s3.get_object(Bucket=self.bucket, Key=key)
            body = resp["Body"].read()
            etag = resp.get("ETag")
            return body, etag if isinstance(etag, str) else None
        except Exception as e:
            if self._is_not_found(e):
                if required:
                    raise
                return None, None
            raise

    def _put_json(
        self,
        *,
        key: str,
        obj: dict[str, Any],
        if_none_match: str | None = None,
        if_match: str | None = None,
    ) -> str:
        body = json.dumps(obj, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return self._put_bytes(
            key=key,
            body=body,
            if_none_match=if_none_match,
            if_match=if_match,
            content_type="application/json",
        )

    def _get_json(
        self, key: str, *, required: bool = True
    ) -> tuple[dict[str, Any] | None, str | None]:
        body, etag = self._get_bytes(key, required=required)
        if body is None:
            return None, etag
        return json.loads(body.decode("utf-8")), etag

    def _put_json_with_lock_cas(self, *, key: str, obj: dict[str, Any]) -> str:
        """Write JSON with optimistic CAS when a write lock is currently held."""
        if self._lock_owner_id is None:
            return self._put_json(key=key, obj=obj)

        self._ensure_lease_safe()
        _current, etag = self._get_json(key, required=False)
        try:
            if etag is None:
                return self._put_json(key=key, obj=obj, if_none_match="*")
            return self._put_json(key=key, obj=obj, if_match=etag)
        except _PreconditionFailed as e:
            raise LeaseExpiredError() from e

    def _download(self, rel_path: str) -> str:
        cached = self._download_cache.get(rel_path)
        if cached is not None and Path(cached).exists():
            return cached

        digest = hashlib.sha256(rel_path.encode("utf-8")).hexdigest()
        out_path = str(Path(self._tmpdir.name) / f"{digest}.parquet")

        body, _ = self._get_bytes(self._k(rel_path), required=True)
        assert body is not None
        Path(out_path).write_bytes(body)
        self._download_cache[rel_path] = out_path
        return out_path

    # --- Bootstrap / metadata ---

    def _default_head(self) -> dict[str, Any]:
        return {
            "commit_id": 0,
            "manifest_path": None,
            "updated_at": _now_iso(),
            "runtime_id": "onto-init",
        }

    def initialize_storage(
        self,
        *,
        force: bool = False,
        token: str | None = None,
        dry_run: bool = True,
        engine_version: str | None = None,
    ) -> dict[str, Any]:
        selected_engine = engine_version or self.engine_version
        head = self._read_head(required=False)
        already_initialized = head is not None

        confirm_token = hashlib.sha256(
            f"{self.storage_uri}:force-init".encode("utf-8")
        ).hexdigest()[:16]

        plan = {
            "storage_uri": self.storage_uri,
            "already_initialized": already_initialized,
            "planned_objects": [
                self._head_key(),
                self._registry_key(),
                self._types_key(),
                self._engine_key(),
            ],
            "force_token": confirm_token,
            "engine_version": selected_engine,
        }

        if dry_run:
            return plan

        if already_initialized and not force:
            raise StorageBackendError(
                "init",
                "Storage already initialized. Use --force with confirmation token.",
            )

        if already_initialized and force and token != confirm_token:
            raise StorageBackendError(
                "init",
                "Invalid confirmation token for forced initialization",
            )

        self._put_json(key=self._head_key(), obj=self._default_head())
        self._put_json(
            key=self._registry_key(),
            obj={"entity": {}, "relation": {}, "updated_at": _now_iso()},
        )
        self._put_json(
            key=self._types_key(),
            obj={"entities": [], "relations": [], "updated_at": _now_iso()},
        )
        self._put_json(
            key=self._engine_key(),
            obj={
                "backend": "s3",
                "engine_version": selected_engine,
                "created_at": _now_iso(),
            },
        )
        return {**plan, "status": "initialized"}

    def _read_head(self, *, required: bool = True) -> dict[str, Any] | None:
        obj, _ = self._get_json(self._head_key(), required=required)
        return obj

    def _require_head(self) -> dict[str, Any]:
        head = self._read_head(required=True)
        if head is None:
            raise UninitializedStorageError(self.storage_uri)
        return head

    def _read_registry(self) -> dict[str, Any]:
        obj, _ = self._get_json(self._registry_key(), required=False)
        if obj is None:
            return {"entity": {}, "relation": {}, "updated_at": _now_iso()}
        if "entity" not in obj:
            obj["entity"] = {}
        if "relation" not in obj:
            obj["relation"] = {}
        return obj

    def _write_registry(self, data: dict[str, Any]) -> None:
        data["updated_at"] = _now_iso()
        self._put_json_with_lock_cas(key=self._registry_key(), obj=data)

    def _read_types_catalog(self, *, required: bool) -> dict[str, Any] | None:
        obj, _ = self._get_json(self._types_key(), required=False)
        if obj is None:
            if required:
                raise StorageBackendError("schema_types", "Missing meta/schema/types.json")
            return None
        if not isinstance(obj.get("entities"), list) or not isinstance(obj.get("relations"), list):
            if required:
                raise StorageBackendError("schema_types", "Malformed meta/schema/types.json")
            return None
        return obj

    def _write_types_catalog(self, data: dict[str, Any]) -> None:
        data["updated_at"] = _now_iso()
        self._put_json_with_lock_cas(key=self._types_key(), obj=data)

    def _ensure_type_catalog(self, kind: str, type_name: str) -> None:
        catalog = self._read_types_catalog(required=False)
        if catalog is None:
            catalog = {"entities": [], "relations": [], "updated_at": _now_iso()}
        key = "entities" if kind == "entity" else "relations"
        names = [str(v) for v in catalog.get(key, [])]
        if type_name not in names:
            names.append(type_name)
            catalog[key] = sorted(set(names))
            self._write_types_catalog(catalog)
        self._clear_dropped_record(kind, type_name)

    def _remove_type_from_catalog(self, kind: str, type_name: str) -> None:
        catalog = self._read_types_catalog(required=False)
        if catalog is None:
            return
        key = "entities" if kind == "entity" else "relations"
        values = [str(v) for v in catalog.get(key, []) if str(v) != type_name]
        catalog[key] = values
        self._write_types_catalog(catalog)

    def _load_schema_versions(self, kind: str, type_name: str) -> list[dict[str, Any]]:
        obj, _ = self._get_json(self._schema_versions_key(kind, type_name), required=False)
        if obj is None:
            return []
        versions = obj.get("versions")
        if not isinstance(versions, list):
            return []
        return [dict(v) for v in versions if isinstance(v, dict)]

    def _write_schema_versions(
        self, kind: str, type_name: str, versions: list[dict[str, Any]]
    ) -> None:
        self._put_json_with_lock_cas(
            key=self._schema_versions_key(kind, type_name),
            obj={"type_kind": kind, "type_name": type_name, "versions": versions},
        )

    def _read_dropped_map(self) -> dict[str, dict[str, dict[str, Any]]]:
        obj, _ = self._get_json(self._dropped_key(), required=False)
        if obj is None:
            return {"entity": {}, "relation": {}}
        entity = obj.get("entity")
        relation = obj.get("relation")
        if not isinstance(entity, dict):
            entity = {}
        if not isinstance(relation, dict):
            relation = {}
        return {"entity": entity, "relation": relation}

    def _write_dropped_map(self, dropped: dict[str, dict[str, dict[str, Any]]]) -> None:
        payload = {
            "entity": dropped.get("entity", {}),
            "relation": dropped.get("relation", {}),
            "updated_at": _now_iso(),
        }
        self._put_json_with_lock_cas(key=self._dropped_key(), obj=payload)

    def _get_dropped_record(self, kind: str, type_name: str) -> dict[str, Any] | None:
        staged = self._staged_dropped_updates.get((kind, type_name))
        if staged is not None or (kind, type_name) in self._staged_dropped_updates:
            return staged
        dropped = self._read_dropped_map()
        return dropped.get(kind, {}).get(type_name)

    def _is_type_dropped(self, kind: str, type_name: str) -> bool:
        return self._get_dropped_record(kind, type_name) is not None

    def _is_type_purged(self, kind: str, type_name: str) -> bool:
        rec = self._get_dropped_record(kind, type_name)
        return bool(rec and rec.get("purged"))

    def _set_dropped_record(
        self,
        kind: str,
        type_name: str,
        *,
        commit_id: int,
        purged: bool,
    ) -> None:
        rec = {"commit_id": commit_id, "purged": purged, "updated_at": _now_iso()}
        if self._tx_active:
            self._staged_dropped_updates[(kind, type_name)] = rec
            return
        dropped = self._read_dropped_map()
        dropped.setdefault(kind, {})[type_name] = rec
        self._write_dropped_map(dropped)

    def _clear_dropped_record(self, kind: str, type_name: str) -> None:
        if self._tx_active:
            self._staged_dropped_updates[(kind, type_name)] = None
            return
        dropped = self._read_dropped_map()
        if type_name in dropped.get(kind, {}):
            del dropped[kind][type_name]
            self._write_dropped_map(dropped)

    def _read_type_layout_catalog(self) -> dict[str, Any]:
        obj, _ = self._get_json(self._type_layout_catalog_key(), required=False)
        if obj is None:
            return {"layouts": []}
        layouts = obj.get("layouts")
        if not isinstance(layouts, list):
            return {"layouts": []}
        return {"layouts": [dict(v) for v in layouts if isinstance(v, dict)]}

    def _write_type_layout_catalog(self, catalog: dict[str, Any]) -> None:
        payload = {"layouts": catalog.get("layouts", [])}
        self._put_json_with_lock_cas(key=self._type_layout_catalog_key(), obj=payload)

    def _infer_activation_commit_id(self, kind: str, type_name: str, schema_version_id: int) -> int:
        head = self._read_head(required=True)
        assert head is not None
        manifest_path = head.get("manifest_path")
        if not isinstance(manifest_path, str):
            return int(head.get("commit_id", 0)) + 1
        min_commit: int | None = None
        for manifest in self._walk_manifest_chain(start_path=manifest_path):
            cid = int(manifest.get("commit_id", 0))
            for f in manifest.get("files", []):
                if (
                    str(f.get("kind")) == kind
                    and str(f.get("type_name")) == type_name
                    and int(f.get("schema_version_id") or 0) == schema_version_id
                ):
                    min_commit = cid if min_commit is None else min(min_commit, cid)
        if min_commit is not None:
            return min_commit
        return int(head.get("commit_id", 0)) + 1

    def _get_current_layout(self, kind: str, type_name: str) -> dict[str, Any] | None:
        catalog = self._read_type_layout_catalog()
        for entry in catalog.get("layouts", []):
            if (
                str(entry.get("type_kind")) == kind
                and str(entry.get("type_name")) == type_name
                and bool(entry.get("is_current"))
            ):
                return {
                    "type_kind": kind,
                    "type_name": type_name,
                    "schema_version_id": int(entry.get("schema_version_id", 0)),
                    "activation_commit_id": int(entry.get("activation_commit_id", 0)),
                    "is_current": True,
                }

        # Fallback from schema versions + manifest chain for crash/lag recovery.
        current = self.get_current_schema_version(kind, type_name)
        if current is None:
            return None
        schema_version_id = int(current["schema_version_id"])
        activation_commit_id = self._infer_activation_commit_id(kind, type_name, schema_version_id)
        return {
            "type_kind": kind,
            "type_name": type_name,
            "schema_version_id": schema_version_id,
            "activation_commit_id": activation_commit_id,
            "is_current": True,
        }

    def activate_schema_version(
        self,
        *,
        type_kind: str,
        type_name: str,
        schema_version_id: int,
        activation_commit_id: int,
    ) -> None:
        if self._tx_active:
            # Defer to post-commit metadata update for S3's best-effort semantics.
            self._pending_layout_activations[(type_kind, type_name)] = (
                int(schema_version_id),
                int(activation_commit_id),
            )
            return

        self._apply_layout_activation(
            type_kind=type_kind,
            type_name=type_name,
            schema_version_id=schema_version_id,
            activation_commit_id=activation_commit_id,
        )

    def _apply_layout_activation(
        self,
        *,
        type_kind: str,
        type_name: str,
        schema_version_id: int,
        activation_commit_id: int,
    ) -> None:

        catalog = self._read_type_layout_catalog()
        layouts = [dict(v) for v in catalog.get("layouts", [])]
        next_layouts: list[dict[str, Any]] = []
        found_target = False
        for row in layouts:
            if str(row.get("type_kind")) == type_kind and str(row.get("type_name")) == type_name:
                row["is_current"] = False
                if int(row.get("schema_version_id", 0)) == int(schema_version_id):
                    row["activation_commit_id"] = int(activation_commit_id)
                    row["is_current"] = True
                    found_target = True
            next_layouts.append(row)
        if not found_target:
            next_layouts.append(
                {
                    "type_kind": type_kind,
                    "type_name": type_name,
                    "schema_version_id": int(schema_version_id),
                    "activation_commit_id": int(activation_commit_id),
                    "is_current": True,
                }
            )
        catalog["layouts"] = next_layouts
        self._write_type_layout_catalog(catalog)

    # --- Query-file resolution helpers ---

    def _read_manifest(self, rel_path: str) -> dict[str, Any]:
        obj, _ = self._get_json(self._k(rel_path), required=True)
        assert obj is not None
        return obj

    def _walk_manifest_chain(self, *, start_path: str | None = None) -> Iterator[dict[str, Any]]:
        if start_path is None:
            head = self._read_head(required=True)
            assert head is not None
            start_path = head.get("manifest_path")

        path = start_path
        while path:
            manifest = self._read_manifest(path)
            yield manifest
            path = manifest.get("parent_manifest_path")

    def _read_index(self, kind: str, type_name: str) -> _IndexDoc:
        obj, _ = self._get_json(self._index_key(kind, type_name), required=False)
        if obj is None:
            return _IndexDoc(type_name=type_name, max_indexed_commit=0, entries=[])
        raw_entries = obj.get("entries")
        entries = (
            [e for e in raw_entries if isinstance(e, dict)] if isinstance(raw_entries, list) else []
        )
        return _IndexDoc(
            type_name=type_name,
            max_indexed_commit=int(obj.get("max_indexed_commit", 0)),
            entries=list(entries),
        )

    def _write_index(self, kind: str, doc: _IndexDoc) -> None:
        ordered = sorted(
            doc.entries,
            key=lambda e: (int(e["min_commit_id"]), int(e["max_commit_id"]), str(e["path"])),
        )
        payload = {
            "type_name": doc.type_name,
            "max_indexed_commit": doc.max_indexed_commit,
            "entries": ordered,
        }
        self._put_json_with_lock_cas(key=self._index_key(kind, doc.type_name), obj=payload)

    def _resolve_type_files(
        self,
        *,
        kind: str,
        type_name: str,
        q_head: int,
        lower_exclusive: int,
    ) -> list[str]:
        if self._is_type_dropped(kind, type_name):
            return []
        if q_head <= 0:
            return []

        selected: set[str] = set()
        idx = self._read_index(kind, type_name)
        head_obj: dict[str, Any] | None = None
        head_commit = 0
        head_manifest_path: str | None = None
        touched_head_path: str | None = None
        force_head_manifest_fallback = False
        warning_reason: str | None = None

        head_obj = self._read_head(required=True)
        assert head_obj is not None
        head_commit = int(head_obj.get("commit_id", 0))
        if q_head == head_commit:
            manifest_candidate = head_obj.get("manifest_path")
            if isinstance(manifest_candidate, str):
                head_manifest_path = manifest_candidate
                head_manifest = self._read_manifest(manifest_candidate)
                for file_info in head_manifest.get("files", []):
                    if file_info.get("kind") != kind or file_info.get("type_name") != type_name:
                        continue
                    touched_head_path = str(file_info.get("path"))
                    break

                if touched_head_path is not None:
                    covering = [entry for entry in idx.entries if _entry_covers(entry, q_head)]
                    per_commit = [
                        entry
                        for entry in covering
                        if int(entry["min_commit_id"]) == q_head
                        and int(entry["max_commit_id"]) == q_head
                    ]
                    missing_latest = not covering
                    head_path_mismatch = bool(per_commit) and not any(
                        str(entry["path"]) == touched_head_path for entry in per_commit
                    )
                    force_head_manifest_fallback = missing_latest or head_path_mismatch
                    if force_head_manifest_fallback:
                        warning_reason = (
                            f"index latest coverage mismatch for {kind}:{type_name}; "
                            "using manifest fallback"
                        )

        for entry in idx.entries:
            if (
                force_head_manifest_fallback
                and int(entry["min_commit_id"]) == q_head
                and int(entry["max_commit_id"]) == q_head
            ):
                # Per-commit head coverage is stale/corrupt; use authoritative manifest chain.
                continue
            if _entry_intersects(entry, lower_exclusive, q_head):
                selected.add(str(entry["path"]))

        covered = min(idx.max_indexed_commit, q_head)
        if force_head_manifest_fallback:
            covered = min(covered, q_head - 1)

        if covered < q_head:
            if warning_reason is None and idx.max_indexed_commit < q_head:
                warning_reason = (
                    f"index lag detected for {kind}:{type_name}; using manifest fallback"
                )
            start_path = head_manifest_path or head_obj.get("manifest_path")
            if isinstance(start_path, str):
                for manifest in self._walk_manifest_chain(start_path=start_path):
                    cid = int(manifest["commit_id"])
                    if cid <= covered:
                        break
                    if cid > q_head:
                        continue
                    if cid <= lower_exclusive:
                        break
                    for f in manifest.get("files", []):
                        if f.get("kind") == kind and f.get("type_name") == type_name:
                            selected.add(str(f["path"]))
        if warning_reason is not None:
            self._last_index_warning = warning_reason

        return sorted(selected)

    def _scan_sql_for_files(self, files: list[str]) -> str:
        if not files:
            return ""
        s3_paths = [f"s3://{self.bucket}/{self._k(f)}" for f in files]
        literals = ", ".join("'" + p.replace("'", "''") + "'" for p in s3_paths)
        return f"read_parquet([{literals}], union_by_name=true)"

    def _set_duckdb_s3_config(self, key: str, value: str | None) -> None:
        if value is None:
            return
        escaped = value.replace("'", "''")
        conn = self._duck
        if conn is None:
            raise StorageBackendError("duckdb_init", "DuckDB connection not initialized")
        conn.execute(f"SET {key}='{escaped}'")

    def _duck_conn(self):
        if self._duck is None:
            self._duck = duckdb.connect(database=":memory:")
            self._duck.execute(f"PRAGMA memory_limit='{self._config.s3_duckdb_memory_limit}'")
        conn = self._duck
        assert conn is not None
        if not self._duck_httpfs_configured:
            try:
                conn.execute("INSTALL httpfs")
            except Exception:
                # Extension may already be pre-bundled or unavailable for install.
                pass
            try:
                conn.execute("LOAD httpfs")
            except Exception as e:
                raise StorageBackendError("duckdb_httpfs", str(e)) from e

            creds = self._session.get_credentials()
            frozen = creds.get_frozen_credentials() if creds is not None else None

            self._set_duckdb_s3_config(
                "s3_region",
                self._config.s3_region or self._session.region_name or "us-east-1",
            )
            self._set_duckdb_s3_config(
                "s3_access_key_id",
                frozen.access_key if frozen is not None else None,
            )
            self._set_duckdb_s3_config(
                "s3_secret_access_key",
                frozen.secret_key if frozen is not None else None,
            )
            self._set_duckdb_s3_config(
                "s3_session_token",
                frozen.token if frozen is not None else None,
            )

            endpoint_url = self._config.s3_endpoint_url
            if endpoint_url:
                parsed = urlparse(endpoint_url)
                endpoint = parsed.netloc or parsed.path
                self._set_duckdb_s3_config("s3_endpoint", endpoint)
                self._set_duckdb_s3_config(
                    "s3_use_ssl",
                    "true" if parsed.scheme == "https" else "false",
                )
                self._set_duckdb_s3_config("s3_url_style", "path")

            self._duck_httpfs_configured = True
        return conn

    # --- Filters ---

    def _matches_filter(
        self,
        expr: FilterExpression | None,
        row_fields: dict[str, Any],
        *,
        left_fields: dict[str, Any] | None = None,
        right_fields: dict[str, Any] | None = None,
    ) -> bool:
        if expr is None:
            return True

        if isinstance(expr, ComparisonExpression):
            path = expr.field_path
            if path.startswith("$."):
                value = resolve_nested_path(row_fields, path[2:])
            elif path.startswith("left.$."):
                value = resolve_nested_path(left_fields or {}, path[7:])
            elif path.startswith("right.$."):
                value = resolve_nested_path(right_fields or {}, path[8:])
            else:
                return False

            from ontologia.runtime import _compare_value

            return _compare_value(value, expr.op, expr.value)

        if isinstance(expr, ExistsComparisonExpression):
            path = expr.list_field_path
            if path.startswith("$."):
                list_val = resolve_nested_path(row_fields, path[2:])
            elif path.startswith("left.$."):
                list_val = resolve_nested_path(left_fields or {}, path[7:])
            elif path.startswith("right.$."):
                list_val = resolve_nested_path(right_fields or {}, path[8:])
            else:
                return False
            if not isinstance(list_val, list):
                return False
            from ontologia.runtime import _compare_value

            for item in list_val:
                if isinstance(item, dict):
                    item_val = resolve_nested_path(item, expr.item_path)
                else:
                    item_val = item
                if _compare_value(item_val, expr.op, expr.value):
                    return True
            return False

        if isinstance(expr, LogicalExpression):
            if expr.op == "AND":
                return all(
                    self._matches_filter(
                        c,
                        row_fields,
                        left_fields=left_fields,
                        right_fields=right_fields,
                    )
                    for c in expr.children
                )
            if expr.op == "OR":
                return any(
                    self._matches_filter(
                        c,
                        row_fields,
                        left_fields=left_fields,
                        right_fields=right_fields,
                    )
                    for c in expr.children
                )
            if expr.op == "NOT":
                return not self._matches_filter(
                    expr.children[0],
                    row_fields,
                    left_fields=left_fields,
                    right_fields=right_fields,
                )
        return True

    # --- Base lifecycle ---

    def close(self) -> None:
        if self._duck is not None:
            try:
                self._duck.close()
            except Exception:
                pass
        self._tmpdir.cleanup()

    def storage_info(self) -> dict[str, Any]:
        engine = getattr(self, "engine_version", "v1")
        head = self._read_head(required=False)
        head_commit = int(head.get("commit_id", 0)) if head is not None else 0
        info = {
            "backend": "s3",
            "storage_uri": self.storage_uri,
            "bucket": self.bucket,
            "prefix": self.prefix,
            "engine_version": engine,
            "initialized": head is not None,
            "head_commit_id": head_commit if head_commit > 0 else None,
            "last_index_warning": self._last_index_warning,
        }
        if engine == "v2":
            catalog = self._read_type_layout_catalog()
            grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
            for row in catalog.get("layouts", []):
                key = (str(row.get("type_kind", "")), str(row.get("type_name", "")))
                grouped.setdefault(key, []).append(dict(row))
            type_layouts: dict[str, Any] = {}
            for (type_kind, type_name), entries in grouped.items():
                current = next((r for r in entries if bool(r.get("is_current"))), None)
                if current is None:
                    continue
                current_svid = int(current.get("schema_version_id", 0))
                historical = sorted(
                    int(r.get("schema_version_id", 0))
                    for r in entries
                    if int(r.get("schema_version_id", 0)) != current_svid
                )
                layout_key = type_name
                if layout_key in type_layouts:
                    layout_key = f"{type_kind}:{type_name}"
                type_layouts[layout_key] = {
                    "type_kind": type_kind,
                    "current_schema_version_id": current_svid,
                    "activation_commit_id": int(current.get("activation_commit_id", 0)),
                    "historical_versions": historical,
                }
            info["type_layouts"] = type_layouts
        return info

    def get_last_query_diagnostics(self) -> dict[str, Any] | None:
        return self._last_query_diagnostics

    # --- Locking ---

    def acquire_lock(self, owner_id: str, timeout_ms: int = 5000, lease_ms: int = 30000) -> bool:
        deadline = time.monotonic() + timeout_ms / 1000.0

        while True:
            now = datetime.now(timezone.utc)
            expires = now + timedelta(milliseconds=lease_ms)
            payload = {
                "owner_id": owner_id,
                "acquired_at": now.isoformat(),
                "expires_at": expires.isoformat(),
                "lease_ttl_ms": lease_ms,
            }

            try:
                etag = self._put_json(key=self._lock_key(), obj=payload, if_none_match="*")
                self._lock_owner_id = owner_id
                self._lock_etag = etag
                self._lease_expires_at = expires
                self._lease_ttl_ms = lease_ms
                self._lease_unsafe = False
                return True
            except _PreconditionFailed:
                # Existing lock: inspect and attempt takeover if expired.
                lock_obj, etag = self._get_json(self._lock_key(), required=True)
                assert lock_obj is not None
                try:
                    expires_at = _parse_iso(str(lock_obj["expires_at"]))
                except Exception:
                    expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)

                if datetime.now(timezone.utc) >= expires_at and etag is not None:
                    try:
                        new_etag = self._put_json(
                            key=self._lock_key(),
                            obj=payload,
                            if_match=etag,
                        )
                        self._lock_owner_id = owner_id
                        self._lock_etag = new_etag
                        self._lease_expires_at = expires
                        self._lease_ttl_ms = lease_ms
                        self._lease_unsafe = False
                        return True
                    except _PreconditionFailed:
                        pass

                if time.monotonic() >= deadline:
                    return False
                time.sleep(0.01 + random.uniform(0.0, 0.02))
                continue
            except Exception as e:
                raise StorageBackendError("acquire_lock", str(e)) from e

    def renew_lock(self, owner_id: str, lease_ms: int = 30000) -> bool:
        if self._lock_owner_id != owner_id:
            return False

        lock_obj, etag = self._get_json(self._lock_key(), required=False)
        if lock_obj is None or etag is None:
            self._lease_unsafe = True
            return False
        if lock_obj.get("owner_id") != owner_id:
            self._lease_unsafe = True
            return False

        now = datetime.now(timezone.utc)
        expires = now + timedelta(milliseconds=lease_ms)
        lock_obj["expires_at"] = expires.isoformat()
        lock_obj["lease_ttl_ms"] = lease_ms

        try:
            new_etag = self._put_json(key=self._lock_key(), obj=lock_obj, if_match=etag)
        except _PreconditionFailed:
            self._lease_unsafe = True
            return False
        except Exception:
            self._lease_unsafe = True
            return False

        self._lock_etag = new_etag
        self._lease_expires_at = expires
        self._lease_ttl_ms = lease_ms
        self._lease_unsafe = False
        return True

    def release_lock(self, owner_id: str) -> None:
        try:
            lock_obj, etag = self._get_json(self._lock_key(), required=False)
        except Exception:
            lock_obj = None
            etag = None

        if lock_obj is not None and etag is not None and lock_obj.get("owner_id") == owner_id:
            try:
                self._s3.delete_object(Bucket=self.bucket, Key=self._lock_key(), IfMatch=etag)
            except ParamValidationError:
                # Fallback for stacks without conditional delete support.
                refreshed, _ = self._get_json(self._lock_key(), required=False)
                if refreshed is not None and refreshed.get("owner_id") == owner_id:
                    self._s3.delete_object(Bucket=self.bucket, Key=self._lock_key())
            except Exception:
                pass

        if self._lock_owner_id == owner_id:
            self._lock_owner_id = None
            self._lock_etag = None
            self._lease_expires_at = None
            self._lease_unsafe = False

    def _assert_lock_ownership(self, owner_id: str) -> None:
        lock_obj, _etag = self._get_json(self._lock_key(), required=False)
        if lock_obj is None or lock_obj.get("owner_id") != owner_id:
            self._lease_unsafe = True
            raise LeaseExpiredError()
        try:
            expires_at = _parse_iso(str(lock_obj["expires_at"]))
        except Exception as e:
            self._lease_unsafe = True
            raise LeaseExpiredError() from e
        if datetime.now(timezone.utc) >= expires_at:
            self._lease_unsafe = True
            raise LeaseExpiredError()
        self._lease_expires_at = expires_at

    def _ensure_lease_safe(self) -> None:
        if self._lease_unsafe:
            raise LeaseExpiredError()
        if self._lock_owner_id is not None:
            self._assert_lock_ownership(self._lock_owner_id)
        if self._lease_expires_at is None:
            return
        margin = timedelta(milliseconds=max(1, self._lease_ttl_ms // 3))
        if datetime.now(timezone.utc) + margin >= self._lease_expires_at:
            self._lease_unsafe = True
            raise LeaseExpiredError()

    @contextmanager
    def _lease_keepalive(self, owner_id: str) -> Iterator[None]:
        """Renew lock periodically while long write-side operations are running."""
        interval_s = max(0.1, self._lease_ttl_ms / 3000.0)
        stop_event = threading.Event()
        unsafe_event = threading.Event()

        def _heartbeat() -> None:
            while not stop_event.wait(interval_s):
                if not self.renew_lock(owner_id, lease_ms=self._lease_ttl_ms):
                    unsafe_event.set()
                    self._lease_unsafe = True
                    return

        thread = threading.Thread(target=_heartbeat, daemon=True)
        thread.start()
        try:
            yield
            if unsafe_event.is_set():
                raise LeaseExpiredError()
        finally:
            stop_event.set()
            thread.join(timeout=interval_s + 0.2)

    # --- Transaction staging ---

    def begin_transaction(self) -> None:
        if self._tx_active:
            return
        self._tx_active = True
        self._implicit_tx = False
        self._next_commit_id = None
        self._staged_commits.clear()
        self._staged_order.clear()
        self._staged_schema_registry.clear()
        self._staged_schema_deletes.clear()
        self._staged_schema_versions.clear()
        self._staged_dropped_updates.clear()
        self._pending_layout_activations.clear()

    def create_commit(self, metadata: dict[str, Any] | None = None) -> int:
        if self._lock_owner_id is None:
            raise StorageBackendError("create_commit", "Write lock must be acquired before commit")

        if not self._tx_active:
            self.begin_transaction()
            self._implicit_tx = True

        if self._next_commit_id is None:
            head = self._read_head(required=True)
            assert head is not None
            self._next_commit_id = int(head["commit_id"]) + 1

        commit_id = self._next_commit_id
        self._next_commit_id += 1

        staged = _StagedCommit(commit_id=commit_id, metadata=metadata)
        self._staged_commits[commit_id] = staged
        self._staged_order.append(commit_id)
        return commit_id

    def insert_entity(
        self,
        type_name: str,
        key: str,
        fields: dict[str, Any],
        commit_id: int,
        schema_version_id: int | None = None,
    ) -> None:
        if getattr(self, "engine_version", "v1") == "v2":
            if schema_version_id is None:
                raise StorageBackendError(
                    "insert_entity", "schema_version_id is required for s3/v2"
                )
            current = self.get_current_schema_version("entity", type_name)
            if current is None:
                raise StorageBackendError(
                    "insert_entity", f"No schema version registered for entity '{type_name}'"
                )
            expected = int(current["schema_version_id"])
            if int(schema_version_id) != expected:
                raise StorageBackendError(
                    "insert_entity",
                    f"schema_version_id mismatch for entity '{type_name}': expected {expected}, got {schema_version_id}",
                )
            layout = self._get_current_layout("entity", type_name)
            if layout is None:
                self._pending_layout_activations[("entity", type_name)] = (expected, int(commit_id))
            elif int(layout["schema_version_id"]) != expected:
                raise StorageBackendError(
                    "insert_entity",
                    f"entity '{type_name}' current layout is v{layout['schema_version_id']}, expected v{expected}",
                )
        staged = self._staged_commits.get(commit_id)
        if staged is None:
            raise StorageBackendError("insert_entity", f"Unknown staged commit_id {commit_id}")
        staged.entities.setdefault(type_name, []).append(
            _StagedEntityRow(
                type_name=type_name,
                key=key,
                fields=dict(fields),
                schema_version_id=schema_version_id,
            )
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
        if getattr(self, "engine_version", "v1") == "v2":
            if schema_version_id is None:
                raise StorageBackendError(
                    "insert_relation", "schema_version_id is required for s3/v2"
                )
            current = self.get_current_schema_version("relation", type_name)
            if current is None:
                raise StorageBackendError(
                    "insert_relation", f"No schema version registered for relation '{type_name}'"
                )
            expected = int(current["schema_version_id"])
            if int(schema_version_id) != expected:
                raise StorageBackendError(
                    "insert_relation",
                    f"schema_version_id mismatch for relation '{type_name}': expected {expected}, got {schema_version_id}",
                )
            layout = self._get_current_layout("relation", type_name)
            if layout is None:
                self._pending_layout_activations[("relation", type_name)] = (
                    expected,
                    int(commit_id),
                )
            elif int(layout["schema_version_id"]) != expected:
                raise StorageBackendError(
                    "insert_relation",
                    f"relation '{type_name}' current layout is v{layout['schema_version_id']}, expected v{expected}",
                )
        staged = self._staged_commits.get(commit_id)
        if staged is None:
            raise StorageBackendError("insert_relation", f"Unknown staged commit_id {commit_id}")
        staged.relations.setdefault(type_name, []).append(
            _StagedRelationRow(
                type_name=type_name,
                left_key=left_key,
                right_key=right_key,
                instance_key=instance_key,
                fields=dict(fields),
                schema_version_id=schema_version_id,
            )
        )

    def rollback_transaction(self) -> None:
        self._tx_active = False
        self._implicit_tx = False
        self._next_commit_id = None
        self._staged_commits.clear()
        self._staged_order.clear()
        self._staged_schema_registry.clear()
        self._staged_schema_deletes.clear()
        self._staged_schema_versions.clear()
        self._staged_dropped_updates.clear()
        self._pending_layout_activations.clear()

    def commit_transaction(self) -> None:
        if not self._tx_active:
            return

        try:
            owner = self._lock_owner_id
            if owner is not None:
                with self._lease_keepalive(owner):
                    for commit_id in self._staged_order:
                        staged = self._staged_commits[commit_id]
                        self._publish_staged_commit(staged)
                    self._flush_staged_schema_changes()
                    pending = dict(self._pending_layout_activations)
                    if pending:
                        for (kind, type_name), (
                            schema_version_id,
                            activation_commit_id,
                        ) in pending.items():
                            self._apply_layout_activation(
                                type_kind=kind,
                                type_name=type_name,
                                schema_version_id=schema_version_id,
                                activation_commit_id=activation_commit_id,
                            )
            else:
                for commit_id in self._staged_order:
                    staged = self._staged_commits[commit_id]
                    self._publish_staged_commit(staged)
                self._flush_staged_schema_changes()
                pending = dict(self._pending_layout_activations)
                if pending:
                    for (kind, type_name), (
                        schema_version_id,
                        activation_commit_id,
                    ) in pending.items():
                        self._apply_layout_activation(
                            type_kind=kind,
                            type_name=type_name,
                            schema_version_id=schema_version_id,
                            activation_commit_id=activation_commit_id,
                        )
        finally:
            self.rollback_transaction()

    # --- Commit publication ---

    def _entity_table(self, rows: list[_StagedEntityRow], commit_id: int) -> Any:
        field_names = sorted({k for row in rows for k in row.fields.keys()})

        data: dict[str, list[Any]] = {
            "commit_id": [commit_id for _ in rows],
            "entity_type": [rows[0].type_name for _ in rows],
            "entity_key": [r.key for r in rows],
            "schema_version_id": [r.schema_version_id for r in rows],
            "fields_json": [
                json.dumps(r.fields, separators=(",", ":"), sort_keys=True) for r in rows
            ],
        }

        for fname in field_names:
            data[fname] = [r.fields.get(fname) for r in rows]

        return pa.table(data)

    def _relation_table(self, rows: list[_StagedRelationRow], commit_id: int) -> Any:
        field_names = sorted({k for row in rows for k in row.fields.keys()})

        data: dict[str, list[Any]] = {
            "commit_id": [commit_id for _ in rows],
            "relation_type": [rows[0].type_name for _ in rows],
            "left_key": [r.left_key for r in rows],
            "right_key": [r.right_key for r in rows],
            "instance_key": [r.instance_key for r in rows],
            "schema_version_id": [r.schema_version_id for r in rows],
            "fields_json": [
                json.dumps(r.fields, separators=(",", ":"), sort_keys=True) for r in rows
            ],
        }

        for fname in field_names:
            data[fname] = [r.fields.get(fname) for r in rows]

        return pa.table(data)

    def _write_parquet_object(self, rel_path: str, table: Any) -> tuple[int, str]:
        sink = pa.BufferOutputStream()
        pq.write_table(table, sink)
        data = sink.getvalue().to_pybytes()
        sha = hashlib.sha256(data).hexdigest()
        self._put_bytes(key=self._k(rel_path), body=data, content_type="application/octet-stream")
        self._download_cache.pop(rel_path, None)
        return int(table.num_rows), sha

    def _publish_staged_commit(self, staged: _StagedCommit) -> None:
        head, head_etag = self._get_json(self._head_key(), required=True)
        assert head is not None
        if not head_etag:
            raise StorageBackendError("head_cas", "Missing ETag for head object")
        current_head = int(head.get("commit_id", 0))

        expected_parent = staged.commit_id - 1
        if current_head != expected_parent:
            raise HeadMismatchError(1)

        attempt = uuid.uuid4().hex[:8]
        base = f"commits/{staged.commit_id}-{attempt}"
        files: list[dict[str, Any]] = []

        for type_name, rows in staged.entities.items():
            if not rows:
                continue
            self._ensure_lease_safe()
            if getattr(self, "engine_version", "v1") == "v2":
                schema_versions = {int(r.schema_version_id or 0) for r in rows}
                if len(schema_versions) != 1 or 0 in schema_versions:
                    raise StorageBackendError(
                        "_publish_staged_commit",
                        f"v2 entity commit file requires one non-null schema_version_id for '{type_name}'",
                    )
                svid = next(iter(schema_versions))
                rel_path = f"{base}/entities/{type_name}/v{svid}.parquet"
            else:
                rel_path = f"{base}/entities/{type_name}.parquet"
            table = self._entity_table(rows, staged.commit_id)
            row_count, sha = self._write_parquet_object(rel_path, table)
            files.append(
                {
                    "kind": "entity",
                    "type_name": type_name,
                    "path": rel_path,
                    "row_count": row_count,
                    "schema_version_id": rows[0].schema_version_id,
                    "content_sha256": sha,
                }
            )

        for type_name, rows in staged.relations.items():
            if not rows:
                continue
            self._ensure_lease_safe()
            if getattr(self, "engine_version", "v1") == "v2":
                schema_versions = {int(r.schema_version_id or 0) for r in rows}
                if len(schema_versions) != 1 or 0 in schema_versions:
                    raise StorageBackendError(
                        "_publish_staged_commit",
                        f"v2 relation commit file requires one non-null schema_version_id for '{type_name}'",
                    )
                svid = next(iter(schema_versions))
                rel_path = f"{base}/relations/{type_name}/v{svid}.parquet"
            else:
                rel_path = f"{base}/relations/{type_name}.parquet"
            table = self._relation_table(rows, staged.commit_id)
            row_count, sha = self._write_parquet_object(rel_path, table)
            files.append(
                {
                    "kind": "relation",
                    "type_name": type_name,
                    "path": rel_path,
                    "row_count": row_count,
                    "schema_version_id": rows[0].schema_version_id,
                    "content_sha256": sha,
                }
            )

        manifest_path = f"{base}/manifest.json"
        manifest = {
            "commit_id": staged.commit_id,
            "parent_commit_id": current_head if current_head > 0 else None,
            "parent_manifest_path": head.get("manifest_path"),
            "created_at": _now_iso(),
            "runtime_id": self._config.runtime_id or "unknown-runtime",
            "metadata": staged.metadata or {},
            "files": files,
        }
        self._put_json(key=self._k(manifest_path), obj=manifest)

        self._ensure_lease_safe()

        next_head = {
            "commit_id": staged.commit_id,
            "manifest_path": manifest_path,
            "updated_at": _now_iso(),
            "runtime_id": self._config.runtime_id or "unknown-runtime",
        }

        try:
            self._put_json(key=self._head_key(), obj=next_head, if_match=head_etag)
        except _PreconditionFailed as e:
            raise HeadMismatchError(1) from e

        # Post-CAS index update is best effort.
        try:
            self._ensure_lease_safe()
            self._update_indices_after_commit(
                previous_head=current_head,
                previous_manifest_path=head.get("manifest_path"),
                commit_id=staged.commit_id,
                files=files,
            )
        except Exception as e:
            self._last_index_warning = f"Index update skipped/degraded: {e}"

    def _repair_index_gap(
        self,
        *,
        kind: str,
        type_name: str,
        index: _IndexDoc,
        previous_head: int,
        previous_manifest_path: str | None,
    ) -> _IndexDoc:
        if previous_head <= index.max_indexed_commit:
            return index

        if previous_manifest_path is None:
            index.max_indexed_commit = previous_head
            return index

        covered_entries = list(index.entries)

        for manifest in self._walk_manifest_chain(start_path=previous_manifest_path):
            cid = int(manifest["commit_id"])
            if cid <= index.max_indexed_commit:
                break
            for f in manifest.get("files", []):
                if f.get("kind") != kind or f.get("type_name") != type_name:
                    continue
                if any(_entry_covers(e, cid) for e in covered_entries):
                    continue
                covered_entries.append(
                    {
                        "min_commit_id": cid,
                        "max_commit_id": cid,
                        "path": f["path"],
                    }
                )

        index.entries = covered_entries
        index.max_indexed_commit = previous_head
        return index

    def _update_indices_after_commit(
        self,
        *,
        previous_head: int,
        previous_manifest_path: str | None,
        commit_id: int,
        files: list[dict[str, Any]],
    ) -> None:
        self._ensure_lease_safe()
        catalog = self._read_types_catalog(required=False)
        if catalog is None:
            self._last_index_warning = "types.json missing/malformed; index mutation skipped"
            return

        touched: dict[tuple[str, str], str] = {}
        for f in files:
            touched[(str(f["kind"]), str(f["type_name"]))] = str(f["path"])

        all_types: list[tuple[str, str]] = []
        all_types.extend(("entity", t) for t in catalog.get("entities", []))
        all_types.extend(("relation", t) for t in catalog.get("relations", []))
        per_type_errors: list[str] = []

        for kind, type_name in all_types:
            try:
                self._ensure_lease_safe()
                idx = self._read_index(kind, type_name)
                if idx.max_indexed_commit < previous_head:
                    idx = self._repair_index_gap(
                        kind=kind,
                        type_name=type_name,
                        index=idx,
                        previous_head=previous_head,
                        previous_manifest_path=previous_manifest_path,
                    )

                touched_path = touched.get((kind, type_name))
                if touched_path is not None:
                    idx.entries = [e for e in idx.entries if not _entry_covers(e, commit_id)]
                    idx.entries.append(
                        {
                            "min_commit_id": commit_id,
                            "max_commit_id": commit_id,
                            "path": touched_path,
                        }
                    )

                idx.max_indexed_commit = commit_id
                self._write_index(kind, idx)
            except Exception as e:
                per_type_errors.append(f"{kind}:{type_name}: {e}")
                continue

        if per_type_errors:
            self._last_index_warning = "Index update skipped/degraded for types: " + "; ".join(
                per_type_errors[:5]
            )

    # --- Commit reads ---

    def get_head_commit_id(self) -> int | None:
        head = self._read_head(required=True)
        assert head is not None
        cid = int(head.get("commit_id", 0))
        return cid if cid > 0 else None

    def get_commit(self, commit_id: int) -> dict[str, Any] | None:
        head = self._read_head(required=True)
        assert head is not None
        if commit_id <= 0 or commit_id > int(head.get("commit_id", 0)):
            return None

        for manifest in self._walk_manifest_chain(start_path=head.get("manifest_path")):
            cid = int(manifest["commit_id"])
            if cid == commit_id:
                return {
                    "id": cid,
                    "created_at": manifest.get("created_at"),
                    "metadata": manifest.get("metadata") or None,
                }
            if cid < commit_id:
                break
        return None

    def list_commits(
        self,
        *,
        limit: int = 10,
        since_commit_id: int | None = None,
    ) -> list[dict[str, Any]]:
        head = self._read_head(required=True)
        assert head is not None

        out: list[dict[str, Any]] = []
        start = head.get("manifest_path")
        if not isinstance(start, str):
            return out

        for manifest in self._walk_manifest_chain(start_path=start):
            cid = int(manifest["commit_id"])
            if since_commit_id is not None and cid <= since_commit_id:
                break
            out.append(
                {
                    "id": cid,
                    "created_at": manifest.get("created_at"),
                    "metadata": manifest.get("metadata") or None,
                }
            )
            if len(out) >= limit:
                break

        return out

    def list_commit_changes(self, commit_id: int) -> list[dict[str, Any]]:
        head = self._read_head(required=True)
        assert head is not None
        start = head.get("manifest_path")
        if not isinstance(start, str):
            return []

        target: dict[str, Any] | None = None
        for manifest in self._walk_manifest_chain(start_path=start):
            cid = int(manifest["commit_id"])
            if cid == commit_id:
                target = manifest
                break
            if cid < commit_id:
                break

        if target is None:
            return []

        changes: list[dict[str, Any]] = []
        for f in target.get("files", []):
            kind = str(f.get("kind"))
            type_name = str(f.get("type_name"))
            path = str(f.get("path"))
            table = pq.read_table(self._download(path))
            cols = set(table.column_names)
            if kind == "entity":
                keys = table.column("entity_key").to_pylist() if "entity_key" in cols else []
                for key in keys:
                    changes.append(
                        {
                            "kind": "entity",
                            "type_name": type_name,
                            "key": key,
                            "operation": "insert_or_update",
                        }
                    )
            else:
                left = table.column("left_key").to_pylist() if "left_key" in cols else []
                right = table.column("right_key").to_pylist() if "right_key" in cols else []
                inst = table.column("instance_key").to_pylist() if "instance_key" in cols else []
                for i in range(min(len(left), len(right))):
                    changes.append(
                        {
                            "kind": "relation",
                            "type_name": type_name,
                            "left_key": left[i],
                            "right_key": right[i],
                            "instance_key": inst[i] if i < len(inst) else "",
                            "operation": "insert_or_update",
                        }
                    )

        return changes

    def count_commit_operations(self, commit_id: int) -> int:
        head = self._read_head(required=True)
        assert head is not None
        start = head.get("manifest_path")
        if not isinstance(start, str):
            return 0

        for manifest in self._walk_manifest_chain(start_path=start):
            cid = int(manifest["commit_id"])
            if cid == commit_id:
                return int(sum(int(f.get("row_count", 0)) for f in manifest.get("files", [])))
            if cid < commit_id:
                return 0
        return 0

    # --- Entity/relation reads ---

    def _temporal_window(
        self,
        *,
        with_history: bool,
        history_since: int | None,
        as_of: int | None,
    ) -> tuple[int, int, bool]:
        head = self._read_head(required=True)
        assert head is not None
        head_id = int(head.get("commit_id", 0))

        if head_id == 0:
            return 0, 0, True

        if as_of is not None:
            q_head = min(max(as_of, 0), head_id)
            if q_head == 0:
                return 0, 0, True
            return q_head, 0, False

        if with_history:
            return head_id, 0, False

        if history_since is not None:
            return head_id, max(history_since, 0), False

        return head_id, 0, False

    def _entity_rows_raw(
        self,
        type_name: str,
        *,
        with_history: bool = False,
        history_since: int | None = None,
        as_of: int | None = None,
    ) -> list[dict[str, Any]]:
        q_head, lower_exclusive, empty = self._temporal_window(
            with_history=with_history,
            history_since=history_since,
            as_of=as_of,
        )
        if empty:
            return []

        files = self._resolve_type_files(
            kind="entity",
            type_name=type_name,
            q_head=q_head,
            lower_exclusive=lower_exclusive,
        )
        if not files:
            return []

        scan = self._scan_sql_for_files(files)
        conn = self._duck_conn()

        params: list[Any] = [type_name]
        if with_history or history_since is not None:
            sql = (
                "SELECT entity_key, fields_json, commit_id, schema_version_id "
                f"FROM {scan} WHERE entity_type = ?"
            )
            if history_since is not None:
                sql += " AND commit_id > ?"
                params.append(history_since)
        else:
            sql = (
                "SELECT entity_key, fields_json, commit_id, schema_version_id FROM ("
                "  SELECT entity_key, fields_json, commit_id, schema_version_id, "
                "         ROW_NUMBER() OVER (PARTITION BY entity_key "
                "ORDER BY commit_id DESC) AS _rn "
                f"  FROM {scan} WHERE entity_type = ?"
            )
            if as_of is not None:
                sql += " AND commit_id <= ?"
                params.append(as_of)
            sql += ") t WHERE _rn = 1"

        rows = conn.execute(sql, params).fetchall()
        return [
            {
                "key": r[0],
                "fields": json.loads(r[1]),
                "commit_id": int(r[2]),
                "schema_version_id": r[3],
            }
            for r in rows
        ]

    def _relation_rows_raw(
        self,
        type_name: str,
        *,
        with_history: bool = False,
        history_since: int | None = None,
        as_of: int | None = None,
    ) -> list[dict[str, Any]]:
        q_head, lower_exclusive, empty = self._temporal_window(
            with_history=with_history,
            history_since=history_since,
            as_of=as_of,
        )
        if empty:
            return []

        files = self._resolve_type_files(
            kind="relation",
            type_name=type_name,
            q_head=q_head,
            lower_exclusive=lower_exclusive,
        )
        if not files:
            return []

        scan = self._scan_sql_for_files(files)
        conn = self._duck_conn()

        params: list[Any] = [type_name]
        if with_history or history_since is not None:
            sql = (
                "SELECT left_key, right_key, instance_key, fields_json, "
                "commit_id, schema_version_id "
                f"FROM {scan} WHERE relation_type = ?"
            )
            if history_since is not None:
                sql += " AND commit_id > ?"
                params.append(history_since)
        else:
            sql = (
                "SELECT left_key, right_key, instance_key, fields_json, "
                "commit_id, schema_version_id FROM ("
                "  SELECT left_key, right_key, instance_key, fields_json, "
                "commit_id, schema_version_id, "
                "         ROW_NUMBER() OVER ("
                "           PARTITION BY left_key, right_key, instance_key ORDER BY commit_id DESC"
                "         ) AS _rn "
                f"  FROM {scan} WHERE relation_type = ?"
            )
            if as_of is not None:
                sql += " AND commit_id <= ?"
                params.append(as_of)
            sql += ") t WHERE _rn = 1"

        rows = conn.execute(sql, params).fetchall()
        return [
            {
                "left_key": r[0],
                "right_key": r[1],
                "instance_key": r[2] or "",
                "fields": json.loads(r[3]),
                "commit_id": int(r[4]),
                "schema_version_id": r[5],
            }
            for r in rows
        ]

    def get_latest_entity(self, type_name: str, key: str) -> dict[str, Any] | None:
        rows = self.query_entities(type_name)
        for row in rows:
            if row["key"] == key:
                return {"fields": row["fields"], "commit_id": row["commit_id"]}
        return None

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
        if getattr(self, "engine_version", "v1") == "v2":
            layout = self._get_current_layout("entity", type_name)
            if layout is None:
                return []
            activation_commit_id = int(layout["activation_commit_id"])
            current_schema_version_id = int(layout["schema_version_id"])
            if as_of is not None and as_of < activation_commit_id:
                self._last_query_diagnostics = {
                    "reason": "commit_before_activation",
                    "activation_commit_id": activation_commit_id,
                }
                return []
            if with_history or history_since is not None:
                history_since = max(history_since or 0, activation_commit_id - 1)
                with_history = False
            elif as_of is None:
                head_now = self.get_head_commit_id()
                if head_now is None or head_now < activation_commit_id:
                    return []
                as_of = head_now
            schema_version_id = current_schema_version_id

        q_head, lower_exclusive, empty = self._temporal_window(
            with_history=with_history,
            history_since=history_since,
            as_of=as_of,
        )
        if empty:
            return []

        files = self._resolve_type_files(
            kind="entity",
            type_name=type_name,
            q_head=q_head,
            lower_exclusive=lower_exclusive,
        )
        if not files:
            return []

        scan = self._scan_sql_for_files(files)
        conn = self._duck_conn()
        params: list[Any] = [type_name]

        # Determine whether to apply schema version filtering (temporal queries only)
        _apply_sv = schema_version_id is not None and (
            with_history or history_since is not None or as_of is not None
        )

        if with_history or history_since is not None:
            base_sql = (
                "SELECT eh.entity_key, eh.fields_json, eh.commit_id, eh.schema_version_id "
                f"FROM {scan} eh WHERE eh.entity_type = ?"
            )
            if history_since is not None:
                base_sql += " AND eh.commit_id > ?"
                params.append(history_since)
            if _apply_sv:
                base_sql += " AND eh.schema_version_id = ?"
                params.append(schema_version_id)
        else:
            base_sql = (
                "SELECT q.entity_key, q.fields_json, q.commit_id, q.schema_version_id "
                "FROM ("
                "  SELECT eh.entity_key, eh.fields_json, eh.commit_id, eh.schema_version_id, "
                "         ROW_NUMBER() OVER (PARTITION BY eh.entity_key "
                "ORDER BY eh.commit_id DESC) AS _rn "
                f"  FROM {scan} eh WHERE eh.entity_type = ?"
            )
            if as_of is not None:
                base_sql += " AND eh.commit_id <= ?"
                params.append(as_of)
            if _apply_sv:
                base_sql += " AND eh.schema_version_id = ?"
                params.append(schema_version_id)
            base_sql += ") q WHERE q._rn = 1"

        sql = (
            "SELECT q.entity_key, q.fields_json, q.commit_id, "
            f"q.schema_version_id FROM ({base_sql}) q"
        )
        if filter_expr is not None:
            where_sql = _compile_filter(filter_expr, params, table_alias="q")
            sql += f" WHERE {where_sql}"

        if order_by:
            field_name = order_by.removeprefix("$.")
            direction = "DESC" if order_desc else "ASC"
            sql += (
                f" ORDER BY json_extract(q.fields_json, '$.{field_name}') IS NULL, "
                f"json_extract(q.fields_json, '$.{field_name}') {direction}"
            )
        elif with_history or history_since is not None:
            sql += " ORDER BY q.commit_id ASC, q.entity_key ASC"

        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        if offset is not None:
            sql += " OFFSET ?"
            params.append(offset)

        rows = conn.execute(sql, params).fetchall()
        return [
            {
                "key": r[0],
                "fields": json.loads(r[1]),
                "commit_id": int(r[2]),
            }
            for r in rows
        ]

    def count_entities(
        self,
        type_name: str,
        *,
        filter_expr: FilterExpression | None = None,
    ) -> int:
        return len(self.query_entities(type_name, filter_expr=filter_expr))

    def aggregate_entities(
        self,
        type_name: str,
        agg_func: str,
        field_name: str,
        *,
        filter_expr: FilterExpression | None = None,
    ) -> Any:
        rows = self.query_entities(type_name, filter_expr=filter_expr)
        values = [r["fields"].get(field_name) for r in rows]
        values_num = _numeric_values(values)

        fn = agg_func.upper()
        if fn == "COUNT":
            return len(rows)
        if not values:
            return None
        if fn == "SUM":
            return sum(values_num)
        if fn == "AVG":
            return (sum(values_num) / len(values_num)) if values_num else None
        if fn == "MIN":
            try:
                return min(v for v in values if v is not None)
            except ValueError:
                return None
        if fn == "MAX":
            try:
                return max(v for v in values if v is not None)
            except ValueError:
                return None
        raise ValueError(f"Unsupported aggregation function: {agg_func}")

    def _having_passes(
        self,
        group_rows: list[dict[str, Any]],
        *,
        having_sql_fragment: str | None,
        having_params: list[Any] | None,
    ) -> bool:
        if not having_sql_fragment:
            return True

        frag = having_sql_fragment.strip()
        param = having_params[0] if having_params else None

        op: str | None = None
        expr = ""
        for candidate in (">=", "<=", "!=", "=", ">", "<"):
            token = f" {candidate} ?"
            if token in frag:
                op = candidate
                expr = frag.split(token, 1)[0].strip()
                break
        if op is None:
            return True

        expr_u = expr.upper()
        if expr_u.startswith("COUNT("):
            lhs = len(group_rows)
        else:
            # e.g. SUM(json_extract(eh.fields_json, '$.total_amount'))
            fn = expr_u.split("(", 1)[0]
            field_marker = "$."
            idx = expr.find(field_marker)
            if idx < 0:
                return True
            remain = expr[idx + len(field_marker) :]
            field = remain.split("'", 1)[0].split('"', 1)[0].split(")", 1)[0]
            vals = [r["fields"].get(field) for r in group_rows]
            nums = _numeric_values(vals)
            if fn == "SUM":
                lhs = sum(nums)
            elif fn == "AVG":
                lhs = (sum(nums) / len(nums)) if nums else None
            elif fn == "MIN":
                lhs = min((v for v in vals if v is not None), default=None)
            elif fn == "MAX":
                lhs = max((v for v in vals if v is not None), default=None)
            else:
                return True

        if op == "=":
            return lhs == param
        if op == "!=":
            return lhs != param
        if lhs is None:
            return False
        if op in {">", ">=", "<", "<="}:
            if param is None:
                return False
            try:
                if op == ">":
                    return lhs > param
                if op == ">=":
                    return lhs >= param
                if op == "<":
                    return lhs < param
                return lhs <= param
            except Exception:
                return False
        return True

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
        rows = self.query_entities(type_name, filter_expr=filter_expr)

        grouped: dict[Any, list[dict[str, Any]]] = {}
        for r in rows:
            key = r["fields"].get(group_field)
            grouped.setdefault(key, []).append(r)

        out: list[dict[str, Any]] = []
        for group_key, items in grouped.items():
            if not self._having_passes(
                items,
                having_sql_fragment=having_sql_fragment,
                having_params=having_params,
            ):
                continue
            rec: dict[str, Any] = {group_field: group_key}
            for alias, (func, field_name) in agg_specs.items():
                fn = func.upper()
                if fn == "COUNT":
                    rec[alias] = len(items)
                    continue
                vals = [item["fields"].get(field_name or "") for item in items]
                nums = _numeric_values(vals)
                if fn == "SUM":
                    rec[alias] = sum(nums)
                elif fn == "AVG":
                    rec[alias] = (sum(nums) / len(nums)) if nums else None
                elif fn == "MIN":
                    rec[alias] = min((v for v in vals if v is not None), default=None)
                elif fn == "MAX":
                    rec[alias] = max((v for v in vals if v is not None), default=None)
                else:
                    raise ValueError(f"Unsupported aggregate: {func}")
            out.append(rec)

        return out

    def get_latest_relation(
        self, type_name: str, left_key: str, right_key: str, instance_key: str = ""
    ) -> dict[str, Any] | None:
        rows = self.query_relations(type_name)
        for row in rows:
            if (
                row["left_key"] == left_key
                and row["right_key"] == right_key
                and row.get("instance_key", "") == instance_key
            ):
                return {"fields": row["fields"], "commit_id": row["commit_id"]}
        return None

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
        if getattr(self, "engine_version", "v1") == "v2":
            layout = self._get_current_layout("relation", type_name)
            if layout is None:
                return []
            activation_commit_id = int(layout["activation_commit_id"])
            current_schema_version_id = int(layout["schema_version_id"])
            if as_of is not None and as_of < activation_commit_id:
                self._last_query_diagnostics = {
                    "reason": "commit_before_activation",
                    "activation_commit_id": activation_commit_id,
                }
                return []
            if with_history or history_since is not None:
                history_since = max(history_since or 0, activation_commit_id - 1)
                with_history = False
            elif as_of is None:
                head_now = self.get_head_commit_id()
                if head_now is None or head_now < activation_commit_id:
                    return []
                as_of = head_now
            schema_version_id = current_schema_version_id

        left_filter_needed = _needs_endpoint_join(filter_expr, "left")
        right_filter_needed = _needs_endpoint_join(filter_expr, "right")
        if left_filter_needed and left_entity_type is None:
            raise ValueError("left_entity_type is required for left endpoint filters")
        if right_filter_needed and right_entity_type is None:
            raise ValueError("right_entity_type is required for right endpoint filters")

        q_head, lower_exclusive, empty = self._temporal_window(
            with_history=with_history,
            history_since=history_since,
            as_of=as_of,
        )
        if empty:
            return []

        relation_files = self._resolve_type_files(
            kind="relation",
            type_name=type_name,
            q_head=q_head,
            lower_exclusive=lower_exclusive,
        )
        if not relation_files:
            return []

        scan = self._scan_sql_for_files(relation_files)
        conn = self._duck_conn()
        params: list[Any] = [type_name]

        # Determine whether to apply schema version filtering (temporal queries only)
        _apply_sv = schema_version_id is not None and (
            with_history or history_since is not None or as_of is not None
        )

        if with_history or history_since is not None:
            base_sql = (
                "SELECT rh.left_key, rh.right_key, rh.instance_key, rh.fields_json, rh.commit_id, "
                "rh.schema_version_id "
                f"FROM {scan} rh WHERE rh.relation_type = ?"
            )
            if history_since is not None:
                base_sql += " AND rh.commit_id > ?"
                params.append(history_since)
            if _apply_sv:
                base_sql += " AND rh.schema_version_id = ?"
                params.append(schema_version_id)
        else:
            base_sql = (
                "SELECT q.left_key, q.right_key, q.instance_key, q.fields_json, q.commit_id, "
                "q.schema_version_id FROM ("
                "  SELECT rh.left_key, rh.right_key, rh.instance_key, rh.fields_json, "
                "rh.commit_id, "
                "rh.schema_version_id, "
                "         ROW_NUMBER() OVER ("
                "           PARTITION BY rh.left_key, rh.right_key, rh.instance_key "
                "ORDER BY rh.commit_id DESC"
                "         ) AS _rn "
                f"  FROM {scan} rh WHERE rh.relation_type = ?"
            )
            if as_of is not None:
                base_sql += " AND rh.commit_id <= ?"
                params.append(as_of)
            if _apply_sv:
                base_sql += " AND rh.schema_version_id = ?"
                params.append(schema_version_id)
            base_sql += ") q WHERE q._rn = 1"

        sql = (
            "SELECT q.left_key, q.right_key, q.instance_key, q.fields_json, q.commit_id "
            f"FROM ({base_sql}) q WHERE 1=1"
        )

        if left_filter_needed and left_entity_type is not None:
            left_files = self._resolve_type_files(
                kind="entity",
                type_name=left_entity_type,
                q_head=q_head,
                lower_exclusive=lower_exclusive,
            )
            if not left_files:
                return []
            left_scan = self._scan_sql_for_files(left_files)
            if with_history or history_since is not None:
                left_exists = (
                    "SELECT 1 FROM "
                    f"{left_scan} le WHERE le.entity_type = ? AND le.entity_key = q.left_key"
                )
                params.append(left_entity_type)
                if history_since is not None:
                    left_exists += " AND le.commit_id > ?"
                    params.append(history_since)
            else:
                left_exists = (
                    "SELECT 1 FROM ("
                    "  SELECT le.entity_key, le.fields_json, le.commit_id, "
                    "         ROW_NUMBER() OVER (PARTITION BY le.entity_key "
                    "ORDER BY le.commit_id DESC) "
                    "AS _rn "
                    f"  FROM {left_scan} le WHERE le.entity_type = ?"
                )
                params.append(left_entity_type)
                if as_of is not None:
                    left_exists += " AND le.commit_id <= ?"
                    params.append(as_of)
                left_exists += ") le WHERE le._rn = 1 AND le.entity_key = q.left_key"
            left_filter = _extract_prefix_filter(filter_expr, "left")
            if left_filter is not None:
                left_where = _compile_filter(left_filter, params, table_alias="le")
                left_exists += f" AND {left_where}"
            sql += f" AND EXISTS ({left_exists})"

        if right_filter_needed and right_entity_type is not None:
            right_files = self._resolve_type_files(
                kind="entity",
                type_name=right_entity_type,
                q_head=q_head,
                lower_exclusive=lower_exclusive,
            )
            if not right_files:
                return []
            right_scan = self._scan_sql_for_files(right_files)
            if with_history or history_since is not None:
                right_exists = (
                    "SELECT 1 FROM "
                    f"{right_scan} re WHERE re.entity_type = ? AND re.entity_key = q.right_key"
                )
                params.append(right_entity_type)
                if history_since is not None:
                    right_exists += " AND re.commit_id > ?"
                    params.append(history_since)
            else:
                right_exists = (
                    "SELECT 1 FROM ("
                    "  SELECT re.entity_key, re.fields_json, re.commit_id, "
                    "         ROW_NUMBER() OVER (PARTITION BY re.entity_key "
                    "ORDER BY re.commit_id DESC) "
                    "AS _rn "
                    f"  FROM {right_scan} re WHERE re.entity_type = ?"
                )
                params.append(right_entity_type)
                if as_of is not None:
                    right_exists += " AND re.commit_id <= ?"
                    params.append(as_of)
                right_exists += ") re WHERE re._rn = 1 AND re.entity_key = q.right_key"
            right_filter = _extract_prefix_filter(filter_expr, "right")
            if right_filter is not None:
                right_where = _compile_filter(right_filter, params, table_alias="re")
                right_exists += f" AND {right_where}"
            sql += f" AND EXISTS ({right_exists})"

        direct_filter = _extract_direct_filter(filter_expr)
        if direct_filter is not None:
            direct_where = _compile_filter(direct_filter, params, table_alias="q")
            sql += f" AND {direct_where}"

        if order_by:
            field_name = order_by.removeprefix("$.")
            direction = "DESC" if order_desc else "ASC"
            sql += (
                f" ORDER BY json_extract(q.fields_json, '$.{field_name}') IS NULL, "
                f"json_extract(q.fields_json, '$.{field_name}') {direction}"
            )
        elif with_history or history_since is not None:
            sql += " ORDER BY q.commit_id ASC, q.left_key ASC, q.right_key ASC, q.instance_key ASC"

        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        if offset is not None:
            sql += " OFFSET ?"
            params.append(offset)

        rows = conn.execute(sql, params).fetchall()
        return [
            {
                "left_key": r[0],
                "right_key": r[1],
                "instance_key": r[2] or "",
                "fields": json.loads(r[3]),
                "commit_id": int(r[4]),
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
        return len(
            self.query_relations(
                type_name,
                left_entity_type=left_entity_type,
                right_entity_type=right_entity_type,
                filter_expr=filter_expr,
            )
        )

    def aggregate_relations(
        self,
        type_name: str,
        agg_func: str,
        field_name: str,
        *,
        filter_expr: FilterExpression | None = None,
    ) -> Any:
        rows = self.query_relations(type_name, filter_expr=filter_expr)
        values = [r["fields"].get(field_name) for r in rows]
        values_num = _numeric_values(values)

        fn = agg_func.upper()
        if fn == "COUNT":
            return len(rows)
        if not values:
            return None
        if fn == "SUM":
            return sum(values_num)
        if fn == "AVG":
            return (sum(values_num) / len(values_num)) if values_num else None
        if fn == "MIN":
            return min((v for v in values if v is not None), default=None)
        if fn == "MAX":
            return max((v for v in values if v is not None), default=None)
        raise ValueError(f"Unsupported aggregation function: {agg_func}")

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
        rows = self.query_relations(
            type_name,
            left_entity_type=left_entity_type,
            right_entity_type=right_entity_type,
            filter_expr=filter_expr,
        )

        def _group_value(row: dict[str, Any]) -> Any:
            if group_field.startswith("left.$."):
                lf = (
                    self.get_latest_entity(left_entity_type or "", row["left_key"])
                    if left_entity_type
                    else None
                )
                return (lf or {}).get("fields", {}).get(group_field[7:])
            if group_field.startswith("right.$."):
                rf = (
                    self.get_latest_entity(right_entity_type or "", row["right_key"])
                    if right_entity_type
                    else None
                )
                return (rf or {}).get("fields", {}).get(group_field[8:])
            return row["fields"].get(group_field)

        grouped: dict[Any, list[dict[str, Any]]] = {}
        for r in rows:
            grouped.setdefault(_group_value(r), []).append(r)

        result_key = group_field
        if group_field.startswith("left.$."):
            result_key = group_field[7:]
        elif group_field.startswith("right.$."):
            result_key = group_field[8:]

        out: list[dict[str, Any]] = []
        for gkey, items in grouped.items():
            if not self._having_passes(
                items,
                having_sql_fragment=having_sql_fragment,
                having_params=having_params,
            ):
                continue
            rec: dict[str, Any] = {result_key: gkey}
            for alias, (func, fname) in agg_specs.items():
                fn = func.upper()
                if fn == "COUNT":
                    rec[alias] = len(items)
                else:
                    vals = [it["fields"].get(fname or "") for it in items]
                    nums = _numeric_values(vals)
                    if fn == "SUM":
                        rec[alias] = sum(nums)
                    elif fn == "AVG":
                        rec[alias] = (sum(nums) / len(nums)) if nums else None
                    elif fn == "MIN":
                        rec[alias] = min((v for v in vals if v is not None), default=None)
                    elif fn == "MAX":
                        rec[alias] = max((v for v in vals if v is not None), default=None)
                    else:
                        raise ValueError(f"Unsupported aggregate: {func}")
            out.append(rec)

        return out

    def get_relations_for_entity(
        self,
        relation_type: str,
        left_entity_type: str,
        entity_key: str,
        *,
        direction: str = "left",
    ) -> list[dict[str, Any]]:
        rows = self.query_relations(relation_type)
        if direction == "left":
            return [r for r in rows if r["left_key"] == entity_key]
        return [r for r in rows if r["right_key"] == entity_key]

    # --- Schema registry/versioning ---

    def get_schema(self, type_kind: str, type_name: str) -> dict[str, Any] | None:
        if self._is_type_dropped(type_kind, type_name):
            return None
        if (type_kind, type_name) in self._staged_schema_deletes:
            return None
        if (type_kind, type_name) in self._staged_schema_registry:
            return self._staged_schema_registry[(type_kind, type_name)]
        reg = self._read_registry()
        section = reg.get(type_kind, {})
        return section.get(type_name)

    def store_schema(self, type_kind: str, type_name: str, schema: dict[str, Any]) -> None:
        if self._tx_active:
            self._staged_schema_registry[(type_kind, type_name)] = dict(schema)
            self._staged_schema_deletes.discard((type_kind, type_name))
            self._staged_dropped_updates[(type_kind, type_name)] = None
            return

        reg = self._read_registry()
        reg.setdefault(type_kind, {})
        reg[type_kind][type_name] = dict(schema)
        self._write_registry(reg)
        self._ensure_type_catalog(type_kind, type_name)

    def list_schemas(self, type_kind: str) -> list[dict[str, Any]]:
        reg = self._read_registry()
        section = dict(reg.get(type_kind, {}))
        dropped = self._read_dropped_map().get(type_kind, {})
        for name in list(section.keys()):
            if name in dropped:
                del section[name]
        for kind, name in self._staged_schema_deletes:
            if kind == type_kind and name in section:
                del section[name]

        for (kind, name), schema in self._staged_schema_registry.items():
            if kind == type_kind:
                section[name] = schema

        return [{"type_name": n, "schema": s} for n, s in sorted(section.items())]

    def create_schema_version(
        self,
        type_kind: str,
        type_name: str,
        schema_json: str,
        schema_hash: str,
        runtime_id: str | None = None,
        reason: str | None = None,
    ) -> int:
        if self._tx_active:
            self._staged_schema_deletes.discard((type_kind, type_name))
            self._staged_dropped_updates[(type_kind, type_name)] = None
        else:
            self._clear_dropped_record(type_kind, type_name)
        persisted = self._load_schema_versions(type_kind, type_name)
        staged = self._staged_schema_versions.get((type_kind, type_name), [])
        next_id = len(persisted) + len(staged) + 1

        row = {
            "schema_version_id": next_id,
            "schema_json": schema_json,
            "schema_hash": schema_hash,
            "created_at": _now_iso(),
            "runtime_id": runtime_id,
            "reason": reason,
        }

        if self._tx_active:
            self._staged_schema_versions.setdefault((type_kind, type_name), []).append(row)
        else:
            persisted.append(row)
            self._write_schema_versions(type_kind, type_name, persisted)
            self._ensure_type_catalog(type_kind, type_name)

        return next_id

    def _flush_staged_schema_changes(self) -> None:
        if (
            not self._staged_schema_registry
            and not self._staged_schema_versions
            and not self._staged_schema_deletes
            and not self._staged_dropped_updates
        ):
            return

        self._ensure_lease_safe()
        reg = self._read_registry()
        for kind, name in self._staged_schema_deletes:
            section = reg.get(kind, {})
            if name in section:
                del section[name]
                reg[kind] = section
        for (kind, name), schema in self._staged_schema_registry.items():
            reg.setdefault(kind, {})
            reg[kind][name] = schema
        self._ensure_lease_safe()
        self._write_registry(reg)

        for kind, name in self._staged_schema_deletes:
            self._ensure_lease_safe()
            self._write_schema_versions(kind, name, [])
        for (kind, name), staged_rows in self._staged_schema_versions.items():
            self._ensure_lease_safe()
            persisted = self._load_schema_versions(kind, name)
            persisted.extend(staged_rows)
            self._write_schema_versions(kind, name, persisted)

        for kind, name in self._staged_schema_deletes:
            self._remove_type_from_catalog(kind, name)
        for kind, name in set(self._staged_schema_registry.keys()) | set(
            self._staged_schema_versions.keys()
        ):
            self._ensure_type_catalog(kind, name)

        if self._staged_dropped_updates:
            dropped = self._read_dropped_map()
            for (kind, name), rec in self._staged_dropped_updates.items():
                if rec is None:
                    if name in dropped.get(kind, {}):
                        del dropped[kind][name]
                else:
                    dropped.setdefault(kind, {})[name] = rec
            self._ensure_lease_safe()
            self._write_dropped_map(dropped)

    def get_current_schema_version(self, type_kind: str, type_name: str) -> dict[str, Any] | None:
        if self._is_type_dropped(type_kind, type_name):
            return None
        if (type_kind, type_name) in self._staged_schema_deletes:
            return None
        versions = self._load_schema_versions(type_kind, type_name)
        versions.extend(self._staged_schema_versions.get((type_kind, type_name), []))
        if not versions:
            return None
        return dict(versions[-1])

    def get_schema_version(
        self,
        type_kind: str,
        type_name: str,
        version_id: int,
    ) -> dict[str, Any] | None:
        if self._is_type_dropped(type_kind, type_name):
            return None
        if (type_kind, type_name) in self._staged_schema_deletes:
            return None
        versions = self._load_schema_versions(type_kind, type_name)
        versions.extend(self._staged_schema_versions.get((type_kind, type_name), []))
        for row in versions:
            if int(row["schema_version_id"]) == version_id:
                return dict(row)
        return None

    def list_schema_versions(self, type_kind: str, type_name: str) -> list[dict[str, Any]]:
        if self._is_type_dropped(type_kind, type_name):
            return []
        if (type_kind, type_name) in self._staged_schema_deletes:
            return []
        versions = self._load_schema_versions(type_kind, type_name)
        versions.extend(self._staged_schema_versions.get((type_kind, type_name), []))
        return [dict(v) for v in versions]

    def count_latest_entities(self, type_name: str) -> int:
        return len(self.query_entities(type_name))

    def count_latest_relations(self, type_name: str) -> int:
        return len(self.query_relations(type_name))

    def iter_latest_entities(
        self,
        type_name: str,
        batch_size: int = 1000,
    ) -> Iterator[list[tuple[str, dict[str, Any], int, int | None]]]:
        rows = self._entity_rows_raw(type_name)
        rows.sort(key=lambda r: r["key"])
        for i in range(0, len(rows), batch_size):
            chunk = rows[i : i + batch_size]
            yield [
                (r["key"], r["fields"], r["commit_id"], r.get("schema_version_id")) for r in chunk
            ]

    def iter_latest_relations(
        self,
        type_name: str,
        batch_size: int = 1000,
    ) -> Iterator[list[tuple[str, str, str, dict[str, Any], int, int | None]]]:
        rows = self._relation_rows_raw(type_name)
        rows.sort(key=lambda r: (r["left_key"], r["right_key"], r.get("instance_key", "")))
        for i in range(0, len(rows), batch_size):
            chunk = rows[i : i + batch_size]
            yield [
                (
                    r["left_key"],
                    r["right_key"],
                    r.get("instance_key", ""),
                    r["fields"],
                    r["commit_id"],
                    r.get("schema_version_id"),
                )
                for r in chunk
            ]

    def apply_schema_drop(
        self,
        *,
        affected_types: list[tuple[str, str]],
        purge_history: bool,
        commit_meta: dict[str, str] | None = None,
    ) -> int:
        """Apply schema-drop state for S3 backend and return admin commit ID."""
        owner = f"schema-drop-{self._config.runtime_id or 'runtime'}-{uuid.uuid4().hex[:8]}"
        if not self.acquire_lock(
            owner,
            timeout_ms=self._config.s3_lock_timeout_ms,
            lease_ms=self._config.s3_lease_ttl_ms,
        ):
            raise StorageBackendError("schema_drop", "Could not acquire write lock")

        try:
            with self._lease_keepalive(owner):
                self.begin_transaction()
                try:
                    commit_id = self.create_commit(commit_meta)
                    self.commit_transaction()
                except Exception:
                    self.rollback_transaction()
                    raise

                current_head = self.get_head_commit_id() or commit_id

                reg = self._read_registry()
                catalog = self._read_types_catalog(required=False)
                if catalog is None:
                    catalog = {"entities": [], "relations": [], "updated_at": _now_iso()}
                dropped = self._read_dropped_map()

                for tk, tn in sorted(set(affected_types)):
                    dropped.setdefault(tk, {})[tn] = {
                        "commit_id": commit_id,
                        "purged": purge_history,
                        "updated_at": _now_iso(),
                    }

                # Persist dropped markers first so crash interruption cannot hide a type
                # without recording dropped state.
                self._write_dropped_map(dropped)

                for tk, tn in sorted(set(affected_types)):
                    section = reg.get(tk, {})
                    if tn in section:
                        del section[tn]
                        reg[tk] = section

                    key = "entities" if tk == "entity" else "relations"
                    catalog[key] = [name for name in catalog.get(key, []) if name != tn]

                    self._write_schema_versions(tk, tn, [])
                    self._write_index(
                        tk,
                        _IndexDoc(type_name=tn, max_indexed_commit=current_head, entries=[]),
                    )

                if self.engine_version == "v2":
                    layout_catalog = self._read_type_layout_catalog()
                    layouts = [dict(v) for v in layout_catalog.get("layouts", [])]
                    for row in layouts:
                        for tk, tn in affected_types:
                            if str(row.get("type_kind")) == tk and str(row.get("type_name")) == tn:
                                row["is_current"] = False
                    layout_catalog["layouts"] = layouts
                    self._write_type_layout_catalog(layout_catalog)

                self._write_registry(reg)
                self._write_types_catalog(catalog)
                return commit_id
        finally:
            self.release_lock(owner)

    # --- S3 index/ops commands ---

    def index_verify(self) -> dict[str, Any]:
        head = self._read_head(required=True)
        assert head is not None
        head_commit = int(head.get("commit_id", 0))
        if head_commit == 0:
            return {"head_commit_id": 0, "lagged_types": [], "missing_latest": [], "ok": True}

        catalog = self._read_types_catalog(required=True)
        assert catalog is not None

        lagged: list[str] = []
        missing_latest: list[str] = []

        head_manifest_path = head.get("manifest_path")
        touched: dict[str, str] = {}
        if isinstance(head_manifest_path, str):
            manifest = self._read_manifest(head_manifest_path)
            for f in manifest.get("files", []):
                key = f"{f.get('kind')}:{f.get('type_name')}"
                touched[key] = str(f.get("path"))

        for kind, names in (
            ("entity", catalog.get("entities", [])),
            ("relation", catalog.get("relations", [])),
        ):
            for type_name in names:
                idx = self._read_index(kind, type_name)
                if idx.max_indexed_commit < head_commit:
                    lagged.append(f"{kind}:{type_name}")

                tkey = f"{kind}:{type_name}"
                touched_path = touched.get(tkey)
                if touched_path is None:
                    continue
                covering = [e for e in idx.entries if _entry_covers(e, head_commit)]
                if not covering:
                    missing_latest.append(tkey)
                    continue

                # For direct per-commit entries at head, ensure path match.
                per_commit = [
                    e
                    for e in covering
                    if int(e["min_commit_id"]) == head_commit
                    and int(e["max_commit_id"]) == head_commit
                ]
                if per_commit and not any(str(e["path"]) == touched_path for e in per_commit):
                    missing_latest.append(tkey)

        return {
            "head_commit_id": head_commit,
            "lagged_types": sorted(set(lagged)),
            "missing_latest": sorted(set(missing_latest)),
            "ok": not lagged and not missing_latest,
        }

    def _rebuild_index_for_type(self, kind: str, type_name: str, repair_head: int) -> _IndexDoc:
        entries: list[dict[str, Any]] = []
        head = self._read_head(required=True)
        assert head is not None

        start_path = head.get("manifest_path")
        if isinstance(start_path, str):
            for manifest in self._walk_manifest_chain(start_path=start_path):
                cid = int(manifest["commit_id"])
                if cid > repair_head:
                    continue
                for f in manifest.get("files", []):
                    if f.get("kind") == kind and f.get("type_name") == type_name:
                        entries.append(
                            {
                                "min_commit_id": cid,
                                "max_commit_id": cid,
                                "path": str(f["path"]),
                            }
                        )

        return _IndexDoc(type_name=type_name, max_indexed_commit=repair_head, entries=entries)

    def index_repair(self, *, apply: bool = False) -> dict[str, Any]:
        verify = self.index_verify()
        catalog = self._read_types_catalog(required=True)
        assert catalog is not None

        all_types: list[tuple[str, str]] = []
        all_types.extend(("entity", t) for t in catalog.get("entities", []))
        all_types.extend(("relation", t) for t in catalog.get("relations", []))

        planned: list[str] = []
        for kind, name in all_types:
            idx = self._read_index(kind, name)
            if idx.max_indexed_commit < int(verify["head_commit_id"]):
                planned.append(f"{kind}:{name}")
        planned.extend([str(v) for v in verify.get("missing_latest", [])])
        planned = sorted(set(planned))

        result: dict[str, Any] = {
            "head_commit_id": verify["head_commit_id"],
            "planned_types": planned,
            "applied": False,
        }

        if not apply:
            return result

        owner = f"index-repair-{self._config.runtime_id or 'runtime'}-{uuid.uuid4().hex[:8]}"
        if not self.acquire_lock(
            owner, timeout_ms=self._config.s3_lock_timeout_ms, lease_ms=self._config.s3_lease_ttl_ms
        ):
            raise StorageBackendError("index_repair", "Could not acquire write lock")

        try:
            with self._lease_keepalive(owner):
                repair_head = int(self._require_head()["commit_id"])
                verify_locked = self.index_verify()
                locked_planned = sorted(
                    set(
                        [str(v) for v in verify_locked.get("lagged_types", [])]
                        + [str(v) for v in verify_locked.get("missing_latest", [])]
                    )
                )

                # Re-check stability immediately before writing any index objects.
                stable_head = int(self._require_head()["commit_id"])
                if stable_head != repair_head:
                    raise HeadMismatchError(1)

                for item in locked_planned:
                    self._ensure_lease_safe()
                    kind, name = item.split(":", 1)
                    rebuilt = self._rebuild_index_for_type(kind, name, repair_head)
                    self._write_index(kind, rebuilt)

                result["applied"] = True
                result["repair_head"] = repair_head
                result["planned_types"] = locked_planned
                return result
        finally:
            self.release_lock(owner)

    def compact(self, *, type_name: str | None = None, apply: bool = False) -> dict[str, Any]:
        catalog = self._read_types_catalog(required=True)
        assert catalog is not None

        types: list[tuple[str, str]] = []
        for t in catalog.get("entities", []):
            if type_name is None or type_name == t:
                types.append(("entity", t))
        for t in catalog.get("relations", []):
            if type_name is None or type_name == t:
                types.append(("relation", t))

        plan_head = int(self._require_head()["commit_id"])
        plan: list[dict[str, Any]] = []
        for kind, name in types:
            idx = self._rebuild_index_for_type(kind, name, plan_head)
            per_commit = [
                e for e in idx.entries if int(e["min_commit_id"]) == int(e["max_commit_id"])
            ]
            if len(per_commit) <= 1:
                continue
            cmin = min(int(e["min_commit_id"]) for e in per_commit)
            cmax = max(int(e["max_commit_id"]) for e in per_commit)
            plan.append(
                {
                    "kind": kind,
                    "type_name": name,
                    "entry_count": len(per_commit),
                    "min_commit_id": cmin,
                    "max_commit_id": cmax,
                }
            )

        result: dict[str, Any] = {"planned": plan, "applied": False}
        if not apply:
            return result

        owner = f"compact-{self._config.runtime_id or 'runtime'}-{uuid.uuid4().hex[:8]}"
        if not self.acquire_lock(
            owner, timeout_ms=self._config.s3_lock_timeout_ms, lease_ms=self._config.s3_lease_ttl_ms
        ):
            raise StorageBackendError("compact", "Could not acquire write lock")

        try:
            with self._lease_keepalive(owner):
                head_start = int(self._require_head()["commit_id"])
                rewrites: list[dict[str, Any]] = []

                for item in plan:
                    self._ensure_lease_safe()
                    kind = item["kind"]
                    name = item["type_name"]
                    idx = self._rebuild_index_for_type(kind, name, head_start)
                    per_commit = [
                        e for e in idx.entries if int(e["min_commit_id"]) == int(e["max_commit_id"])
                    ]
                    if len(per_commit) <= 1:
                        continue

                    files = [
                        str(e["path"])
                        for e in sorted(per_commit, key=lambda e: int(e["min_commit_id"]))
                    ]
                    tables = [pq.read_table(self._download(p)) for p in files]
                    merged = pa.concat_tables(tables, promote=True)

                    cmin = min(int(e["min_commit_id"]) for e in per_commit)
                    cmax = max(int(e["max_commit_id"]) for e in per_commit)
                    kind_dir = "entities" if kind == "entity" else "relations"
                    snap_path = f"snapshots/{kind_dir}/{name}-{cmin}-{cmax}.parquet"
                    self._write_parquet_object(snap_path, merged)
                    rewrites.append(
                        {
                            "kind": kind,
                            "type_name": name,
                            "min_commit_id": cmin,
                            "max_commit_id": cmax,
                            "snapshot_path": snap_path,
                            "head_commit_id": head_start,
                        }
                    )

                # Validate head/lease stability before publishing index mutations.
                self._ensure_lease_safe()
                head_end = int(self._require_head()["commit_id"])
                if head_end != head_start:
                    raise HeadMismatchError(1)

                for rewrite in rewrites:
                    self._ensure_lease_safe()
                    kind = str(rewrite["kind"])
                    name = str(rewrite["type_name"])
                    self._write_index(
                        kind,
                        _IndexDoc(
                            type_name=name,
                            max_indexed_commit=int(rewrite["head_commit_id"]),
                            entries=[
                                {
                                    "min_commit_id": int(rewrite["min_commit_id"]),
                                    "max_commit_id": int(rewrite["max_commit_id"]),
                                    "path": str(rewrite["snapshot_path"]),
                                }
                            ],
                        ),
                    )

                result["applied"] = True
                return result
        finally:
            self.release_lock(owner)


class S3RepositoryV1(S3Repository):
    """S3 v1 repository wrapper."""

    def __init__(
        self,
        *,
        bucket: str,
        prefix: str,
        storage_uri: str,
        config: OntologiaConfig,
        allow_uninitialized: bool = False,
    ) -> None:
        super().__init__(
            bucket=bucket,
            prefix=prefix,
            storage_uri=storage_uri,
            config=config,
            allow_uninitialized=allow_uninitialized,
            engine_version="v1",
        )


class S3RepositoryV2(S3Repository):
    """S3 v2 repository wrapper."""

    def __init__(
        self,
        *,
        bucket: str,
        prefix: str,
        storage_uri: str,
        config: OntologiaConfig,
        allow_uninitialized: bool = False,
    ) -> None:
        super().__init__(
            bucket=bucket,
            prefix=prefix,
            storage_uri=storage_uri,
            config=config,
            allow_uninitialized=allow_uninitialized,
            engine_version="v2",
        )

    def initialize_storage(
        self,
        *,
        force: bool = False,
        token: str | None = None,
        dry_run: bool = True,
        engine_version: str | None = None,
    ) -> dict[str, Any]:
        return super().initialize_storage(
            force=force,
            token=token,
            dry_run=dry_run,
            engine_version=engine_version or "v2",
        )


S3Repository = S3RepositoryV1


__all__ = [
    "S3Repository",
    "S3RepositoryV1",
    "S3RepositoryV2",
    "detect_s3_engine_version",
]
