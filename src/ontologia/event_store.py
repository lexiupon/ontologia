"""Event store backends for SQLite and S3 namespaces."""

from __future__ import annotations

import json
import random
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Protocol
from urllib.parse import urlparse

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError, ParamValidationError

from ontologia.config import OntologiaConfig
from ontologia.errors import StorageBackendError
from ontologia.events import Event, EventDeadLetter


@dataclass(frozen=True)
class ClaimedEvent:
    """A claimed event and its lease state."""

    event: Event
    lease_until: datetime


class EventStore(Protocol):
    """Store protocol used by Session runtime and CLI."""

    def enqueue(self, event: Event, namespace: str) -> None: ...

    def claim(
        self,
        namespace: str,
        handler_id: str,
        session_id: str,
        event_types: list[str],
        limit: int,
        lease_ms: int,
        event_registry: dict[str, type[Event]],
    ) -> list[ClaimedEvent]: ...

    def ack(self, handler_id: str, event_id: str, namespace: str) -> None: ...

    def release(
        self,
        handler_id: str,
        event_id: str,
        namespace: str,
        *,
        error: str | None = None,
    ) -> None: ...

    def register_session(
        self,
        session_id: str,
        namespace: str,
        metadata: dict[str, Any],
    ) -> None: ...

    def heartbeat(self, session_id: str, namespace: str) -> None: ...

    def list_namespaces(self, *, session_ttl_ms: int) -> list[dict[str, Any]]: ...

    def list_sessions(self, namespace: str, *, session_ttl_ms: int) -> list[dict[str, Any]]: ...

    def list_events(self, namespace: str, *, limit: int) -> list[dict[str, Any]]: ...

    def list_dead_letters(self, namespace: str, *, limit: int = 100) -> list[dict[str, Any]]: ...

    def cleanup_events(self, namespace: str, *, before: datetime) -> int: ...

    def replay_event(self, namespace: str, event_id: str) -> str: ...

    def inspect_event(
        self,
        event_id: str,
        namespace: str | None = None,
    ) -> dict[str, Any] | None: ...


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _now_iso() -> str:
    return _now().isoformat()


def _parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _event_to_row(event: Event) -> dict[str, Any]:
    evt_id = event.id or str(uuid.uuid4())
    created_at = event.created_at or _now_iso()
    root_event_id = event.root_event_id or evt_id
    return {
        "id": evt_id,
        "type": event.__class__.__event_type__,
        "payload": json.dumps(event.model_dump(), sort_keys=True),
        "created_at": created_at,
        "priority": int(event.priority),
        "root_event_id": root_event_id,
        "chain_depth": int(event.chain_depth),
    }


def _row_to_event(
    row: dict[str, Any],
    registry: dict[str, type[Event]],
) -> Event:
    event_type = str(row["type"])
    event_cls = registry.get(event_type)
    if event_cls is None:
        raise StorageBackendError(
            "event_deserialize",
            f"No event class registered for '{event_type}'",
        )
    payload = row.get("payload")
    if isinstance(payload, str):
        data = json.loads(payload)
    elif isinstance(payload, dict):
        data = payload
    else:
        raise StorageBackendError("event_deserialize", f"Invalid payload for event '{event_type}'")

    event = event_cls.model_validate(data)
    event.id = str(row["id"])
    event.created_at = str(row["created_at"])
    event.priority = int(row["priority"])
    event.root_event_id = str(row["root_event_id"])
    event.chain_depth = int(row["chain_depth"])
    return event


class SQLiteEventStore:
    """SQLite-backed event store with claim/ack/retry semantics."""

    def __init__(self, conn: sqlite3.Connection, config: OntologiaConfig) -> None:
        self._conn = conn
        self._config = config
        self._create_tables()

    def _create_tables(self) -> None:
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS events (
                id TEXT PRIMARY KEY,
                namespace TEXT NOT NULL,
                type TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                priority INTEGER NOT NULL DEFAULT 100,
                root_event_id TEXT NOT NULL,
                chain_depth INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_events_namespace_type_order
                ON events(namespace, type, priority DESC, created_at ASC, id ASC);

            CREATE TABLE IF NOT EXISTS event_claims (
                event_id TEXT NOT NULL,
                handler_id TEXT NOT NULL,
                session_id TEXT NOT NULL,
                claimed_at TEXT NOT NULL,
                lease_until TEXT NOT NULL,
                ack_at TEXT,
                attempts INTEGER NOT NULL DEFAULT 0,
                available_at TEXT NOT NULL,
                last_error TEXT,
                dead_lettered_at TEXT,
                PRIMARY KEY (event_id, handler_id)
            );
            CREATE INDEX IF NOT EXISTS idx_event_claims_handler_state
                ON event_claims(handler_id, ack_at, dead_lettered_at, lease_until, available_at);
            CREATE INDEX IF NOT EXISTS idx_event_claims_event
                ON event_claims(event_id);

            CREATE TABLE IF NOT EXISTS dead_letters (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL,
                handler_id TEXT NOT NULL,
                namespace TEXT NOT NULL,
                failed_at TEXT NOT NULL,
                attempts INTEGER NOT NULL,
                last_error TEXT NOT NULL,
                event_type TEXT NOT NULL,
                event_payload TEXT NOT NULL,
                root_event_id TEXT NOT NULL,
                chain_depth INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_dead_letters_namespace_failed
                ON dead_letters(namespace, failed_at DESC);

            CREATE TABLE IF NOT EXISTS sessions (
                session_id TEXT PRIMARY KEY,
                namespace TEXT NOT NULL,
                started_at TEXT NOT NULL,
                last_heartbeat TEXT NOT NULL,
                metadata TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_sessions_heartbeat
                ON sessions(last_heartbeat);
            CREATE INDEX IF NOT EXISTS idx_sessions_namespace
                ON sessions(namespace);
            """
        )

    def enqueue(self, event: Event, namespace: str) -> None:
        started_in_tx = self._conn.in_transaction
        row = _event_to_row(event)
        self._conn.execute(
            """
            INSERT INTO events
                (id, namespace, type, payload, created_at, priority, root_event_id, chain_depth)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["id"],
                namespace,
                row["type"],
                row["payload"],
                row["created_at"],
                row["priority"],
                row["root_event_id"],
                row["chain_depth"],
            ),
        )
        if not started_in_tx:
            self._conn.commit()

    def claim(
        self,
        namespace: str,
        handler_id: str,
        session_id: str,
        event_types: list[str],
        limit: int,
        lease_ms: int,
        event_registry: dict[str, type[Event]],
    ) -> list[ClaimedEvent]:
        if limit <= 0 or not event_types:
            return []

        now_iso = _now_iso()
        lease_until_iso = (_now() + timedelta(milliseconds=lease_ms)).isoformat()
        placeholders = ", ".join("?" for _ in event_types)

        self._conn.execute("BEGIN IMMEDIATE")
        try:
            rows = self._conn.execute(
                f"""
                SELECT
                    e.id, e.type, e.payload, e.created_at, e.priority,
                    e.root_event_id, e.chain_depth
                FROM events e
                LEFT JOIN event_claims c
                    ON e.id = c.event_id AND c.handler_id = ?
                WHERE e.namespace = ?
                  AND e.type IN ({placeholders})
                  AND (
                      c.event_id IS NULL OR (
                          c.ack_at IS NULL
                          AND c.dead_lettered_at IS NULL
                          AND c.lease_until <= ?
                          AND c.available_at <= ?
                      )
                  )
                ORDER BY e.priority DESC, e.created_at ASC, e.id ASC
                LIMIT ?
                """,
                [handler_id, namespace, *event_types, now_iso, now_iso, limit],
            ).fetchall()

            claimed: list[ClaimedEvent] = []
            for row in rows:
                event_id = str(row[0])
                self._conn.execute(
                    """
                    INSERT INTO event_claims
                        (
                            event_id, handler_id, session_id, claimed_at,
                            lease_until, attempts, available_at
                        )
                    VALUES (?, ?, ?, ?, ?, 0, ?)
                    ON CONFLICT(event_id, handler_id) DO UPDATE SET
                        session_id = excluded.session_id,
                        claimed_at = excluded.claimed_at,
                        lease_until = excluded.lease_until
                    WHERE event_claims.ack_at IS NULL
                      AND event_claims.dead_lettered_at IS NULL
                      AND event_claims.lease_until <= excluded.claimed_at
                      AND event_claims.available_at <= excluded.claimed_at
                    """,
                    (event_id, handler_id, session_id, now_iso, lease_until_iso, now_iso),
                )

                claim_row = self._conn.execute(
                    """
                    SELECT session_id, claimed_at, lease_until
                    FROM event_claims
                    WHERE event_id = ? AND handler_id = ?
                    """,
                    (event_id, handler_id),
                ).fetchone()
                if claim_row is None:
                    continue
                if claim_row[0] != session_id or claim_row[1] != now_iso:
                    continue

                evt = _row_to_event(
                    {
                        "id": row[0],
                        "type": row[1],
                        "payload": row[2],
                        "created_at": row[3],
                        "priority": row[4],
                        "root_event_id": row[5],
                        "chain_depth": row[6],
                    },
                    event_registry,
                )
                claimed.append(ClaimedEvent(event=evt, lease_until=_parse_iso(str(claim_row[2]))))

            self._conn.commit()
            return claimed
        except Exception:
            self._conn.rollback()
            raise

    def ack(self, handler_id: str, event_id: str, namespace: str) -> None:
        # namespace is accepted for API consistency with the EventStore protocol
        # but is not needed for filtering — (event_id, handler_id) is the primary key.
        _ = namespace
        started_in_tx = self._conn.in_transaction
        self._conn.execute(
            """
            UPDATE event_claims
            SET ack_at = ?
            WHERE event_id = ? AND handler_id = ?
            """,
            (_now_iso(), event_id, handler_id),
        )
        if not started_in_tx:
            self._conn.commit()

    def release(
        self,
        handler_id: str,
        event_id: str,
        namespace: str,
        *,
        error: str | None = None,
    ) -> None:
        now_iso = _now_iso()
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            row = self._conn.execute(
                """
                SELECT
                    c.attempts,
                    e.type,
                    e.payload,
                    e.root_event_id,
                    e.chain_depth
                FROM event_claims c
                JOIN events e ON e.id = c.event_id
                WHERE c.event_id = ? AND c.handler_id = ? AND e.namespace = ?
                """,
                (event_id, handler_id, namespace),
            ).fetchone()
            if row is None:
                self._conn.commit()
                return

            attempts = int(row[0]) + 1
            event_type = str(row[1])
            payload = str(row[2])
            root_event_id = str(row[3])
            chain_depth = int(row[4])
            last_error = error or "handler failure"

            if attempts >= self._config.event_max_attempts:
                self._conn.execute(
                    """
                    UPDATE event_claims
                    SET attempts = ?,
                        last_error = ?,
                        dead_lettered_at = ?,
                        lease_until = ?,
                        available_at = ?
                    WHERE event_id = ? AND handler_id = ?
                    """,
                    (attempts, last_error, now_iso, now_iso, now_iso, event_id, handler_id),
                )
                self._conn.execute(
                    """
                    INSERT INTO dead_letters
                        (event_id, handler_id, namespace, failed_at, attempts, last_error,
                         event_type, event_payload, root_event_id, chain_depth)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event_id,
                        handler_id,
                        namespace,
                        now_iso,
                        attempts,
                        last_error,
                        event_type,
                        payload,
                        root_event_id,
                        chain_depth,
                    ),
                )

                dead_evt = EventDeadLetter(
                    event_id=event_id,
                    handler_id=handler_id,
                    attempts=attempts,
                    last_error=last_error,
                )
                dead_evt.root_event_id = root_event_id
                dead_evt.chain_depth = chain_depth + 1
                self.enqueue(dead_evt, namespace)
            else:
                jitter = random.randint(0, 100)
                backoff_ms = min(
                    self._config.event_backoff_base_ms * (2**attempts),
                    self._config.event_backoff_max_ms,
                )
                available_at = (_now() + timedelta(milliseconds=backoff_ms + jitter)).isoformat()
                self._conn.execute(
                    """
                    UPDATE event_claims
                    SET attempts = ?,
                        last_error = ?,
                        lease_until = ?,
                        available_at = ?
                    WHERE event_id = ? AND handler_id = ?
                    """,
                    (attempts, last_error, now_iso, available_at, event_id, handler_id),
                )

            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise

    def register_session(self, session_id: str, namespace: str, metadata: dict[str, Any]) -> None:
        now_iso = _now_iso()
        started_in_tx = self._conn.in_transaction
        self._conn.execute(
            """
            INSERT INTO sessions (session_id, namespace, started_at, last_heartbeat, metadata)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(session_id) DO UPDATE SET
                namespace = excluded.namespace,
                last_heartbeat = excluded.last_heartbeat,
                metadata = excluded.metadata
            """,
            (session_id, namespace, now_iso, now_iso, json.dumps(metadata, sort_keys=True)),
        )
        if not started_in_tx:
            self._conn.commit()

    def heartbeat(self, session_id: str, namespace: str) -> None:
        _ = namespace
        started_in_tx = self._conn.in_transaction
        self._conn.execute(
            "UPDATE sessions SET last_heartbeat = ? WHERE session_id = ?",
            (_now_iso(), session_id),
        )
        if not started_in_tx:
            self._conn.commit()

    def list_namespaces(self, *, session_ttl_ms: int) -> list[dict[str, Any]]:
        now = _now()
        rows = self._conn.execute("SELECT DISTINCT namespace FROM events").fetchall()
        sess_rows = self._conn.execute("SELECT DISTINCT namespace FROM sessions").fetchall()
        dl_rows = self._conn.execute("SELECT DISTINCT namespace FROM dead_letters").fetchall()
        namespaces = sorted({str(r[0]) for r in rows + sess_rows + dl_rows if r[0] is not None})

        result: list[dict[str, Any]] = []
        for ns in namespaces:
            pending = int(
                self._conn.execute(
                    "SELECT COUNT(*) FROM events WHERE namespace = ?",
                    (ns,),
                ).fetchone()[0]
            )
            dead_letters = int(
                self._conn.execute(
                    "SELECT COUNT(*) FROM dead_letters WHERE namespace = ?",
                    (ns,),
                ).fetchone()[0]
            )
            active_sessions = 0
            for (last_heartbeat,) in self._conn.execute(
                "SELECT last_heartbeat FROM sessions WHERE namespace = ?",
                (ns,),
            ).fetchall():
                hb = _parse_iso(str(last_heartbeat))
                if now - hb <= timedelta(milliseconds=session_ttl_ms):
                    active_sessions += 1

            result.append(
                {
                    "namespace": ns,
                    "sessions": active_sessions,
                    "pending_events": pending,
                    "dead_letters": dead_letters,
                }
            )
        return result

    def list_sessions(self, namespace: str, *, session_ttl_ms: int) -> list[dict[str, Any]]:
        now = _now()
        out: list[dict[str, Any]] = []
        rows = self._conn.execute(
            (
                "SELECT session_id, started_at, last_heartbeat, metadata "
                "FROM sessions WHERE namespace = ?"
            ),
            (namespace,),
        ).fetchall()
        for row in rows:
            last_heartbeat = _parse_iso(str(row[2]))
            is_dead = now - last_heartbeat > timedelta(milliseconds=session_ttl_ms)
            metadata = json.loads(row[3]) if row[3] else {}
            out.append(
                {
                    "session_id": str(row[0]),
                    "namespace": namespace,
                    "started_at": str(row[1]),
                    "last_heartbeat": str(row[2]),
                    "is_dead": is_dead,
                    "metadata": metadata,
                }
            )
        out.sort(key=lambda x: x["last_heartbeat"], reverse=True)
        return out

    def list_events(self, namespace: str, *, limit: int) -> list[dict[str, Any]]:
        now_iso = _now_iso()
        rows = self._conn.execute(
            """
            SELECT e.id, e.type, e.created_at, e.priority, e.payload,
                   MAX(CASE WHEN c.dead_lettered_at IS NOT NULL THEN 1 ELSE 0 END) AS dead,
                   MAX(CASE WHEN c.ack_at IS NOT NULL THEN 1 ELSE 0 END) AS ack,
                   MAX(CASE WHEN c.ack_at IS NULL AND c.dead_lettered_at IS NULL
                                AND c.lease_until > ? THEN 1 ELSE 0 END) AS claimed,
                   MAX(c.handler_id) AS any_handler
            FROM events e
            LEFT JOIN event_claims c ON e.id = c.event_id
            WHERE e.namespace = ?
            GROUP BY e.id, e.type, e.created_at, e.priority, e.payload
            ORDER BY e.priority DESC, e.created_at ASC, e.id ASC
            LIMIT ?
            """,
            (now_iso, namespace, limit),
        ).fetchall()

        out: list[dict[str, Any]] = []
        for row in rows:
            status = "pending"
            if int(row[5] or 0) > 0:
                status = "dead_lettered"
            elif int(row[6] or 0) > 0:
                status = "acked"
            elif int(row[7] or 0) > 0:
                status = "claimed"

            out.append(
                {
                    "id": str(row[0]),
                    "type": str(row[1]),
                    "created_at": str(row[2]),
                    "priority": int(row[3]),
                    "status": status,
                    "handler": str(row[8]) if row[8] else None,
                    "payload": json.loads(row[4]),
                }
            )
        return out

    def list_dead_letters(self, namespace: str, *, limit: int = 100) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            """
            SELECT event_id, event_type, handler_id, attempts, last_error, failed_at
            FROM dead_letters
            WHERE namespace = ?
            ORDER BY failed_at DESC
            LIMIT ?
            """,
            (namespace, limit),
        ).fetchall()
        return [
            {
                "event_id": str(r[0]),
                "type": str(r[1]),
                "handler_id": str(r[2]),
                "attempts": int(r[3]),
                "last_error": str(r[4]),
                "failed_at": str(r[5]),
            }
            for r in rows
        ]

    def cleanup_events(self, namespace: str, *, before: datetime) -> int:
        started_in_tx = self._conn.in_transaction
        cutoff = before.isoformat()
        rows = self._conn.execute(
            "SELECT id FROM events WHERE namespace = ? AND created_at < ?",
            (namespace, cutoff),
        ).fetchall()
        event_ids = [str(r[0]) for r in rows]
        if not event_ids:
            return 0

        placeholders = ", ".join("?" for _ in event_ids)
        self._conn.execute(
            f"DELETE FROM event_claims WHERE event_id IN ({placeholders})",
            event_ids,
        )
        self._conn.execute(
            f"DELETE FROM events WHERE id IN ({placeholders})",
            event_ids,
        )
        if not started_in_tx:
            self._conn.commit()
        return len(event_ids)

    def replay_event(self, namespace: str, event_id: str) -> str:
        started_in_tx = self._conn.in_transaction
        row = self._conn.execute(
            """
            SELECT type, payload, priority
            FROM events
            WHERE id = ? AND namespace = ?
            """,
            (event_id, namespace),
        ).fetchone()
        if row is None:
            raise StorageBackendError("replay_event", f"Event '{event_id}' not found")

        new_id = str(uuid.uuid4())
        created_at = _now_iso()
        self._conn.execute(
            """
            INSERT INTO events
                (id, namespace, type, payload, created_at, priority, root_event_id, chain_depth)
            VALUES (?, ?, ?, ?, ?, ?, ?, 0)
            """,
            (new_id, namespace, str(row[0]), str(row[1]), created_at, int(row[2]), new_id),
        )
        if not started_in_tx:
            self._conn.commit()
        return new_id

    def inspect_event(self, event_id: str, namespace: str | None = None) -> dict[str, Any] | None:
        if namespace is None:
            row = self._conn.execute(
                """
                SELECT
                    id, namespace, type, payload,
                    created_at, priority, root_event_id, chain_depth
                FROM events
                WHERE id = ?
                """,
                (event_id,),
            ).fetchone()
        else:
            row = self._conn.execute(
                """
                SELECT
                    id, namespace, type, payload,
                    created_at, priority, root_event_id, chain_depth
                FROM events
                WHERE id = ? AND namespace = ?
                """,
                (event_id, namespace),
            ).fetchone()
        if row is None:
            return None

        claims = self._conn.execute(
            """
            SELECT handler_id, session_id, attempts, last_error, dead_lettered_at, ack_at,
                   claimed_at, lease_until, available_at
            FROM event_claims
            WHERE event_id = ?
            ORDER BY handler_id ASC
            """,
            (event_id,),
        ).fetchall()

        return {
            "id": str(row[0]),
            "namespace": str(row[1]),
            "type": str(row[2]),
            "payload": json.loads(str(row[3])),
            "created_at": str(row[4]),
            "priority": int(row[5]),
            "root_event_id": str(row[6]),
            "chain_depth": int(row[7]),
            "claims": [
                {
                    "handler_id": str(c[0]),
                    "session_id": str(c[1]),
                    "attempts": int(c[2]),
                    "last_error": str(c[3]) if c[3] is not None else None,
                    "dead_lettered_at": str(c[4]) if c[4] is not None else None,
                    "ack_at": str(c[5]) if c[5] is not None else None,
                    "claimed_at": str(c[6]),
                    "lease_until": str(c[7]),
                    "available_at": str(c[8]),
                }
                for c in claims
            ],
        }


class _PreconditionFailed(Exception):
    pass


class S3EventStore:
    """S3-backed event store using CAS object updates for claims."""

    def __init__(self, datastore_uri: str, config: OntologiaConfig) -> None:
        parsed = urlparse(datastore_uri)
        if parsed.scheme != "s3":
            raise StorageBackendError("event_store", f"Invalid S3 URI '{datastore_uri}'")
        bucket = parsed.netloc
        prefix = parsed.path.lstrip("/").rstrip("/")
        if not bucket:
            raise StorageBackendError("event_store", f"Invalid S3 URI '{datastore_uri}'")

        self._bucket = bucket
        self._prefix = prefix
        self._config = config
        self._s3 = boto3.client(
            "s3",
            region_name=config.s3_region,
            endpoint_url=config.s3_endpoint_url,
            config=BotoConfig(
                connect_timeout=config.s3_request_timeout_s,
                read_timeout=config.s3_request_timeout_s,
                retries={"max_attempts": 5, "mode": "standard"},
            ),
        )

    def _k(self, rel: str) -> str:
        return f"{self._prefix}/{rel}" if self._prefix else rel

    def _event_key(self, namespace: str, event_id: str, created_at: str) -> str:
        ts = created_at.replace(":", "-")
        return self._k(f"events/{namespace}/{ts}_{event_id}.json")

    def _claim_key(self, namespace: str, event_id: str, handler_id: str) -> str:
        return self._k(f"claims/{namespace}/{event_id}/{handler_id}.json")

    def _dead_key(self, namespace: str, event_id: str, handler_id: str) -> str:
        return self._k(f"dead_letters/{namespace}/{event_id}/{handler_id}.json")

    def _session_key(self, namespace: str, session_id: str) -> str:
        return self._k(f"sessions/{namespace}/{session_id}.json")

    def _iter_keys(self, prefix: str) -> list[str]:
        paginator = self._s3.get_paginator("list_objects_v2")
        keys: list[str] = []
        for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
            for item in page.get("Contents", []):
                key = item.get("Key")
                if isinstance(key, str):
                    keys.append(key)
        return keys

    def _get_json(self, key: str) -> tuple[dict[str, Any] | None, str | None]:
        try:
            resp = self._s3.get_object(Bucket=self._bucket, Key=key)
            body = resp["Body"].read().decode("utf-8")
            etag = resp.get("ETag")
            return json.loads(body), etag if isinstance(etag, str) else None
        except Exception as e:
            if isinstance(e, ClientError):
                code = e.response.get("Error", {}).get("Code", "")
                if code in {"NoSuchKey", "404", "NotFound"}:
                    return None, None
            raise

    def _put_json(
        self,
        key: str,
        obj: dict[str, Any],
        *,
        if_match: str | None = None,
        if_none_match: str | None = None,
    ) -> str:
        kwargs: dict[str, Any] = {
            "Bucket": self._bucket,
            "Key": key,
            "Body": json.dumps(obj, sort_keys=True, separators=(",", ":")).encode("utf-8"),
            "ContentType": "application/json",
        }
        if if_match is not None:
            kwargs["IfMatch"] = if_match
        if if_none_match is not None:
            kwargs["IfNoneMatch"] = if_none_match

        try:
            resp = self._s3.put_object(**kwargs)
            etag = resp.get("ETag")
            return etag if isinstance(etag, str) else ""
        except ParamValidationError as e:
            raise StorageBackendError(
                "s3_conditional_write",
                "S3 endpoint does not support If-Match/If-None-Match for PUT",
            ) from e
        except Exception as e:
            if isinstance(e, ClientError):
                code = e.response.get("Error", {}).get("Code", "")
                if code in {"PreconditionFailed", "412"}:
                    raise _PreconditionFailed() from e
            raise

    def enqueue(self, event: Event, namespace: str) -> None:
        row = _event_to_row(event)
        payload = {
            "id": row["id"],
            "namespace": namespace,
            "type": row["type"],
            "payload": json.loads(str(row["payload"])),
            "created_at": row["created_at"],
            "priority": row["priority"],
            "root_event_id": row["root_event_id"],
            "chain_depth": row["chain_depth"],
        }
        self._put_json(self._event_key(namespace, str(row["id"]), str(row["created_at"])), payload)

    def claim(
        self,
        namespace: str,
        handler_id: str,
        session_id: str,
        event_types: list[str],
        limit: int,
        lease_ms: int,
        event_registry: dict[str, type[Event]],
    ) -> list[ClaimedEvent]:
        if limit <= 0 or not event_types:
            return []

        candidates: list[dict[str, Any]] = []
        for key in self._iter_keys(self._k(f"events/{namespace}/")):
            obj, _ = self._get_json(key)
            if obj is None:
                continue
            if str(obj.get("type")) not in event_types:
                continue
            candidates.append(obj)

        candidates.sort(
            key=lambda x: (
                -int(x.get("priority", 100)),
                str(x.get("created_at", "")),
                str(x.get("id", "")),
            )
        )

        now = _now()
        now_iso = now.isoformat()
        lease_until = (now + timedelta(milliseconds=lease_ms)).isoformat()

        out: list[ClaimedEvent] = []
        for cand in candidates:
            if len(out) >= limit:
                break

            event_id = str(cand["id"])
            claim_key = self._claim_key(namespace, event_id, handler_id)
            claim_obj, etag = self._get_json(claim_key)

            write_obj = {
                "event_id": event_id,
                "handler_id": handler_id,
                "session_id": session_id,
                "claimed_at": now_iso,
                "lease_until": lease_until,
                "ack_at": None,
                "attempts": int(claim_obj.get("attempts", 0)) if claim_obj else 0,
                "available_at": now_iso,
                "last_error": None,
                "dead_lettered_at": None,
            }

            try:
                if claim_obj is None:
                    self._put_json(claim_key, write_obj, if_none_match="*")
                else:
                    if (
                        claim_obj.get("ack_at") is not None
                        or claim_obj.get("dead_lettered_at") is not None
                    ):
                        continue
                    lease_old = _parse_iso(str(claim_obj.get("lease_until", now_iso)))
                    avail_old = _parse_iso(str(claim_obj.get("available_at", now_iso)))
                    if now < lease_old or now < avail_old:
                        continue
                    if etag is None:
                        continue
                    write_obj["attempts"] = int(claim_obj.get("attempts", 0))
                    write_obj["available_at"] = str(claim_obj.get("available_at", now_iso))
                    self._put_json(claim_key, write_obj, if_match=etag)
            except _PreconditionFailed:
                continue

            evt = _row_to_event(
                {
                    "id": cand["id"],
                    "type": cand["type"],
                    "payload": cand["payload"],
                    "created_at": cand["created_at"],
                    "priority": cand.get("priority", 100),
                    "root_event_id": cand.get("root_event_id", cand["id"]),
                    "chain_depth": cand.get("chain_depth", 0),
                },
                event_registry,
            )
            out.append(ClaimedEvent(event=evt, lease_until=_parse_iso(lease_until)))

        return out

    def ack(self, handler_id: str, event_id: str, namespace: str) -> None:
        claim_key = self._claim_key(namespace, event_id, handler_id)
        claim_obj, etag = self._get_json(claim_key)
        if claim_obj is None or etag is None:
            return
        claim_obj["ack_at"] = _now_iso()
        try:
            self._put_json(claim_key, claim_obj, if_match=etag)
        except _PreconditionFailed:
            return

    def release(
        self,
        handler_id: str,
        event_id: str,
        namespace: str,
        *,
        error: str | None = None,
    ) -> None:
        claim_key = self._claim_key(namespace, event_id, handler_id)
        claim_obj, etag = self._get_json(claim_key)
        if claim_obj is None or etag is None:
            return

        attempts = int(claim_obj.get("attempts", 0)) + 1
        claim_obj["attempts"] = attempts
        claim_obj["last_error"] = error or "handler failure"
        claim_obj["lease_until"] = _now_iso()

        if attempts >= self._config.event_max_attempts:
            claim_obj["dead_lettered_at"] = _now_iso()
            dead = {
                "event_id": event_id,
                "handler_id": handler_id,
                "namespace": namespace,
                "failed_at": claim_obj["dead_lettered_at"],
                "attempts": attempts,
                "last_error": claim_obj["last_error"],
            }
            self._put_json(self._dead_key(namespace, event_id, handler_id), dead)
            dead_evt = EventDeadLetter(
                event_id=event_id,
                handler_id=handler_id,
                attempts=attempts,
                last_error=str(claim_obj["last_error"]),
            )
            self.enqueue(dead_evt, namespace)
        else:
            jitter = random.randint(0, 100)
            backoff_ms = min(
                self._config.event_backoff_base_ms * (2**attempts),
                self._config.event_backoff_max_ms,
            )
            claim_obj["available_at"] = (
                _now() + timedelta(milliseconds=backoff_ms + jitter)
            ).isoformat()

        try:
            self._put_json(claim_key, claim_obj, if_match=etag)
        except _PreconditionFailed:
            return

    def register_session(self, session_id: str, namespace: str, metadata: dict[str, Any]) -> None:
        now = _now_iso()
        obj = {
            "session_id": session_id,
            "namespace": namespace,
            "started_at": now,
            "last_heartbeat": now,
            "metadata": metadata,
        }
        self._put_json(self._session_key(namespace, session_id), obj)

    def heartbeat(self, session_id: str, namespace: str) -> None:
        key = self._session_key(namespace, session_id)
        obj, etag = self._get_json(key)
        if obj is None:
            self.register_session(session_id, namespace, {})
            return
        obj["last_heartbeat"] = _now_iso()
        if etag is None:
            self._put_json(key, obj)
        else:
            try:
                self._put_json(key, obj, if_match=etag)
            except _PreconditionFailed:
                return

    def list_namespaces(self, *, session_ttl_ms: int) -> list[dict[str, Any]]:
        _ = session_ttl_ms
        ns: set[str] = set()
        for key in self._iter_keys(self._k("events/")):
            rel = key[len(self._k("events/")) :]
            parts = rel.split("/")
            if parts and parts[0]:
                ns.add(parts[0])
        for key in self._iter_keys(self._k("sessions/")):
            rel = key[len(self._k("sessions/")) :]
            parts = rel.split("/")
            if parts and parts[0]:
                ns.add(parts[0])

        out: list[dict[str, Any]] = []
        for namespace in sorted(ns):
            event_count = len(self._iter_keys(self._k(f"events/{namespace}/")))
            dead_count = len(self._iter_keys(self._k(f"dead_letters/{namespace}/")))
            sessions = len(self._iter_keys(self._k(f"sessions/{namespace}/")))
            out.append(
                {
                    "namespace": namespace,
                    "sessions": sessions,
                    "pending_events": event_count,
                    "dead_letters": dead_count,
                }
            )
        return out

    def list_sessions(self, namespace: str, *, session_ttl_ms: int) -> list[dict[str, Any]]:
        now = _now()
        out: list[dict[str, Any]] = []
        for key in self._iter_keys(self._k(f"sessions/{namespace}/")):
            obj, _ = self._get_json(key)
            if obj is None:
                continue
            hb = _parse_iso(str(obj.get("last_heartbeat", _now_iso())))
            is_dead = now - hb > timedelta(milliseconds=session_ttl_ms)
            out.append(
                {
                    "session_id": str(obj.get("session_id", "")),
                    "namespace": namespace,
                    "started_at": str(obj.get("started_at", "")),
                    "last_heartbeat": str(obj.get("last_heartbeat", "")),
                    "is_dead": is_dead,
                    "metadata": obj.get("metadata", {}),
                }
            )
        out.sort(key=lambda x: x["last_heartbeat"], reverse=True)
        return out

    def list_events(self, namespace: str, *, limit: int) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        now = _now()
        for key in self._iter_keys(self._k(f"events/{namespace}/")):
            obj, _ = self._get_json(key)
            if obj is None:
                continue
            event_id = str(obj.get("id", ""))
            status = "pending"
            any_handler: str | None = None
            for claim_key in self._iter_keys(self._k(f"claims/{namespace}/{event_id}/")):
                claim, _ = self._get_json(claim_key)
                if claim is None:
                    continue
                any_handler = str(claim.get("handler_id", "")) or any_handler
                if claim.get("dead_lettered_at"):
                    status = "dead_lettered"
                    break
                if claim.get("ack_at"):
                    status = "acked"
                elif status == "pending":
                    lease_until = _parse_iso(str(claim.get("lease_until", _now_iso())))
                    if lease_until > now:
                        status = "claimed"
            out.append(
                {
                    "id": event_id,
                    "type": str(obj.get("type", "")),
                    "created_at": str(obj.get("created_at", "")),
                    "priority": int(obj.get("priority", 100)),
                    "status": status,
                    "handler": any_handler,
                    "payload": obj.get("payload", {}),
                }
            )
        out.sort(key=lambda x: (-int(x["priority"]), str(x["created_at"]), str(x["id"])))
        return out[:limit]

    def list_dead_letters(self, namespace: str, *, limit: int = 100) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for key in self._iter_keys(self._k(f"dead_letters/{namespace}/")):
            obj, _ = self._get_json(key)
            if obj is None:
                continue
            out.append(
                {
                    "event_id": str(obj.get("event_id", "")),
                    "type": str(obj.get("event_type", "")),
                    "handler_id": str(obj.get("handler_id", "")),
                    "attempts": int(obj.get("attempts", 0)),
                    "last_error": str(obj.get("last_error", "")),
                    "failed_at": str(obj.get("failed_at", "")),
                }
            )
        out.sort(key=lambda x: x["failed_at"], reverse=True)
        return out[:limit]

    def cleanup_events(self, namespace: str, *, before: datetime) -> int:
        deleted = 0
        cutoff = before.isoformat()
        for key in self._iter_keys(self._k(f"events/{namespace}/")):
            obj, _ = self._get_json(key)
            if obj is None:
                continue
            created_at = str(obj.get("created_at", ""))
            if created_at and created_at < cutoff:
                self._s3.delete_object(Bucket=self._bucket, Key=key)
                deleted += 1
                event_id = str(obj.get("id", ""))
                for claim_key in self._iter_keys(self._k(f"claims/{namespace}/{event_id}/")):
                    self._s3.delete_object(Bucket=self._bucket, Key=claim_key)
        return deleted

    def replay_event(self, namespace: str, event_id: str) -> str:
        evt = self.inspect_event(event_id, namespace=namespace)
        if evt is None:
            raise StorageBackendError("replay_event", f"Event '{event_id}' not found")
        new_id = str(uuid.uuid4())
        payload = {
            "id": new_id,
            "namespace": namespace,
            "type": evt["type"],
            "payload": evt["payload"],
            "created_at": _now_iso(),
            "priority": evt["priority"],
            "root_event_id": new_id,
            "chain_depth": 0,
        }
        self._put_json(
            self._event_key(namespace, new_id, str(payload["created_at"])),
            payload,
        )
        return new_id

    def inspect_event(self, event_id: str, namespace: str | None = None) -> dict[str, Any] | None:
        search_prefix = self._k(f"events/{namespace}/") if namespace else self._k("events/")
        for key in self._iter_keys(search_prefix):
            obj, _ = self._get_json(key)
            if obj is None or str(obj.get("id")) != event_id:
                continue
            ns = str(obj.get("namespace", namespace or ""))
            claims: list[dict[str, Any]] = []
            for claim_key in self._iter_keys(self._k(f"claims/{ns}/{event_id}/")):
                c, _ = self._get_json(claim_key)
                if c is not None:
                    claims.append(c)
            return {
                "id": event_id,
                "namespace": ns,
                "type": str(obj.get("type", "")),
                "payload": obj.get("payload", {}),
                "created_at": str(obj.get("created_at", "")),
                "priority": int(obj.get("priority", 100)),
                "root_event_id": str(obj.get("root_event_id", "")),
                "chain_depth": int(obj.get("chain_depth", 0)),
                "claims": claims,
            }
        return None


def create_event_store(
    *,
    datastore_uri: str,
    repo: Any,
    config: OntologiaConfig,
) -> EventStore:
    info = repo.storage_info()
    backend = str(info.get("backend", ""))
    if backend == "sqlite":
        conn = getattr(repo, "_conn", None)
        if not isinstance(conn, sqlite3.Connection):
            raise StorageBackendError("event_store", "SQLite repository connection not available")
        return SQLiteEventStore(conn, config)
    if backend == "s3":
        return S3EventStore(datastore_uri, config)
    raise StorageBackendError("event_store", f"Unsupported backend '{backend}'")
