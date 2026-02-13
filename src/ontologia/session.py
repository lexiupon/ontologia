"""RFC 0005 session runtime with explicit commit and event bus processing."""

from __future__ import annotations

import os
import socket
import time
import uuid
from collections.abc import Callable
from collections.abc import Iterable as ABCIterable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from ontologia.config import OntologiaConfig
from ontologia.errors import (
    BatchSizeExceededError,
    EventLoopLimitError,
    HandlerError,
    LeaseExpiredError,
    LockContentionError,
)
from ontologia.event_handlers import EventHandlerMeta, HandlerContext
from ontologia.event_store import ClaimedEvent, EventStore, create_event_store
from ontologia.events import Event, EventDeadLetter, Schedule
from ontologia.intents import Intent
from ontologia.query import QueryBuilder
from ontologia.runtime import Ontology
from ontologia.runtime import Session as LegacySession
from ontologia.storage import parse_storage_target
from ontologia.types import Entity, Relation


@dataclass(frozen=True)
class _HandlerEntry:
    func: Callable[[HandlerContext[Any]], None]
    meta: EventHandlerMeta


@dataclass(frozen=True)
class _CronSpec:
    minutes: set[int]
    hours: set[int]
    days: set[int]
    months: set[int]
    weekdays: set[int]


@dataclass
class _ScheduleState:
    schedule: Schedule
    cron: _CronSpec
    next_fire: datetime


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_cron_field(field: str, minimum: int, maximum: int) -> set[int]:
    values: set[int] = set()
    parts = field.split(",")
    for part in parts:
        token = part.strip()
        if token == "*":
            values.update(range(minimum, maximum + 1))
            continue
        if token.startswith("*/"):
            step = int(token[2:])
            if step <= 0:
                raise ValueError(f"invalid cron step '{token}'")
            values.update(range(minimum, maximum + 1, step))
            continue
        if "-" in token:
            lo_str, hi_str = token.split("-", 1)
            lo = int(lo_str)
            hi = int(hi_str)
            if lo > hi:
                raise ValueError(f"invalid cron range '{token}'")
            if lo < minimum or hi > maximum:
                raise ValueError(f"cron range out of bounds '{token}'")
            values.update(range(lo, hi + 1))
            continue
        value = int(token)
        if value < minimum or value > maximum:
            raise ValueError(f"cron value out of bounds '{token}'")
        values.add(value)
    return values


def _compile_cron(expr: str) -> _CronSpec:
    parts = expr.split()
    if len(parts) != 5:
        raise ValueError(f"cron expression must have 5 fields: '{expr}'")

    return _CronSpec(
        minutes=_parse_cron_field(parts[0], 0, 59),
        hours=_parse_cron_field(parts[1], 0, 23),
        days=_parse_cron_field(parts[2], 1, 31),
        months=_parse_cron_field(parts[3], 1, 12),
        weekdays=_parse_cron_field(parts[4], 0, 7),
    )


def _cron_matches(spec: _CronSpec, dt: datetime) -> bool:
    # Cron weekday: 0/7=Sunday, 1=Monday, ..., 6=Saturday.
    cron_weekday = (dt.weekday() + 1) % 7
    weekday_match = cron_weekday in spec.weekdays or (cron_weekday == 0 and 7 in spec.weekdays)

    return (
        dt.minute in spec.minutes
        and dt.hour in spec.hours
        and dt.day in spec.days
        and dt.month in spec.months
        and weekday_match
    )


def _next_fire(spec: _CronSpec, after: datetime) -> datetime:
    candidate = after.replace(second=0, microsecond=0) + timedelta(minutes=1)
    for _ in range(0, 366 * 24 * 60):
        if _cron_matches(spec, candidate):
            return candidate
        candidate += timedelta(minutes=1)
    raise ValueError("unable to find next cron trigger within one year")


class Session:
    """Session API introduced by RFC 0005."""

    def __init__(
        self,
        datastore_uri: str,
        namespace: str | None = None,
        *,
        entity_types: list[type[Entity]] | None = None,
        relation_types: list[type[Relation[Any, Any]]] | None = None,
        instance_metadata: dict[str, Any] | None = None,
        config: OntologiaConfig | None = None,
    ) -> None:
        self._config = config or OntologiaConfig()
        self.namespace = namespace or self._config.default_namespace
        self.session_id = str(uuid.uuid4())

        storage_uri: str | None = None
        db_path: str | None = None
        if "://" in datastore_uri:
            storage_uri = datastore_uri
        else:
            db_path = datastore_uri

        target = parse_storage_target(db_path=db_path, storage_uri=storage_uri)
        self.datastore_uri = target.uri

        self._ontology = Ontology(
            db_path=db_path,
            storage_uri=storage_uri,
            config=self._config,
            entity_types=entity_types,
            relation_types=relation_types,
        )

        self._repo = self._ontology.repo
        self._event_store: EventStore = create_event_store(
            datastore_uri=self.datastore_uri,
            repo=self._repo,
            config=self._config,
        )

        self._intents: list[Intent] = []
        self._stop_requested = False
        self._legacy_session: LegacySession | None = None

        metadata = {
            "hostname": socket.gethostname(),
            "pid": os.getpid(),
            "namespace": self.namespace,
        }
        if instance_metadata:
            metadata.update(instance_metadata)
        self._instance_metadata = metadata

    def _ensure_schema_validated(self) -> None:
        if self._ontology._schema_validated:
            return
        if not (self._ontology._entity_types or self._ontology._relation_types):
            return
        self._ontology.validate()

    def __enter__(self) -> Session:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: Any,
    ) -> None:
        """Exit the context manager, auto-committing on clean exit.

        When no exception occurred, any pending ``ensure()`` intents are
        committed automatically.  This is required for backward compatibility
        with imperative mode (``with session: session.ensure(...)``).

        In event-driven mode, handlers should call ``ctx.commit()``
        explicitly — intents that are not committed inside a handler are
        cleared before the next handler executes and will **not** leak into
        this auto-commit.
        """
        if exc_type is None:
            self.commit()

    def close(self) -> None:
        self._ontology.close()

    @property
    def repo(self) -> Any:
        """Expose backend repository for low-level tests and tooling."""
        return self._repo

    @property
    def _schema_version_ids(self) -> dict[str, int]:
        """Compatibility access to validated schema version map."""
        return self._ontology._schema_version_ids

    def session(self) -> Session:
        """Compatibility API mirroring Ontology.session()."""
        self._ensure_schema_validated()
        return self

    def stop(self) -> None:
        self._stop_requested = True

    def validate(self) -> None:
        """Validate code-defined schemas against stored schemas."""
        self._ontology.validate()

    def migrate(self, *args: Any, **kwargs: Any) -> Any:
        """Pass-through migration API for compatibility with Ontology workflows."""
        return self._ontology.migrate(*args, **kwargs)

    def ensure(
        self,
        obj: Entity | Relation[Any, Any] | ABCIterable[Entity | Relation[Any, Any]],
    ) -> None:
        if isinstance(obj, (Entity, Relation)):
            self._intents.append(Intent(obj))
            return

        if isinstance(obj, ABCIterable) and not isinstance(obj, (str, bytes)):
            for item in obj:
                if not isinstance(item, (Entity, Relation)):
                    raise TypeError(f"Expected Entity or Relation, got {type(item)}")
                self._intents.append(Intent(item))
            return

        raise TypeError(
            f"Expected Entity, Relation, or Iterable of Entity/Relation, got {type(obj)}"
        )

    def query(self) -> QueryBuilder:
        return self._ontology.query()

    def list_commits(
        self,
        *,
        limit: int = 10,
        since_commit_id: int | None = None,
    ) -> list[dict[str, Any]]:
        return self._ontology.list_commits(limit=limit, since_commit_id=since_commit_id)

    def get_commit(self, commit_id: int) -> dict[str, Any] | None:
        return self._ontology.get_commit(commit_id)

    def list_commit_changes(self, commit_id: int) -> list[dict[str, Any]]:
        return self._repo.list_commit_changes(commit_id)

    def commit(
        self,
        *,
        event: Event | None = None,
    ) -> int | None:
        self._ensure_schema_validated()
        return self._commit_internal(
            event=event,
            commit_meta={},
            parent_event=None,
            lease_until=None,
        )

    def _commit_from_handler(
        self,
        ctx: HandlerContext[Any],
        *,
        event: Event | None,
        commit_meta: dict[str, str],
    ) -> int | None:
        return self._commit_internal(
            event=event,
            commit_meta=commit_meta,
            parent_event=ctx.event,
            lease_until=ctx.lease_until,
        )

    def _prepare_event(self, event: Event, *, parent_event: Event | None) -> Event:
        event_id = event.id or str(uuid.uuid4())
        event.id = event_id
        event.created_at = _now().isoformat()

        if parent_event is None:
            event.root_event_id = event.root_event_id or event_id
            event.chain_depth = 0
            return event

        root_id = parent_event.root_event_id or parent_event.id or event_id
        chain_depth = int(parent_event.chain_depth) + 1
        if chain_depth > self._config.max_event_chain_depth:
            raise EventLoopLimitError(chain_depth, self._config.max_event_chain_depth)
        event.root_event_id = root_id
        event.chain_depth = chain_depth
        return event

    def _compute_entity_delta(self, entity: Entity) -> dict[str, Any] | None:
        type_name = entity.__entity_name__
        pk_field = entity._primary_key_field
        key = getattr(entity, pk_field)
        fields = entity.model_dump()

        current = self._repo.get_latest_entity(type_name, str(key))
        if current is None or current["fields"] != fields:
            return {
                "kind": "entity",
                "type_name": type_name,
                "key": str(key),
                "fields": fields,
            }
        return None

    def _compute_relation_delta(self, relation: Relation[Any, Any]) -> dict[str, Any] | None:
        type_name = relation.__relation_name__
        left_key = relation.left_key
        right_key = relation.right_key
        instance_key = relation.instance_key
        fields = relation.model_dump()

        current = self._repo.get_latest_relation(
            type_name, left_key, right_key, instance_key=instance_key
        )
        if current is None or current["fields"] != fields:
            return {
                "kind": "relation",
                "type_name": type_name,
                "left_key": left_key,
                "right_key": right_key,
                "instance_key": instance_key,
                "fields": fields,
            }
        return None

    def _commit_internal(
        self,
        *,
        event: Event | None,
        commit_meta: dict[str, str],
        parent_event: Event | None,
        lease_until: datetime | None,
    ) -> int | None:
        if lease_until is not None and _now() > lease_until:
            raise LeaseExpiredError()

        if not self._intents and event is None:
            return None

        if len(self._intents) > self._config.max_batch_size:
            raise BatchSizeExceededError(len(self._intents), self._config.max_batch_size)

        intents = list(self._intents)
        self._intents.clear()

        timeout_ms = self._config.s3_lock_timeout_ms
        if not self._repo.acquire_lock(self.session_id, timeout_ms=timeout_ms):
            raise LockContentionError(timeout_ms)

        changes: list[dict[str, Any]] = []
        for intent in intents:
            obj = intent.obj
            if isinstance(obj, Entity):
                change = self._compute_entity_delta(obj)
            else:
                change = self._compute_relation_delta(obj)
            if change is not None:
                changes.append(change)

        metadata = {"namespace": self.namespace}
        metadata.update(commit_meta)

        commit_id: int | None = None
        backend = str(self._repo.storage_info().get("backend", ""))

        try:
            self._repo.begin_transaction()
            if changes:
                self._ontology._assert_no_schema_drift(changes)
                commit_id = self._repo.create_commit(metadata)
                for change in changes:
                    svid = self._ontology._schema_version_ids.get(change["type_name"])
                    if change["kind"] == "entity":
                        self._repo.insert_entity(
                            change["type_name"],
                            change["key"],
                            change["fields"],
                            commit_id,
                            schema_version_id=svid,
                        )
                    else:
                        self._repo.insert_relation(
                            change["type_name"],
                            change["left_key"],
                            change["right_key"],
                            change["fields"],
                            commit_id,
                            schema_version_id=svid,
                            instance_key=change.get("instance_key", ""),
                        )

            if event is not None and backend == "sqlite":
                prepared = self._prepare_event(event, parent_event=parent_event)
                self._event_store.enqueue(prepared, self.namespace)

            self._repo.commit_transaction()
        except Exception:
            self._repo.rollback_transaction()
            raise
        finally:
            try:
                self._repo.release_lock(self.session_id)
            except Exception:
                pass

        if event is not None and backend != "sqlite":
            prepared = self._prepare_event(event, parent_event=parent_event)
            self._event_store.enqueue(prepared, self.namespace)

        return commit_id

    def _build_handlers(
        self,
        handlers: list[Callable[..., Any]],
    ) -> list[_HandlerEntry]:
        entries: list[_HandlerEntry] = []
        seen: set[str] = set()

        for func in handlers:
            if not callable(func):
                raise HandlerError(f"Handler must be callable, got {type(func)}")

            meta = getattr(func, "_ontologia_event_handler", None)
            if not isinstance(meta, EventHandlerMeta):
                raise HandlerError(
                    f"Function {getattr(func, '__qualname__', func)} "
                    "is not decorated with @on_event"
                )

            if meta.handler_id in seen:
                raise HandlerError(f"Duplicate handler: {meta.handler_id}")
            seen.add(meta.handler_id)

            entries.append(_HandlerEntry(func=func, meta=meta))

        entries.sort(key=lambda e: (-e.meta.priority, e.meta.handler_id))
        return entries

    def _clone_event(self, event: Event) -> Event:
        cloned = event.__class__.model_validate(event.model_dump())
        cloned.priority = event.priority
        return cloned

    def _flush_buffered_events(self, buffered: list[Event], parent_event: Event) -> None:
        for out_evt in buffered:
            prepared = self._prepare_event(out_evt, parent_event=parent_event)
            self._event_store.enqueue(prepared, self.namespace)

    def run(
        self,
        handlers: list[Callable[..., Any]],
        *,
        schedules: list[Schedule] | None = None,
        max_iterations: int | None = None,
    ) -> None:
        self._ensure_schema_validated()

        legacy_handlers = [h for h in handlers if hasattr(h, "_ontologia_handler")]
        event_handlers = [h for h in handlers if hasattr(h, "_ontologia_event_handler")]

        if legacy_handlers and event_handlers:
            raise HandlerError(
                "Cannot mix legacy @on_commit/@on_schedule handlers with @on_event handlers"
            )

        if legacy_handlers:
            if len(legacy_handlers) != len(handlers):
                raise HandlerError(
                    "All handlers must use the same decorator style in a single run() call"
                )
            if schedules is not None:
                raise HandlerError("Legacy handlers do not support schedules= in Session.run()")
            if max_iterations is not None:
                raise HandlerError("Legacy handlers do not support max_iterations in Session.run()")

            if self._legacy_session is None:
                self._legacy_session = self._ontology.session()

            if self._intents:
                for intent in self._intents:
                    self._legacy_session.ensure(intent.obj)
                self._intents.clear()

            self._legacy_session.run(handlers)
            return

        handler_entries = self._build_handlers(handlers)

        event_registry: dict[str, type[Event]] = {
            EventDeadLetter.__event_type__: EventDeadLetter,
        }
        for entry in handler_entries:
            event_registry[entry.meta.event_cls.__event_type__] = entry.meta.event_cls

        schedule_states: list[_ScheduleState] = []
        for schedule in schedules or []:
            spec = _compile_cron(schedule.cron)
            schedule_states.append(
                _ScheduleState(
                    schedule=schedule,
                    cron=spec,
                    next_fire=_next_fire(spec, _now()),
                )
            )
            event_registry[schedule.event.__class__.__event_type__] = schedule.event.__class__

        self._event_store.register_session(self.session_id, self.namespace, self._instance_metadata)

        heartbeat_interval = timedelta(milliseconds=self._config.session_heartbeat_interval_ms)
        poll_interval = self._config.event_poll_interval_ms / 1000.0
        next_heartbeat = _now()

        self._stop_requested = False

        # Track outstanding claims so they can be released on graceful shutdown.
        outstanding_claims: list[tuple[str, str]] = []  # (handler_id, event_id)

        try:
            iterations = 0
            while not self._stop_requested:
                if max_iterations is not None and iterations >= max_iterations:
                    break
                now = _now()
                if now >= next_heartbeat:
                    self._event_store.heartbeat(self.session_id, self.namespace)
                    next_heartbeat = now + heartbeat_interval

                for state in schedule_states:
                    while now >= state.next_fire:
                        evt = self._clone_event(state.schedule.event)
                        prepared = self._prepare_event(evt, parent_event=None)
                        self._event_store.enqueue(prepared, self.namespace)
                        state.next_fire = _next_fire(state.cron, state.next_fire)

                processed = 0
                for entry in handler_entries:
                    if processed >= self._config.max_events_per_iteration:
                        break

                    remaining = self._config.max_events_per_iteration - processed
                    claim_limit = min(self._config.event_claim_limit, remaining)

                    claimed: list[ClaimedEvent] = self._event_store.claim(
                        self.namespace,
                        entry.meta.handler_id,
                        self.session_id,
                        [entry.meta.event_cls.__event_type__],
                        claim_limit,
                        self._config.event_claim_lease_ms,
                        event_registry,
                    )

                    for claimed_event in claimed:
                        if processed >= self._config.max_events_per_iteration:
                            break

                        event = claimed_event.event
                        if event.id is None:
                            self._event_store.release(
                                entry.meta.handler_id,
                                "",
                                self.namespace,
                                error="claimed event missing id",
                            )
                            continue

                        outstanding_claims.append((entry.meta.handler_id, event.id))

                        self._intents.clear()
                        ctx = HandlerContext(
                            event=event,
                            session=self,
                            lease_until=claimed_event.lease_until,
                        )

                        try:
                            entry.func(ctx)
                        except Exception as e:
                            self._intents.clear()
                            outstanding_claims.pop()
                            self._event_store.release(
                                entry.meta.handler_id,
                                event.id,
                                self.namespace,
                                error=str(e),
                            )
                            processed += 1
                            continue

                        # Handler succeeded — ack first, then flush buffered events.
                        # Acking before flush ensures that a flush failure does not
                        # cause the handler to be retried (which could duplicate
                        # events already enqueued via ctx.commit(event=...)).
                        try:
                            self._event_store.ack(entry.meta.handler_id, event.id, self.namespace)
                            outstanding_claims.pop()
                        except Exception:
                            outstanding_claims.pop()
                            # Ack failed — the claim will expire and the handler
                            # may be retried.  Skip flush to avoid partial state.
                            processed += 1
                            continue

                        try:
                            self._flush_buffered_events(ctx._buffered_events, event)
                        except Exception:
                            # Handler is already acked; buffered events from
                            # ctx.emit() are lost but the handler will not be
                            # retried, preventing duplicate side-effects from
                            # ctx.commit(event=...) calls.
                            pass

                        processed += 1

                time.sleep(poll_interval)
                iterations += 1
        except KeyboardInterrupt:
            for handler_id, event_id in outstanding_claims:
                try:
                    self._event_store.release(
                        handler_id,
                        event_id,
                        self.namespace,
                        error="session interrupted",
                    )
                except Exception:
                    pass
            return
