"""Ontology runtime: initialization, handler discovery, event loop, delta, commit."""

from __future__ import annotations

import inspect
import json
import random
import time
import uuid
from collections.abc import Iterable as ABCIterable
from typing import Any, Callable, Literal, cast, overload

from ontologia.config import OntologiaConfig
from ontologia.errors import (
    BatchSizeExceededError,
    CommitChainDepthError,
    HandlerError,
    HeadMismatchError,
    LockContentionError,
    MigrationError,
    MigrationTokenError,
    MissingUpgraderError,
    SchemaOutdatedError,
    TypeSchemaDiff,
)
from ontologia.filters import ComparisonExpression, FilterExpression, resolve_nested_path
from ontologia.type_spec import build_type_spec, synthesize_type_spec_from_legacy
from ontologia.handlers import HandlerContext, HandlerMeta
from ontologia.intents import Intent
from ontologia.migration import (
    MigrationPreview,
    MigrationResult,
    _chain_upgraders,
    _compute_migration_token,
    _compute_plan_hash,
    _LeaseKeepAlive,
    _verify_token,
)
from ontologia.query import QueryBuilder
from ontologia.storage import (
    _schema_hash,
    open_repository,
    parse_storage_target,
)
from ontologia.types import Entity, Meta, Relation


def _get_handler_id(func: Callable[..., Any]) -> str:
    """Compute stable handler identity from module.qualname."""
    return f"{func.__module__}.{func.__qualname__}"


def _entity_schema(cls: type[Entity]) -> dict[str, Any]:
    """Extract schema definition from an Entity class."""
    fields = {}
    for name, f in cls._field_definitions.items():
        field_info: dict[str, Any] = {
            "primary_key": f.primary_key,
            "index": f.index,
        }
        # Store type as string and canonical type_spec
        ann = f.annotation
        if ann is not None:
            field_info["type"] = str(ann)
            field_info["type_spec"] = build_type_spec(ann)
        fields[name] = field_info
    return {"entity_name": cls.__entity_name__, "fields": fields}


def _relation_schema(cls: type[Relation]) -> dict[str, Any]:  # type: ignore[type-arg]
    """Extract schema definition from a Relation class."""
    fields = {}
    for name, f in cls._field_definitions.items():
        field_info: dict[str, Any] = {
            "index": f.index,
        }
        ann = f.annotation
        if ann is not None:
            field_info["type"] = str(ann)
            field_info["type_spec"] = build_type_spec(ann)
        fields[name] = field_info
    return {
        "relation_name": cls.__relation_name__,
        "left_type": cls._left_type.__entity_name__,
        "right_type": cls._right_type.__entity_name__,
        "instance_key_field": cls._instance_key_field,
        "fields": fields,
    }


def _compare_value(value: Any, op: str, rhs: Any) -> bool:
    """Compare a single value against an operator and right-hand side."""
    if op == "==":
        return value == rhs
    elif op == "!=":
        return value != rhs
    elif op == ">":
        return value is not None and value > rhs
    elif op == ">=":
        return value is not None and value >= rhs
    elif op == "<":
        return value is not None and value < rhs
    elif op == "<=":
        return value is not None and value <= rhs
    elif op == "IN":
        return value in rhs
    elif op == "IS_NULL":
        return value is None
    elif op == "IS_NOT_NULL":
        return value is not None
    elif op == "LIKE":
        if value is None:
            return False
        pattern = rhs
        if pattern.startswith("%") and pattern.endswith("%"):
            return pattern[1:-1] in str(value)
        elif pattern.startswith("%"):
            return str(value).endswith(pattern[1:])
        elif pattern.endswith("%"):
            return str(value).startswith(pattern[:-1])
        return str(value) == pattern
    return False


def _matches_filter(data: dict[str, Any], expr: FilterExpression) -> bool:
    """Evaluate a FilterExpression against a dict of field values (for on_commit filtering)."""
    if isinstance(expr, ComparisonExpression):
        path = expr.field_path
        if path.startswith("$."):
            field_name = path[2:]
            value = resolve_nested_path(data, field_name)
        else:
            # Endpoint filters not evaluated at dispatch time
            return True

        return _compare_value(value, expr.op, expr.value)

    from ontologia.filters import ExistsComparisonExpression

    if isinstance(expr, ExistsComparisonExpression):
        path = expr.list_field_path
        if path.startswith("$."):
            list_val = resolve_nested_path(data, path[2:])
        else:
            return True
        if not isinstance(list_val, list):
            return False
        for item in list_val:
            if isinstance(item, dict):
                item_val = resolve_nested_path(item, expr.item_path)
            else:
                item_val = item
            if _compare_value(item_val, expr.op, expr.value):
                return True
        return False

    from ontologia.filters import LogicalExpression

    if isinstance(expr, LogicalExpression):
        if expr.op == "AND":
            return all(_matches_filter(data, c) for c in expr.children)
        elif expr.op == "OR":
            return any(_matches_filter(data, c) for c in expr.children)
        elif expr.op == "NOT":
            return not _matches_filter(data, expr.children[0])

    return True


class _HandlerEntry:
    """Internal registry entry for a discovered handler."""

    def __init__(
        self,
        func: Callable[..., Any],
        meta: HandlerMeta,
        handler_id: str,
        accepts_trigger: bool,
    ) -> None:
        self.func = func
        self.meta = meta
        self.handler_id = handler_id
        self.accepts_trigger = accepts_trigger


class Ontology:
    """Main runtime class for Ontologia."""

    def __init__(
        self,
        db_path: str | None = None,
        config: OntologiaConfig | None = None,
        *,
        storage_uri: str | None = None,
        entity_types: list[type[Entity]] | None = None,
        relation_types: list[type[Relation]] | None = None,  # type: ignore[type-arg]
    ) -> None:
        self._config = config or OntologiaConfig()
        target = parse_storage_target(db_path=db_path, storage_uri=storage_uri)
        self._storage_uri = target.uri
        self._repo = open_repository(db_path=db_path, storage_uri=storage_uri, config=self._config)
        self._runtime_id = self._config.runtime_id or str(uuid.uuid4())
        self._entity_types: dict[str, type[Entity]] = {}
        self._relation_types: dict[str, type[Relation]] = {}  # type: ignore[type-arg]
        self._schema_version_ids: dict[str, int] = {}
        self._schema_validated = False

        # Register explicitly provided types
        if entity_types:
            for cls in entity_types:
                self._entity_types[cls.__entity_name__] = cls
        if relation_types:
            for cls in relation_types:
                self._relation_types[cls.__relation_name__] = cls

    def session(self) -> Session:
        """Create a new session. Auto-validates schema when typed models are registered."""
        if not self._schema_validated and (self._entity_types or self._relation_types):
            self.validate()
        return Session(self)

    def query(self) -> QueryBuilder:
        """Public query entry point."""
        return QueryBuilder(self._repo, schema_version_ids=self._schema_version_ids)

    @property
    def repo(self) -> Any:
        return self._repo

    def list_commits(
        self,
        *,
        limit: int = 10,
        since_commit_id: int | None = None,
    ) -> list[dict[str, Any]]:
        return self._repo.list_commits(limit=limit, since_commit_id=since_commit_id)

    def get_commit(self, commit_id: int) -> dict[str, Any] | None:
        return self._repo.get_commit(commit_id)

    def close(self) -> None:
        self._repo.close()

    # --- Schema validation ---

    def validate(self) -> None:
        """Validate code-defined schemas against the latest stored schema versions."""
        self._validate_and_store_schema()

    def _validate_and_store_schema(self) -> None:
        """Validate code schema against stored versions. Auto-store version 1 for new types."""
        lock_owner: str | None = None
        try:
            if self._repo.storage_info().get("backend") == "s3":
                lock_owner = f"schema-validate-{self._runtime_id}"
                if not self._repo.acquire_lock(
                    lock_owner,
                    timeout_ms=self._config.s3_lock_timeout_ms,
                    lease_ms=self._config.s3_lease_ttl_ms,
                ):
                    raise LockContentionError(self._config.s3_lock_timeout_ms)

            diffs: list[TypeSchemaDiff] = []
            schema_version_ids: dict[str, int] = {}

            for name, cls in self._entity_types.items():
                version_id = self._validate_type_schema("entity", name, _entity_schema(cls), diffs)
                if version_id is not None:
                    schema_version_ids[name] = version_id
            for name, cls in self._relation_types.items():
                version_id = self._validate_type_schema(
                    "relation", name, _relation_schema(cls), diffs
                )
                if version_id is not None:
                    schema_version_ids[name] = version_id

            if diffs:
                self._schema_validated = False
                self._schema_version_ids.clear()
                raise SchemaOutdatedError(diffs)

            self._schema_version_ids = schema_version_ids
            self._schema_validated = True
        finally:
            if lock_owner is not None:
                try:
                    self._repo.release_lock(lock_owner)
                except Exception:
                    pass

    def _validate_type_schema(
        self,
        kind: str,
        name: str,
        code_schema: dict[str, Any],
        diffs: list[TypeSchemaDiff],
    ) -> int | None:
        """Validate a single type's schema against stored version."""
        code_json = json.dumps(code_schema, sort_keys=True)
        code_hash = _schema_hash(code_json)

        stored = self._repo.get_current_schema_version(kind, name)

        if stored is None:
            vid = self._repo.create_schema_version(
                kind,
                name,
                code_json,
                code_hash,
                runtime_id=self._runtime_id,
                reason="initial",
            )
            self._repo.store_schema(kind, name, code_schema)
            return vid
        elif stored["schema_hash"] == code_hash:
            return cast(int, stored["schema_version_id"])
        else:
            # Check if drift is only due to missing type_spec (legacy upgrade)
            stored_schema = json.loads(stored["schema_json"])
            if self._try_legacy_type_spec_upgrade(stored_schema, code_schema):
                # Stored schema was missing type_spec; synthesized specs match code.
                # Re-store with the upgraded schema to avoid future drift.
                upgraded_json = json.dumps(code_schema, sort_keys=True)
                upgraded_hash = _schema_hash(upgraded_json)
                vid = self._repo.create_schema_version(
                    kind,
                    name,
                    upgraded_json,
                    upgraded_hash,
                    runtime_id=self._runtime_id,
                    reason="type_spec_upgrade",
                )
                self._repo.store_schema(kind, name, code_schema)
                return vid

            diff = self._build_schema_diff(
                kind,
                name,
                stored["schema_version_id"],
                stored_schema,
                code_schema,
            )
            diffs.append(diff)
        return None

    @staticmethod
    def _try_legacy_type_spec_upgrade(
        stored_schema: dict[str, Any], code_schema: dict[str, Any]
    ) -> bool:
        """Check if stored schema only differs from code schema by missing type_spec.

        If so, attempt to synthesize type_spec from legacy type strings and verify
        they match the code schema's type_spec. Returns True if upgrade is safe.
        """
        stored_fields = stored_schema.get("fields", {})
        code_fields = code_schema.get("fields", {})

        # Must have the same field names
        if set(stored_fields.keys()) != set(code_fields.keys()):
            return False

        # Check non-fields keys match (entity_name, relation_name, etc.)
        for key in stored_schema:
            if key == "fields":
                continue
            if stored_schema.get(key) != code_schema.get(key):
                return False
        for key in code_schema:
            if key == "fields":
                continue
            if key not in stored_schema:
                return False

        for field_name in stored_fields:
            sf = stored_fields[field_name]
            cf = code_fields[field_name]

            if sf == cf:
                continue

            # The only allowed difference: stored is missing type_spec, code has it
            sf_without_spec = {k: v for k, v in sf.items() if k != "type_spec"}
            cf_without_spec = {k: v for k, v in cf.items() if k != "type_spec"}
            if sf_without_spec != cf_without_spec:
                return False
            if "type_spec" in sf:
                # Stored already has type_spec but different — real drift
                return False

            # Try to synthesize from legacy type string
            type_str = sf.get("type")
            if type_str is None:
                return False
            synthesized = synthesize_type_spec_from_legacy(type_str)
            if synthesized is None:
                # Can't synthesize — report as real drift
                return False
            if synthesized != cf.get("type_spec"):
                return False

        return True

    @staticmethod
    def _build_schema_diff(
        type_kind: str,
        type_name: str,
        stored_version: int,
        stored_schema: dict[str, Any],
        code_schema: dict[str, Any],
    ) -> TypeSchemaDiff:
        """Compare stored and code schemas to produce a TypeSchemaDiff."""
        stored_fields = set(stored_schema.get("fields", {}).keys())
        code_fields = set(code_schema.get("fields", {}).keys())

        added = sorted(code_fields - stored_fields)
        removed = sorted(stored_fields - code_fields)

        changed: dict[str, dict[str, Any]] = {}
        for f in stored_fields & code_fields:
            sf = stored_schema["fields"][f]
            cf = code_schema["fields"][f]
            if sf != cf:
                changed[f] = {"stored": sf, "code": cf}

        # Check for instance_key_field changes (relations only)
        stored_ik = stored_schema.get("instance_key_field")
        code_ik = code_schema.get("instance_key_field")
        if stored_ik != code_ik:
            changed["__instance_key_field__"] = {"stored": stored_ik, "code": code_ik}

        return TypeSchemaDiff(
            type_kind=type_kind,
            type_name=type_name,
            stored_version=stored_version,
            added_fields=added,
            removed_fields=removed,
            changed_fields=changed,
        )

    # --- Migration API ---

    def _compute_migration_plan(
        self,
    ) -> tuple[list[TypeSchemaDiff], dict[str, int], list[str], list[str]]:
        """Compute migration plan: (diffs, estimated_rows, schema_only_types, upgrader_types)."""
        diffs: list[TypeSchemaDiff] = []
        estimated_rows: dict[str, int] = {}
        schema_only: list[str] = []
        needs_upgrader: list[str] = []

        for name, cls in self._entity_types.items():
            self._check_type_diff(
                "entity",
                name,
                _entity_schema(cls),
                self._repo.count_latest_entities,
                diffs,
                estimated_rows,
                schema_only,
                needs_upgrader,
            )
        for name, cls in self._relation_types.items():
            self._check_type_diff(
                "relation",
                name,
                _relation_schema(cls),
                self._repo.count_latest_relations,
                diffs,
                estimated_rows,
                schema_only,
                needs_upgrader,
            )

        return diffs, estimated_rows, schema_only, needs_upgrader

    def _check_type_diff(
        self,
        kind: str,
        name: str,
        code_schema: dict[str, Any],
        count_fn: Callable[[str], int],
        diffs: list[TypeSchemaDiff],
        estimated_rows: dict[str, int],
        schema_only: list[str],
        needs_upgrader: list[str],
    ) -> None:
        code_json = json.dumps(code_schema, sort_keys=True)
        code_hash = _schema_hash(code_json)

        stored = self._repo.get_current_schema_version(kind, name)
        if stored is None or stored["schema_hash"] == code_hash:
            return

        diff = self._build_schema_diff(
            kind,
            name,
            stored["schema_version_id"],
            json.loads(stored["schema_json"]),
            code_schema,
        )
        diffs.append(diff)

        row_count = count_fn(name)
        estimated_rows[name] = row_count

        if row_count == 0:
            schema_only.append(name)
        else:
            needs_upgrader.append(name)

    @overload
    def migrate(
        self,
        *,
        dry_run: Literal[True] = ...,
        upgraders: dict[tuple[str, int], Callable[..., Any]] | None = ...,
    ) -> MigrationPreview: ...

    @overload
    def migrate(
        self,
        *,
        dry_run: Literal[False],
        token: str | None = ...,
        force: bool = ...,
        upgraders: dict[tuple[str, int], Callable[..., Any]] | None = ...,
    ) -> MigrationResult: ...

    def migrate(
        self,
        *,
        dry_run: bool = True,
        token: str | None = None,
        force: bool = False,
        upgraders: dict[tuple[str, int], Callable[..., Any]] | None = None,
    ) -> MigrationPreview | MigrationResult:
        """Preview or execute schema migration.

        Args:
            dry_run: If True, return MigrationPreview without applying changes.
            token: Migration token from a previous dry_run preview. Required if not force.
            force: Skip token verification (still validates under lock).
            upgraders: Dict of (type_name, from_version) -> upgrader function.
        """
        if dry_run:
            return self._migrate_preview(upgraders)
        return self._migrate_apply(token=token, force=force, upgraders=upgraders)

    def _migrate_preview(
        self,
        upgraders: dict[tuple[str, int], Callable[..., Any]] | None = None,
    ) -> MigrationPreview:
        diffs, estimated_rows, schema_only, needs_upgrader = self._compute_migration_plan()

        if not diffs:
            return MigrationPreview(
                has_changes=False,
                token="",
                diffs=[],
            )

        plan_hash = _compute_plan_hash(diffs)
        head = self._repo.get_head_commit_id()
        token = _compute_migration_token(plan_hash, head)

        # Check upgrader coverage
        missing: list[str] = []
        if upgraders is not None:
            for name in needs_upgrader:
                diff = next(d for d in diffs if d.type_name == name)
                stored_ver = diff.stored_version
                target_ver = stored_ver + 1
                for v in range(stored_ver, target_ver):
                    if (name, v) not in upgraders:
                        missing.append(name)
                        break
        else:
            missing = list(needs_upgrader)

        return MigrationPreview(
            has_changes=True,
            token=token,
            diffs=diffs,
            estimated_rows=estimated_rows,
            types_requiring_upgraders=needs_upgrader,
            types_schema_only=schema_only,
            missing_upgraders=missing,
        )

    def _migrate_apply(
        self,
        *,
        token: str | None = None,
        force: bool = False,
        upgraders: dict[tuple[str, int], Callable[..., Any]] | None = None,
    ) -> MigrationResult:
        if not force and not token:
            raise MigrationError("Either token or force=True is required for apply")
        if force and token:
            raise MigrationError("Cannot specify both token and force=True")

        upgraders = upgraders or {}
        lease_ttl_s = 60.0
        owner_id = f"migration-{self._runtime_id}-{uuid.uuid4()}"

        if not self._repo.acquire_lock(
            owner_id, timeout_ms=10000, lease_ms=int(lease_ttl_s * 1000)
        ):
            raise MigrationError("Could not acquire write lock for migration")

        keep_alive = _LeaseKeepAlive(self._repo, owner_id, lease_ttl_s)
        keep_alive.start()
        start_time = time.monotonic()

        try:
            # Recompute plan under lock
            diffs, _estimated_rows, schema_only, needs_upgrader = self._compute_migration_plan()

            if not diffs:
                return MigrationResult(success=True, duration_s=time.monotonic() - start_time)

            # Verify token
            if not force:
                plan_hash = _compute_plan_hash(diffs)
                head = self._repo.get_head_commit_id()
                if not _verify_token(token, plan_hash, head):  # type: ignore[arg-type]
                    raise MigrationTokenError(
                        "Migration token is stale. Schema or data changed since preview. "
                        "Run migrate(dry_run=True) again."
                    )

            # Validate upgrader coverage for types with data
            missing: dict[str, list[int]] = {}
            for name in needs_upgrader:
                diff = next(d for d in diffs if d.type_name == name)
                stored_ver = diff.stored_version
                target_ver = stored_ver + 1  # Each migration bumps by 1
                missing_versions: list[int] = []
                for v in range(stored_ver, target_ver):
                    if (name, v) not in upgraders:
                        missing_versions.append(v)
                if missing_versions:
                    missing[name] = missing_versions

            if missing:
                raise MissingUpgraderError(missing)

            # Execute migration in a transaction
            self._repo.begin_transaction()
            try:
                types_migrated: list[str] = []
                rows_migrated: dict[str, int] = {}
                new_versions: dict[str, int] = {}
                migrated_types_meta = [
                    {
                        "type_kind": d.type_kind,
                        "type_name": d.type_name,
                        "from_schema_version_id": d.stored_version,
                        "to_schema_version_id": d.stored_version + 1,
                        "rows_rewritten": int(_estimated_rows.get(d.type_name, 0)),
                    }
                    for d in diffs
                ]
                migration_commit_id = self._repo.create_commit(
                    {
                        "kind": "migration",
                        "migrated_types": migrated_types_meta,
                    }
                )

                for diff in diffs:
                    name = diff.type_name
                    kind = diff.type_kind
                    code_schema = self._get_code_schema(kind, name)
                    code_json = json.dumps(code_schema, sort_keys=True)
                    code_hash = _schema_hash(code_json)
                    vid = self._repo.create_schema_version(
                        kind,
                        name,
                        code_json,
                        code_hash,
                        runtime_id=self._runtime_id,
                        reason="migration",
                    )

                    if name in schema_only:
                        row_count = 0
                    else:
                        # Needs upgrader: transform data
                        stored_ver = diff.stored_version
                        chain_fn = _chain_upgraders(upgraders, name, stored_ver, stored_ver + 1)

                        row_count = 0
                        if kind == "entity":
                            cls = self._entity_types[name]
                            for batch in self._repo.iter_latest_entities(name):
                                for key, fields, _old_cid, _old_svid in batch:
                                    try:
                                        new_fields = chain_fn(dict(fields))
                                        # Validate through type
                                        cls(**new_fields)
                                    except Exception as e:
                                        raise MigrationError(
                                            f"Upgrader failed for {kind} '{name}' "
                                            f"key='{key}': {e}\n"
                                            f"Old data: {fields}"
                                        ) from e
                                    self._repo.insert_entity(
                                        name,
                                        key,
                                        new_fields,
                                        migration_commit_id,
                                        schema_version_id=vid,
                                    )
                                    row_count += 1
                        else:
                            cls = self._relation_types[name]
                            for batch in self._repo.iter_latest_relations(name):
                                for left_key, right_key, ik, fields, _old_cid, _old_svid in batch:
                                    try:
                                        new_fields = chain_fn(dict(fields))
                                        ctor_kwargs: dict[str, Any] = {
                                            **new_fields,
                                            "left_key": left_key,
                                            "right_key": right_key,
                                        }
                                        if ik and cls._instance_key_field:
                                            ctor_kwargs[cls._instance_key_field] = ik
                                        cls(**ctor_kwargs)
                                    except Exception as e:
                                        raise MigrationError(
                                            f"Upgrader failed for {kind} '{name}' "
                                            f"key='{left_key}:{right_key}': {e}\n"
                                            f"Old data: {fields}"
                                        ) from e
                                    self._repo.insert_relation(
                                        name,
                                        left_key,
                                        right_key,
                                        new_fields,
                                        migration_commit_id,
                                        schema_version_id=vid,
                                        instance_key=ik,
                                    )
                                    row_count += 1

                    activator = getattr(self._repo, "activate_schema_version", None)
                    if callable(activator):
                        activator(
                            type_kind=kind,
                            type_name=name,
                            schema_version_id=vid,
                            activation_commit_id=migration_commit_id,
                        )

                    self._repo.store_schema(kind, name, code_schema)
                    new_versions[name] = vid
                    types_migrated.append(name)
                    rows_migrated[name] = row_count

                self._repo.commit_transaction()
            except Exception:
                self._repo.rollback_transaction()
                raise

            # Force a fresh validate() on next session/write path after migration.
            self._schema_validated = False
            self._schema_version_ids.clear()

            return MigrationResult(
                success=True,
                types_migrated=types_migrated,
                rows_migrated=rows_migrated,
                new_schema_versions=new_versions,
                duration_s=time.monotonic() - start_time,
            )
        finally:
            keep_alive.stop()
            try:
                self._repo.release_lock(owner_id)
            except Exception:
                pass

    def _get_code_schema(self, kind: str, name: str) -> dict[str, Any]:
        if kind == "entity":
            return _entity_schema(self._entity_types[name])
        return _relation_schema(self._relation_types[name])

    def _assert_no_schema_drift(self, changes: list[dict[str, Any]]) -> None:
        """Abort writes when touched type schema versions drift from the validated snapshot."""
        if not self._schema_validated:
            return

        touched_types: set[tuple[str, str]] = set()
        for change in changes:
            kind = cast(str, change["kind"])
            type_name = cast(str, change["type_name"])
            touched_types.add((kind, type_name))

        diffs: list[TypeSchemaDiff] = []
        for kind, type_name in sorted(touched_types):
            expected_version = self._schema_version_ids.get(type_name)
            if expected_version is None:
                continue

            stored = self._repo.get_current_schema_version(kind, type_name)
            if stored is None:
                code_schema = self._get_code_schema(kind, type_name)
                diffs.append(
                    self._build_schema_diff(kind, type_name, 0, {"fields": {}}, code_schema)
                )
                continue

            current_version = cast(int, stored["schema_version_id"])
            if current_version == expected_version:
                continue

            code_schema = self._get_code_schema(kind, type_name)
            stored_schema = json.loads(cast(str, stored["schema_json"]))
            diffs.append(
                self._build_schema_diff(
                    kind,
                    type_name,
                    current_version,
                    stored_schema,
                    code_schema,
                )
            )

        if diffs:
            self._schema_validated = False
            raise SchemaOutdatedError(diffs)

    # --- Event execution ---

    # Execution logic moved to Session class


class Session:
    """Unit-of-work and execution context for Ontologia."""

    def __init__(self, ontology: Ontology) -> None:
        self._ontology = ontology
        self._intents: list[Intent] = []
        self._handlers: list[_HandlerEntry] = []
        self._has_run = False

    def __enter__(self) -> Session:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: Any,
    ) -> None:
        if exc_type is None:
            self.commit()

    def ensure(
        self, obj: Entity | Relation[Any, Any] | ABCIterable[Entity | Relation[Any, Any]]
    ) -> None:
        """Queue an intent (or intents from an iterable) for the next commit.

        Args:
            obj: Single Entity/Relation or an iterable of Entity/Relation instances

        Raises:
            TypeError: If obj or any item in iterable is not Entity or Relation
        """
        # Check if obj is iterable (but not string, which is iterable)
        if isinstance(obj, (Entity, Relation)):
            # Single object case
            self._intents.append(Intent(obj))
        elif isinstance(obj, ABCIterable) and not isinstance(obj, (str, bytes)):
            # Iterable case - process each item
            for item in obj:
                if not isinstance(item, (Entity, Relation)):
                    raise TypeError(f"Expected Entity or Relation, got {type(item)}")
                self._intents.append(Intent(item))
        else:
            raise TypeError(
                f"Expected Entity, Relation, or Iterable of Entity/Relation, got {type(obj)}"
            )

    def commit(self) -> int | None:
        """Commit queued intents."""
        if not self._intents:
            return None

        batch_intents = list(self._intents)
        self._intents.clear()

        # Imperative commit uses a fresh event scope
        return self._apply_intents(
            batch_intents,
            commit_meta={},
            snapshot_commit_id=self._ontology.repo.get_head_commit_id(),
            root_event_id=str(uuid.uuid4()),
            chain_depth=0,
            authoring_handler_ids=set(),
            dispatch_log=set(),
        )

    def run(self, handlers: list[Callable[..., Any]]) -> None:
        """Execute event loop with provided handler functions.

        Args:
            handlers: List of decorated handler functions (@on_commit or @on_schedule)

        Raises:
            HandlerError: If handlers invalid or run() already called
        """
        if self._has_run:
            raise HandlerError("Session.run() can only be called once per session")

        # Build handler entries from functions
        handler_entries: list[_HandlerEntry] = []
        seen_ids: set[str] = set()

        for func in handlers:
            if not callable(func):
                raise HandlerError(f"Handler must be callable, got {type(func)}")

            if not hasattr(func, "_ontologia_handler"):
                raise HandlerError(
                    f"Function {func.__qualname__} is not decorated with @on_commit or @on_schedule"
                )

            meta: HandlerMeta = cast(Any, func)._ontologia_handler

            # Reject ON_STARTUP handlers (removed)
            if meta.event_type == "ON_STARTUP":
                raise HandlerError(
                    f"@on_startup is no longer supported. "
                    f"Use imperative session.ensure() for initialization. "
                    f"Handler: {func.__qualname__}"
                )

            handler_id = _get_handler_id(func)

            if handler_id in seen_ids:
                raise HandlerError(f"Duplicate handler: {handler_id}")
            seen_ids.add(handler_id)

            # Validate signature
            accepts_trigger = self._validate_handler_signature(func, meta)
            self._validate_commit_handler_target(func, meta)

            handler_entries.append(_HandlerEntry(func, meta, handler_id, accepts_trigger))

        # Sort by priority, then handler_id
        handler_entries.sort(key=lambda h: (h.meta.priority, h.handler_id))

        # Store in session for ON_COMMIT chains BEFORE auto-commit
        # This ensures handlers can react to the initial commit
        self._handlers = handler_entries
        self._has_run = True

        # Auto-commit: persist any pending intents before running handlers
        # This allows handlers to see data from pre-run ensure() calls
        # and triggers ON_COMMIT handlers on the initial commit
        if self._intents:
            batch_intents = list(self._intents)
            self._intents.clear()

            self._apply_intents(
                batch_intents,
                commit_meta={},
                snapshot_commit_id=self._ontology.repo.get_head_commit_id(),
                root_event_id=str(uuid.uuid4()),
                chain_depth=0,
                authoring_handler_ids=set(),
                dispatch_log=set(),
            )

        # Execute ON_SCHEDULE handlers (may trigger more ON_COMMIT chains)
        self._fire_event("ON_SCHEDULE", handler_entries)

    @staticmethod
    def _can_call_with_positional_args(sig: inspect.Signature, arg_count: int) -> bool:
        required = 0
        positional = 0
        has_varargs = False
        for param in sig.parameters.values():
            if param.kind in (
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            ):
                positional += 1
                if param.default is inspect.Parameter.empty:
                    required += 1
            elif param.kind == inspect.Parameter.VAR_POSITIONAL:
                has_varargs = True

        if arg_count < required:
            return False
        if has_varargs:
            return True
        return arg_count <= positional

    def _validate_handler_signature(self, func: Callable[..., Any], meta: HandlerMeta) -> bool:
        """Validate handler function signature."""
        sig = inspect.signature(func)
        accepts_ctx = self._can_call_with_positional_args(sig, 1)
        accepts_trigger = self._can_call_with_positional_args(sig, 2)

        if meta.event_type == "ON_SCHEDULE":
            if not accepts_ctx:
                raise HandlerError(
                    f"ON_SCHEDULE handler {func.__qualname__} must accept one "
                    "positional argument (ctx)"
                )
            return accepts_trigger

        if meta.event_type == "ON_COMMIT":
            if meta.target_kind is not None:
                if not accepts_trigger:
                    raise HandlerError(
                        f"ON_COMMIT handler {func.__qualname__} with typed target "
                        "must accept two positional arguments (ctx, data)"
                    )
                return True

            if not (accepts_ctx or accepts_trigger):
                raise HandlerError(
                    f"ON_COMMIT handler {func.__qualname__} must accept at least "
                    "one positional argument (ctx)"
                )
            return accepts_trigger

        if not accepts_ctx:
            raise HandlerError(
                f"Handler {func.__qualname__} must accept one positional argument (ctx)"
            )
        return accepts_trigger

    def _validate_commit_handler_target(self, func: Callable[..., Any], meta: HandlerMeta) -> None:
        if meta.event_type != "ON_COMMIT" or meta.target_kind is None:
            return

        target_type = meta.target_type
        if target_type is None:
            raise HandlerError(
                f"ON_COMMIT handler {func.__qualname__} has typed target kind but no target type"
            )
        if not inspect.isclass(target_type):
            raise HandlerError(
                f"ON_COMMIT handler {func.__qualname__} target must be a class, got {target_type!r}"
            )

        if meta.target_kind == "entity":
            if not issubclass(target_type, Entity):
                raise HandlerError(
                    f"ON_COMMIT entity handler {func.__qualname__} target must be an Entity class, "
                    f"got {target_type.__qualname__}"
                )
            type_name = target_type.__entity_name__
            if type_name not in self._ontology._entity_types:
                raise HandlerError(
                    f"ON_COMMIT entity handler {func.__qualname__} target '{type_name}' is not "
                    "registered in Ontology(entity_types=[...])"
                )
            return

        if not issubclass(target_type, Relation):
            raise HandlerError(
                f"ON_COMMIT relation handler {func.__qualname__} target must be a Relation class, "
                f"got {target_type.__qualname__}"
            )
        relation_cls = cast(type[Relation[Any, Any]], target_type)
        type_name = relation_cls.__relation_name__
        if type_name not in self._ontology._relation_types:
            raise HandlerError(
                f"ON_COMMIT relation handler {func.__qualname__} target '{type_name}' is not "
                "registered in Ontology(relation_types=[...])"
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

    # --- Internal execution methods (moved from Ontology) ---

    def _fire_event(
        self,
        event_type: str,
        handlers: list[_HandlerEntry],
        trigger_data: list[dict[str, Any]] | None = None,
        root_event_id: str | None = None,
        chain_depth: int = 0,
        authoring_handler_ids: set[str] | None = None,
        dispatch_log: set[tuple[str, int, str, str | tuple[str, str, str]]] | None = None,
    ) -> None:
        if root_event_id is None:
            root_event_id = str(uuid.uuid4())
        if dispatch_log is None:
            dispatch_log = set()
        if authoring_handler_ids is None:
            authoring_handler_ids = set()

        if chain_depth > self._ontology._config.max_commit_chain_depth:
            raise CommitChainDepthError(chain_depth, self._ontology._config.max_commit_chain_depth)

        # Filter handlers by event type
        filtered_handlers = [h for h in handlers if h.meta.event_type == event_type]

        if event_type == "ON_COMMIT" and trigger_data:
            self._execute_commit_handlers(
                filtered_handlers,
                trigger_data,
                root_event_id,
                chain_depth,
                authoring_handler_ids,
                dispatch_log,
            )
        elif event_type == "ON_SCHEDULE":
            self._execute_simple_handlers(
                filtered_handlers,
                event_type,
                root_event_id,
                chain_depth,
                dispatch_log,
            )

    def _execute_simple_handlers(
        self,
        handlers: list[_HandlerEntry],
        event_type: str,
        root_event_id: str,
        chain_depth: int,
        dispatch_log: set[tuple[str, int, str, str | tuple[str, str, str]]],
    ) -> None:
        # Use session queue (self._intents) but track what's added in this run
        all_meta: dict[str, str] = {}
        contributing_handler_ids: set[str] = set()

        snapshot_commit_id = self._ontology.repo.get_head_commit_id()
        initial_intents_len = len(self._intents)

        for handler in handlers:
            ctx = HandlerContext(
                event=event_type,
                commit_id=None,
                root_event_id=root_event_id,
                chain_depth=chain_depth,
                session=self,
            )

            # Handler should use ctx.ensure(...) which appends to self._intents
            handler.func(ctx)

            # Check if intents were added
            current_len = len(self._intents)
            if current_len > initial_intents_len:
                contributing_handler_ids.add(handler.handler_id)
                initial_intents_len = current_len

            all_meta.update(ctx._commit_meta)

        if not self._intents:
            return

        if len(self._intents) > self._ontology._config.max_batch_size:
            raise BatchSizeExceededError(len(self._intents), self._ontology._config.max_batch_size)

        # Snapshot intents and clear queue before processing
        batch_intents = list(self._intents)
        self._intents.clear()

        # Apply all intents in the queue
        self._apply_intents(
            batch_intents,
            all_meta,
            snapshot_commit_id,
            root_event_id,
            chain_depth,
            contributing_handler_ids,
            dispatch_log,
        )

    def _execute_commit_handlers(
        self,
        handlers: list[_HandlerEntry],
        trigger_data: list[dict[str, Any]],
        root_event_id: str,
        chain_depth: int,
        authoring_handler_ids: set[str],
        dispatch_log: set[tuple[str, int, str, str | tuple[str, str, str]]],
    ) -> None:
        all_meta: dict[str, str] = {}
        contributing_handler_ids: set[str] = set()

        snapshot_commit_id = self._ontology.repo.get_head_commit_id()
        initial_intents_len = len(self._intents)

        for change in trigger_data:
            kind = cast(Literal["entity", "relation"], change.get("kind", "entity"))
            type_name = change["type_name"]
            fields = change["fields"]
            commit_id = change["commit_id"]

            for handler in handlers:
                target_kind = handler.meta.target_kind
                target_type = handler.meta.target_type
                if target_kind is not None:
                    if target_kind != kind or target_type is None:
                        continue
                    if target_kind == "entity":
                        if cast(type[Entity], target_type).__entity_name__ != type_name:
                            continue
                    elif cast(type[Relation[Any, Any]], target_type).__relation_name__ != type_name:
                        continue

                # Self-trigger check: skip if handler authored the triggering commit
                if (
                    not handler.meta.allow_self_trigger
                    and handler.handler_id in authoring_handler_ids
                ):
                    continue

                if change.get("key"):
                    identity = change["key"]
                else:
                    ik = change.get("instance_key", "")
                    identity = (change.get("left_key", ""), change.get("right_key", ""), ik)
                dispatch_key = (handler.handler_id, commit_id, type_name, identity)
                if dispatch_key in dispatch_log:
                    continue
                dispatch_log.add(dispatch_key)

                if handler.meta.when is not None:
                    if not _matches_filter(fields, handler.meta.when):
                        continue

                ctx = HandlerContext(
                    event="ON_COMMIT",
                    commit_id=commit_id,
                    root_event_id=root_event_id,
                    chain_depth=chain_depth,
                    session=self,
                )

                if not handler.accepts_trigger:
                    handler.func(ctx)
                else:
                    trigger_obj = self._build_trigger_object(change)
                    if trigger_obj is None:
                        raise HandlerError(
                            f"ON_COMMIT handler {handler.func.__qualname__} "
                            "expects a trigger object, "
                            f"but type '{type_name}' (kind='{kind}') is not registered"
                        )
                    handler.func(ctx, trigger_obj)

                current_len = len(self._intents)
                if current_len > initial_intents_len:
                    # print(f"DEBUG: Contributing {handler.handler_id}")
                    contributing_handler_ids.add(handler.handler_id)
                    initial_intents_len = current_len

                all_meta.update(ctx._commit_meta)

        if not self._intents:
            return

        if len(self._intents) > self._ontology._config.max_batch_size:
            raise BatchSizeExceededError(len(self._intents), self._ontology._config.max_batch_size)

        batch_intents = list(self._intents)
        self._intents.clear()

        self._apply_intents(
            batch_intents,
            all_meta,
            snapshot_commit_id,
            root_event_id,
            chain_depth,
            contributing_handler_ids,
            dispatch_log,
        )

    def _build_trigger_object(self, change: dict[str, Any]) -> Entity | Relation[Any, Any] | None:
        type_name = change["type_name"]
        fields = change["fields"]
        kind = change.get("kind", "entity")

        if kind == "entity" and type_name in self._ontology._entity_types:
            cls = self._ontology._entity_types[type_name]
            obj = cls(**fields)
            obj.__onto_meta__ = Meta(
                commit_id=change["commit_id"],
                type_name=type_name,
                key=change.get("key"),
            )
            return obj
        elif kind == "relation" and type_name in self._ontology._relation_types:
            cls = self._ontology._relation_types[type_name]
            data = {
                **fields,
                "left_key": change.get("left_key", ""),
                "right_key": change.get("right_key", ""),
            }
            ik = change.get("instance_key", "")
            if ik and cls._instance_key_field:
                data[cls._instance_key_field] = ik
            obj = cls(**data)
            obj.__onto_meta__ = Meta(
                commit_id=change["commit_id"],
                type_name=type_name,
                left_key=change.get("left_key"),
                right_key=change.get("right_key"),
                instance_key=ik if ik else None,
            )
            return obj

        return None

    def _apply_intents(
        self,
        intents: list[Intent],
        commit_meta: dict[str, str],
        snapshot_commit_id: int | None,
        root_event_id: str,
        chain_depth: int,
        authoring_handler_ids: set[str],
        dispatch_log: set[tuple[str, int, str, str | tuple[str, str, str]]],
        _retry: int = 0,
    ) -> int | None:
        max_retries = 3

        if not self._ontology.repo.acquire_lock(self._ontology._runtime_id, timeout_ms=5000):
            raise LockContentionError(5000)

        try:
            current_head = self._ontology.repo.get_head_commit_id()
            if current_head != snapshot_commit_id:
                self._ontology.repo.release_lock(self._ontology._runtime_id)
                if _retry >= max_retries:
                    raise HeadMismatchError(max_retries)
                time.sleep(0.01 * (2**_retry) + random.uniform(0, 0.01))
                return self._apply_intents(
                    intents,
                    commit_meta,
                    current_head,
                    root_event_id,
                    chain_depth,
                    authoring_handler_ids,
                    dispatch_log,
                    _retry + 1,
                )

            changes: list[dict[str, Any]] = []

            for intent in intents:
                obj = intent.obj
                if isinstance(obj, Entity):
                    change = self._compute_entity_delta(obj)
                    if change:
                        changes.append(change)
                elif isinstance(obj, Relation):
                    change = self._compute_relation_delta(obj)
                    if change:
                        changes.append(change)

            if not changes:
                self._ontology.repo.release_lock(self._ontology._runtime_id)
                return None

            self._ontology._assert_no_schema_drift(changes)

            commit_id = self._ontology.repo.create_commit(commit_meta if commit_meta else None)

            for change in changes:
                svid = self._ontology._schema_version_ids.get(change["type_name"])
                if change["kind"] == "entity":
                    self._ontology.repo.insert_entity(
                        change["type_name"],
                        change["key"],
                        change["fields"],
                        commit_id,
                        schema_version_id=svid,
                    )
                elif change["kind"] == "relation":
                    self._ontology.repo.insert_relation(
                        change["type_name"],
                        change["left_key"],
                        change["right_key"],
                        change["fields"],
                        commit_id,
                        schema_version_id=svid,
                        instance_key=change.get("instance_key", ""),
                    )

            self._ontology.repo.commit_transaction()
        except Exception:
            try:
                self._ontology.repo.rollback_transaction()
            except Exception:
                pass
            raise
        finally:
            try:
                self._ontology.repo.release_lock(self._ontology._runtime_id)
            except Exception:
                pass

        trigger_data = [{**change, "commit_id": commit_id} for change in changes]

        self._fire_event(
            "ON_COMMIT",
            self._handlers,  # Use session's handlers for chains
            trigger_data=trigger_data,
            root_event_id=root_event_id,
            chain_depth=chain_depth + 1,
            authoring_handler_ids=authoring_handler_ids,
            dispatch_log=dispatch_log,
        )
        return commit_id

    def _compute_entity_delta(self, entity: Entity) -> dict[str, Any] | None:
        type_name = entity.__entity_name__
        pk_field = entity._primary_key_field
        key = getattr(entity, pk_field)
        fields = entity.model_dump()

        current = self._ontology.repo.get_latest_entity(type_name, str(key))
        if current is None:
            return {
                "kind": "entity",
                "type_name": type_name,
                "key": str(key),
                "fields": fields,
            }
        elif current["fields"] != fields:
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

        current = self._ontology.repo.get_latest_relation(
            type_name, left_key, right_key, instance_key=instance_key
        )
        if current is None:
            return {
                "kind": "relation",
                "type_name": type_name,
                "left_key": left_key,
                "right_key": right_key,
                "instance_key": instance_key,
                "fields": fields,
            }
        elif current["fields"] != fields:
            return {
                "kind": "relation",
                "type_name": type_name,
                "left_key": left_key,
                "right_key": right_key,
                "instance_key": instance_key,
                "fields": fields,
            }
        return None
