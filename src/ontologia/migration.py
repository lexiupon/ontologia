"""Schema migration: upgrader decorator, preview, execution, token helpers."""

from __future__ import annotations

import base64
import hashlib
import importlib
import json
import threading
from dataclasses import dataclass, field
from typing import Any, Callable

from ontologia.errors import (
    MigrationError,
    MissingUpgraderError,
    TypeSchemaDiff,
)
from ontologia.storage import RepositoryProtocol

__all__ = [
    "upgrader",
    "load_upgraders",
    "MigrationPreview",
    "MigrationResult",
    "_compute_plan_hash",
    "_compute_migration_token",
    "_verify_token",
    "_chain_upgraders",
    "_LeaseKeepAlive",
]


# --- Upgrader decorator ---


def upgrader(type_name: str, *, from_version: int) -> Callable[..., Any]:
    """Decorator marking a function as a schema upgrader for a specific type and version.

    The decorated function takes a dict of old fields and returns a dict of new fields.

    Example::

        @upgrader("Customer", from_version=1)
        def upgrade_customer_v1(fields: dict) -> dict:
            fields["email"] = fields.pop("mail", None)
            return fields
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        func._ontologia_upgrader = {  # type: ignore[attr-defined]
            "type_name": type_name,
            "from_version": from_version,
        }
        return func

    return decorator


def load_upgraders(module_path: str) -> dict[tuple[str, int], Callable[..., Any]]:
    """Import a module and collect all @upgrader-decorated functions.

    Returns dict mapping (type_name, from_version) -> function.
    Raises MigrationError on duplicate (type_name, from_version).
    """
    module = importlib.import_module(module_path)
    registry: dict[tuple[str, int], Callable[..., Any]] = {}

    for attr_name in dir(module):
        obj = getattr(module, attr_name)
        meta = getattr(obj, "_ontologia_upgrader", None)
        if meta is None:
            continue
        key = (meta["type_name"], meta["from_version"])
        if key in registry:
            raise MigrationError(
                f"Duplicate upgrader for {key[0]} from_version={key[1]}: "
                f"{registry[key].__qualname__} and {obj.__qualname__}"
            )
        registry[key] = obj

    return registry


# --- Data classes ---


@dataclass
class MigrationPreview:
    """Result of migrate(dry_run=True)."""

    has_changes: bool
    token: str
    diffs: list[TypeSchemaDiff]
    estimated_rows: dict[str, int] = field(default_factory=dict)
    types_requiring_upgraders: list[str] = field(default_factory=list)
    types_schema_only: list[str] = field(default_factory=list)
    missing_upgraders: list[str] = field(default_factory=list)


@dataclass
class MigrationResult:
    """Result of migrate(dry_run=False)."""

    success: bool
    types_migrated: list[str] = field(default_factory=list)
    rows_migrated: dict[str, int] = field(default_factory=dict)
    new_schema_versions: dict[str, int] = field(default_factory=dict)
    duration_s: float = 0.0


# --- Token helpers ---


def _compute_plan_hash(diffs: list[TypeSchemaDiff]) -> str:
    """SHA-256 of canonical JSON representation of diffs."""
    canonical = json.dumps(
        [
            {
                "type_kind": d.type_kind,
                "type_name": d.type_name,
                "stored_version": d.stored_version,
                "added_fields": sorted(d.added_fields),
                "removed_fields": sorted(d.removed_fields),
                "changed_fields": dict(sorted(d.changed_fields.items())),
            }
            for d in sorted(diffs, key=lambda d: (d.type_kind, d.type_name))
        ],
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


def _compute_migration_token(plan_hash: str, head_commit_id: int | None) -> str:
    """Base64 encode plan_hash:head_commit_id as a migration token."""
    raw = f"{plan_hash}:{head_commit_id if head_commit_id is not None else 'none'}"
    return base64.urlsafe_b64encode(raw.encode()).decode()


def _verify_token(token: str, plan_hash: str, head_commit_id: int | None) -> bool:
    """Verify a migration token against the current plan and head."""
    expected = _compute_migration_token(plan_hash, head_commit_id)
    return token == expected


# --- Chain builder ---


def _chain_upgraders(
    registry: dict[tuple[str, int], Callable[..., Any]],
    type_name: str,
    from_version: int,
    to_version: int,
) -> Callable[[dict[str, Any]], dict[str, Any]]:
    """Build a composed upgrader function from from_version to to_version.

    Validates that every step in the chain exists.
    Raises MissingUpgraderError if any step is missing.
    """
    missing: list[int] = []
    chain: list[Callable[..., Any]] = []

    for v in range(from_version, to_version):
        key = (type_name, v)
        if key not in registry:
            missing.append(v)
        else:
            chain.append(registry[key])

    if missing:
        raise MissingUpgraderError({type_name: missing})

    def composed(fields: dict[str, Any]) -> dict[str, Any]:
        result = fields
        for fn in chain:
            result = fn(result)
        return result

    return composed


# --- Keep-alive thread ---


class _LeaseKeepAlive:
    """Daemon thread that renews a write lock lease periodically."""

    def __init__(self, repo: RepositoryProtocol, owner_id: str, lease_ttl_s: float) -> None:
        self._repo = repo
        self._owner_id = owner_id
        self._lease_ttl_s = lease_ttl_s
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        interval = self._lease_ttl_s / 3
        self._thread = threading.Thread(target=self._run, args=(interval,), daemon=True)
        self._thread.start()

    def _run(self, interval: float) -> None:
        while not self._stop_event.wait(timeout=interval):
            self._repo.renew_lock(self._owner_id, lease_ms=int(self._lease_ttl_s * 1000))

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
