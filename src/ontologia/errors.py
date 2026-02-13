"""Structured error types for Ontologia."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


class OntologiaError(Exception):
    """Base error for all Ontologia errors."""


class MetadataUnavailableError(OntologiaError):
    """Raised when meta() is called on a non-query-hydrated instance."""

    def __init__(self) -> None:
        super().__init__(
            "Metadata is only available on query-hydrated instances. "
            "Use ontology.query() to retrieve instances with metadata."
        )


@dataclass
class TypeSchemaDiff:
    """Describes the difference between stored and code schema for one type."""

    type_kind: str  # 'entity' or 'relation'
    type_name: str
    stored_version: int
    added_fields: list[str] = field(default_factory=list)
    removed_fields: list[str] = field(default_factory=list)
    changed_fields: dict[str, dict[str, Any]] = field(default_factory=dict)


class SchemaOutdatedError(OntologiaError):
    """Raised when code schema differs from stored schema at session time."""

    def __init__(self, diffs: list[TypeSchemaDiff]) -> None:
        self.diffs = diffs
        names = [d.type_name for d in diffs]
        super().__init__(
            f"Schema outdated for {len(diffs)} type(s): {names}. "
            "Call onto.migrate() to preview and apply migration."
        )


class SchemaValidationError(SchemaOutdatedError):
    """Deprecated alias for SchemaOutdatedError. Use SchemaOutdatedError instead."""

    def __init__(self, mismatches: list[str]) -> None:
        # Build minimal diffs from legacy mismatches
        diffs = [
            TypeSchemaDiff(
                type_kind="unknown", type_name=m.split("'")[1] if "'" in m else m, stored_version=0
            )
            for m in mismatches
        ]
        self.mismatches = mismatches
        super().__init__(diffs)


class MigrationError(OntologiaError):
    """Raised when a migration operation fails."""


class MigrationTokenError(MigrationError):
    """Raised when migration token is invalid or stale."""


class MissingUpgraderError(MigrationError):
    """Raised when required upgrader functions are not provided."""

    def __init__(self, missing: dict[str, list[int]]) -> None:
        self.missing = missing
        details = ", ".join(f"{name}: versions {vers}" for name, vers in missing.items())
        super().__init__(f"Missing upgraders: {details}")


class HandlerError(OntologiaError):
    """Raised for handler discovery, validation, or execution errors."""


class BatchSizeExceededError(OntologiaError):
    """Raised when a handler emits more intents than max_batch_size."""

    def __init__(self, count: int, limit: int) -> None:
        self.count = count
        self.limit = limit
        super().__init__(f"Handler emitted {count} intents, exceeding max_batch_size of {limit}")


class CommitChainDepthError(OntologiaError):
    """Raised when commit chain depth exceeds max_commit_chain_depth."""

    def __init__(self, depth: int, limit: int) -> None:
        self.depth = depth
        self.limit = limit
        super().__init__(f"Commit chain depth {depth} exceeds max_commit_chain_depth of {limit}")


class EventLoopLimitError(OntologiaError):
    """Raised when event processing exceeds max_event_chain_depth."""

    def __init__(self, depth: int, limit: int) -> None:
        self.depth = depth
        self.limit = limit
        super().__init__(f"Event chain depth {depth} exceeds max_event_chain_depth of {limit}")


class LockContentionError(OntologiaError):
    """Raised when write lock cannot be acquired within timeout."""

    def __init__(self, timeout_ms: int) -> None:
        self.timeout_ms = timeout_ms
        super().__init__(f"Could not acquire write lock within {timeout_ms}ms timeout")


class HeadMismatchError(OntologiaError):
    """Raised when head mismatch retries are exhausted."""

    def __init__(self, retries: int) -> None:
        self.retries = retries
        super().__init__(f"Head mismatch after {retries} retries; aborting commit")


class LeaseExpiredError(OntologiaError):
    """Raised when a write lease expires before commit finalization."""

    def __init__(self) -> None:
        super().__init__("Write lease expired before commit finalization")


class UninitializedStorageError(OntologiaError):
    """Raised when an S3 storage prefix has not been initialized."""

    def __init__(self, storage_uri: str) -> None:
        self.storage_uri = storage_uri
        super().__init__(
            f"Storage not initialized for '{storage_uri}'. Run `onto init --storage-uri ...` first."
        )


class StorageBackendError(OntologiaError):
    """Raised when backend storage operations fail."""

    def __init__(self, operation: str, detail: str) -> None:
        self.operation = operation
        self.detail = detail
        super().__init__(f"Storage backend error during {operation}: {detail}")


class ValidationError(OntologiaError):
    """Raised when data fails validation (e.g., unique constraint violation)."""

    def __init__(self, message: str) -> None:
        super().__init__(message)


class ConcurrentWriteError(OntologiaError):
    """Raised when write lock contention is detected and automatic retry is exhausted."""

    def __init__(self, message: str = "Concurrent write detected; please retry") -> None:
        super().__init__(message)
