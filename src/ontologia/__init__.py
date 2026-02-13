"""Ontologia: Typed, event-driven ontology engine."""

__version__ = "0.3.0"

from ontologia.config import OntologiaConfig
from ontologia.errors import (
    ConcurrentWriteError,
    EventLoopLimitError,
    LeaseExpiredError,
    MetadataUnavailableError,
    SchemaOutdatedError,
    StorageBackendError,
    TypeSchemaDiff,
    UninitializedStorageError,
    ValidationError,
)
from ontologia.event_handlers import HandlerContext, on_event
from ontologia.events import Event, EventDeadLetter, Schedule
from ontologia.filters import left, right
from ontologia.migration import MigrationPreview, MigrationResult, load_upgraders, upgrader
from ontologia.query import avg, count, max, min, sum
from ontologia.session import Session
from ontologia.types import Entity, Field, Meta, Relation, meta

__all__ = [
    "__version__",
    "Entity",
    "Relation",
    "Field",
    "Meta",
    "meta",
    "Event",
    "EventDeadLetter",
    "Schedule",
    "left",
    "right",
    "on_event",
    "HandlerContext",
    "count",
    "sum",
    "avg",
    "min",
    "max",
    "OntologiaConfig",
    "MetadataUnavailableError",
    "SchemaOutdatedError",
    "TypeSchemaDiff",
    "LeaseExpiredError",
    "UninitializedStorageError",
    "StorageBackendError",
    "ValidationError",
    "ConcurrentWriteError",
    "EventLoopLimitError",
    "MigrationPreview",
    "MigrationResult",
    "upgrader",
    "load_upgraders",
    "Session",
]
