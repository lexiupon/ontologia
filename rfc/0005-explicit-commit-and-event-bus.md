# RFC 0005: Explicit Commit in Handlers and Event Bus Runtime

## Status

Draft

## Created

2026-02-11

---

## Overview

This RFC fundamentally redesigns Ontologia's reactive model by replacing
implicit per-handler commits with explicit `commit()` calls and introducing a
first-class event bus runtime.

### What Changes

#### 1. Initialization API

**Before:**

```python
from ontologia import Ontology

# Two-step initialization
onto = Ontology(db_path="app.db")
session = onto.session()
```

**After:**

```python
from ontologia import Session

# Single-step initialization
session = Session(datastore_uri="sqlite:///app.db")
```

**Why:** Simplifies the API by removing the `Ontology` factory layer. Session is
the primary abstraction users interact with, and in the event-driven model,
sessions are long-lived and namespace-isolated, reducing the value of a shared
parent object.

#### 2. Handler Model

**Before (implicit model):**

```python
@on_commit
def index_document(ctx: CommitContext):
    ctx.ensure(DocumentIndex(...))
    # implicit commit at handler return
```

**After (explicit event-driven model):**

```python
from ontologia import Event, Field, on_event, HandlerContext

class DocumentCreated(Event):
    document_id: Field[str]
    title: Field[str]

class IndexUpdated(Event):
    document_id: Field[str]

@on_event(DocumentCreated, priority=50)
def index_document(ctx: HandlerContext[DocumentCreated]) -> None:
    doc_id = ctx.event.document_id  # type-safe!
    ctx.ensure(DocumentIndex(document_id=doc_id, ...))
    ctx.emit(IndexUpdated(document_id=doc_id))  # type-safe!
    ctx.commit()  # explicit!
```

### Why This Matters

The current reactive model implicitly commits intents after each handler
executes. This creates several problems:

1. **Opaque transactional boundaries** — handlers cannot control when state is
   persisted, making it hard to:

   - Batch multiple operations in a single commit
   - Implement streaming/incremental processing
   - Reason about partial progress and retry semantics

2. **Limited trigger sources** — only commits can trigger reactive work:

   - No native support for scheduled tasks
   - No way to integrate external events (webhooks, messages)
   - Custom workflows require workarounds

3. **Tight coupling** — commit and reaction are inseparable:
   - Cannot commit without triggering downstream handlers
   - Cannot trigger handlers without a commit
   - Hard to compose reactive chains with imperative code

An explicit commit API gives handlers full control over transactional
boundaries. A unified event bus provides a single, extensible abstraction for
commit-triggered work, scheduled tasks, and custom events.

### Core Principles

- **Explicit over implicit**: `commit()` is the only write boundary, called
  explicitly by handlers
- **Everything is event-driven**: Handlers respond to events; schedules and
  commits are just event sources
- **Opt-in everywhere**: Schedule events, commit events, and custom events all
  require explicit registration
- **Cross-session visibility**: A persistent event store enables multi-process
  coordination
- **Best-effort exactly-once**: Per-handler acknowledgements with transactional
  claims; handlers must be idempotent

---

## API Overview with Examples

### Example 1: Simple Event Handler

```python
from ontologia import Session, on_event, HandlerContext, Event, Field

class UserSignup(Event):
    user_id: Field[str]
    email: Field[str]

@on_event(UserSignup)
def send_welcome_email(ctx: HandlerContext[UserSignup]) -> None:
    # ctx.event is strongly typed as UserSignup
    user_id = ctx.event.user_id
    email = ctx.event.email

    # ... send email via external API ...

    ctx.ensure(EmailLog(
        user_id=user_id,
        type="welcome",
        sent_at=datetime.now()
    ))
    ctx.commit()  # explicit commit required
```

### Example 2: Event Chaining

```python
class OrderPlaced(Event):
    order_id: Field[str]
    user_id: Field[str]
    total: Field[float]
    priority: int = 10

class PaymentCompleted(Event):
    order_id: Field[str]
    priority: int = 20

class OrderFulfilled(Event):
    order_id: Field[str]

@on_event(OrderPlaced, priority=10)
def process_payment(ctx: HandlerContext[OrderPlaced]) -> None:
    order_id = ctx.event.order_id
    # ... charge payment ...

    ctx.ensure(Payment(order_id=order_id, status="completed"))
    # Emit event atomically with commit
    ctx.commit(event=PaymentCompleted(order_id=order_id))

@on_event(PaymentCompleted, priority=20)
def fulfill_order(ctx: HandlerContext[PaymentCompleted]) -> None:
    order_id = ctx.event.order_id
    # ... ship order ...

    # Alternative: buffer event first, then commit
    ctx.emit(OrderFulfilled(order_id=order_id))
    ctx.commit()
```

### Example 3: Scheduled Tasks

```python
from ontologia import Schedule

class SystemCleanup(Event):
    cutoff_days: Field[int] = 90
    priority: int = 50

# Define schedule
cleanup_schedule = Schedule(
    event=SystemCleanup(cutoff_days=90),
    cron="0 2 * * *"  # daily at 2am
)

# Handle scheduled event
@on_event(SystemCleanup)
def cleanup_old_data(ctx: HandlerContext[SystemCleanup]) -> None:
    cutoff_days = ctx.event.cutoff_days
    cutoff = datetime.now() - timedelta(days=cutoff_days)
    # ... query and delete old records ...
    ctx.commit()

# Register with session
session = Session(
    datastore_uri="sqlite:///app.db",
    namespace="system-tasks"  # or omit to use default namespace
)
session.run(
    handlers=[cleanup_old_data],
    schedules=[cleanup_schedule]
)
```

### Example 4: Batched Commits

```python
class BulkImport(Event):
    file_path: Field[str]

@on_event(BulkImport)
def import_large_dataset(ctx: HandlerContext[BulkImport]) -> None:
    file_path = ctx.event.file_path

    batch = []
    for row in read_csv(file_path):
        batch.append(Record(**row))

        if len(batch) >= 1000:
            ctx.ensure(batch)
            ctx.commit()  # commit every 1000 records
            batch = []

    if batch:
        ctx.ensure(batch)
        ctx.commit()  # commit remainder
```

### Example 5: Imperative + Reactive

```python
class UserCreated(Event):
    user_id: Field[str]

# Imperative: trigger event manually
session = Session(
    datastore_uri="sqlite:///app.db",
    namespace="users"  # or omit to use default namespace
)
session.ensure(User(id="u1", email="user@example.com"))
session.commit(event=UserCreated(user_id="u1"))

# Reactive: handler picks up the event (in same namespace)
@on_event(UserCreated)
def setup_user_workspace(ctx: HandlerContext[UserCreated]) -> None:
    user_id = ctx.event.user_id  # type-safe access!
    ctx.ensure(Workspace(user_id=user_id, ...))
    ctx.commit()
```

### Example 6: Multi-Namespace Processing

```python
import asyncio
from ontologia import Session

# Single process running multiple sessions (different namespaces)
async def main():
    # Orders namespace
    orders_session = Session(
        datastore_uri="sqlite:///app.db",
        namespace="orders"
    )

    # Payments namespace (different partition)
    payments_session = Session(
        datastore_uri="sqlite:///app.db",
        namespace="payments"
    )

    # Run both sessions concurrently in same process
    await asyncio.gather(
        asyncio.to_thread(orders_session.run, handlers=order_handlers),
        asyncio.to_thread(payments_session.run, handlers=payment_handlers)
    )

# Events in "orders" namespace never visible to "payments" namespace and vice versa
```

---

## Design Goals

1. **Make commit boundaries explicit** — handlers control exactly when state is
   persisted
2. **Unified event abstraction** — commits, schedules, and custom triggers are
   all events
3. **Namespace isolation** — events partitioned by namespace; no cross-namespace
   visibility
4. **Preserve delta semantics** — existing commit and reconciliation logic
   remains unchanged
5. **Atomic commit+event** — co-located event store ensures commits and events
   are atomically persisted

---

## Non-Goals

- **Nested transactions or savepoints** — each `commit()` is an independent
  transaction
- **Exactly-once for external side effects** — handlers must implement
  idempotency using standard distributed systems patterns (idempotency keys,
  deduplication tokens, etc.). This is outside the scope of Ontologia.
- **Historical event replay** — events expire after retention window; new
  handlers see only future events
- **Distributed coordination** — no cross-datastore transactions or distributed
  locking
- **Cross-namespace events** — events are partitioned by namespace; sessions
  cannot emit events to other namespaces
- **External side effect management** — the library provides event delivery and
  retry semantics for state changes; handling external API calls, emails,
  webhooks, etc. remains the application's responsibility following standard
  distributed systems best practices

---

## Terminology

| Term                      | Definition                                                                                                                            |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| **Event**                 | A strongly-typed dataclass inheriting from `Event` base class                                                                         |
| **Event Type**            | String identifier derived from event class name (e.g., `UserCreated` → `"user.created"`)                                              |
| **Namespace**             | Logical event partition (user-provided string, e.g., `"orders"`, `"payments"`, `"tenant-a"`)                                          |
| **Default Namespace**     | The namespace used when none is explicitly specified (configurable, defaults to `"default"`)                                          |
| **Session**               | Operational unit that processes events for a namespace, with auto-generated `session_id` (UUID) + metadata (hostname, PID, namespace) |
| **Handler**               | A function decorated with `@on_event(EventClass)` that processes events                                                               |
| **Handler ID**            | Stable identifier for a handler (`module.path:function_name`)                                                                         |
| **Event Chain**           | A causal sequence of events where each emitted event inherits `root_event_id` from its triggering event                               |
| **Root Event ID**         | The originating event ID for a chain; equals `event.id` for external events (schedules, imperative commits)                           |
| **Chain Depth**           | Number of handler hops from the root event; used to prevent infinite cascades                                                         |
| **Claim**                 | Transactional reservation of an event for a specific handler, with a lease timeout                                                    |
| **Acknowledgement (Ack)** | Per-handler confirmation that an event was successfully processed                                                                     |
| **Dead Letter**           | An event that exceeded `event_max_attempts` failures for a specific handler                                                           |

---

## Detailed API Surface

### Event Base Class

All events inherit from the `Event` base class (similar to `Entity` and
`Relation`).

**Design Rationale:**

- **Type safety**: Events are strongly-typed dataclasses, not string types with
  dict payloads
- **Compile-time validation**: Event fields and handler signatures are checked
  by type checkers
- **IDE support**: Autocomplete, refactoring, and type checking work seamlessly
- **No built-in commit events**: Applications must define custom event types for
  their domain
- **Explicit over implicit**: You define exactly which domain events you need

Implementation:

```python
from typing import ClassVar, Any

class Event:
    """Base class for all events. Subclass to define custom events.

    Uses Pydantic for validation and serialization (like Entity and Relation).
    """

    # Class variables
    __event_type__: ClassVar[str]           # Auto-derived: UserCreated -> "user.created"
    __event_fields__: ClassVar[tuple[str, ...]]  # User-defined field names
    _pydantic_model: ClassVar[type[BaseModel]]   # Internal Pydantic model
    _field_definitions: ClassVar[dict[str, Field[Any]]]  # Field metadata

    # Runtime metadata (set by event store, not by user)
    id: str | None = None                    # UUID v4
    created_at: str | None = None            # ISO 8601 timestamp
    priority: int = 100                      # Higher = processed first
    root_event_id: str | None = None         # Root of event chain
    chain_depth: int = 0                     # Hops from root

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        # Auto-derive event type from class name
        cls.__event_type__ = cls._derive_event_type(cls.__name__)

        # Collect Field definitions (like Entity/Relation)
        fields = _collect_fields(cls, {})
        cls._field_definitions = fields
        cls.__event_fields__ = tuple(fields.keys())

        # Build Pydantic model for validation
        cls._pydantic_model = _build_pydantic_model(f"_{cls.__name__}Model", fields)

    def __init__(self, **data: Any) -> None:
        """Initialize event with validation via Pydantic."""
        # Validate user fields through Pydantic
        validated = self._pydantic_model(**data)
        for name in self.__event_fields__:
            setattr(self, name, getattr(validated, name))

        # Runtime metadata defaults (overridden by event store)
        self.id = None
        self.created_at = None
        self.priority = 100
        self.root_event_id = None
        self.chain_depth = 0

    def model_dump(self) -> dict[str, Any]:
        """Serialize event to dict (for JSON storage).

        Returns only user-defined fields (not runtime metadata).
        Runtime metadata is stored in separate columns in event store.
        """
        return {name: getattr(self, name) for name in self.__event_fields__}

    @classmethod
    def model_validate(cls, data: dict[str, Any]) -> Event:
        """Deserialize event from dict (from JSON storage).

        Creates event instance from payload dict.
        Runtime metadata is set separately by event store.
        """
        return cls(**data)

    @staticmethod
    def _derive_event_type(class_name: str) -> str:
        """Convert PascalCase to dot.case: UserCreated -> user.created"""
        import re
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1.\2', class_name)
        return re.sub('([a-z0-9])([A-Z])', r'\1.\2', s1).lower()
```

### Defining Custom Events

Events use the same pattern as `Entity` and `Relation` - inherit from base
class, use `Field[T]` annotations:

```python
from ontologia import Event, Field

class UserCreated(Event):
    user_id: Field[str]
    email: Field[str]
    # priority can be overridden per event type if needed

class OrderPlaced(Event):
    order_id: Field[str]
    user_id: Field[str]
    total: Field[float]
    # Override default priority (higher = more urgent)
    priority: int = 50

class SystemCleanup(Event):
    cutoff_days: Field[int] = 90  # default value
```

Event types are auto-derived:

- `UserCreated` → `"user.created"`
- `OrderPlaced` → `"order.placed"`
- `SystemCleanup` → `"system.cleanup"`

**Note:** Like `Entity` and `Relation`, `Event` uses Pydantic validation under
the hood. No need for `@dataclass` decorator.

**Serialization:**

```python
# User creates event
event = UserCreated(user_id="u1", email="user@example.com")

# Runtime sets metadata before storage
event.id = "evt_123"
event.created_at = "2026-02-11T10:00:00Z"
event.priority = 100
event.root_event_id = "evt_123"
event.chain_depth = 0

# Serialize to JSON (user fields only)
payload = json.dumps(event.model_dump())
# {"user_id": "u1", "email": "user@example.com"}

# Store in event store (runtime metadata in separate columns)
INSERT INTO events (id, type, payload, created_at, priority, root_event_id, chain_depth)
VALUES ('evt_123', 'user.created', payload, '2026-02-11T10:00:00Z', 100, 'evt_123', 0)

# Deserialize from storage
payload_data = json.loads(row['payload'])
event = UserCreated.model_validate(payload_data)
# Runtime sets metadata from columns
event.id = row['id']
event.created_at = row['created_at']
event.priority = row['priority']
event.root_event_id = row['root_event_id']
event.chain_depth = row['chain_depth']
```

---

### Handler Context

**Design Rationale:**

- **Explicit commit required**: Handlers must call `ctx.commit()` — no implicit
  commit at handler return
- **Makes transactional boundaries obvious**: You control exactly when state is
  persisted
- **Enables batched processing**: Commit every N items, or commit multiple times
  in one handler
- **Buffered event emission**: Events via `ctx.emit()` are only enqueued if
  handler succeeds
- **Atomic handler semantics**: Handler either fully succeeds (including all
  commits and emits) or is retried

Implementation:

```python
from typing import TypeVar, Generic

TEvent = TypeVar('TEvent', bound=Event)

class HandlerContext(Generic[TEvent]):
    event: TEvent  # strongly typed!

    def ensure(
        self,
        obj: Entity | Relation | Iterable[Entity | Relation]
    ) -> None:
        """Queue entities/relations for reconciliation (same as Session.ensure)"""
        ...

    def emit(self, event: Event) -> None:
        """Buffer an event for emission (enqueued only if handler succeeds)"""
        ...

    def add_commit_meta(self, key: str, value: str) -> None:
        """Attach metadata to the next commit() in this handler"""
        ...

    def commit(
        self,
        *,
        event: Event | None = None,
    ) -> int | None:
        """
        Persist queued intents and optionally emit an event.

        Returns:
            commit_id if changes were persisted, None if no changes

        Raises:
            LeaseExpiredError: if the claim lease has expired
            DatabaseError: if commit or event enqueue fails (atomic transaction)

        The commit checks if the claim lease has expired (now > lease_until).
        If expired, raises LeaseExpiredError to prevent duplicate processing.
        This forces handlers to complete within event_claim_lease_ms duration.
        """
        ...
```

**Important Notes on `commit()`:**

When `ctx.commit()` is called:

1. **Lease expiration check**: Verifies claim lease is still valid (now <
   lease_until)

   - If expired → raises `LeaseExpiredError`
   - Buffered events are discarded
   - Handler is treated as failed and will be retried
   - **Prevents duplicate processing** by another session

2. **Database/constraint failures**: Database errors, conflicts, constraint
   violations
   - Raises exception (e.g., `DatabaseError`, `IntegrityError`)
   - All buffered events are discarded
   - Handler is treated as failed and will be retried
   - Event claim is released with exponential backoff

Handlers must be **idempotent** (safe to retry):

- **State changes via `ctx.ensure()` are naturally idempotent** — delta
  reconciliation makes replayed commits no-ops
- **External side effects must use idempotency keys** — API calls, emails,
  webhooks, etc.
- With multiple commits: if the 3rd fails, first 2 are durable and become no-ops
  on retry

**Example of safe multiple commits:**

```python
@on_event(BulkImport)
def import_data(ctx: HandlerContext[BulkImport]) -> None:
    # Batch 1
    ctx.ensure([Record(id=1), Record(id=2)])
    ctx.commit()  # ✅ Succeeds, durable

    # Batch 2
    ctx.ensure([Record(id=3), Record(id=4)])
    ctx.commit()  # ❌ FAILS

# On retry:
# - Batch 1 commit is a no-op (records already exist)
# - Batch 2 commit retries successfully
# Result: No duplicate state, safe to retry
```

---

### Handler Decorator

**Design Rationale:**

- **Explicit opt-in**: Handlers subscribe to specific user-defined event classes
- **No automatic commit events**: Commits don't trigger handlers unless you emit
  a custom event
- **Decouples commit from reaction**: You can commit without triggering handlers
- **Handler priority**: Higher priority handlers process events first within the
  same event type

Implementation:

```python
from typing import TypeVar, Callable, Any

TEvent = TypeVar('TEvent', bound=Event)

def on_event(
    event_cls: type[TEvent],
    *,
    priority: int = 100,
) -> Callable[[Callable[[HandlerContext[TEvent]], None]], Callable[..., Any]]:
    """
    Register a handler for the given event class.

    Args:
        event_cls: Event class to subscribe to (e.g., UserCreated)
        priority: Handler priority (higher = runs first; default 100)

    Handler signature:
        def handler(ctx: HandlerContext[TEvent]) -> None: ...

    Example:
        @on_event(UserCreated, priority=50)
        def on_user_created(ctx: HandlerContext[UserCreated]) -> None:
            # ctx.event is type UserCreated
            print(ctx.event.user_id)  # type-safe!
    """
    ...
```

**Handler Priority Semantics:**

- Within the same event type, handlers are executed in priority order (highest
  first)
- Handlers with the same priority are sorted by handler ID (deterministic)
- Handler priority is independent of event priority

---

### Schedule Definition

**Design Rationale:**

- **Separates "when" from "what"**: Schedule config object separate from handler
  decorator
- **Dynamic registration**: Enable/disable schedules at runtime
- **Uniform semantics**: Schedule events handled by `@on_event` like any other
  event
- **Per-namespace isolation**: Each namespace defines its own schedules
- **Coordination via existing infrastructure**: Multiple instances coordinate
  via claim/lease

Implementation:

```python
from dataclasses import dataclass, field

class Schedule:
    event: Event                # event instance to emit (with field values)
    cron: str                   # cron expression (standard 5-field)
```

**Example:**

```python
cleanup_schedule = Schedule(
    event=SystemCleanup(cutoff_days=90, priority=50),
    cron="0 2 * * *"  # daily at 2am
)
```

**Cron Expressions:**

- `"0 2 * * *"` — daily at 2:00 AM
- `"*/15 * * * *"` — every 15 minutes
- `"0 0 * * 0"` — weekly on Sunday at midnight

---

### Session

**Design Rationale:**

- **Namespace partitioning**: Events logically isolated by namespace for
  multi-tenancy or domain separation
- **Default namespace**: Simplifies single-namespace applications
- **Atomic commit+event**: `commit(event=...)` persists state and enqueues event
  in single transaction
- **No partial state**: If event enqueue fails, entire commit fails (no need for
  outbox pattern)
- **Multi-namespace support**: Single runtime can process multiple namespaces
  via multiple Sessions
- **Simplified API**: Session created directly from datastore URI (change from
  current `Ontology` → `session()` pattern)

**Note:** This RFC proposes simplifying the current two-step initialization
(`Ontology(...)` → `ontology.session()`) into a single-step `Session(...)`
constructor. The Session will handle repository initialization and schema
management internally.

Implementation:

```python
class Session:
    def __init__(
        self,
        datastore_uri: str,
        namespace: str | None = None,
        *,
        entity_types: list[type[Entity]] | None = None,
        relation_types: list[type[Relation]] | None = None,
        instance_metadata: dict[str, Any] | None = None,
        config: OntologiaConfig | None = None,
    ):
        """
        Create a new session.

        Args:
            datastore_uri: URI for the datastore (e.g., "sqlite:///app.db")
            namespace: Event namespace for this session (defaults to configured default namespace)
            entity_types: Entity classes to register (optional, can be auto-discovered from handlers)
            relation_types: Relation classes to register (optional, can be auto-discovered from handlers)
            instance_metadata: Optional metadata for this session
                              (e.g., {"hostname": "host-1", "pid": 1234})
            config: Optional configuration override

        The session auto-generates a session_id (UUID) and registers itself
        in the sessions table with the namespace and metadata.

        Note: This is a simplification from the current Ontology → session() pattern.
        """
        ...

    def ensure(
        self,
        obj: Entity | Relation | Iterable[Entity | Relation]
    ) -> None:
        """Queue entities/relations for reconciliation (imperative mode)"""
        ...

    def commit(
        self,
        *,
        event: Event | None = None,
    ) -> int | None:
        """
        Persist queued intents and optionally emit an event (imperative mode).

        Args:
            event: Event instance to emit after commit (optional)

        Returns:
            commit_id if changes were persisted, None if no changes

        Raises:
            LeaseExpiredError: if called from a handler and claim lease has expired
            DatabaseError: if commit or event enqueue fails (atomic transaction)

        The event is enqueued to this session's namespace.
        Commit metadata includes the namespace.

        When called from a handler context (reactive mode), checks claim lease
        before committing. In imperative mode (no active claim), no lease check.

        Example:
            session.commit(event=UserCreated(user_id="u1", email="x@y.com"))
        """
        ...

    def list_commit_changes(self, commit_id: int) -> list[dict[str, Any]]:
        """
        Retrieve change records for a specific commit.

        Returns:
            List of dicts with keys: entity_id, relation_id, change_type
        """
        ...

    def run(
        self,
        handlers: list[Callable[..., Any]],
        *,
        schedules: list[Schedule] | None = None,
    ) -> None:
        """
        Start the event loop: claim events, execute handlers, drain queue.

        Runs until interrupted (Ctrl+C) or an unrecoverable error occurs.

        The runtime builds an EVENT_REGISTRY mapping event type strings to Event classes
        by inspecting the handlers list. This enables deserialization from storage:
        EVENT_REGISTRY['user.created'] -> UserCreated class

        Only events with namespace matching this session's namespace are processed.

        Runs indefinitely until interrupted (Ctrl+C) or an unrecoverable error occurs.

        Raises:
            EventLoopLimitError: if max_event_chain_depth exceeded
        """
        ...
```

---

### Exceptions

```python
class EventLoopLimitError(RuntimeError):
    """Raised when event processing exceeds max_event_chain_depth"""
    pass

class LeaseExpiredError(RuntimeError):
    """Raised when handler attempts to commit after claim lease has expired"""
    pass
```

---

## Configuration Changes

```python

class OntologiaConfig:
    # ... existing fields ...

    # Namespace configuration
    default_namespace: str = "default"
        # Default namespace when Session doesn't specify one

    # Event loop behavior
    event_poll_interval_ms: int = 1000
        # Minimum sleep time between polling iterations (milliseconds)
        # Rate limits event processing and ensures responsive shutdown

    event_claim_limit: int = 100
        # Max events to claim per handler per poll
        # Fair distribution: prevents one handler from claiming all events

    max_events_per_iteration: int = 1000
        # Max total events to process in one iteration (across all handlers)
        # Safety valve: prevents runaway cascades from blocking the loop
        # Loop resets counter after sleeping for event_poll_interval_ms

    event_claim_lease_ms: int = 30000
        # How long a claimed event is reserved (milliseconds)
        # Should be >> handler execution time

    # Event retention and cleanup
    event_retention_ms: int = 604800000  # 7 days
        # How long events are retained before garbage collection

    # Runtime instance heartbeats
    session_heartbeat_interval_ms: int = 5000
        # How often to update session heartbeat (milliseconds)

    session_ttl_ms: int = 60000
        # After this time without heartbeat, session is considered dead

    # Failure handling
    event_max_attempts: int = 10
        # Max retries before dead-lettering an event for a handler

    event_backoff_base_ms: int = 250
        # Base backoff interval (milliseconds)

    event_backoff_max_ms: int = 30000
        # Max backoff interval (milliseconds)

    # Loop guards
    max_event_chain_depth: int = 20
        # Max hops from root event before rejecting emit
        # Prevents infinite event loops (A → B → A → B ...)

    # Note: max_events_per_run removed - sessions run indefinitely
    # Use max_events_per_iteration to limit events per polling cycle
```

**Configuration Notes:**

- **`default_namespace`**: Namespace used when Session doesn't specify one
- **`event_poll_interval_ms`**: Minimum sleep between iterations; rate limits
  event processing
- **`event_claim_limit`**: Per-handler batch size; prevents one handler from
  claiming all events
  - **For multi-session deployments**: Use smaller values (10-50) for better
    work distribution
  - **For single session**: Use larger values (100+) for batch efficiency
- **`max_events_per_iteration`**: Total events per iteration (all handlers);
  safety valve against runaway cascades
- **`event_claim_lease_ms`**: Should be at least 2-3x your longest handler
  execution time
  - **Trade-off**: Shorter lease = faster recovery, but risk duplicate
    processing
  - **Multi-session deployments**: Use shorter leases (10-15s) to reduce
    head-of-line blocking
- **`event_retention_ms`**: Balance storage cost vs. new handler grace period
- **`session_heartbeat_interval_ms`**: Heartbeat frequency for liveness tracking
- **`session_ttl_ms`**: How long before a silent session is considered dead
- **`max_event_chain_depth`**: Increase only if you have verified non-circular
  deep chains

---

## Event Store Interface

**Design Rationale:**

- **Claim/ack/release pattern**: Enables best-effort exactly-once delivery per
  handler
- **Per-handler acknowledgements**: Handlers can be added/removed dynamically
  without global coordination
- **Transactional claims**: Events claimed atomically with lease timeout
- **Exponential backoff**: Failed events retry with increasing delay to prevent
  overwhelming failing handlers
- **Per-handler dead-lettering**: Allows partial progress (some handlers
  succeed, others fail)

Implementation:

```python
from typing import Protocol

class EventStore(Protocol):
    def enqueue(self, event: Event, namespace: str) -> None:
        """
        Add an event to the store for the specified namespace.

        Args:
            event: Event instance to enqueue
            namespace: Namespace identifier (partitioning key)

        Raises:
            Exception: if enqueue fails (implementation-specific)
        """
        ...

    def claim(
        self,
        namespace: str,
        handler_id: str,
        session_id: str,
        event_types: list[str],
        limit: int,
        lease_ms: int,
    ) -> list[Event]:
        """
        Claim up to `limit` events of given types for the handler.

        Only events with matching namespace are visible.

        Must be transactional: events are claimed atomically and marked with
        lease_until = now + lease_ms.

        Returns:
            List of claimed events (may be fewer than `limit`)
        """
        ...

    def ack(self, handler_id: str, event_id: str) -> None:
        """
        Mark an event as successfully processed by the handler.

        Sets ack_at timestamp for the (handler_id, event_id) claim.
        """
        ...

    def release(
        self,
        handler_id: str,
        event_id: str,
        *,
        error: str | None = None,
    ) -> None:
        """
        Release a claimed event due to handler failure.

        - Increments attempts counter
        - Sets available_at with exponential backoff
        - If attempts >= event_max_attempts, dead-letters the event for this handler
        - Emits event.dead_letter event if dead-lettered
        """
        ...

    def register_session(
        self,
        session_id: str,
        namespace: str,
        metadata: dict[str, Any],
    ) -> None:
        """
        Register a session.

        Args:
            session_id: Unique session identifier (UUID)
            namespace: Namespace this session is processing
            metadata: Session metadata (hostname, pid, etc.)
        """
        ...

    def heartbeat(self, session_id: str) -> None:
        """
        Update the last_heartbeat timestamp for a session.

        Args:
            session_id: Session identifier
        """
        ...
```

---

### Default SQLite Backend

The default implementation uses SQLite (co-located with datastore) with four
tables.

**Design Rationale:**

- **Co-located with datastore**: Enables atomic commit+event transactions
- **Namespace partitioning**: Each namespace has its own event queue
- **Runtime metadata in columns**: id, created_at, priority, root_event_id,
  chain_depth stored for queryability
- **User fields in JSON**: Payload stored as JSON for flexibility
- **Event identity**: UUID v4 provides globally unique IDs without coordination
- **Chain propagation**: Root event ID enables tracing; chain depth enables loop
  detection

Schema:

```sql
CREATE TABLE events (
    id TEXT PRIMARY KEY,              -- UUID v4
    namespace TEXT NOT NULL,          -- Event namespace (logical partition)
    type TEXT NOT NULL,               -- "user.created", "order.placed", etc.
    payload TEXT NOT NULL,            -- JSON serialized user fields (via event.model_dump())
    created_at TEXT NOT NULL,         -- ISO 8601 timestamp
    priority INTEGER NOT NULL DEFAULT 100,  -- Higher = more urgent
    root_event_id TEXT NOT NULL,      -- Root of event chain (for tracing)
    chain_depth INTEGER NOT NULL DEFAULT 0,  -- Hops from root (for loop detection)
    INDEX idx_namespace_type (namespace, type, priority DESC, created_at ASC)
);

CREATE TABLE event_claims (
    event_id TEXT NOT NULL,
    handler_id TEXT NOT NULL,
    session_id TEXT NOT NULL,        -- Which session claimed it
    claimed_at TEXT NOT NULL,
    lease_until TEXT NOT NULL,
    ack_at TEXT,                      -- NULL if not acked
    attempts INTEGER NOT NULL DEFAULT 0,
    available_at TEXT NOT NULL,       -- ISO 8601, per-handler exponential backoff
    last_error TEXT,
    dead_lettered_at TEXT,            -- NULL if not dead-lettered
    PRIMARY KEY (event_id, handler_id)
);

CREATE TABLE dead_letters (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL,
    handler_id TEXT NOT NULL,
    namespace TEXT NOT NULL,          -- For filtering by namespace
    failed_at TEXT NOT NULL,
    attempts INTEGER NOT NULL,
    last_error TEXT NOT NULL,
    event_type TEXT NOT NULL,
    event_payload TEXT NOT NULL,      -- JSON (copy of original payload)
    root_event_id TEXT NOT NULL,
    chain_depth INTEGER NOT NULL
);

CREATE TABLE sessions (
    session_id TEXT PRIMARY KEY,     -- UUID
    namespace TEXT NOT NULL,          -- Which namespace this session processes
    started_at TEXT NOT NULL,
    last_heartbeat TEXT NOT NULL,
    metadata TEXT,                     -- JSON: {hostname, pid, ...}
    INDEX idx_sessions_heartbeat (last_heartbeat),
    INDEX idx_sessions_namespace (namespace)
);
```

**Storage Strategy:**

- **Runtime metadata** (id, created_at, priority, root_event_id, chain_depth,
  namespace) stored in **columns** for queryability
- **User-defined fields** stored in **`payload` column** as JSON via
  `event.model_dump()`
- This allows efficient querying by priority, created_at, type, namespace, etc.
  while keeping user fields flexible

**Example:**

```python
# Event instance
event = UserCreated(user_id="u1", email="user@example.com")

# Runtime sets metadata
event.id = "evt_abc123"
event.created_at = "2026-02-11T10:00:00Z"
event.priority = 100
event.root_event_id = "evt_abc123"
event.chain_depth = 0

# Storage
payload_json = json.dumps(event.model_dump())
# payload_json = '{"user_id": "u1", "email": "user@example.com"}'

INSERT INTO events (id, type, payload, created_at, priority, root_event_id, chain_depth, available_at)
VALUES ('evt_abc123', 'user.created', payload_json, '2026-02-11T10:00:00Z', 100, 'evt_abc123', 0, '2026-02-11T10:00:00Z');

# Retrieval
SELECT * FROM events WHERE type = 'user.created' AND available_at <= datetime('now')
ORDER BY priority DESC, created_at ASC LIMIT 10;

# Deserialize
event_class = EVENT_REGISTRY['user.created']  # UserCreated
payload_data = json.loads(row['payload'])
event = event_class.model_validate(payload_data)
event.id = row['id']
event.created_at = row['created_at']
event.priority = row['priority']
event.root_event_id = row['root_event_id']
event.chain_depth = row['chain_depth']
```

---

#### Claim Logic

```sql
-- Find claimable events for handler (filtered by namespace)
SELECT e.* FROM events e
LEFT JOIN event_claims c ON e.id = c.event_id AND c.handler_id = ?
WHERE e.namespace = ?  -- FILTER BY NAMESPACE
  AND e.type IN (?, ?, ...)
  AND (
    c.event_id IS NULL  -- never claimed by this handler
    OR (
      c.ack_at IS NULL
      AND c.dead_lettered_at IS NULL
      AND c.lease_until < datetime('now')  -- lease expired
      AND c.available_at <= datetime('now')  -- backoff elapsed (per-handler)
    )
  )
ORDER BY e.priority DESC, e.created_at ASC, e.id ASC
LIMIT ?;

-- Insert claim (with session_id)
INSERT INTO event_claims (event_id, handler_id, session_id, claimed_at, lease_until, attempts)
VALUES (?, ?, ?, datetime('now'), datetime('now', '+' || ? || ' milliseconds'), COALESCE(c.attempts, 0))
ON CONFLICT (event_id, handler_id) DO UPDATE SET
  claimed_at = excluded.claimed_at,
  lease_until = excluded.lease_until,
  session_id = excluded.session_id;
```

#### Ack Logic

```sql
UPDATE event_claims
SET ack_at = datetime('now')
WHERE event_id = ? AND handler_id = ?;
```

#### Release Logic (with backoff and dead-lettering)

```python
def release(handler_id: str, event_id: str, error: str | None):
    attempts = get_attempts(event_id, handler_id) + 1

    if attempts >= config.event_max_attempts:
        # Dead-letter
        insert_dead_letter(event_id, handler_id, attempts, error)
        update_claim(event_id, handler_id, dead_lettered_at=now())
        enqueue(Event(
            type="event.dead_letter",
            payload={
                "event_id": event_id,
                "handler_id": handler_id,
                "attempts": attempts,
                "last_error": error,
            }
        ))
    else:
        # Backoff
        backoff_ms = min(
            config.event_backoff_base_ms * (2 ** attempts),
            config.event_backoff_max_ms
        ) + random.randint(0, 100)  # jitter

        available_at = now() + timedelta(milliseconds=backoff_ms)
        update_claim(event_id, handler_id, attempts=attempts, last_error=error, available_at=available_at)
```

#### Claim State Machine

An `(event_id, handler_id)` pair is in exactly one of the following states:

| State             | Claim Row                                                         | Meaning                         |
| ----------------- | ----------------------------------------------------------------- | ------------------------------- |
| **Unclaimed**     | No row exists                                                     | Never seen by this handler      |
| **Claimed**       | `lease_until > now`, `ack_at IS NULL`, `dead_lettered_at IS NULL` | Handler is actively processing  |
| **Backoff**       | `lease_until < now`, `available_at > now`, `ack_at IS NULL`       | Waiting for retry after failure |
| **Reclaimable**   | `lease_until < now`, `available_at <= now`, `ack_at IS NULL`      | Ready for retry                 |
| **Acked**         | `ack_at IS NOT NULL`                                              | Successfully processed          |
| **Dead-lettered** | `dead_lettered_at IS NOT NULL`                                    | Permanently failed              |

**Transitions:**

```
Unclaimed ──claim──► Claimed ──handler success──► Acked
                        │
                        ├──handler failure──► Backoff ──backoff elapsed──► Reclaimable ──claim──► Claimed
                        │
                        └──lease expires (no ack)──► Reclaimable ──claim──► Claimed

Backoff/Reclaimable ──max attempts exceeded──► Dead-lettered
```

**Key invariants:**

- **Acked and dead-lettered are terminal** — once set, the claim is never
  reclaimed
- **Dead-lettering is per-handler** — handler A dead-lettering an event does not
  affect handler B's ability to process the same event
- **Lease is the coordination primitive** — only one session can hold a valid
  lease for a given `(event_id, handler_id)` at a time
- **Backoff is per-handler** — `available_at` is on the claim row, so handler A
  failing with backoff does not delay handler B from claiming the same event

#### Garbage Collection

Periodically (e.g., daily):

```sql
-- Delete old events
DELETE FROM events
WHERE created_at < datetime('now', '-' || ? || ' milliseconds');

-- Cascade delete claims
DELETE FROM event_claims
WHERE event_id NOT IN (SELECT id FROM events);
```

---

### S3 Event Store Implementation

For distributed deployments or multi-region scenarios, an S3-based event store
can be used:

#### S3 Storage Layout

```
s3://bucket/events/
├── orders/                           # Namespace
│   ├── events/
│   │   ├── evt_001.json              # One event per file
│   │   ├── evt_002.json
│   │   └── ...
│   └── index/
│       └── index.json                # Namespace-level index/cursor
└── payments/                         # Different namespace
    ├── events/
    └── index/
```

#### Event File Format

```json
{
  "id": "evt_001",
  "namespace": "orders",
  "type": "order.placed",
  "payload": { "order_id": "ord_123", "total": 99.99 },
  "created_at": "2026-02-11T10:00:00Z",
  "priority": 100,
  "root_event_id": "evt_001",
  "chain_depth": 0,
  "available_at": "2026-02-11T10:00:00Z"
}
```

#### Index File Format

```json
{
  "namespace": "orders",
  "last_processed_event_id": "evt_001",
  "last_processed_at": "2026-02-11T10:00:01Z",
  "handlers": {
    "process_order": {
      "last_ack_event_id": "evt_001",
      "last_ack_at": "2026-02-11T10:00:01Z"
    }
  }
}
```

#### Claim Coordination

Since S3 doesn't support transactional claims, one of these approaches is
needed:

**Option 1: DynamoDB for Claims**

- Store `event_claims` table in DynamoDB
- Use conditional writes for atomic claim operations
- S3 for event storage (cheap), DynamoDB for coordination (fast)

**Option 2: SQLite for Claims**

- Store events in S3, claims in local SQLite
- Each instance maintains its own claim database
- Works for single-instance runtimes or with leader election

**Option 3: Separate Claim Service**

- Dedicated service manages claims across S3 events
- Multiple sessions communicate with claim service
- Adds complexity but enables true multi-session S3 processing

**Recommendation:** Start with SQLite event store. Add S3 support later for
specific use cases (e.g., multi-region, archival, cold storage).

---

## Event Processing Loop

The `Session.run()` method implements the event processing loop.

**Design Rationale:**

- **Priority-then-FIFO ordering**: Events ordered by
  `priority DESC, created_at ASC, id ASC`
  - Priority enables urgency control (user-facing > background)
  - FIFO within priority is simple and predictable
- **Loop guards**: Prevent runaway cascades
  - `max_event_chain_depth` limits cascades (default 20 hops)
  - `max_events_per_iteration` bounds events per polling cycle (default 1,000)
- **Buffered emission**: Events enqueued only after handler success
  - Prevents cascading partial failures
  - Maintains event causality (failed handler's events never materialize)
- **Graceful shutdown**: Outstanding claims released on interrupt

Loop implementation:

```
1. Register session in sessions table with namespace

2. Sort registered handlers by priority DESC, then handler_id ASC

3. While not interrupted:
   a. Update session heartbeat

   b. For each handler:
      - Claim events for its subscribed types (filtered by namespace, up to event_claim_limit)
      - For each claimed event:
        * Create HandlerContext with the event
        * Execute handler(ctx)
        * On success:
          - Enqueue buffered events from ctx.emit() (with same namespace)
          - Ack the event claim
        * On failure:
          - Discard buffered events
          - Release the event claim with backoff
          - If max attempts exceeded, dead-letter

   c. Drain in-memory event queue (newly emitted events)

   d. Check iteration limit:
      - If total events processed in this iteration >= max_events_per_iteration:
        * Stop processing events for this iteration
        * Counter resets on next iteration

   e. Sleep for event_poll_interval_ms

4. On interrupt (Ctrl+C):
   - Release all outstanding claims
   - Mark session as stopped
   - Exit cleanly
```

**Key Properties:**

- **Handler priority**: Higher priority handlers claim and process events first
- **Event priority**: Within a handler, higher priority events are claimed first
- **Buffered emission**: Events are enqueued only after handler success
- **Best-effort exactly-once**: Transactional claims prevent duplicate
  processing (with rare exceptions on crashes)
- **Graceful shutdown**: Outstanding claims are released on interrupt
- **Runs indefinitely**: Session continues until interrupted or unrecoverable
  error

**Polling Behavior:**

Each iteration:

1. Claims up to `event_claim_limit` events per handler (e.g., 100
   events/handler)
2. Processes events until `max_events_per_iteration` reached (e.g., 1,000 total
   events)
3. Sleeps for `event_poll_interval_ms` (e.g., 1 second)
4. Resets counter and repeats

This ensures:

- ✅ Runaway cascades can't block the loop indefinitely
- ✅ Fair processing across handlers (`event_claim_limit`)
- ✅ Responsive shutdown (sleeps periodically)
- ✅ Rate limiting (`event_poll_interval_ms` minimum)
- ✅ Session runs forever (long-lived)

**Multi-Session Coordination:**

When multiple sessions process the same namespace:

- **Claims are per (handler, event) pair**: Same handler in different sessions
  cannot claim same event simultaneously
- **Lease-based coordination**: Session A's claim blocks Session B for
  `event_claim_lease_ms` duration
- **Non-blocking queue**: Session B claims next available events (not blocked by
  Session A's claims)
- **Recovery on crash**: If Session A crashes, its claims expire after lease
  duration and Session B can retry

**Tuning for Multi-Session:**

- **Small `event_claim_limit` (10-50)**: Better work distribution, less
  head-of-line blocking
- **Short `event_claim_lease_ms` (10-15s)**: Faster recovery, but requires fast
  handlers
- **Separate handlers by priority**: Dedicate sessions to urgent vs normal work

**Trade-offs:**

- Larger claims = better throughput per session, worse distribution across
  sessions
- Longer lease = safer (no duplicate processing), slower recovery on crash
- More sessions = better parallelism, more claim contention

---

## Commit Semantics

### Commit Process

1. Handler calls `ctx.commit()` or `session.commit()`
2. Queued intents are reconciled using existing delta logic
3. Changes are persisted in a single transaction (respects `max_batch_size`)
4. A commit record is created with metadata
5. If `event=...` is provided, the **custom event** is enqueued atomically with
   the commit
6. Returns `commit_id` (or `None` if no changes)

### Commit Metadata

- `ctx.add_commit_meta(key, value)` attaches metadata to the next commit
- Metadata is persisted in the commit record
- Useful for audit trails, debugging, observability

### Custom Commit Events

Applications can define custom events to emit on commit:

```python
class DataSyncCompleted(Event):
    commit_id: Field[int]
    namespace: Field[str]
    record_count: Field[int]

# Emit custom event on commit
session.ensure(records)
session.commit(event=DataSyncCompleted(
    commit_id=ctx.commit_id,  # if available
    namespace="orders",
    record_count=len(records)
))
```

Handlers can use `session.list_commit_changes(commit_id)` to fetch change
identities, then query with `as_of(commit_id)` to inspect values.

---

## Other Considerations

### Why Best-Effort Exactly-Once (Not Strict Exactly-Once)

Events are acknowledged **per handler** with transactional claims, providing
best-effort exactly-once delivery:

- Transactional claims prevent duplicate processing within event store semantics
- Duplicates possible on crashes between side-effects and ack (rare but
  possible)
- Acknowledges the impossibility of exactly-once for external side effects
- Delta-based commits naturally provide idempotency for state changes

**Implication**: Handlers must be idempotent:

- State mutations via `ctx.ensure()` are idempotent by design (delta
  reconciliation)
- External side effects (API calls, emails) require idempotency keys

### Why Event Buffering Until Handler Success

`ctx.emit()` buffers events in-memory during handler execution:

- Buffered events enqueued **only if handler returns successfully** (including
  all commits)
- On handler error or commit failure, buffered events discarded

**Benefits**:

- Prevents cascading partial failures
- Maintains event causality (failed handler's events never materialize)
- Simplifies error recovery (no need to "undo" emitted events)

**Trade-off**: Events not visible until emitting handler completes (handlers
should complete quickly)

### Why Type-Safe Events Over String Types

Events are strongly-typed dataclasses, not string types with dict payloads:

- **Compile-time validation**: Event fields and handler signatures checked by
  type checkers
- **Better IDE support**: Autocomplete, refactoring, type checking work
  seamlessly
- **Simplifies event store**: Treats all events uniformly (no entity-specific
  logic)
- **Enables flexible filtering**: Handlers can filter on any criteria
  (imperatively)
- **Explicit over implicit**: Applications define exactly which domain events
  they need

**Recommended pattern**: Always use custom typed events for domain-specific
needs.

### Why Chain Depth Limits

Events track `root_event_id` and `chain_depth` for loop detection:

- External events (schedules, imperative commits) have `chain_depth == 0`
- Events emitted by handlers increment `chain_depth`
- `max_event_chain_depth` (default 20) prevents infinite loops

**Benefits**:

- Root event ID enables tracing and observability
- Chain depth enables cascade limiting
- UUID v4 provides globally unique IDs without coordination

### Alternative Approaches Considered

**Implicit commit at handler return**: Rejected because it makes transactional
boundaries opaque and prevents batched processing.

**Built-in commit events**: Rejected in favor of custom typed events for better
type safety and explicit opt-in.

**Global event acknowledgements**: Rejected because per-handler acks allow
dynamic handler addition/removal.

**Separate event store**: Rejected because co-located store enables atomic
commit+event transactions.

**Exactly-once guarantees**: Impossible for external side effects; best-effort
with idempotency is more honest and practical.

---

## Compatibility and Migration

### Breaking Changes

This RFC introduces breaking changes. No backward compatibility is provided.

**Removed:**

- `@on_commit` decorator → replaced by `@on_event("commit")` or custom event
  types
- `@on_commit_entity(entity_type=...)` decorator → replaced by
  `@on_event("commit")` with manual filtering inside handler
- `@on_schedule` decorator → replaced by `Schedule` + `@on_event`
- Implicit commit at handler return → must call `ctx.commit()` explicitly
- Declarative entity-type filtering → filtering now happens imperatively inside
  handlers

**Migration Path:**

1. **Update handler decorators to use custom events:**

   ```python
   # Before
   @on_commit
   def my_handler(ctx): ...

   # After: Define custom event for your domain
   class DataUpdated(Event):
       updated_by: Field[str]

   @on_event(DataUpdated)
   def my_handler(ctx: HandlerContext[DataUpdated]) -> None: ...
   ```

2. **Add explicit commits:**

   ```python
   # Before
   @on_commit
   def my_handler(ctx):
       ctx.ensure(...)
       # implicit commit here

   # After: Use custom events
   @on_event(DataUpdated)
   def my_handler(ctx: HandlerContext[DataUpdated]) -> None:
       ctx.ensure(...)
       ctx.commit()  # explicit!
   ```

3. **Update entity-filtered handlers with typed events:**

   ```python
   # Before (declarative filtering, no type safety)
   @on_commit_entity(entity_type=User)
   def on_user_change(ctx, entity: User):
       send_email(entity.email)

   # After: Custom typed events (recommended approach)
   class UserUpdated(Event):
       user_id: Field[str]
       email: Field[str]

   # Imperative code emits typed event
   session.ensure(User(id="u1", email="new@example.com"))
   session.commit(event=UserUpdated(user_id="u1", email="new@example.com"))

   # Handler is type-safe (no filtering needed)
   @on_event(UserUpdated)
   def on_user_change(ctx: HandlerContext[UserUpdated]) -> None:
       user_id = ctx.event.user_id  # IDE autocomplete!
       email = ctx.event.email  # type-checked!
       send_email(email)
   ```

4. **Update schedules with typed events:**

   ```python
   # Before
   @on_schedule("0 2 * * *")
   def cleanup(ctx): ...

   # After
   class SystemCleanup(Event):
       cutoff_days: Field[int] = 90

   cleanup_schedule = Schedule(
       event=SystemCleanup(cutoff_days=90),
       cron="0 2 * * *"
   )

   @on_event(SystemCleanup)
   def cleanup(ctx: HandlerContext[SystemCleanup]) -> None: ...

   session.run([cleanup], schedules=[cleanup_schedule])
   ```

5. **Configure namespace (optional):**

   ```python
   # Explicit namespace
   session = Session(
       datastore_uri="sqlite:///app.db",
       namespace="orders"
   )

   # Or use default namespace (omit parameter)
   session = Session(
       datastore_uri="sqlite:///app.db"
   )
   ```

### Migration Checklist

- [ ] **Define custom event classes** for your domain (e.g., `UserCreated`,
      `OrderPlaced`)
- [ ] Update all `@on_commit` to `@on_event(YourCustomEvent)`
- [ ] Update all `@on_commit_entity` handlers to use custom typed events (see
      Decision 13)
- [ ] Update all `@on_schedule` to `Schedule` + `@on_event`
- [ ] Add `ctx.commit()` calls in all handlers
- [ ] **Update imperative commits** to emit custom events when needed:
      `commit(event=YourCustomEvent(...))`
- [ ] **Add `namespace` parameter to Session constructors** (or use default
      namespace)
- [ ] **Ensure namespace naming follows logical partitioning boundaries** (e.g.,
      "orders", "payments", "tenant-a")
- [ ] **Configure `default_namespace` in config** if needed
- [ ] **Ensure all handlers are idempotent** (safe to retry after commit
      failure)
- [ ] Add idempotency keys to external API calls (Stripe, email, etc.)
- [ ] Test commit failure scenarios (database errors, conflicts)
- [ ] Review handler idempotency (external side effects)
- [ ] Update monitoring/alerting for new error types
- [ ] Test multi-instance coordination (same namespace, different instances)
- [ ] Test multi-namespace scenarios (if applicable)

---

## Testing and Validation

### Required Test Coverage

1. **Explicit commit:**

   - Handler without `ctx.commit()` produces no commit
   - Handler with multiple `ctx.commit()` calls produces multiple commits

2. **Commit event opt-in:**

   - `commit(event=...)` enqueues the specified event
   - `commit()` without event parameter does not enqueue an event
   - Custom event type and payload are respected

3. **Event ordering:**

   - Higher priority events are processed before lower priority
   - Within same priority, events are processed FIFO
   - Handler priority determines execution order for same event type

4. **Per-handler acknowledgement:**

   - Same event processed by multiple handlers
   - Each handler acks independently
   - Acked events are not re-claimed by that handler

5. **Claim lease expiry:**

   - Handler crashes without ack
   - Event is re-claimed after lease expires
   - Attempts counter increments

6. **Lease expiration during handler execution:**

   - Handler takes longer than `event_claim_lease_ms` to complete
   - `ctx.commit()` raises `LeaseExpiredError`
   - Buffered events are discarded
   - Handler is retried (another session may already be processing it)
   - **Prevents duplicate processing** when handler is too slow

7. **Event buffering:**

   - Handler emits event then succeeds → event is enqueued
   - Handler emits event then fails → event is discarded
   - Handler emits multiple events → all enqueued on success
   - Handler emits events then commit fails → events are discarded
   - Handler emits events then lease expires → events are discarded

8. **Commit failure handling:**

   - Handler calls `ctx.commit()` which fails → handler is retried
   - Buffered events are discarded on commit failure
   - Handler with multiple commits: 2nd commit fails → handler retries from
     beginning
   - Idempotent handler: retried after commit failure → no duplicate side
     effects
   - Database constraint violation → handler retried → eventually dead-lettered
   - Lease expiration → handler retried → another session may process in
     parallel

9. **Chain depth limit:**

   - Event chain exceeding `max_event_chain_depth` raises `EventLoopLimitError`
   - Chain depth is correctly propagated through emit

10. **Iteration limit:**

    - Processing `max_events_per_iteration` events stops the current iteration
    - Counter resets after sleeping for `event_poll_interval_ms`
    - Session continues indefinitely (does not exit)

11. **Exponential backoff:**

    - Failed event has increasing `available_at` delay
    - Backoff respects `event_backoff_max_ms`
    - Jitter is applied

12. **Dead-lettering:**

    - Event exceeding `event_max_attempts` is dead-lettered
    - `event.dead_letter` event is emitted
    - Dead-lettered event is not re-claimed by that handler
    - Other handlers can still claim the event

13. **Schedule execution:**

    - Schedule enqueues event at correct time
    - Schedule event is processed by subscribed handlers
    - Multiple schedules work independently

14. **Cross-namespace isolation:**

    - Events are partitioned by namespace
    - Namespace A cannot see or process events from Namespace B
    - Multiple instances can process same namespace

15. **Session coordination:**

    - Multiple sessions processing same namespace claim events via lease
    - Session metadata tracked for observability
    - Heartbeats track session liveness

16. **Default namespace:**

    - Sessions without explicit namespace use default namespace
    - Default namespace is configurable

17. **Type safety:**
    - Handler receives correct event type (compile-time check)
    - Accessing non-existent event field raises type error
    - Emitting wrong event type raises type error

---

## CLI Tooling

A command-line tool for managing events and sessions:

### List Namespaces

```bash
ontologia events list-namespaces

# Output:
# Namespace       Sessions  Pending Events  Dead Letters
# orders          3         45              2
# payments        1         2               0
# default         2         123             5
```

### Show Sessions

```bash
ontologia events sessions --namespace orders

# Output:
# Session ID                          Hostname  PID    Started At           Last Heartbeat
# abc123-def456-789...                 host-1    1234   2026-02-11 10:00:00  2s ago
# def456-abc789-012...                 host-2    5678   2026-02-11 09:30:00  1s ago
# ghi789-jkl012-345...                 host-3    9012   2026-02-11 09:00:00  10m ago (DEAD)
```

### Show Events

```bash
ontologia events show --namespace orders --limit 10

# Output:
# Event ID    Type            Created At           Priority  Status     Handler
# evt_001     order.placed    2026-02-11 10:00:00  100       pending    -
# evt_002     order.placed    2026-02-11 10:00:01  100       claimed    process_order
# evt_003     payment.done    2026-02-11 10:00:02  50        acked      fulfill_order
```

### Show Dead Letters

```bash
ontologia events dead-letters --namespace orders

# Output:
# Event ID    Type            Handler           Attempts  Last Error
# evt_123     order.placed    process_payment   10        DatabaseError: constraint violation
# evt_456     email.send      send_notification 10        TimeoutError: SMTP timeout
```

### Cleanup Old Events

```bash
ontologia events cleanup --namespace orders --before 7d

# Output:
# Deleted 1,234 events older than 7 days for namespace 'orders'
```

### Replay Event

```bash
ontologia events replay --namespace orders --event-id evt_123

# Output:
# Event evt_123 re-enqueued for namespace 'orders'
# Available for processing immediately
```

### Show Event Details

```bash
ontologia events inspect --event-id evt_123

# Output (JSON):
# {
#   "id": "evt_123",
#   "namespace": "orders",
#   "type": "order.placed",
#   "payload": {"order_id": "ord_456", "total": 99.99},
#   "created_at": "2026-02-11T10:00:00Z",
#   "priority": 100,
#   "root_event_id": "evt_123",
#   "chain_depth": 0,
#   "claims": [
#     {
#       "handler_id": "process_order",
#       "session_id": "abc123-def456",
#       "attempts": 3,
#       "last_error": "DatabaseError: ...",
#       "dead_lettered_at": "2026-02-11T10:05:00Z"
#     }
#   ]
# }
```

---

## Rollout Plan

### Phase 1: Foundation (Week 1-2)

- [ ] Implement `Event` dataclass
- [ ] Implement `EventStore` protocol with namespace partitioning
- [ ] Implement SQLite backend (co-located with datastore)
- [ ] Add `session_id` to Session
- [ ] Add `sessions` table and heartbeat mechanism
- [ ] Unit tests for event store operations

### Phase 2: Handler Context (Week 3-4)

- [ ] Implement `HandlerContext` with `emit()` and `commit()`
- [ ] Implement atomic commit+event in single transaction
- [ ] Implement event buffering logic
- [ ] Implement `@on_event` decorator
- [ ] Unit tests for handler context and emission

### Phase 3: Event Loop (Week 5-6)

- [ ] Implement `Session.run()` event loop with namespace filtering
- [ ] Implement claim/ack/release logic with session tracking
- [ ] Implement loop guards (chain depth, max events)
- [ ] Implement exponential backoff
- [ ] Integration tests for event processing

### Phase 4: Schedules (Week 7)

- [ ] Implement `Schedule` dataclass
- [ ] Implement per-runtime schedule polling and event emission
- [ ] Integration tests for scheduled tasks

### Phase 5: Dead Letters (Week 8)

- [ ] Implement dead-letter table and logic
- [ ] Implement `event.dead_letter` emission
- [ ] Update dead_letters table with session tracking

### Phase 6: CLI Tooling (Week 9)

- [ ] Implement `ontologia events list-namespaces` command
- [ ] Implement `ontologia events sessions` command
- [ ] Implement `ontologia events show` command
- [ ] Implement `ontologia events dead-letters` command
- [ ] Implement `ontologia events cleanup` command
- [ ] Implement `ontologia events replay` command
- [ ] Implement `ontologia events inspect` command

### Phase 7: S3 Event Store (Week 10-11)

- [ ] Design S3 event store layout (one event per object)
- [ ] Implement S3 EventStore backend
- [ ] Implement index file management per session
- [ ] Handle claim/lease coordination (external state or DynamoDB)
- [ ] Integration tests for S3 backend

### Phase 8: Migration & Docs (Week 12-13)

- [ ] Remove `@on_commit` and `@on_schedule`
- [ ] Update all examples to use new API with session_id
- [ ] Write migration guide
- [ ] Update API documentation
- [ ] Performance testing and tuning

### Phase 9: Production Rollout (Week 14+)

- [ ] Deploy to staging
- [ ] Run migration scripts on staging data
- [ ] Monitor for errors and performance issues
- [ ] Deploy to production with gradual rollout
- [ ] Post-deployment monitoring and tuning

---

## Open Questions

1. **Schedule coordination:** Should the runtime include built-in leader
   election to prevent duplicate schedule events across sessions?

   - **Current answer:** No, operators are responsible for ensuring only one
     session polls schedules per event store

2. **Observability:** Should the runtime emit built-in metrics (events/sec,
   handler latency, queue depth)?

   - **Current answer:** Deferred to future work; users can implement via custom
     handlers

3. **Event payload validation:** Should there be optional schema validation
   (JSON Schema, Pydantic)?

   - **Current answer:** Deferred to future work; users can validate in handlers

4. ~~**Atomic commit+event:** Should the default SQLite backend co-locate event
   store with datastore for atomicity?~~

   - **Resolved:** Yes. The event store is co-located with the datastore in the
     same SQLite database. This enables atomic commit+event transactions without
     an outbox pattern. There are no built-in commit events — applications
     define custom event types and emit them explicitly via `commit(event=...)`.

5. **Backfill mechanism:** Should there be a built-in way to replay historical
   events to new handlers?
   - **Current answer:** No, new handlers should query existing state to
     initialize

---

## References

- Existing Ontologia commit and reconciliation logic (unchanged by this RFC)
- Event sourcing patterns (inspiration for event chains and replay)
- Job queue systems (inspiration for claim/ack/backoff semantics)
- Temporal workflow engine (inspiration for durable event processing)

---

**End of RFC**
