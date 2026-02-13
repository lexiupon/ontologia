# API Reference

This document provides detailed API documentation for Ontologia's core
abstractions: typed schema (Entity, Relation, Field), reactive handler runtime,
and query/transaction APIs.

## Table of Contents

- [Overview](#overview)
- [Type System](#type-system)
  - [Entity Base Class](#entity-base-class)
  - [Relation Generic Class](#relation-generic-class)
  - [Field Descriptor](#field-descriptor)
- [Session Runtime](#session-runtime)
  - [Initialization](#initialization)
  - [Session API](#session-api)
  - [Schema Migration](#schema-migration)
  - [Delta Computation](#delta-computation)
  - [Commits](#commits)
- [Query API](#query-api)
  - [Entity Queries](#entity-queries)
  - [Relation Queries](#relation-queries)
  - [Traversals](#traversals)
  - [Filter Expressions](#filter-expressions)
  - [Aggregation Queries](#aggregation-queries)
  - [History Queries](#history-queries)
  - [Metadata API](#metadata-api)
- [Handler API](#handler-api)
  - [Handler Decorator](#handler-decorator)
  - [Events](#events)
  - [Commit Metadata via Context](#commit-metadata-via-context)
  - [Handler Context](#handler-context)
  - [Execution Model](#execution-model)

---

## Overview

Ontologia models state as typed entities and relations, and changes that state
through session commits.

**Simple example:**

```python
from ontologia import Entity, Field, Relation, Session

class Customer(Entity):
    id: Field[str] = Field(primary_key=True)
    name: Field[str]
    tier: Field[str]

class Product(Entity):
    sku: Field[str] = Field(primary_key=True)
    name: Field[str]

class Subscription(Relation[Customer, Product]):
    active: Field[bool] = Field(default=True)

session = Session(
    "my.db",
    entity_types=[Customer, Product],
    relation_types=[Subscription],
)

# Imperative mode: declare intent with ensure(...)
with session:
    session.ensure(Customer(id="c1", name="Alice", tier="Gold"))
    session.ensure(Product(sku="p1", name="Analytics"))
    session.ensure(Subscription(left_key="c1", right_key="p1", active=True))
    # On commit/exit: runtime computes delta vs current state
    # If delta is non-empty, it commits atomically

# Reactive mode: handlers consume typed events and commit explicitly
from myapp.handlers import process_orders, sync_customers
from myapp.events import NightlySyncRequested

session.commit(event=NightlySyncRequested(source="startup"))
session.run([sync_customers, process_orders], max_iterations=1)
```

**Key concepts:**

- **Entity**: Typed node record (for example `Customer`)
- **Relation**: Typed edge record between entities (for example `Subscription`)
- **Session**: Public entry point for state, queries, event processing, and
  commit history APIs
- **Ensure intent**: `ensure(...)` declares the intended state for targeted
  identities
- **Delta update**: Runtime computes the delta (insert/update/no-op) from intent
  vs current state
- **Commit**: Non-empty delta is persisted atomically as a new commit
- **Reactive execution**: `session.run(...)` executes `@on_event` handlers;
  state persists only when handlers call `ctx.commit(...)`
- **Lifecycle semantics**: No built-in `Delete`/`Retract`; model logical delete
  using lifecycle fields

---

## Type System

### Entity Base Class

The `Entity` base class provides a foundation for defining typed entities in
your domain model. It uses `__init_subclass__` for metadata configuration and
generates a shadow Pydantic model for runtime validation.

#### Class Definition

```python
class Entity:
    """Base class for typed entities with automatic validation."""
```

#### Subclass Parameters

Configure entity metadata by passing parameters to the class definition:

```python
class Customer(Entity):
    id: Field[str] = Field(primary_key=True)
    name: Field[str]
    age: Field[int]
    email: Field[str | None] = Field(default=None, index=True)
```

**Parameters:**

- **`name`** (`str | None`, default: `None`): Optional explicit entity name.
  Defaults to the class name if not provided.

**Field-level configuration:**

- **Primary key**: Exactly one field must have `Field(primary_key=True)`
- **Composite keys**: Not supported for entities
- **Multi-part identity**: Encode into one deterministic key field (for example,
  `"order-123#line-2"`)
- **Indexes**: Fields can be indexed with `Field(index=True)`
- **Defaults**: Use `Field(default=...)` or `Field(default_factory=...)`

#### Class Variables

Automatically set by `__init_subclass__`:

- **`__entity_name__`** (`ClassVar[str]`): The entity type name
- **`__entity_fields__`** (`ClassVar[tuple[str, ...]]`): Tuple of all field
  names
- **`_pydantic_model`** (`ClassVar[type[BaseModel]]`): Shadow Pydantic model
- **`_field_definitions`** (`ClassVar[dict[str, Field[Any]]]`): Field
  descriptors

#### Constructor

```python
def __init__(self, **data: Any) -> None:
    """Initialize entity with validated data.

    Args:
        **data: Field values to set

    Raises:
        ValidationError: If data fails Pydantic validation
    """
```

**Example:**

```python
customer = Customer(id="c1", name="Alice", age=25, email="alice@example.com")
```

#### Instance Methods

**`model_dump() -> dict[str, Any]`**

Export entity data as a dictionary.

```python
customer = Customer(id="c1", name="Alice", age=25)
data = customer.model_dump()
# {'id': 'c1', 'name': 'Alice', 'age': 25, 'email': None}
```

#### Class Methods

**`model_validate(data: dict[str, Any]) -> E`**

Create and validate an entity instance from a dictionary.

```python
data = {'id': 'c1', 'name': 'Alice', 'age': 25}
customer = Customer.model_validate(data)
```

#### Usage Examples

**Basic entity definition:**

```python
class Person(Entity):
    email: Field[str] = Field(primary_key=True)
    name: Field[str]
    age: Field[int]
    city: Field[str | None] = Field(default=None)

# Create instance
person = Person(email="alice@example.com", name="Alice", age=32)

# Access fields
print(person.name)  # "Alice"
print(person.age)   # 32
```

**With indexes for query optimization:**

```python
class Customer(Entity):
    id: Field[str] = Field(primary_key=True)
    name: Field[str]
    email: Field[str] = Field(index=True)
    tier: Field[str] = Field(index=True)  # Fast filtering
```

**With optional fields and defaults:**

```python
class Product(Entity):
    sku: Field[str] = Field(primary_key=True)
    name: Field[str]
    price: Field[float]
    description: Field[str | None] = None
    active: Field[bool] = Field(default=True)
    tags: Field[list[str]] = Field(default_factory=list)
```

**Junction-style entity with synthesized key:**

```python
class OrderLine(Entity):
    key: Field[str] = Field(primary_key=True)  # f"{order_id}#{line_number}"
    order_id: Field[str] = Field(index=True)
    line_number: Field[int]
    sku: Field[str]
    quantity: Field[int]

line = OrderLine(
    key="order-123#2",
    order_id="order-123",
    line_number=2,
    sku="sku-42",
    quantity=3,
)
```

---

### Relation Generic Class

The `Relation[L, R]` generic class provides a foundation for defining typed
relationships between entities. The generic type parameters `L` and `R` specify
the left and right endpoint entity types.

#### Class Definition

```python
class Relation(Generic[L, R]):
    """Base class for typed relationships with generic endpoints."""
```

#### Subclass Parameters

```python
class Subscription(Relation[Customer, Product]):
    """Current subscription state between customer and product."""
    seat_count: Field[int]
    started_at: Field[str]
    active: Field[bool]
```

**Parameters:**

- **`name`** (`str | None`, default: `None`): Optional explicit relation name

**Field-level configuration:**

- **Attributes**: All `Field[T]` declarations become relation attributes
- **Indexes**: Use `Field(index=True)` for indexed attributes

#### Class Variables

Automatically set by `__init_subclass__`:

- **`__relation_name__`** (`ClassVar[str]`): The relation type name
- **`__relation_fields__`** (`ClassVar[tuple[str, ...]]`): Tuple of field names
- **`_pydantic_model`** (`ClassVar[type[BaseModel]]`): Shadow Pydantic model
- **`_field_definitions`** (`ClassVar[dict[str, Field[Any]]]`): Field
  descriptors
- **`_left_type`** (`ClassVar[type[Any]]`): Left endpoint entity type
- **`_right_type`** (`ClassVar[type[Any]]`): Right endpoint entity type
- **`_instance_key_field`** (`ClassVar[str | None]`): Name of the instance key
  field, or `None` for unkeyed relations

#### Endpoint Accessors

Type-safe accessors for querying fields on related entities:

```python
from ontologia import left, right

# Access customer fields via left endpoint
expr = left(Subscription).tier == "Gold"

# Access product fields via right endpoint
expr = right(Subscription).price > 100
```

#### Constructor

```python
def __init__(self, **data: Any) -> None:
    """Initialize relation with validated data.

    Args:
        **data: Field values to set

    Raises:
        ValidationError: If data fails Pydantic validation
    """
```

For write intents, relation endpoint identity is provided by reserved arguments:

- `left_key: str`
- `right_key: str`
- `instance_key: str` (only for keyed relations)

**Unkeyed relations** have identity `(relation_type, left_key, right_key)` — one
current edge state per endpoint pair.

**Keyed relations** declare one `Field(instance_key=True)` field, giving
identity `(relation_type, left_key, right_key, instance_key)` — multiple
concurrent instances per endpoint pair (for example, employment stints).

For repeatable facts (transactions, logs, events), use an `Entity` with its own
primary key or a keyed relation. There is no built-in relation retract
operation; retire edges through application-defined lifecycle attributes (for
example `active=False`, `ended_at`) and query filters.

#### Instance Methods

**`model_dump() -> dict[str, Any]`**

Export relation data as a dictionary.

```python
subscription = Subscription(
    left_key="c1",
    right_key="p1",
    seat_count=5,
    started_at="2024-01-15T10:30:00Z",
    active=True,
)
data = subscription.model_dump()
```

#### Usage Examples

**Simple relationship (no attributes):**

```python
class Follows(Relation[Person, Person]):
    """One person follows another."""
    pass

# Minimal edge - just the connection
```

**Relationship with attributes:**

```python
class Subscription(Relation[Customer, Product]):
    """Current subscription state."""
    seat_count: Field[int]
    started_at: Field[str]
    active: Field[bool] = Field(default=True)
    plan_tier: Field[str] = Field(default="Standard")
```

**Multiple relation types between same entities:**

```python
class Customer(Entity):
    id: Field[str] = Field(primary_key=True)
    name: Field[str]

class Product(Entity):
    sku: Field[str] = Field(primary_key=True)
    category: Field[str]

# Different relation types - query via class!
class Subscription(Relation[Customer, Product]):
    """Current subscription."""
    seat_count: Field[int]

class Wishlisted(Relation[Customer, Product]):
    """Product is in customer's wishlist."""
    added_at: Field[str]

class Suppressed(Relation[Customer, Product]):
    """Product suppressed for this customer."""
    reason: Field[str]

# Query via relation class (unambiguous!)
subscriptions = session.query().entities(Customer).via(Subscription).collect()
wishlisted = session.query().entities(Customer).via(Wishlisted).collect()
suppressed = session.query().entities(Customer).via(Suppressed).collect()
```

**Modeling repeatable transactions/events:**

Use an `Entity` when you need many records for the same endpoint pair.

```python
class PurchaseEvent(Entity):
    id: Field[str] = Field(primary_key=True)
    customer_id: Field[str] = Field(index=True)
    product_sku: Field[str] = Field(index=True)
    quantity: Field[int]
    purchased_at: Field[str]
    unit_price: Field[float]
```

`PurchaseEvent` rows are independent records by `id`.

**Keyed relation (multiple instances per endpoint pair):**

For domains where multiple relation instances between the same pair are
meaningful (employment stints, repeated enrollments), use
`Field(instance_key=True)`:

```python
class Person(Entity):
    id: Field[str] = Field(primary_key=True)
    name: Field[str]

class Company(Entity):
    id: Field[str] = Field(primary_key=True)
    name: Field[str]

class Employment(Relation[Person, Company]):
    stint_id: Field[str] = Field(instance_key=True)
    role: Field[str]
    started_at: Field[str]

# Two concurrent employment stints at the same company
with Session("my.db") as session:
    session.ensure(Employment(
        left_key="p1", right_key="c1",
        stint_id="stint-1", role="Engineer", started_at="2020",
    ))
    session.ensure(Employment(
        left_key="p1", right_key="c1",
        stint_id="stint-2", role="Manager", started_at="2023",
    ))
```

**Keyed relation rules:**

- Instance key must be `str` type, required, non-empty, no default
- At most one `Field(instance_key=True)` per relation
- Instance key is excluded from `model_dump()` (it's part of identity, not data)
- Instance key is passed via its declared field name (e.g.
  `stint_id="stint-1"`), and accessible as both `rel.stint_id` and
  `rel.instance_key`
- Entities cannot use `Field(instance_key=True)`
- Relations cannot use `Field(primary_key=True)`

---

### Field Descriptor

The `Field[T]` descriptor enables type-safe field definitions with optional
defaults and query building capabilities.

#### Annotation Style and Type-Safety

Ontologia currently standardizes on `field_name: Field[T]` declarations.

```python
class Customer(Entity):
    id: Field[str] = Field(primary_key=True)
    name: Field[str]
```

`field_name: T = Field(...)` is intentionally not the canonical style in v1.
That conventional style is common in ORMs, but Ontologia relies on class-level
`Field[T]` typing so static checkers understand query expressions. With
`field_name: T`, `Customer.name` is typed as `str`, and class-level query
expressions (`Customer.name == "Alice"`) are inferred as Python `bool` instead
of Ontologia `FilterExpression`.

`Field[T]` keeps class-level query composition type-safe and discoverable in
editors without plugins.

#### Constructor

```python
def __init__(
    self,
    default: T | None = None,
    default_factory: Callable[[], T] | None = None,
    primary_key: bool = False,
    instance_key: bool = False,
    index: bool = False,
) -> None:
    """Initialize a field.

    Args:
        default: Static default value
        default_factory: Callable that returns a default value
        primary_key: Mark as primary key (exactly one per entity, entities only)
        instance_key: Mark as instance key (at most one per relation, relations only)
        index: Create index for fast querying
    """
```

**Examples:**

```python
# Required field (no default)
id: Field[str]

# Primary key
id: Field[str] = Field(primary_key=True)

# Indexed field
email: Field[str] = Field(index=True)

# Optional field with None default
email: Field[str | None] = None

# Field with static default
active: Field[bool] = Field(default=True)

# Field with factory default (for mutable types)
tags: Field[list[str]] = Field(default_factory=list)

# Structured payload field
profile: Field[dict[str, str]]

# List of structured payloads
events: Field[list[dict[str, str]]] = Field(default_factory=list)

# Instance key (keyed relations only, must be str, no default)
stint_id: Field[str] = Field(instance_key=True)
```

#### Comparison Operators

All comparison operators return `FilterExpression` for query building:

```python
# Equality
Customer.name == "Alice"

# Inequality
Customer.tier != "VIP"

# Numeric comparisons
Customer.age > 30
Customer.age >= 21
Customer.age < 65
Customer.age <= 100
```

#### String Methods

```python
# Prefix match
Customer.name.startswith("A")

# Suffix match
Customer.email.endswith("@example.com")

# Substring match
Customer.email.contains("@")
```

#### Collection Methods

```python
# Membership test
Customer.tier.in_(["Gold", "Platinum", "Diamond"])
```

#### Path Composition Methods

Path composition lets filters and aggregations target nested values inside
structured payload fields.

```python
# Dotted path
Customer.profile.path("address.city") == "SF"

# Bracket sugar
Customer.profile["address"]["city"] == "SF"
```

Path rules:

- path must be non-empty
- each segment must match `[A-Za-z_][A-Za-z0-9_]*`
- invalid path syntax raises `ValueError` at query-build time

#### Null Check Methods

```python
# Explicit null check
Customer.email.is_null()

# Explicit not-null check
Customer.email.is_not_null()
```

`Customer.email` is a class-level `Field` descriptor, so comparison operators
build `FilterExpression` objects. For null predicates, use `.is_null()` and
`.is_not_null()`. `== None` and `!= None` are intentionally unsupported and
raise `TypeError`. Use `is None` only for regular Python object checks (outside
query DSL).

#### Boolean Check Methods

```python
# Explicit true check
Customer.active.is_true()

# Explicit false check
Customer.active.is_false()
```

For explicit boolean comparisons, use `.is_true()` and `.is_false()` methods.
`== True`, `== False`, `!= True`, and `!= False` are intentionally unsupported
and raise `TypeError`. This enforces clarity between explicit boolean checks and
truthiness evaluations.

---

## Session Runtime

### Initialization

Create a session instance:

```python
from ontologia import Session
from ontologia.config import OntologiaConfig

# Basic initialization
session = Session("my_ontology.db")

# Preferred backend-neutral binding
session = Session("sqlite:///my_ontology.db")

# With explicit types for schema validation
session = Session(
    "my_ontology.db",
    entity_types=[Customer, Product, Order],
    relation_types=[Subscription, WorksAt],
)

# With configuration
session = Session(
    "s3://my-bucket/my-prefix",
    namespace="prod",
    instance_metadata={"role": "worker-a"},
    config=OntologiaConfig(
        max_batch_size=10000,  # Max operations per commit
        max_event_chain_depth=20,
        event_poll_interval_ms=1000,
        s3_region="us-west-2",
        s3_lock_timeout_ms=5000,
        s3_lease_ttl_ms=30000,
        s3_duckdb_memory_limit="256MB",
    ),
    entity_types=[Customer, Product],
    relation_types=[Subscription],
)
```

Storage engine default:

- New SQLite storages default to engine `v2` (including `:memory:`).
- Existing SQLite storages without engine metadata continue as `v1` for
  compatibility.

**Schema verification:**

When `entity_types` or `relation_types` are provided, call `session.validate()`
to check stored schema against code-defined types and cache the validated schema
version IDs:

```python
session.validate()  # Raises SchemaOutdatedError on mismatch
```

Schema comparison uses canonical field type metadata, including structured
`type_spec` trees for nested container and typed-dict shapes, not only legacy
type strings. Nested shape changes are treated as schema drift.

If typed models are registered and validation has not yet run, `commit()` and
`run()` auto-validate before processing. Runtime also re-checks schema version
IDs for touched types under the write lock before persisting a commit; drift
raises `SchemaOutdatedError` and aborts the write.

**Batch-size enforcement:**

Runtime enforces `config.max_batch_size` per commit attempt. If a handler emits
more intents than the limit for a single commit, execution fails and no commit
is created.

### Session API

All state changes happen through `Session` in one of two modes:

1. **Imperative mode**: call `session.ensure(...)` and `session.commit(...)`.
2. **Reactive mode**: call `session.run(...)` with `@on_event` handlers.

#### Mode 1: Imperative (`ensure` + commit)

```python
with Session("my.db", entity_types=[Customer, Product]) as session:
    session.ensure(Customer(id="c1", name="Alice"))
    session.ensure([
        Product(sku="p1", name="Widget"),
        Product(sku="p2", name="Gadget"),
    ])
    session.commit()  # Optional; context exit also calls commit()
```

#### Mode 2: Reactive (`run` + handlers)

```python
from ontologia import Event, Field, HandlerContext, Schedule, Session, on_event

class SyncRequested(Event):
    source: Field[str]

@on_event(SyncRequested, priority=200)
def sync_customers(ctx: HandlerContext[SyncRequested]) -> None:
    for row in fetch_rows():
        ctx.ensure(Customer(id=row["id"], name=row["name"]))
    ctx.add_commit_meta("handler", "sync_customers")
    ctx.commit()

session = Session("my.db", entity_types=[Customer])
session.commit(event=SyncRequested(source="startup"))
session.run(
    [sync_customers],
    schedules=[Schedule(event=SyncRequested(source="cron"), cron="0 * * * *")],
    max_iterations=10,
)
```

#### `ensure`

Declare expected state using typed `Entity` / `Relation` objects via session or
handler context. Supports both single objects and iterables for batch
operations.

**Signature:**

```python
# On session (imperative mode)
session.ensure(obj: Entity | Relation | Iterable[Entity | Relation]) -> None

# In handler (reactive mode)
ctx.ensure(obj: Entity | Relation | Iterable[Entity | Relation]) -> None
```

**Single-object examples:**

```python
# Entity intent (typed)
ctx.ensure(Customer(id="c1", name="Alice", age=32))

# Relation intent (typed)
ctx.ensure(
    Subscription(
        left_key="c1",
        right_key="p1",
        seat_count=2,
        started_at="2026-01-15T10:30:00Z",
        active=True,
    )
)
```

**Batch examples:**

```python
# List of entities
session.ensure([
    Customer(id="c1", name="Alice", age=32),
    Customer(id="c2", name="Bob", age=28),
    Customer(id="c3", name="Carol", age=35),
])

# Mixed entities and relations in one call
ctx.ensure([
    Customer(id="c1", name="Alice", age=32),
    Product(sku="p1", name="Widget", price=9.99),
    Subscription(left_key="c1", right_key="p1", active=True),
])

# Generator expression for ingestion
ctx.ensure(
    Customer(id=row["id"], name=row["name"], age=row["age"])
    for row in read_csv("customers.csv")
)

# Any iterable works (tuple, set, etc.)
session.ensure(tuple(entities))
```

**Behavior:**

- Accepts any `Iterable` (list, tuple, generator, etc.) except strings/bytes
- Mixed Entity and Relation types in same iterable are allowed
- Empty iterables are no-ops (no intents added)
- Validation is fail-fast: stops on first invalid item with clear error message
- Order is preserved: items processed in iteration order

For relations, endpoint references in `ensure(...)` are by endpoint keys
(`left_key`, `right_key`), not by embedding full endpoint entity objects. There
is no built-in `Delete`/`Retract` intent; represent deletion as a
lifecycle-state update (for example `active=False`, `deleted_at=...`) and query
with lifecycle filters.

#### `commit`

Persist currently queued intents as one reconciliation batch, and optionally
enqueue one event.

**Signature:**

```python
def commit(self, *, event: Event | None = None) -> int | None:
    """Commit queued intents.

    Returns:
        commit_id when a non-empty delta is persisted, otherwise None.
    """
```

**Rules:**

- Empty queue and no event: returns `None`
- No-op delta and no event: returns `None` (no commit created)
- Event-only commit (`event=...`, no delta): event is enqueued, return `None`
- Non-empty delta: returns created `commit_id` as `int`
- Context-manager exit calls `commit()` automatically when no exception occurred
  (without event)

#### `run`

Execute reactive handlers in the current session.

**Signature:**

```python
def run(
    self,
    handlers: list[Callable[..., Any]],
    *,
    schedules: list[Schedule] | None = None,
    max_iterations: int | None = None,
) -> None:
    """Execute event loop with provided handler functions.

    Args:
        handlers: List of functions decorated with @on_event
        schedules: Optional cron schedules that enqueue typed events
        max_iterations: Optional hard stop for loop iterations

    Raises:
        HandlerError: If handlers are invalid
    """
```

**Rules:**

- Handlers must be decorated with `@on_event(EventType, ...)`
- Handlers are sorted by descending priority (higher value runs first)
- `schedules` enqueue copies of `Schedule.event` when cron expressions fire
- `max_iterations` lets callers bound long-running loops
- `session.stop()` can be used to request loop shutdown
- `ctx.ensure(...)` only queues intents; **handlers must call `ctx.commit(...)`
  to persist state**
- There is no automatic commit-trigger model in the Session-first API

### Schema Migration

When code-defined schemas change, `session.migrate()` provides a
preview-then-apply workflow.

**Lifecycle:**

```
Session(datastore_uri, entity_types=[...])  # 1. Construction (no validation)
    │
    ├── session.validate()             # 2. Explicit validation (cache schema versions)
    │
    ├── session.migrate(dry_run=True)  # 3. Preview changes → MigrationPreview (if needed)
    │
    ├── session.migrate(dry_run=False, # 4. Apply changes → MigrationResult (if needed)
    │       token=preview.token,
    │       upgraders={...})
```

**Preview:**

```python
from ontologia import SchemaOutdatedError, Session

session = Session("my.db", entity_types=[CustomerV2])

preview = session.migrate(dry_run=True)
# MigrationPreview:
#   has_changes: bool
#   token: str                    — deterministic, tied to plan + head commit
#   diffs: list[TypeSchemaDiff]   — per-type field-level diffs
#   estimated_rows: dict          — row counts per type
#   types_requiring_upgraders     — types with data that need upgrader functions
#   types_schema_only             — types with zero rows (no upgrader needed)
#   missing_upgraders             — types needing upgraders not yet provided
```

**Upgrader functions:**

Upgrader functions transform row data from one schema version to the next:

```python
from ontologia import upgrader

@upgrader("Customer", from_version=1)
def upgrade_customer_v1(fields: dict) -> dict:
    fields["email"] = fields.pop("mail", None)
    return fields
```

Upgraders are chained automatically for multi-version jumps (v1 → v2 → v3).

**Apply:**

```python
result = session.migrate(
    dry_run=False,
    token=preview.token,           # or force=True to skip token check
    upgraders={("Customer", 1): upgrade_customer_v1},
)
# MigrationResult:
#   success: bool
#   types_migrated: list[str]
#   rows_migrated: dict[str, int]
#   new_schema_versions: dict[str, int]
#   duration_s: float
```

**Loading upgraders from a module:**

```python
from ontologia import load_upgraders

upgraders = load_upgraders("myapp.migrations")
result = session.migrate(dry_run=False, token=preview.token, upgraders=upgraders)
```

**Key behaviors:**

- Schema-only types (zero data rows) migrate without upgraders.
- Types with data require an `@upgrader` for each version step.
- Migration runs under a write lock with lease keep-alive.
- Token verification ensures the plan hasn't changed since preview. Use
  `force=True` to skip (still validates upgrader coverage).
- Upgrader output is validated through the target type's Pydantic model.
- Errors include the failing type, identity key, and old data for debugging.
- Migration is atomic: all types succeed or none are committed.
- Legacy schema rows missing canonical `type_spec` may be upgraded during
  validation when they can be safely synthesized from legacy type strings.

**Error types:**

- `SchemaOutdatedError` — raised by `validate()`, `commit()`/`run()`
  auto-validation, or commit-time drift checks when schema doesn't match
- `MigrationTokenError` — stale token (schema or data changed since preview)
- `MissingUpgraderError` — required upgrader functions not provided
- `MigrationError` — general migration failure (upgrader error, lock timeout)

### Delta Computation

Delta is an internal runtime step, but API-visible behavior is:

- Intents target identities (`(type_name, key)` or `(type_name, left, right)`).
- Missing identity: insert.
- Existing identity with changed value/attrs: append new version.
- Existing identity with same value/attrs: no-op.
- Identities not targeted by any intent are unchanged.

### Commits

Commit flow in the public API:

```python
from ontologia import Event, Field, HandlerContext, Session, on_event

class CustomerImported(Event):
    source: Field[str]

class SyncCompleted(Event):
    source: Field[str]

# Imperative mode - commit on exit
with Session("my.db", entity_types=[Customer]) as session:
    session.ensure(Customer(id="c1", name="Alice"))
    session.commit(event=CustomerImported(source="bootstrap"))

# Handler mode - state persists only when ctx.commit() is called
@on_event(CustomerImported)
def sync_data(ctx: HandlerContext[CustomerImported]):
    ctx.ensure(Customer(id="c2", name="Bob"))
    ctx.commit(event=SyncCompleted(source=ctx.event.source))

session.run([sync_data], max_iterations=10)

# Query state at a specific commit
snapshot = session.query().entities(Customer).as_of(commit_id=42).collect()

# Query changes after a commit
changes = session.query().entities(Customer).history_since(commit_id=42).collect()

# Inspect commit log
commits = session.list_commits(limit=10)
commit_42 = session.get_commit(commit_id=42)
commit_42_changes = session.list_commit_changes(commit_id=42)
```

Public commit inspection methods are exposed on `Session`:

```python
def list_commits(
    self,
    *,
    limit: int = 10,
    since_commit_id: int | None = None,
) -> list[dict[str, Any]]

def get_commit(self, commit_id: int) -> dict[str, Any] | None

def list_commit_changes(self, commit_id: int) -> list[dict[str, Any]]
```

`session.repo` is an internal repository implementation detail and not part of
the stable public API contract.

**Commit guarantees:**

1. **Atomic**: All changes in a commit apply or none do.
2. **Ordered**: Commits are totally ordered by monotonic `commit_id`.
3. **Append-only**: Updates append new versions (history preserved).
4. **Schema-safe writes**: Before write, runtime re-checks touched type schema
   version IDs under lock; drift aborts write with `SchemaOutdatedError`.

**Commit metadata from handlers/chunks:**

```python
@on_event(CustomerImported)
def tag_commit(ctx: HandlerContext[CustomerImported]):
    ctx.add_commit_meta("handler", "tag_commit")
    ctx.add_commit_meta("event_type", ctx.event.__class__.__event_type__)
    ctx.ensure(
        AuditLog(id=f"audit-{ctx.event.id}", source=ctx.event.source, status="imported")
    )
    ctx.commit()
```

Metadata set by `ctx.add_commit_meta(...)` attaches to the commit produced by
that handler run/chunk. If the same key is set multiple times, the final value
wins.

**Chunked handler output:**

For large workloads, handlers should control chunk boundaries so each run emits
a safe number of intents. Runtime keeps strict batch-size enforcement, and
handlers decide whether multi-commit progression is domain-safe. Chunk sizing is
done by handler logic (for example `fetch_rows(..., limit=...)`).

```python
@on_event(SyncRequested)
def sync_customers(ctx: HandlerContext[SyncRequested]):
    cursor = load_cursor("customers_sync")
    rows = fetch_rows(after=cursor, limit=1000)

    for row in rows:
        ctx.ensure(
            Customer(id=row["id"], name=row["name"], email=row.get("email"))
        )
    ctx.commit()

    if rows:
        save_cursor("customers_sync", rows[-1]["offset"])
```

Guidelines:

- Keep each chunk idempotent by stable identity keys.
- Persist continuation/checkpoint state between runs.
- Assume each chunk can commit independently.

---

## Query API

Type-safe query building using Entity/Relation schemas.

### Entity Queries

Query entities by type with filters:

```python
from ontologia import Session

session = Session("my.db")

# All entities of a type
customers = session.query().entities(Customer).collect()
# Type: list[Customer]

# With filter
adults = (
    session.query()
    .entities(Customer)
    .where(Customer.age >= 18)
    .collect()
)
# Type: list[Customer]

# Complex filter
gold_customers = (
    session.query()
    .entities(Customer)
    .where(
        (Customer.tier == "Gold")
        & Customer.active.is_true()
        & Customer.email.is_not_null()
    )
    .collect()
)
# Type: list[Customer]

# First result
alice = (
    session.query()
    .entities(Customer)
    .where(Customer.name == "Alice")
    .first()
)
# Type: Customer | None

# Pagination (typed builder)
first_page = (
    session.query()
    .entities(Customer)
    .order_by(Customer.id)
    .limit(100)
    .offset(0)
    .collect()
)

second_page = (
    session.query()
    .entities(Customer)
    .order_by(Customer.id)
    .limit(100)
    .offset(100)
    .collect()
)
```

Pagination methods:

- `.limit(n)` limits result count (`n > 0`)
- `.offset(n)` skips the first `n` rows (`n >= 0`)

For deterministic pagination, use `.order_by(...)` before `.limit(...)` and
`.offset(...)`.

**Soft delete pattern (application-defined):**

```python
class Customer(Entity):
    id: Field[str] = Field(primary_key=True)
    name: Field[str]
    active: Field[bool] = Field(default=True, index=True)
    deleted_at: Field[str | None] = Field(default=None, index=True)

# Logical delete: update lifecycle fields via session.ensure(...)
session.ensure(
    Customer(
        id="c1",
        name="Alice",
        active=False,
        deleted_at="2026-02-08T12:00:00Z",
    )
)

# Default "live" reads should filter lifecycle state
live_customers = (
    session.query()
    .entities(Customer)
    .where(Customer.active.is_true())
    .collect()
)
```

### Relation Queries

Query relations by type with filters on endpoints or attributes:

```python
# All relations of a type
subscriptions = session.query().relations(Subscription).collect()
# Type: list[Subscription]

# Filter by left endpoint (customer)
gold_subscriptions = (
    session.query()
    .relations(Subscription)
    .where(left(Subscription).tier == "Gold")
    .collect()
)
# Type: list[Subscription]

# Filter by right endpoint (product)
electronics = (
    session.query()
    .relations(Subscription)
    .where(right(Subscription).category == "Electronics")
    .collect()
)
# Type: list[Subscription]

# Filter by relation attributes
large_seat_subscriptions = (
    session.query()
    .relations(Subscription)
    .where(Subscription.seat_count > 10)
    .collect()
)
# Type: list[Subscription]

# Combine filters
high_value = (
    session.query()
    .relations(Subscription)
    .where(
        (Subscription.seat_count > 5)
        & (left(Subscription).tier.in_(["Gold", "Platinum"]))
        & (right(Subscription).price > 100)
    )
    .collect()
)
# Type: list[Subscription]

# Access endpoints directly from relation instance
for subscription in high_value:
    customer = subscription.left
    product = subscription.right
    seats = subscription.seat_count

# Pagination (typed builder)
page = (
    session.query()
    .relations(Subscription)
    .limit(200)
    .offset(400)
    .collect()
)
```

Relation queries also support `.limit(n)` and `.offset(n)`.

Relation instances carry domain fields and typed endpoints (`.left`, `.right`).
For metadata access (`commit_id`, keys, `type_name`), use `obj.meta()` (or
`meta(obj)`) as documented in [Metadata API](#metadata-api).

### Traversals

Follow relations from entities:

`Path[T]` is one traversal result rooted at source entity `T`. It exposes
`source` and ordered `relations`. Each relation is a typed relation instance, so
endpoint entities are available via `.left` and `.right`. `source` is always
present, including zero-hop results. Returning only `list[Relation]` would lose
root identity when no relations are traversed.

Traversal queries are lookup-only and return path results via `.collect()`.
Aggregation methods are not available on traversal queries.

```python
# Start with customers, traverse via Subscription relation
results = (
    session.query()
    .entities(Customer)
    .via(Subscription)  # Use Relation class directly
    .collect()
)
# Type: list[Path[Customer]]

# Multiple traversal steps
complex_results = (
    session.query()
    .entities(Customer)
    .via(Subscription)  # Customer → Product
    .via(PartOf)        # Product → Category
    .collect()
)
# Type: list[Path[Customer]]

# Filtered traversal
gold_customer_subscriptions = (
    session.query()
    .entities(Customer)
    .where(Customer.tier == "Gold")
    .via(Subscription)
    .collect()
)
# Type: list[Path[Customer]]
```

**Accessing traversal results:**

```python
for result in results:
    customer = result.source
    for rel in result.relations:
        left_entity = rel.left
        right_entity = rel.right
```

### Filter Expressions

Build type-safe filters using Field operators:

**Comparison:**

```python
Customer.age == 30
Customer.age != 30
Customer.age > 30
Customer.age >= 30
Customer.age < 30
Customer.age <= 30
```

**String operations:**

```python
Customer.name.startswith("A")
Customer.email.endswith(".com")
Customer.company.contains("tech")
```

**Collection operations:**

```python
Customer.tier.in_(["Gold", "Platinum", "Diamond"])
```

**Null checks:**

```python
Customer.email.is_null()
Customer.email.is_not_null()
```

**Boolean checks:**

```python
Customer.active.is_true()
Customer.active.is_false()
```

**Nested path filters:**

```python
Customer.profile.path("address.city") == "SF"
Customer.profile["metrics"]["score"] >= 90
left(Subscription).profile.path("city") == "SF"
```

**Existential list filters (`any_path`):**

```python
Customer.events.any_path("kind") == "click"
Customer.events.any_path("payload.geo.lat") > 37.0
```

`any_path(...)` returns an existential predicate expression that composes with
`&`, `|`, and `~` like other filter expressions.

**Logical operators:**

```python
# AND (use &)
(Customer.age > 18) & (Customer.age < 65)

# OR (use |)
(Customer.tier == "Gold") | (Customer.tier == "Platinum")

# NOT (use ~)
~(Customer.active.is_false())
```

**Complex combinations:**

```python
expr = (
    ((Customer.age >= 21) & (Customer.age <= 65))
    & Customer.email.is_not_null()
    & (Customer.tier.in_(["Gold", "Platinum"]))
    & (Customer.name.startswith("A") | Customer.name.startswith("B"))
)
```

`any_path(...)` constraints:

- supports list fields on entity/relation payloads
- endpoint list existential filters (`left(...).x.any_path(...)`,
  `right(...).x.any_path(...)`) are intentionally unsupported and raise
  `ValueError`

### Aggregation Queries

Aggregation is available on `EntityQuery` and `RelationQuery`:

- Scalar aggregations: `.count()`, `.sum(path)`, `.avg(path)`, `.min(path)`,
  `.max(path)`
- List/Predicate helpers: `.count_where(predicate)`, `.avg_len(path)`
- Grouped aggregations: `.group_by(...).having(...).agg(...)`

**Scalar aggregations:**

```python
# Count matching rows
active_count = (
    session.query()
    .entities(Customer)
    .where(Customer.active.is_true())
    .count()
)

# Sum / avg / min / max over numeric fields
total_revenue = session.query().entities(Order).sum(Order.total_amount)
avg_revenue = session.query().entities(Order).avg(Order.total_amount)
min_revenue = session.query().entities(Order).min(Order.total_amount)
max_revenue = session.query().entities(Order).max(Order.total_amount)

# Path-aware scalar aggregations
avg_score = session.query().entities(Customer).avg(Customer.profile.path("metrics.score"))
```

**Grouped aggregations with `group_by` + `agg`:**

```python
from ontologia.query import avg, count, sum

results = (
    session.query()
    .entities(Order)
    .group_by(Order.country)
    .agg(
        order_count=count(),
        total_amount=sum(Order.total_amount),
        avg_amount=avg(Order.total_amount),
    )
)

# Example row:
# {"country": "US", "order_count": 42, "total_amount": 12345.0, "avg_amount": 293.9}
```

**Relation query aggregations:**

```python
# Scalar aggregation on relation attrs
subscription_count = session.query().relations(Subscription).count()
avg_seat_count = session.query().relations(Subscription).avg(Subscription.seat_count)

# Grouped aggregation by endpoint field
total_seats = sum(Subscription.seat_count)
by_tier = (
    session.query()
    .relations(Subscription)
    .group_by(left(Subscription).tier)
    .agg(
        subscription_count=count(),
        total_seats=total_seats,
    )
)
```

**Existential/list helper aggregations:**

```python
# Count rows where existential predicate matches
click_users = (
    session.query()
    .entities(Customer)
    .count_where(Customer.events.any_path("kind") == "click")
)

# Average list length (NULL lists excluded; [] contributes 0)
avg_events = session.query().entities(Customer).avg_len(Customer.events)
```

**`having` with typed aggregate expressions:**

```python
from ontologia.query import count, sum

total_amount = sum(Order.total_amount)
order_count = count()

large_markets = (
    session.query()
    .entities(Order)
    .group_by(Order.country)
    .having(total_amount > 10000)
    .agg(
        total_amount=total_amount,
        order_count=order_count,
    )
)
```

**Notes:**

- `.agg(...)` is terminal and returns `list[dict[str, Any]]`
- Scalar aggregations cannot be used after `.group_by()`; use `.agg(...)`
  instead
- `.having(...)` must be used after `.group_by()` and before `.agg(...)`
- `.agg(...)` accepts aggregate builder objects only (no string DSL)
- `.count_where(predicate)` is equivalent to `.where(predicate).count()`
- `.avg_len(field)` computes `AVG(json_array_length(field))` with SQL NULL
  semantics
- Traversal queries (`.via(...)`) do not support aggregation

### History Queries

Query entity/relation history:

**Latest state (default):**

```python
# Returns latest version of each entity
customers = session.query().entities(Customer).collect()
```

**As of specific commit:**

```python
# Returns state at commit_id=100
snapshot = session.query().entities(Customer).as_of(commit_id=100).collect()
```

**Full history:**

```python
# Returns all versions of each entity
history = (
    session.query()
    .entities(Customer)
    .with_history()
    .collect()
)

# Each result can be inspected with .meta()
for customer in history:
    print(f"Commit {customer.meta().commit_id}: {customer.name}")
```

**History since commit:**

```python
# Returns all versions created after commit_id=100
changes = (
    session.query()
    .entities(Customer)
    .history_since(commit_id=100)
    .collect()
)
```

> **Schema Version Boundary:** Temporal queries (`as_of()`, `with_history()`,
> `history_since()`) are scoped to the **current schema version**. Only rows
> written under the active schema version for the queried type are returned. If
> the specified commit predates the current schema version (e.g., querying
> `as_of(commit_id=1)` after a migration created schema version 2), the result
> will be empty. This prevents hydration failures when historical rows lack
> fields required by the current schema. Non-temporal (latest-state) queries are
> unaffected — after migration, all latest rows have been rewritten under the
> current schema version.

### Metadata API

Query-hydrated `Entity` and `Relation` instances expose metadata directly via a
`meta()` method.

**Protocol:**

```python
class SupportsMeta(Protocol):
    def meta(self) -> Meta: ...
```

`Entity` and `Relation` implement this protocol and return query metadata backed
by runtime-attached `__onto_meta__`.

**Utility function (optional):**

```python
def meta(obj: SupportsMeta) -> Meta:
    return obj.meta()
```

`meta(obj)` is a convenience wrapper only; the primary API is `obj.meta()`.

**Common fields:**

- `obj.meta().commit_id: int`
- `obj.meta().type_name: str`

**Entity metadata fields:**

- `entity.meta().key: str`

**Relation metadata fields:**

- `relation.meta().left_key: str`
- `relation.meta().right_key: str`
- `relation.meta().instance_key: str | None` (non-None for keyed relations)

**Behavior notes:**

- Metadata is available on query-hydrated objects.
- Calling `obj.meta()` on non-query-hydrated instances should raise
  `MetadataUnavailableError`.
- `__onto_meta__` is runtime metadata; serialization/deserialization is not
  required to preserve it.

**Examples:**

```python
# Entity query + metadata
customers = session.query().entities(Customer).collect()
for customer in customers:
    m = customer.meta()
    print(customer.name, m.commit_id, m.key)

# Utility wrapper is equivalent
for customer in customers:
    m = meta(customer)
    print(customer.name, m.commit_id, m.key)

# Relation query + metadata
subscriptions = session.query().relations(Subscription).collect()
for subscription in subscriptions:
    m = subscription.meta()
    print(subscription.seat_count, m.commit_id, m.left_key, m.right_key)
```

---

## Handler API

Handler API defines reactive behavior with typed events, explicit commits, and
session-driven scheduling.

### Handler Decorator

Define handlers with `@on_event(EventType, ...)`. Handlers declare intents via
`ctx.ensure(...)` and persist by calling `ctx.commit(...)`.

```python
from ontologia import Event, Field, HandlerContext, on_event

class SyncRequested(Event):
    source: Field[str]

class SyncCompleted(Event):
    source: Field[str]
    rows: Field[int]

@on_event(SyncRequested, priority=200)
def sync_customers(ctx: HandlerContext[SyncRequested]) -> None:
    rows = fetch_rows()
    for row in rows:
        ctx.ensure(Customer(id=row["id"], name=row["name"]))
    ctx.add_commit_meta("handler", "sync_customers")
    ctx.commit(event=SyncCompleted(source=ctx.event.source, rows=len(rows)))
```

#### Decorator Parameters

- **`event_cls`** (`type[Event]`, required): typed event class handled
- **`priority`** (`int`, default: `100`): execution order; higher runs first

**Handler signature convention:**

- Handlers accept exactly one argument: `ctx: HandlerContext[TEvent]`
- Event payload is read from `ctx.event`

Decorators annotate handler metadata only. Handlers are passed explicitly to
`session.run()`.

#### Reactive Entry Point

Reactive execution is entered explicitly via `session.run(handlers)` in a
session:

```python
from ontologia import Schedule, Session

session = Session("my.db", entity_types=[Customer])

session.commit(event=SyncRequested(source="startup"))
session.run(
    [sync_customers],
    schedules=[Schedule(event=SyncRequested(source="cron"), cron="0 * * * *")],
    max_iterations=100,
)
```

Session mode semantics (`ensure` + `commit` behavior vs `run` behavior) are
documented in [Session API](#session-api).

#### Examples

**Event handler with explicit commit:**

```python
@on_event(SyncRequested)
def sync_customers(ctx: HandlerContext[SyncRequested]) -> None:
    for row in fetch_external_data():
        ctx.ensure(Customer(id=row["id"], name=row["name"]))
    ctx.commit()
```

**Multiple handlers:**

```python
from myapp.handlers import sync_customers, sync_products, compute_metrics

session = Session("my.db")
session.run([sync_customers, sync_products, compute_metrics], max_iterations=200)
```

---

### Events

Reactive runtime uses typed `Event` objects.

```python
from ontologia import Event, Field, Schedule

class SyncRequested(Event):
    source: Field[str]
    batch_id: Field[str]

# Schedule emits this event on cron
hourly = Schedule(
    event=SyncRequested(source="cron", batch_id="hourly"),
    cron="0 * * * *",
)
```

`Event` runtime fields are populated by the engine:

- `id: str | None`
- `created_at: str | None`
- `priority: int`
- `root_event_id: str | None`
- `chain_depth: int`

`EventDeadLetter` is a built-in event emitted when a handler exhausts retry
attempts for an event.

---

### Commit Metadata via Context

Attach metadata to the commit produced by the current handler run/chunk via the
handler context.

**Signature:**

```python
def add_commit_meta(self, key: str, value: str) -> None
```

**Semantics:**

- Applies to the commit created from the current handler run/chunk.
- Keys/values are strings.
- Repeated writes for the same key are allowed; **last write wins**.
- Metadata is persisted only when `ctx.commit(...)` creates a non-empty data
  commit.
- Event-only commits do not create data commits, so commit metadata is dropped.

**Example:**

```python
@on_event(SyncRequested)
def sync_customers(ctx: HandlerContext[SyncRequested]) -> None:
    ctx.add_commit_meta("source", "crm")
    ctx.add_commit_meta("job_id", current_job_id())

    for row in fetch_rows(limit=1000):
        ctx.ensure(Customer(id=row["id"], name=row["name"]))
    ctx.commit()
```

### Handler Context

Context object provided to all handlers.

**Type:**

```python
@dataclass
class HandlerContext(Generic[TEvent]):
    event: TEvent
    session: Session
    lease_until: datetime | None

    def add_commit_meta(self, key: str, value: str) -> None:
        ...

    def ensure(
        self,
        obj: Entity | Relation | Iterable[Entity | Relation],
    ) -> None:
        ...

    def emit(self, event: Event) -> None:
        ...

    def commit(self, *, event: Event | None = None) -> int | None:
        ...
```

**Usage:**

```python
@on_event(SyncRequested)
def my_handler(ctx: HandlerContext[SyncRequested]) -> None:
    # Query current state
    customer = (
        ctx.session.query()
        .entities(Customer)
        .where(Customer.id == "c1")
        .first()
    )

    # Attach commit metadata for this run/chunk
    ctx.add_commit_meta("handler", "my_handler")
    ctx.add_commit_meta("source", ctx.event.source)

    # Declare intents
    ctx.ensure(Customer(id="c1", name="Alice"))
    ctx.commit()

    # Emit follow-up event without writing state
    ctx.emit(SyncCompleted(source=ctx.event.source, rows=1))
```

---

### Execution Model

How handlers run and how deltas are computed.

#### Handler Execution Flow

1. `session.run(...)` registers handler metadata (`@on_event`) and optional
   schedules.
2. Due `Schedule` items enqueue cloned events.
3. Runtime claims events by handler/event type and creates `HandlerContext`.
4. Handler executes outside the global write lock.
5. Handler may call:
   - `ctx.ensure(...)` to queue intents
   - `ctx.add_commit_meta(...)` to stage metadata
   - `ctx.commit(event=...)` to persist current intents and optionally enqueue
     one event
   - `ctx.emit(...)` to enqueue follow-up events after handler success
6. On successful handler return:
   - buffered `emit(...)` events are enqueued
   - claimed event is ACKed
7. On handler error:
   - queued intents are discarded
   - claim is released for retry/backoff
   - after max attempts, runtime emits `EventDeadLetter`

Locking guarantee: no external side effects execute while the write lock is
held.

Contention handling:

- Lock acquisition timeout returns a structured contention error.
- Lease renewal is runtime-managed only; handlers have no lock lease API.
- Commits fail fast when handler lease expires.

#### Delta Computation

For each commit call, runtime reconciles queued intents:

**Entity intents:**

```python
session.ensure(Customer(id="c1", name="Alice", age=32))
```

1. Check if entity `(Customer, c1)` exists
2. If not exists: **Insert** new row
3. If exists with same value: **Skip** (no-op)
4. If exists with different value: **Update** (append new version row)

**Relation intents (unkeyed):**

```python
session.ensure(Subscription(left_key="c1", right_key="p1", ...))
```

1. Check if relation `(Subscription, c1, p1)` exists
2. If not exists: **Insert** new row
3. If exists with same attrs: **Skip** (no-op)
4. If exists with different attrs: **Update** (append new version row)

**Relation intents (keyed):**

```python
session.ensure(Employment(left_key="p1", right_key="c1", stint_id="stint-1", ...))
```

1. Check if relation `(Employment, p1, c1, stint-1)` exists
2. If not exists: **Insert** new row
3. If exists with same attrs: **Skip** (no-op)
4. If exists with different attrs: **Update** (append new version row)

#### Identity and Targeting

**Key insight:** commits affect only **targeted identities**.

- Entity targeted by: `(type_name, key)`
- Unkeyed relation targeted by: `(type_name, left_key, right_key)`
- Keyed relation targeted by: `(type_name, left_key, right_key, instance_key)`

**Entities/relations NOT targeted by any intent are unaffected.**

This enables **partial state reconciliation** - handlers don't need to declare
the entire world, only the identities they care about.

#### Side Effects

Handlers may perform side effects (logging, API calls, alerts):

```python
@on_event(SyncRequested)
def alert_urgent(ctx: HandlerContext[SyncRequested]) -> None:
    # Side effect (before commit)
    send_slack_alert(f"Sync requested from {ctx.event.source}")

    # Intent (will be committed)
    ctx.ensure(Alert(id=f"alert-{ctx.event.batch_id}", ...))
    ctx.commit()
```

**Side effect timing:**

- Execute during handler run (before commit attempt)
- If handler raises exception, commit is aborted
- If side effect fails but handler completes, commit proceeds
- Handler run is outside write lock; side effects cannot stall global commit
  lock ownership
- Handlers can be retried after claim release; side effects must be idempotent
  or guarded by external dedupe/outbox patterns

**Best practice:** Make side effects idempotent (handler may re-run).

#### Commit Loop Prevention

Runtime applies loop guards to event chains:

- Every emitted/committed event carries `root_event_id` and `chain_depth`.
- Runtime enforces `config.max_event_chain_depth`.
- When depth limit is exceeded, runtime raises `EventLoopLimitError`.
- No legacy commit-auto-trigger path exists; follow-up work is produced only
  through `ctx.emit(...)` or `ctx.commit(event=...)`.

---
