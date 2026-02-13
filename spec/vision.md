# Ontologia Specification

## 1. Purpose

Ontologia is a typed, functional-style data management library.

It stores `Entity` and `Relation` data in an ontology datastore and reconciles
declared expected state against current state to produce auditable commits.

Ontologia is best suited for low- to moderate-write-volume domains where
correctness, auditability, and deterministic reconciliation matter more than raw
write throughput. Typical use cases include master data alignment across
systems, golden-record reconciliation, auditable policy/configuration state
management, and regulated point-in-time reporting.

## 2. Data Model

### 2.1 Concepts

- **Entity**: A typed record representing a thing with identity, defined by an
  `Entity` subclass schema (for example, `Customer` or `PurchaseEvent`) with one
  primary key field.
- **Relation**: A typed edge between two entities, defined by a `Relation[L, R]`
  subclass schema with typed endpoints (`left_key`, `right_key`) and optional
  attributes (for example, `Employment(Person, Company)`).
  - **Unkeyed relation identity**: `(relation_name, left_key, right_key)`. This
    is the default and represents one current edge state per endpoint pair.
  - **Keyed relation identity**:
    `(relation_name, left_key, right_key, instance_key)`. This enables multiple
    concurrent instances between the same endpoint pair (for example, employment
    stints). `instance_key` is identity, not relation field payload data.
- **Ontology**: Versioned boundary over one storage binding that includes entity
  and relation schemas, append-only history, commit metadata/logs, and the
  current state of entities and relations.

### 2.2 Normative Rules

1. **Entity Type Rules** Each entity schema MUST define exactly one
   `Field(primary_key=True)`. Composite entity primary keys are unsupported, so
   multipart natural identity MUST be encoded into one deterministic key field
   (for example, `"order-123#line-2"`). Entity schemas MUST NOT declare
   `Field(instance_key=True)`.

2. **Relation Type Rules** Relation schemas MUST NOT declare
   `Field(primary_key=True)`. A relation MAY declare at most one
   `Field(instance_key=True)`. If present, that instance-key field MUST be a
   required `str` with no default. The instance key MUST be treated as identity
   and excluded from relation field payload (`model_dump()` data).

3. **Relation Identity and Write Semantics** Relation identity MUST be unkeyed
   `(relation_name, left_key, right_key)` or keyed
   `(relation_name, left_key, right_key, instance_key)`. Write intents MUST
   provide endpoint keys (`left_key`, `right_key`), and keyed relations MUST
   provide an instance key via the declared instance-key field name.
   Reconciliation MUST be upsert-by-identity: missing identity inserts, changed
   attributes append a new version row, unchanged attributes are no-op. For
   repeatable facts per endpoint pair, prefer keyed relations for edge-centric
   data and fact entities for event-centric data.

4. **Persistence and Lifecycle Semantics** Entity and relation storage MUST be
   append-only and retain full history. Existing history rows MUST NOT be
   updated in place. Ontologia provides no built-in hard delete/retract for
   entities or relations in runtime intent/query APIs; deletion/retirement MUST
   be modeled in application lifecycle fields (for example, `active`,
   `deleted_at`, `status`). Administrative tooling MAY provide explicit,
   destructive, type-level cleanup paths (for example, `schema drop`) with
   strict safety pins: dry-run preview, tokened apply, plan recompute under
   write lock, dependency checks, and explicit destructive flags for history
   purge.

5. **Schema Definition Source** Code-defined entity and relation schemas are the
   canonical schema definition. Runtime schema validation and migration behavior
   is defined in §3.

## 3. Runtime

### 3.1 Concepts

- **Connection**: A runtime-bound handle to one ontology storage binding. It is
  used to open sessions and execute runtime-driven work. Each connection has a
  runtime identifier and is associated with one ontology store. In shared
  deployments, active runtimes use distinct runtime identifiers for lock/lease
  ownership and diagnostics.
- **Intent**: Declarative expected state over explicitly targeted identities
  (for example, expected status for one `Customer` identity). Intents are
  partial assertions over targeted identities only. Records not targeted by any
  intent are out of scope and remain unchanged. Absence of intent never implies
  deletion.
- **Session**: A unit of work created from a connection where intents are
  declared. Intents can be declared within a session imperatively or reactively
  via `run(handlers, schedules=...)`. Reactive handlers are typed `@on_event`
  functions that receive `HandlerContext`. Inside handlers, `ctx.ensure(...)`
  buffers intents and `ctx.commit(...)` explicitly persists them.
- **Event**: A typed message defined by subclassing `Event`. Events carry typed
  payload fields plus runtime envelope fields (`id`, `created_at`, `priority`,
  `root_event_id`, `chain_depth`) used for dispatch and loop protection.
- **Schedule**: A cron-based trigger (`Schedule(event=..., cron=...)`) that
  emits typed root events into the event bus while `run()` is active.
- **Commit**: Atomic, ordered persistence of reconciled changes with one
  monotonic `commit_id` and metadata (for example, system-managed metadata like
  timestamp, runtime identifier, and optional application-provided keys).
  Commits are created only for non-empty deltas.

### 3.2 Normative Rules

1. **Schema Validation and Drift Checks** When typed schemas are registered, a
   connection MUST provide explicit schema validation that compares stored
   schema against code-defined schema and caches validated schema version IDs.
   Validation mismatch MUST fail fast with structured diffs
   (`SchemaOutdatedError`). Before persisting any write commit, the runtime MUST
   re-check schema version IDs for touched types under the write lock. On drift,
   it MUST abort the write and raise `SchemaOutdatedError`.

2. **Schema Evolution Path** Schema evolution for active type shape changes MUST
   occur only through explicit preview-then-apply migration. Preview MUST return
   a deterministic token tied to migration plan and head commit. Apply MUST
   verify the token unless explicitly forced and MUST execute under write lock.
   Types with existing data MUST provide required upgraders for each version
   step. Migration MUST be atomic (all type migrations succeed or none are
   committed). Runtime MUST NOT perform implicit migration during validation,
   session creation, or commit processing. Administrative type-removal cleanup
   (`schema drop`) is an explicit exception path and MUST use equivalent
   preview/apply safety semantics with stale-token abort on apply mismatch.

3. **Session Execution Semantics** A session MUST support imperative (`ensure` +
   commit/exit) and reactive (`run(handlers, schedules=...)`) execution.
   Reactive handlers MUST be decorated with `@on_event(EventSubclass)`. Runtime
   MUST NOT enforce a single-invocation-per-session restriction on `run()`.
   Runtime MUST NOT implicitly commit pending intents when entering `run()`.
   Runtime MUST NOT implicitly commit intents buffered during a handler call.
   Handler intents are persisted only when `ctx.commit()` is called. A commit
   MUST be created only for a non-empty delta; an empty delta MUST produce no
   commit.

4. **Maximum Commit Size** Runtime MUST enforce `max_batch_size` per commit
   attempt, measured as the number of intents in that attempt. If the limit is
   exceeded, execution MUST fail with `BatchSizeExceededError` and no commit
   MUST be created.

5. **Event-Chain Loop Prevention** Runtime MUST carry `root_event_id` and
   `chain_depth` across derived events (for example via `ctx.emit(...)` or
   `ctx.commit(event=...)`). Root events (including scheduled emissions) MUST
   start with `chain_depth = 0`. Each derived event MUST increment chain depth
   by one from its parent event. Runtime MUST enforce `max_event_chain_depth`;
   exceeding it MUST fail with `EventLoopLimitError` and the derived event MUST
   NOT be enqueued.

6. **Concurrency, Locking, and Atomicity** Multiple runtimes MAY operate
   concurrently against one ontology datastore, with distinct runtime IDs.
   Writes MUST be serialized through one ontology-wide write lock. Failure to
   acquire write lock within timeout MUST fail with `LockContentionError`.
   Before persisting changes, runtime MUST perform schema drift checks for
   touched types under the write lock. Commit persistence MUST be atomic; on
   failure, no partial commit MUST be visible.

7. **Critical Section and Side Effects** External I/O and side effects MUST NOT
   execute while the write lock is held. Handler execution MUST occur outside
   the write lock. Schedules MUST emit their typed `Schedule.event` payloads as
   root events during `run()` when cron expressions match. Side effects in
   handlers SHOULD be idempotent or externally deduplicated.

## 4. Query Model

### 4.1 Concepts

- **Type-Safe Query DSL**: Query construction is schema-driven: entity queries
  start from `entities(EntityType)` and relation queries from
  `relations(RelationType)`. Filters and field references are composed from
  typed schema fields and endpoint field paths. Structured field paths are
  supported via path composition on field proxies (for example
  `User.profile.path("city")` or `User.profile["city"]`), including existential
  list predicates on list fields.
- **Entity Query**: Query the latest or historical versions of one entity type
  with filtering, ordering, pagination, and aggregation.
- **Relation Query**: Query one relation type with filtering over relation
  attributes and endpoint entities (`left(...)`, `right(...)`), plus ordering,
  pagination, and aggregation.
- **Traversal Query**: Start from entity roots and traverse one or more relation
  steps via `.via(...)`, returning path results rooted at source identity.
- **Temporal Query**: Query current state, state at a specific commit, full
  history, or changes since a commit.
- **Query Metadata**: Query-hydrated entities and relations carry metadata such
  as `commit_id`, `type_name`, and identity keys.
- **Aggregation Query**: Scalar and grouped aggregation over entity/relation
  queries.

### 4.2 Normative Rules

1. **Type-Safe Construction** Query APIs MUST expose typed builders from schema
   classes (`entities(T)`, `relations(R)`). Filters MUST support typed field
   expressions and logical composition (`&`, `|`, `~`) for entity fields,
   relation fields, and relation endpoint fields. Path-composed field references
   over structured payloads MUST compile to nested JSON extraction semantics.
   Path grammar MUST be restricted to identifier segments joined by `.`, and
   invalid paths MUST fail at query-build time.

2. **Typed Terminal Results** `collect()` MUST return typed domain instances for
   the queried schema type, and `first()` MUST return one typed instance or
   `None`.

3. **Traversal Shape and Scope** Traversal queries (`.via(...)`) MUST return
   path results rooted at source entities. Each path MUST preserve source
   identity even when no relations are traversed. Traversal queries are
   lookup-only and MUST NOT support aggregation.

4. **Temporal Semantics** Query APIs MUST support: latest-state reads (default),
   point-in-time reads (`as_of(commit_id)`), full history (`with_history()`),
   and incremental history (`history_since(commit_id)`). Temporal queries MUST
   be scoped to the current schema version; rows written under prior schema
   versions MUST NOT be returned or hydrated.

5. **Aggregation Semantics** Aggregation MUST be available on entity and
   relation queries (scalar and grouped forms). Grouped aggregation MUST follow
   `group_by(...).having(...).agg(...)` semantics. Scalar and grouped
   aggregations MUST accept path-composed field references. Query APIs MAY also
   expose convenience aggregations for existential predicates and list lengths
   (`count_where`, `avg_len`) on entity/relation queries. Traversal queries MUST
   NOT expose aggregation operations.

6. **Metadata Availability** Query-hydrated entities and relations MUST expose
   commit and identity metadata through `obj.meta()`. Entity metadata MUST
   include `key`. Relation metadata MUST include `left_key`, `right_key`, and
   `instance_key` when applicable.

7. **Metadata Safety** Calling `obj.meta()` on non-query-hydrated instances MUST
   fail with `MetadataUnavailableError`.

8. **Pagination Determinism** Query APIs MAY support `limit`/`offset`
   pagination. For deterministic paging, callers SHOULD provide a stable
   `order_by(...)`.
