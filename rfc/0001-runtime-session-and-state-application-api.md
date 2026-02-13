# RFC 0001: Runtime Session and State Application API

## Status

Implemented (2026-02-11)

## Summary

Introduce an explicit runtime/session model with two first-class state-update
modes:

1. **Reactive mode**: `session.run(...)` executes handlers against events.
2. **Imperative mode**: `session.ensure(obj)` queues intents and
   `session.commit()` computes delta and commits changes.

The session becomes the single write surface for all state modifications.

## Motivation

The current approach would benefit from explicit boundaries between runtime
lifecycle concerns and execution/write surfaces. This RFC establishes clear
separation:

- `Ontology`: runtime container and storage binding.
- `Session`: unit-of-work and write/event execution surface.
- `HandlerContext`: per-handler event scope.

This improves composability, testability, and commit-boundary clarity.

## Non-Goals

- Implicit handler auto-discovery via filesystem/package scanning.
- Exposing raw DB connection to handlers.
- Changing delta semantics, lock semantics, or append-only history model.

## Proposal

### 1. Runtime Object Model

#### `Ontology`

- Binds DB/storage and configuration.
- Owns handler registry and type registry.
- Provides `session(...)` factory.
- Does not expose direct write API.

#### `Session`

- Unit-of-work object created from `Ontology`.
- Owns an in-memory intent queue.
- Exposes:
  - `ensure(obj: Entity | Relation) -> None`
  - `commit() -> int | None`
  - `run(...) -> RunResult`
- Responsible for delta computation + commit application.

#### `HandlerContext`

- Event-scoped object passed to handlers.
- Holds runtime metadata (`event`, `commit_id`, `root_event_id`, `chain_depth`).
- References the active `Session` (not raw storage).
- Exposes `add_commit_meta(...)` and session-backed intent APIs.

### 2. Two State-Update Modes

#### 2.1 Imperative Mode

```python
onto = Ontology(db_path="app.db", ...)
session = onto.session()
session.ensure(Customer(id="c1", name="Alice"))
session.ensure(Subscription(left_key="c1", right_key="p1", seat_count=2))
commit_id = session.commit()
```

Semantics:

- `ensure(...)` appends one intent to session queue.
- `commit()` reconciles queued intents against current state, atomically:
  - empty delta => no commit
  - non-empty delta => single commit for this commit call
- successful `commit()` clears the queue.
- return value is `commit_id` for non-empty delta, otherwise `None`.

#### 2.2 Reactive Mode

```python
onto = Ontology(db_path="app.db", config=...)
session = onto.session()

# List handlers explicitly
from myapp.handlers import on_startup, on_customer_created

session.run(handlers=[on_startup, on_customer_created])
```

Alternatively, handlers can be collected from a module:

```python
from ontologia import collect_handlers

session.run(handlers=collect_handlers("myapp.handlers"))
```

Semantics:

- `run(...)` executes registered handlers and commits runtime-collected intents.
- Handlers declare state via `ctx.ensure(...)` (not by yielding intents).
- Handler execution remains outside write lock; delta+persist remains in
  lock-protected critical section.
- Commit metadata is attached through `ctx.add_commit_meta(...)`.

### 3. `run()` Semantics (Normative)

`session.run(...)` behavior in one invocation:

1. Execute scheduled handlers for provided tick(s) (if any).
2. Process commit-triggered reactions according to run mode:
   - single-pass mode: process currently available commits then return.
   - polling mode: keep polling and processing until stopped.

Configuration for commit reaction looping/polling is explicit in API/config; no
hidden background threads.

**Initialization pattern**: For explicit startup logic, populate state
imperatively before entering reactive mode:

```python
onto = Ontology(db_path="app.db", config=...)
session = onto.session()

# Imperative initialization
session.ensure(InitialConfig(id="config", ...))
session.commit()

# Then enter reactive mode
session.run(handlers=[...])
```

### 4. Handler Registration and Collection

- No implicit filesystem scanning.
- Runtime accepts explicit handler set or explicit module list.
- Provide lightweight utility, e.g. `collect_handlers(modules=[...])`, to
  resolve decorated handlers with minimal magic.

### 5. Public API Surface (Target)

```python
class Ontology:
    def __init__(self, db_path: str, config: OntologiaConfig | None = None, ...) -> None: ...
    def session(self) -> Session: ...

class Session:
    def ensure(self, obj: Entity | Relation) -> None: ...
    def commit(self) -> int | None: ...
    def run(self, ..., handlers: list[Handler] | None = None) -> RunResult: ...
    def query(self) -> QueryBuilder: ...
    def list_commits(...) -> list[dict[str, Any]]: ...
    def get_commit(commit_id: int) -> dict[str, Any] | None: ...

class HandlerContext:
    event: str
    commit_id: int | None
    root_event_id: str
    chain_depth: int
    session: Session
    def ensure(self, obj: Entity | Relation) -> None: ...
    def add_commit_meta(self, key: str, value: str) -> None: ...
```

Notes:

- Handlers use `ctx.ensure(...)` only; yielding/returning intents is invalid.

### 6. Commit and Queue Semantics

- Session queue is FIFO.
- `commit()` processes all currently queued intents as one reconciliation batch.
- If `commit()` fails, transaction is rolled back and queue remains available
  for caller-managed retry or reset.
- No-op commit produces no commit record and does not emit commit events.
- Calling `session.commit()` (or `ctx.session.commit()`) during active handler
  execution is invalid and raises `InvalidExecutionContextError`.

### 7. Concurrency and Correctness

Unchanged guarantees:

- Single ontology-wide lease lock for commit critical section.
- Head verification + bounded retry on mismatch.
- Atomic commit writes.
- Loop-guard protections for commit-handler chains.

## Alternatives Considered

1. Allow handlers to call ontology-level write directly.
   - Rejected: weakens event-scope boundaries and determinism.

## Rollout Plan

1. Update specs:
   - `spec/vision.md`: add `Ontology/Session/Context` model and two update
     modes.
   - `spec/api.md`: document `session.ensure/commit` and `session.run` APIs.
2. Implement code:
   - add `Session` class
   - move write pipeline under session
   - route handler execution via session
   - update handler context to session-backed API
3. Update examples/tests/docs to session APIs.

## Acceptance Criteria

- Public write path is session-only (`ensure/commit`).
- Reactive execution is session-driven (`run`).
- `spec/vision.md` and `spec/api.md` consistently describe the two modes.
- Existing correctness guarantees (delta, no-op, lock, commit ordering) are
  preserved.
