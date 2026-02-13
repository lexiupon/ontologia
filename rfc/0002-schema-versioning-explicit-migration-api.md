# RFC 0002: Schema Versioning and Explicit Migration API

## Status

Implemented (2026-02-11)

## Created

2026-02-09

## Summary

Introduce schema version history in the database for all entity and relation
types, associate each persisted data row with the schema version used to produce
it, and add one explicit public migration API:

- `onto.migrate(...)` (dry-run by default, explicit apply)

Normal runtime behavior remains strict: session creation validates schema and
aborts on mismatch. Handlers run only in `run()`.

## Motivation

Current behavior is strict and safe:

- Runtime validates schema before execution.
- On mismatch, execution aborts.
- Operators apply schema changes via explicit CLI tooling.

That default should remain. This RFC adds a programmatic migration path that is
explicit, auditable, and safe under concurrency.

## Non-Goals

- Implicit migration during normal startup.
- Best-effort or partial migrations.
- In-place mutation of historical rows.
- Exposing raw DB connections to handlers.
- Schema downgrade support.

## Design Goals

1. Preserve fail-fast runtime semantics.
2. Keep schema evolution fully auditable and versioned.
3. Treat add/remove field changes through one upgrade pipeline.
4. Guarantee atomicity for multi-type data/schema upgrades.
5. Avoid preview/apply drift with deterministic token verification.

## Decisions

### 1. Schema versioning model

- Use **per-type schema versions** for entities and relations.
- Keep a migration boundary via global commit/event metadata when data writes
  occur.
- Each data row stores `schema_version_id` for the producing type version.

Rationale: per-type versioning reduces coupling and avoids artificial version
bumps for unchanged types.

### 2. Schema-only migration

Schema-only migration (no data rewrite) applies **only when the affected type
has zero existing data rows**. If a type has any materialized data, the
migration must include data upgrade via an upgrader function.

- When schema-only: persist new schema version rows and a migration audit
  record. No data commit.
- When data exists: upgrader is required; migration rewrites materialized rows
  so all rows reference the current schema version after migration.

Rationale: all materialized rows always reference the current schema version
after migration. No ambiguity about when an upgrader is needed.

### 3. Upgrade path scope

Migration target is always the latest code schema shape, but data upgrades may
require a version chain (for example `v1->v2->v3`) based on stored
`schema_version_id`.

Code does not carry old type classes. Version identifiers are migration metadata
used by upgraders and schema history only. Runtime validates intermediate
upgrade steps against stored JSON schema for each version, and validates final
output against the latest code-defined schema.

### 4. Migration trigger model

No `auto_migrate_on_startup` mode. Migration is explicit via
`onto.migrate(...)`. Session creation remains strict verify-and-abort on
mismatch.

## Lifecycle and Runtime Model

### Two-phase object lifecycle

1. `Ontology(...)` binds storage and config only. No schema validation, no event
   firing.
2. `onto.session()` performs schema validation. On mismatch, raises
   `SchemaOutdatedError` with structured diff. On success, returns runnable
   session context.
3. `s.run()` executes handlers and event loop.

This avoids constructor-time catch-22 and supports pre-session migration.

**Breaking change**: current behavior performs validation in the constructor.
This RFC splits that into constructor (bind) + session (validate) + run (execute
handlers). Existing code that relies on constructor-time validation must be
updated to use `onto.session()` before `run()`.

### Session creation failure

When `onto.session()` detects a schema mismatch, it raises
`SchemaOutdatedError`:

```python
class SchemaOutdatedError(Exception):
    diffs: list[TypeSchemaDiff]   # per-type field diffs
    message: str                   # human-readable summary with next steps
```

The error includes per-type diffs (added fields, removed fields, changed fields)
and a message suggesting `onto.migrate()` or `ontologia migrate` as the next
step.

### Session contract in handlers

Handler `ctx` exposes session/runtime facade, not raw storage connection.
Handlers use typed/query/intents APIs only.

## Proposal

### 1. Persist full schema history

For each entity and relation type:

- Append-only schema history table.
- Monotonic `schema_version_id` per type.
- Metadata: schema hash/fingerprint, created_at, runtime_id, optional reason.

Each entity/relation row references its type `schema_version_id`.

### 2. Upgrader contract

Upgraders are functions decorated with `@upgrader("TypeName", from_version=N)`
that transform data from schema version `N` to `N+1`.

The runtime builds a migration chain: if the database contains version 1 data
and the code is at version 3, the runtime executes `v1->v2` then `v2->v3`
sequentially for each record.

**Signature**: `(old: dict) -> dict`, decorated with
`@upgrader("TypeName", from_version=int)`

```python
from ontologia import upgrader

# Upgrader from v1 to v2
@upgrader("Customer", from_version=1)
def upgrade_customer_v1_v2(old: dict) -> dict:
    """v1 → v2: Added email field."""
    return {**old, "email": None}

# Upgrader from v2 to v3
@upgrader("Customer", from_version=2)
def upgrade_customer_v2_v3(old: dict) -> dict:
    """v2 → v3: Split name into first_name/last_name."""
    name = old.pop("name")
    return {**old, "first_name": name.split()[0], "last_name": name.split()[-1]}
```

**Discovery**:

- `load_upgraders("myapp.migrations")` scans for `@upgrader`-decorated
  functions.
- It returns a registry mapping `(type_name, from_version) -> upgrader_func`.
- Duplicate upgraders for the same `(type_name, from_version)` raise a startup
  error.

**Rules**:

- The `from_version` argument specifies the **input** schema version (the
  version of the data passed _into_ the function).
- The function returns a dict representing the **next** schema version.
- The runtime automatically chains upgraders to bridge the gap between stored
  data and current code.
- If a gap in the chain exists (e.g., have v1, code expects v3, but only v1->v2
  is defined), migration aborts.
- The runtime constructs the final typed instance (`Type(**output_dict)`) only
  after the full chain has executed.

**When upgraders are required**:

- Type has schema diff AND has existing data rows → upgrader required; migration
  aborts without one.
- Type has schema diff AND has zero data rows → schema-only; no upgrader needed.

### 3. Migration API

```python
from ontologia import load_upgraders

upgraders = load_upgraders("myapp.migrations")

preview = onto.migrate(
    upgraders=upgraders,
)  # dry_run=True by default

result = onto.migrate(
    dry_run=False,
    token=preview.token,
    upgraders=upgraders,
    force=False,
    lock_timeout_s=30,
    lease_ttl_s=600,
    meta={"reason": "add-email-field"},
)
```

Key rules:

- `migrate()` runs on storage bound to `onto`; no alternate db/storage
  arguments.
- `dry_run` defaults to `True`.
- Dry-run output returns a `MigrationPreview` with an opaque `token` encoding
  the plan hash and base head commit, per-type diffs, estimated row counts, and
  which types require upgraders.
- On apply, `token` is required (unless `force=True`). Runtime recomputes plan
  under lock and verifies token matches.
- If token mismatches (stale preview), abort.
- `force=True` skips token verification but still recomputes and validates under
  lock.
- `force=True` is mutually exclusive with `token`.

**Preview result object**:

```python
class MigrationPreview:
    has_changes: bool
    token: str                      # opaque; plain base64 of plan_hash + base_head_commit
    diffs: list[TypeSchemaDiff]     # per-type: added/removed/changed fields
    estimated_rows: dict[str, int]  # type_name → row count to upgrade
    types_requiring_upgraders: list[str]
    types_schema_only: list[str]    # types with changes but zero data rows
    missing_upgraders: list[str]    # types that need upgraders but none provided
```

Token is plain-encoded (base64 of plan hash + head commit). No HMAC signing. The
token exists for drift detection, not security: even a crafted token is rejected
if the recomputed plan under lock does not match.

### 4. Unified add/remove upgrade flow

Add/remove fields use the same upgrader pipeline:

1. Read latest materialized rows for affected types.
2. Call upgrader function: `upgrader(old_json_dict) -> new_dict`.
3. Construct typed instance: `Type(**new_dict)`.
4. Validate typed instance against current schema; fail if non-conformant.
5. Persist upgraded rows with new schema version reference.
6. Persist new schema version rows.

**Preview always enumerates all types** with schema diffs and flags which ones
are missing upgraders (`MigrationPreview.missing_upgraders`). Preview never
fails eagerly for missing upgraders — the user sees the full picture in one
call. Apply aborts if any required upgrader is missing.

### 5. Multi-type atomicity and locking

When multiple types are affected:

- Migration operations execute in one lock-protected critical section (Global
  Lock).
- Data + schema writes are atomic.
- Lock is ontology-wide lease lock with configurable timeout.
- **Note**: While per-type locking was considered, a global lock is chosen for
  initial simplicity and to strictly guarantee multi-type atomicity and global
  consistency. Optimization to finer-grained locks is deferred to future work.
- Executor runs a background keep-alive that renews the lease every
  `lease_ttl_s / 3` while the migration is active. If the process crashes, the
  lease expires after TTL and other processes can proceed. This removes the need
  for the user to guess the right TTL for their dataset size.

Required safety:

- Re-check head/schema under lock before apply.
- No external side effects under lock.
- On failure, no partial data/schema apply.

### 6. Read isolation during migration

Concurrent readers during an active migration see the **pre-migration state**.
Migration writes are not visible until the atomic commit completes. This is
consistent with the existing write-lock model where readers see committed state
only.

## Detailed Semantics

### Data selection scope

Migration upgrades current materialized state (latest row per identity), not
historical backfill.

Historical rows remain immutable and keep old schema-version references. Mixed
version history for one identity/type is expected and preserved.

Example for entity type `X`:

- row history may contain `x1@v1`, `x2@v2`, `x3@v2`, `x4@v2`, `x5@v3`.
- schema history for `X` contains `v1`, `v2`, `v3` definitions.
- migration to `v3` rewrites only current materialized rows as needed; prior
  rows stay unchanged.

### Failure semantics

Abort migration (and keep state unchanged) on:

- stale token mismatch,
- missing upgrader for type with existing data,
- typed validation errors from upgrader output,
- lock timeout/acquisition failure,
- transaction failure or head mismatch under lock.

### Upgrader error reporting

When an upgrader raises or returns data that fails schema validation, the error
must include enough context to fix the upgrader in one iteration:

- Type name.
- Identity key of the failing row.
- The specific validation error (which fields failed, expected vs actual).
- The old data dict that was passed to the upgrader.

This surfaces in both the Python API (`MigrationError`) and the CLI output.

## API Sketch

```python
from ontologia import Ontology, load_upgraders

onto = Ontology(db_path="my_ontology.db", config=...)

# Load upgraders from module (scans @upgrader-decorated functions)
upgraders = load_upgraders("myapp.migrations")

# Preview migration
preview = onto.migrate(upgraders=upgraders)
if preview.has_changes:
    print(preview.diffs)
    # Apply with token pinning
    onto.migrate(
        dry_run=False,
        token=preview.token,
        upgraders=upgraders,
    )

# Now safe to start session
with onto.session() as s:
    s.run()
```

**Session failure flow**:

```python
try:
    with onto.session() as s:
        s.run()
except SchemaOutdatedError as e:
    print(e.message)   # "Schema mismatch for Customer: ... Run onto.migrate()"
    print(e.diffs)     # structured per-type diffs
```

## CLI

```bash
# Preview (default dry-run)
ontologia migrate --upgraders myapp.migrations
# Output includes: token, per-type diffs, estimated rows

# Apply with token
ontologia migrate --apply --token <token> --upgraders myapp.migrations

# Force apply (skips token, still validates under lock)
ontologia migrate --apply --force --upgraders myapp.migrations
```

Rules:

- `ontologia migrate` defaults to dry-run.
- `--apply` requires `--token` unless `--force` is used.
- `--force` is mutually exclusive with `--token`.
- `--upgraders` points to a Python module containing `@upgrader`-decorated
  functions. CLI uses `load_upgraders()` to scan and collect them:

```python
# myapp/migrations.py
from ontologia import upgrader

# Upgrader for v1 -> v2
@upgrader("Customer", from_version=1)
def upgrade_customer_v1_v2(old: dict) -> dict:
    """v1 → v2: Added email field."""
    return {**old, "email": None}

# Upgrader for v2 -> v3
@upgrader("Customer", from_version=2)
def upgrade_customer_v2_v3(old: dict) -> dict:
    """v2 → v3: Split name into first_name/last_name."""
    name = old.pop("name")
    return {**old, "first_name": name.split()[0], "last_name": name.split()[-1]}
```

- CLI internally reuses the same planner/executor as `onto.migrate(...)`.
- Token is an opaque string printed to stdout; user passes it back for apply.

## Compatibility and Spec Impact

This RFC introduces a **breaking change** to the Ontology lifecycle:

| Current                       | Proposed                         |
| ----------------------------- | -------------------------------- |
| `Ontology()` validates schema | `Ontology()` binds only          |
| N/A                           | `onto.session()` validates       |
| N/A                           | Handlers run via `session.run()` |

Required spec updates:

- `spec/vision.md`: startup validation moves to session creation; explicit
  migration pathway.
- `spec/api.md`: `migrate()` API, `session()` lifecycle, `SchemaOutdatedError`.
- `spec/cli.md`: `ontologia migrate` contract (default dry-run, token-pinned
  apply, optional `--force`), shared planner/executor.

## Rollout Plan

1. Implement two-phase lifecycle: constructor binds only, `session()` validates.
2. Add schema history tables and row `schema_version_id` references.
3. Add upgrader contract and registration via `migrate(upgraders=...)`.
4. Implement migration planner with deterministic token generation.
5. Implement migration executor with token verification under lock.
6. Add `SchemaOutdatedError` with structured diffs at session creation.
7. Add tests:
   - mismatch raises `SchemaOutdatedError` at session creation,
   - preview token generation and determinism,
   - stale token rejection,
   - single-type and multi-type migration success,
   - schema-only migration (zero data rows, no upgrader needed),
   - missing upgrader abort (type has data),
   - lock timeout abort with no partial writes,
   - concurrent reads see pre-migration state during migration.

## Resolved Questions

1. **Token encoding**: Plain base64 encoding of plan hash + head commit. No HMAC
   signing. The token is for drift detection; the runtime always recomputes and
   validates under lock regardless of token content.

2. **Lease renewal**: Executor runs background keep-alive at `lease_ttl_s / 3`
   intervals. Lease expires naturally on process crash. No need to guess TTL for
   dataset size.

3. **Upgrader error context**: Errors include type name, identity key,
   validation error detail, and the old data dict — enough to fix the upgrader
   in one iteration.

4. **Preview completeness**: Preview always succeeds and enumerates all types
   with diffs, flagging missing upgraders via `missing_upgraders` list. Apply
   fails if any required upgrader is absent.
