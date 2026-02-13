# RFC 0009: V2 Storage Engines with Current-Schema-Only Typed Reads

## Status

Proposed

## Created

2026-02-12

## Supersedes

RFC 0008: Temporal Query Schema Version Boundary

## Summary

Define new storage engine versions for both backends:

- `sqlite/v2`
- `s3/v2`

V2 formalizes current-schema-only typed reads across query and commit-history
surfaces, and introduces a physical layout that partitions data by
`type + schema_version_id`.

Key outcomes:

- Safer typed hydration (no cross-version row hydration by default).
- Backend parity between SQLite and S3 design.
- Cleaner long-term storage evolution through engine versioning.

## Motivation

Typed query hydration uses current code-defined classes. Historical rows written
under older schemas can fail validation or be interpreted incorrectly when
hydrated as latest types.

RFC 0008 introduced schema-version boundaries for temporal query APIs. This RFC
extends that direction into a full storage-engine design:

- explicit engine version metadata and dispatch,
- version-partitioned physical layout,
- consistent typed-read semantics across SQLite and S3.

## Non-Goals

- Removing append-only history retention.
- Providing automatic v1 -> v2 migration in this RFC.
- Introducing dynamic per-row schema-class hydration in typed APIs.
- Removing v1 engine support immediately.

## Phased Delivery

This RFC is designed for incremental delivery in two phases. Each phase is
independently shippable and valuable.

### Phase 1: Engine Versioning Infrastructure

Add engine version metadata and version-aware dispatch without changing physical
layout or query semantics. This is a backward-compatible change that prepares
the runtime for future engine versions.

Scope:

- `storage_meta` table (SQLite) and `meta/engine.json` (S3).
- Version-aware `open_repository()` dispatch.
- `storage_info()` includes `engine_version`.
- `onto init --engine-version` CLI flag.
- Existing storages detected as `v1` (see Decision §7).

Phase 1 ships with `v1` as the only engine version. The infrastructure exists so
that Phase 2 can register `v2` without further dispatch changes.

### Phase 2: V2 Physical Layout and Typed Reads

Introduce the `v2` engine with version-partitioned physical layout, typed column
promotion, and current-schema-only read contract.

Scope:

- All v2-specific decisions below (§2 through §6, §8, §9).
- `type_layout_catalog` / updated index schema.
- Migration activation protocol.
- Write path changes.

## Decisions

### 1. Engine Versioning

Storage engines are versioned per backend:

- SQLite: `v1`, `v2`
- S3: `v1`, `v2`

Engine version is persisted in storage metadata and used by runtime/CLI to
instantiate the correct repository implementation.

Defaults:

- New `init` operations default to latest engine (`v2` once Phase 2 ships; `v1`
  during Phase 1).
- Existing storages continue to run with their current engine version.

### 2. Typed Read Contract

Typed APIs return only rows compatible with the current schema version for the
type being queried.

Applies to:

- `collect()`
- `with_history()`
- `history_since()`
- `as_of()`
- commit/changes read surfaces that materialize typed row payloads

#### 2.1 `collect()` Behavior Change

In v1, `collect()` (latest-state reads) does not filter by `schema_version_id`.
Rows written under any schema version are returned as long as they represent the
latest state for a given key.

In v2, `collect()` reads only from the current schema version partition. This is
an implicit consequence of the physical layout: the current-version table/path
contains only rows written under that version.

**Impact:** After a schema migration in v2, any entity key whose latest state
was written under a prior schema version will not appear in `collect()` results
unless the migration rewrite (Decision §6) copies it forward. The migration
protocol ensures this by rewriting all latest-state rows into the new partition.
Operators must run migration before querying under the new schema.

#### 2.2 Temporal Query Semantics

- Resolve current `(type_name, schema_version_id)` and activation boundary.
- Read only rows from the current schema version partition.
- `with_history()` and `history_since()` are bounded to the current version
  lifetime — they only include rows from the activation commit onward.

#### 2.3 `as_of()` Boundary Behavior

`as_of(commit_id)` where `commit_id` is earlier than the current version's
`activation_commit_id`:

- Returns an **empty result set**.
- Includes a **diagnostic warning** on the result metadata:
  `{"reason": "commit_before_activation", "activation_commit_id": N}`.
- Does **not** raise an exception — callers may legitimately probe historical
  points without knowing activation boundaries.

Callers can discover activation boundaries via `storage_info()` (see API and CLI
Changes section).

### 3. SQLite V2 Physical Layout

#### 3.1 Shared control tables

Keep shared control-plane tables:

- `commits`
- `schema_registry`
- `schema_versions`
- `locks`

Add:

- `storage_meta` for engine metadata (key-value)
- `type_layout_catalog` for type/version -> table mapping and activation commit

`storage_meta` table schema:

```sql
CREATE TABLE storage_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
```

Minimum keys:

- `engine_version` (`"v2"`)
- `backend` (`"sqlite"`)

`type_layout_catalog` table schema:

```sql
CREATE TABLE type_layout_catalog (
    type_kind             TEXT NOT NULL,  -- 'entity' or 'relation'
    type_name             TEXT NOT NULL,
    schema_version_id     INTEGER NOT NULL,
    table_name            TEXT NOT NULL,
    activation_commit_id  INTEGER NOT NULL,
    is_current            INTEGER NOT NULL DEFAULT 0,
    created_at            TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (type_kind, type_name, schema_version_id)
);
```

#### 3.2 Data tables

Per type + schema version:

- `entity_<TypeName>_v<schema_version_id>`
- `relation_<TypeName>_v<schema_version_id>`

Each table stores:

- identity columns (entity key or relation endpoints/instance key),
- `commit_id`,
- `schema_version_id` (redundant with table name; included for self-documenting
  queries and consistency with v1 row format),
- typed columns for scalar fields (see §5 for column mapping),
- JSON columns for complex fields (see §5 for classification).

### 4. S3 V2 Physical Layout

#### 4.1 Metadata

Add:

- `meta/engine.json` with:
  - `backend: "s3"`
  - `engine_version: "v2"`
  - `created_at: "<iso>"`

#### 4.2 Commit files

Per type + schema version object paths:

- `commits/{commit_id}-{attempt}/entities/{TypeName}/v{schema_version_id}.parquet`
- `commits/{commit_id}-{attempt}/relations/{TypeName}/v{schema_version_id}.parquet`

Note: v1 paths use `commits/{commit_id}-{attempt}/entities/{TypeName}.parquet`
(no version subdirectory). V1 and v2 commit file structures are intentionally
incompatible — they are separate engines and never mixed.

#### 4.3 Index Schema

Per-type index files (`meta/indices/entities/{TypeName}.json`,
`meta/indices/relations/{TypeName}.json`) are extended for v2:

```json
{
  "type_name": "Customer",
  "max_indexed_commit": 42,
  "current_schema_version_id": 3,
  "activation_commit_id": 30,
  "entries": [
    {
      "min_commit_id": 30,
      "max_commit_id": 42,
      "schema_version_id": 3,
      "path": "commits/35-a1b2/entities/Customer/v3.parquet"
    }
  ],
  "historical_versions": [
    {
      "schema_version_id": 2,
      "activation_commit_id": 10,
      "superseded_at_commit_id": 30
    }
  ]
}
```

Changes from v1 index entries:

- Each entry gains `schema_version_id` to identify which partition it belongs
  to.
- Top-level `current_schema_version_id` and `activation_commit_id` enable read
  planning to skip files from non-current versions without scanning entries.
- `historical_versions` array records prior version boundaries for audit and
  future cross-version access.

Read planning for typed APIs: filter entries to those where
`schema_version_id == current_schema_version_id`. Since the current-version
partition can only contain rows written at or after activation, all matching
entries inherently satisfy `min_commit_id >= activation_commit_id`.

#### 4.4 Type layout catalog (S3)

Add `meta/type_layout_catalog.json`:

```json
{
  "layouts": [
    {
      "type_kind": "entity",
      "type_name": "Customer",
      "schema_version_id": 3,
      "activation_commit_id": 30,
      "is_current": true
    }
  ]
}
```

This serves the same role as the SQLite `type_layout_catalog` table —
authoritative record of which version is current per type.

### 5. Field Storage Rules (Both Backends)

#### 5.1 Column Classification

Fields are classified by their `type_spec` kind (from `build_type_spec()`):

| Type Spec Kind                                         | Examples                                       | Storage               |
| ------------------------------------------------------ | ---------------------------------------------- | --------------------- |
| `primitive` (scalar)                                   | `str`, `int`, `float`, `bool`, `datetime`      | Typed column          |
| `primitive` (`NoneType`, `Any`)                        | `Any`, forward refs                            | JSON column           |
| `union` where all non-None members are the same scalar | `Optional[str]`, `Optional[int]`               | Nullable typed column |
| `union` (mixed types)                                  | `Union[str, int]`, `Optional[Union[str, int]]` | JSON column           |
| `list`                                                 | `list[str]`, `list[int]`                       | JSON column           |
| `dict`                                                 | `dict[str, int]`                               | JSON column           |
| `typed_dict`                                           | `Address`, `Metadata`                          | JSON column           |
| `ref`                                                  | recursive TypedDict                            | JSON column           |

Rule of thumb: a field gets a typed column only when its runtime value is always
a single scalar (or None). Everything else is stored as JSON.

#### 5.2 SQLite Column Type Mapping

| Python Type                 | SQLite Column Type    |
| --------------------------- | --------------------- |
| `str`                       | `TEXT`                |
| `int`                       | `INTEGER`             |
| `float`                     | `REAL`                |
| `bool`                      | `INTEGER`             |
| `datetime`                  | `TEXT`                |
| `date`                      | `TEXT`                |
| `bytes`                     | `BLOB`                |
| `Optional[T]` (T is scalar) | Same as T, nullable   |
| Everything else             | `TEXT` (JSON-encoded) |

#### 5.3 Parquet Column Type Mapping

| Python Type                 | Parquet Type            |
| --------------------------- | ----------------------- |
| `str`                       | `UTF8`                  |
| `int`                       | `INT64`                 |
| `float`                     | `DOUBLE`                |
| `bool`                      | `BOOLEAN`               |
| `datetime`                  | `TIMESTAMP(us, tz=UTC)` |
| `date`                      | `DATE`                  |
| `bytes`                     | `BYTE_ARRAY`            |
| `Optional[T]` (T is scalar) | Same as T, nullable     |
| Everything else             | `UTF8` (JSON-encoded)   |

This preserves flexibility for structured nested payloads while improving query
performance for scalar fields.

### 6. Migration and Activation

When schema changes for a type:

1. Create new physical partition/table for `schema_version_id = Vnext`.
2. Rewrite latest materialized state into `Vnext` under a migration commit.
3. Persist activation boundary (`activation_commit_id`) in layout metadata.
4. Mark `Vnext` as current in layout catalog/index metadata.

Historical partitions remain immutable and retained for audit/migration
purposes, but are excluded from typed read paths.

#### 6.1 Migration Commit Metadata

The migration commit carries metadata distinguishing it from normal commits:

```json
{
  "kind": "migration",
  "migrated_types": [
    {
      "type_kind": "entity",
      "type_name": "Customer",
      "from_schema_version_id": 2,
      "to_schema_version_id": 3,
      "rows_rewritten": 1500
    }
  ]
}
```

This allows auditing and tooling to identify migration commits.

#### 6.2 Zero-Row Types

If a type has zero rows at migration time, skip data rewrite. Only create the
new partition (empty table or no Parquet file) and update layout metadata. The
activation commit is still recorded.

#### 6.3 Multi-Type Migration

Multiple types may be migrated in a single commit. Each type creates its own
partition. `type_layout_catalog` supports multiple activations per commit — each
row/entry has its own `activation_commit_id` which will share the same value.

#### 6.4 SQLite Atomicity

All migration operations (data rewrite, layout catalog update, schema version
creation) occur within a single SQLite transaction (`BEGIN IMMEDIATE` ...
`COMMIT`). If any step fails, the entire transaction rolls back and no state
changes.

#### 6.5 S3 Atomicity

S3 lacks native transactions. Migration follows this protocol:

1. **Write phase (safe to retry):**

   - Write Parquet files for the new partition under
     `commits/{migration_commit_id}-{attempt}/...`.
   - Write manifest JSON referencing the new files.

2. **Head CAS (atomic commit point):**

   - Conditional PUT to `head.json` with `if_match` on current ETag.
   - If CAS fails (`PreconditionFailed`), the migration commit did not take
     effect. Retry from step 1 with a new attempt suffix.

3. **Post-commit metadata updates (best-effort, idempotent):**
   - Update `meta/type_layout_catalog.json` with new activation entries.
   - Update per-type index files with new entries and
     `current_schema_version_id`.
   - These updates are best-effort: if they fail, subsequent reads fall back to
     manifest-chain walking (existing S3 repair behavior). The next successful
     commit or explicit repair will reconcile them.

**Failure semantics:** If the process crashes between step 2 and step 3, head
has advanced but index/catalog metadata is stale. On next `open_repository()`,
the manifest chain remains authoritative for commit data. To resolve which
schema version is current, the reader consults the schema version registry
(`meta/schema/versions/{kind}/{type_name}.json`), which is updated as part of
the existing `_flush_staged_schema_changes()` flow during the commit
transaction. The latest version in that registry is current. A subsequent index
repair pass reconciles the per-type index files with the authoritative state.
This matches the existing v1 S3 commit model where index updates are
best-effort.

#### 6.6 Concurrent Reads During Migration

Migration holds the write lock. Readers are not blocked:

- **Before head CAS:** Readers see the pre-migration state. The old version
  partition is still current.
- **After head CAS:** Readers see the new head and resolve the new version as
  current. Even if index metadata is not yet updated, manifest-chain walking
  provides correct results.

There is no window where readers see partial migration state.

### 7. Repository Dispatch

`open_repository()` must:

1. Resolve backend from storage URI.
2. Read engine metadata from storage.
3. Dispatch to backend+engine implementation:
   - `SqliteRepositoryV1` / `SqliteRepositoryV2`
   - `S3RepositoryV1` / `S3RepositoryV2`

Unknown or incompatible engine versions must fail with explicit operator-facing
errors.

#### 7.1 V1 Detection Fallback

Existing storages created before engine versioning have no `storage_meta` table
(SQLite) or `meta/engine.json` (S3). Detection logic:

- **SQLite:** If the `storage_meta` table does not exist, treat the storage as
  `v1`. (Check via `sqlite_master` lookup.)
- **S3:** If `meta/engine.json` does not exist (404 on GET), treat the storage
  as `v1`.

This ensures backward compatibility: existing v1 storages continue to work
without any manual intervention after upgrading the runtime.

### 8. Write Path

V2 changes the write path for `insert_entity()` and `insert_relation()`.

#### 8.1 Target Resolution

On each insert, the repository resolves the target table/path:

1. Look up `(type_kind, type_name)` in `type_layout_catalog` (SQLite) or the
   in-memory layout catalog (S3) where `is_current = true`.
2. Use the `table_name` (SQLite) or construct the versioned object path (S3)
   from the resolved `schema_version_id`.

If no current layout entry exists for the type (first write after type
registration), create the initial partition and catalog entry within the same
commit transaction, with `activation_commit_id` set to the current commit.

#### 8.2 Schema Version Enforcement

In v2, `schema_version_id` is **required** on all inserts (not `None`). The
repository validates that the provided `schema_version_id` matches the current
version for the type in `type_layout_catalog`. Mismatches raise an error — this
prevents writes to stale partitions.

#### 8.3 Field Serialization

On write, fields are split according to the column classification (§5.1):

- Scalar fields are written to their typed columns directly.
- Complex fields are JSON-encoded and written to their JSON columns.

The `fields_json` column is **not** written in v2 data tables. Individual
columns replace it entirely.

### 9. Dropped Types

When a type is dropped in v2:

- All version partitions for the type (current and historical) are retained on
  disk/S3. Append-only guarantees are preserved.
- The `type_layout_catalog` entry is updated: `is_current` is set to false for
  all versions of the dropped type.
- No new writes are accepted for the dropped type.
- Typed reads for a dropped type return empty (no current partition to read
  from).
- The `schema_registry` and `dropped.json` (S3) tracking remain unchanged from
  v1 behavior.

### 10. EventStore / Session Interaction

The RFC 0005 `session.Session` and its `EventStore` interact with the repository
for commits. V2 engine changes are transparent to this layer:

- `session.Session` calls `repo.insert_entity()` / `repo.insert_relation()` and
  `repo.commit_transaction()` — these are the same protocol methods whose
  internal implementation changes in v2.
- The EventStore enqueues events using the repo's transaction (SQLite) or
  post-commit (S3). This sequencing is unaffected by v2 layout changes.
- Event handlers that read data via `Ontology.query()` will observe v2 read
  semantics (current-schema-only) when running against a v2 storage.

No changes to the `session.Session` or `EventStore` interfaces are required.

## API and CLI Changes

### Public/Operator-facing

- `storage_info()` includes:
  - `backend`
  - `engine_version`
  - `type_layouts` (v2 only): per-type layout summary

Example `storage_info()` return for v2:

```python
{
    "backend": "sqlite",
    "engine_version": "v2",
    "db_path": "/data/onto.db",
    "type_layouts": {
        "Customer": {
            "type_kind": "entity",
            "current_schema_version_id": 3,
            "activation_commit_id": 30,
            "historical_versions": [1, 2]
        },
        "Order": {
            "type_kind": "entity",
            "current_schema_version_id": 1,
            "activation_commit_id": 1,
            "historical_versions": []
        }
    }
}
```

- `onto init` supports optional `--engine-version`.
- Default `onto init` engine version is latest supported for target backend.

### Behavior changes

- Temporal typed query methods are current-schema-only by contract.
- `collect()` is current-schema-only by physical layout (v2).
- Commit-history typed materialization surfaces are current-schema-only by
  contract.

## Compatibility and Rollout

- v1 engines remain supported during migration window.
- v1 and v2 are not implicitly mixed inside a single storage target.
- Cross-engine migration tooling is a follow-up scope.
- Engine deprecation/removal timing is a separate policy decision.
- Phase 1 (engine versioning infrastructure) can ship independently of Phase 2
  (v2 layout), reducing risk.

## Test Plan

### Engine metadata and dispatch

- Initialize SQLite/S3 with explicit and default engine versions.
- Open existing v1/v2 storages and verify correct repository class selection.
- Verify explicit error on unknown engine metadata.
- Verify v1 fallback: open a storage with no `storage_meta` / `engine.json` and
  confirm it is treated as v1.

### Typed read contract

- After schema migration, `as_of(commit_before_activation)` returns empty with
  diagnostic warning.
- `with_history()` includes only current-version rows.
- `history_since(old_commit)` starts from activation boundary for typed APIs.
- `collect()` returns only rows from the current version partition.
- `collect()` returns complete latest state after migration rewrite (no keys
  lost).

### Write path

- `insert_entity()` writes to the correct versioned table/path.
- `insert_entity()` with mismatched `schema_version_id` raises error.
- `insert_entity()` with `schema_version_id = None` raises error in v2.
- Fields are correctly split into typed columns and JSON columns per §5.1.

### Migration correctness

- New version partition/table created per migrated type.
- Latest-state rewrite is complete and typed validation passes.
- Activation metadata is atomically updated with migration commit.
- Zero-row type migration creates empty partition and updates catalog.
- Multi-type migration in a single commit activates all types correctly.

### S3 migration atomicity

- Migration commit survives process crash after head CAS but before index
  update; subsequent reads are correct via manifest-chain fallback.
- Index repair reconciles stale metadata after partial migration.

### Backend parity

- Equivalent semantic results for SQLite v2 and S3 v2 on:
  - latest reads
  - temporal reads
  - commit change reads

### Field encoding

- Scalar fields (`str`, `int`, `float`, `bool`, `datetime`, `date`, `bytes`) are
  stored as typed columns in both backends.
- `Optional[scalar]` fields are stored as nullable typed columns.
- `list`, `dict`, `TypedDict`, `Union` (mixed), and `Any` fields are stored as
  JSON in both backends.

### Dropped types

- Dropping a type in v2 marks all layout entries as not current.
- Reads for dropped types return empty.
- Historical partitions remain on disk.

### Concurrent access

- Readers during an in-progress migration see consistent pre-migration or
  post-migration state, never partial.

## Risks and Mitigations

- Risk: activation-boundary bugs can hide expected rows.
  - Mitigation: strict boundary tests and migration integration tests.
  - Mitigation: diagnostic warnings on `as_of()` below activation boundary.
- Risk: operator confusion across v1/v2 behavior.
  - Mitigation: clear `storage_info` with per-type layout details, `init` flag,
    and docs messaging.
- Risk: delayed migration tooling slows adoption.
  - Mitigation: maintain v1 compatibility until migration tools are available.
- Risk: `collect()` behavior change (current-schema-only) breaks existing
  workflows that depend on pre-migration data appearing.
  - Mitigation: migration protocol rewrites all latest state forward; operators
    must migrate before querying under new schema (same as v1).
- Risk: table/object proliferation from many type × version combinations.
  - Mitigation: historical partitions are inert (no reads in typed path); future
    compaction/cleanup tooling can be added without schema changes.
- Risk: S3 index metadata becomes stale after migration crash.
  - Mitigation: manifest-chain walking provides correct fallback; existing index
    repair logic reconciles on next operation.

## Assumptions

- Typed API safety is prioritized over cross-version typed history exposure.
- Full cross-version history access, if required, should be explicit raw/untyped
  APIs in a separate RFC.
- Existing append-only retention and audit guarantees remain unchanged.
- Phase 1 can ship before Phase 2 is ready, providing engine versioning
  infrastructure without requiring v2 layout changes.
