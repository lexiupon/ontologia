# RFC 0004: S3 Storage Backend with Parquet Commit Log and Pushdown Queries

## Status

Implemented (2026-02-11)

## Created

2026-02-10

## Summary

Introduce an S3-backed storage backend as a first-class alternative to SQLite,
using append-only Parquet commit files and DuckDB query execution with pushdown
filtering/projection.

Core decisions:

- Backend model is pluggable (`SQLite` and `S3`) behind one repository
  interface.
- Commit layout is per-type-per-commit Parquet files.
- Write concurrency uses S3 conditional-write lock plus conditional head update
  (CAS).
- Query pushdown engine is DuckDB scanning Parquet directly from S3.
- SQLite behavior remains supported and unchanged by default.

## Motivation

Current implementation is SQLite-centric and works well for local and
single-node deployments. We also need a cloud-native storage option that:

- keeps append-only commit/audit semantics,
- supports multiple runtimes over shared object storage,
- scales reads through columnar files,
- supports predicate/projection pushdown for efficient querying,
- preserves existing typed APIs and commit/query semantics.

S3 + Parquet meets these goals for Ontologia's target workload profile:
low-to-moderate write throughput, correctness-first reconciliation, and
history-preserving analytics.

## Non-Goals

- Replacing SQLite backend.
- Changing Entity/Relation type system semantics.
- Changing commit-chain loop-prevention semantics.
- Building a distributed transaction coordinator across multiple buckets.
- Introducing row-level delete/retract operations.
- Lossless commit-graph migration between backends (cross-backend data migration
  uses export/import which collapses history into a single commit; see §8).

## Design Goals

1. Preserve current correctness guarantees: append-only history, atomic commit
   visibility, monotonic commit ordering, bounded retries.
2. Keep public typed/query/runtime APIs stable where possible.
3. Make backend selection explicit and pluggable.
4. Ensure deterministic query semantics (`latest`, `as_of`, `with_history`,
   `history_since`) across SQLite and S3.
5. Enable practical pushdown filtering/projection on S3 Parquet data.
6. Keep failure recovery simple: incomplete writes must never become visible as
   committed head state.

## Decisions

### 1. Backend architecture: pluggable repository implementations

A storage abstraction layer will support multiple backend implementations while
preserving runtime behavior.

- `SqliteRepository`: existing behavior.
- `S3Repository`: new behavior defined by this RFC.

`Ontology` runtime logic (session, intent reconciliation, handler dispatch,
migration orchestration) stays backend-agnostic.

### 2. Commit file layout: per-type-per-commit

Each non-empty commit writes one Parquet file per touched type and kind.

Rationale:

- avoids one tiny file per row,
- preserves commit-level audit grouping,
- enables type-pruned reads,
- keeps implementation simpler than rolling append-bucket strategies.

### 3. Read Scalability: Indexing and Compaction

To ensure read performance scales with history size (avoiding O(N) manifest
scans and O(M) data scans), the design includes:

- **Per-Type Indices**: `meta/indices/{kind}/{TypeName}.json` (e.g.,
  `meta/indices/entities/Customer.json`) tracks which commits modified a type
  and their file paths. Readers use this to skip irrelevant manifests. Index
  size grows linearly with commits touching that type; compaction collapses
  ranges into single snapshot entries, bounding index size in practice.
- **Compaction**: A maintenance operation (`onto compact`) that merges history
  into snapshot Parquet files, reducing the number of files readers must scan
  and the number of entries in index objects.

### 4. Query engine: DuckDB over S3 Parquet

DuckDB is the required execution engine for S3 backend query scans.

Rationale:

- mature Parquet scanning with predicate/projection pushdown,
- SQL planner already maps naturally from current query DSL translation,
- low custom engine surface compared with bespoke Arrow scanning logic.

### 5. Write safety model: lock + head CAS

Write serialization requires both:

- lock object acquisition via conditional write,
- conditional head-pointer update against observed version/etag.

Lock-only is insufficient against stale/buggy clients; CAS-only increases wasted
work and contention. Combined model gives explicit ownership and authoritative
linearization.

### 6. Adoption model: additive backend

SQLite remains a supported backend. S3 is introduced as an additional backend
selected explicitly by storage URI. Existing SQLite users can upgrade without
behavior changes.

## Proposal

### 1. Runtime and public API surface

#### 1.1 Storage binding

`Ontology` accepts backend-neutral storage binding:

```python
class Ontology:
    def __init__(
        self,
        db_path: str | None = None,
        config: OntologiaConfig | None = None,
        *,
        storage_uri: str | None = None,
        entity_types: list[type[Entity]] | None = None,
        relation_types: list[type[Relation]] | None = None,
    ) -> None: ...
```

Rules:

- `db_path` remains the first positional parameter for backward compatibility.
  Existing `Ontology("onto.db")` and `Ontology(db_path="onto.db")` calls
  continue to work without changes.
- `storage_uri` is keyword-only and is the primary backend selector for new
  code.
- If both are provided, they must resolve to the same SQLite target or raise
  configuration error.
- If neither is provided, default remains local SQLite path (`onto.db`) unless
  explicitly changed in a later RFC.

Supported URI schemes in this RFC:

- `sqlite:///path/to/onto.db`
- `s3://bucket/prefix`

#### 1.2 Config additions

`OntologiaConfig` gains optional backend settings:

- `s3_region: str | None`
- `s3_endpoint_url: str | None`
- `s3_lock_timeout_ms: int` (default aligns with current lock timeout semantics)
- `s3_lease_ttl_ms: int`
- `s3_request_timeout_s: float`
- `s3_duckdb_memory_limit: str` (default `"256MB"`)

Credential resolution uses standard AWS SDK chain (env, shared config,
instance/task role, etc). No static credential fields are added to
`OntologiaConfig`.

### 2. S3 object model and commit layout

Assume `storage_uri = s3://my-bucket/onto-prod`.

#### 2.1 Control-plane objects

- `s3://my-bucket/onto-prod/meta/head.json`
- `s3://my-bucket/onto-prod/meta/locks/ontology_write.json`
- `s3://my-bucket/onto-prod/meta/schema/...` (schema registry + version history)
- `s3://my-bucket/onto-prod/meta/schema/types.json` (authoritative known-type
  catalog for index maintenance/verification)

#### 2.2 Commit-plane objects

For commit `123` written by attempt `a1b2c3`:

- `s3://my-bucket/onto-prod/commits/123-a1b2c3/manifest.json`
- `s3://my-bucket/onto-prod/commits/123-a1b2c3/entities/Customer.parquet`
- `s3://my-bucket/onto-prod/commits/123-a1b2c3/relations/Subscription.parquet`

Each commit attempt uses the path `commits/{commit_id}-{attempt_uuid}/...` where
`attempt_uuid` is a short random identifier generated per write attempt. This
ensures that two writers targeting the same `commit_id` (e.g., after a stale
lease takeover) write to non-overlapping keys and cannot corrupt each other's
data. The winning writer's path is recorded in `head.json` via `manifest_path`;
the loser's orphan directory is ignored by readers and cleaned by the janitor.

#### 2.3 Index objects

- `s3://my-bucket/onto-prod/meta/indices/entities/Customer.json`
- `s3://my-bucket/onto-prod/meta/indices/relations/Subscription.json`

Indices map `TypeName` to a list of commit entries:

```json
{
  "type_name": "Customer",
  "max_indexed_commit": 123,
  "entries": [
    {
      "min_commit_id": 1,
      "max_commit_id": 100,
      "path": "snapshots/entities/Customer-1-100.parquet"
    },
    {
      "min_commit_id": 105,
      "max_commit_id": 105,
      "path": "commits/105-cd34/entities/Customer.parquet"
    },
    {
      "min_commit_id": 123,
      "max_commit_id": 123,
      "path": "commits/123-ef56/entities/Customer.parquet"
    }
  ]
}
```

Each entry records a commit range (`min_commit_id`, `max_commit_id`) and the S3
object `path` for the data file. For per-commit files, `min_commit_id` equals
`max_commit_id`. For compacted snapshots, the range spans all merged commits.
Entries must not overlap — compaction replaces the individual entries it merges
with a single snapshot entry. `max_indexed_commit` tracks overall coverage for
gap detection (§4.1).

**Coverage semantics:** `max_indexed_commit` represents the head commit ID at
the time the index was last written, not the highest commit that modified this
type. For example, if head is at commit 100 and `Customer` was last modified at
commit 5, the index has `max_indexed_commit: 100` with a single entry for
commit 5. This tells readers: "all commits up to 100 have been considered;
`Customer` was only modified in the listed entries." Readers only need to walk
the manifest chain for commits between `max_indexed_commit` and the current
head, avoiding unnecessary walks for sparse types.

**Writer update algorithm (normative):**

- On each successful commit, step 8 MUST attempt to update per-type index
  objects for all currently known types in the schema registry (`entity` and
  `relation` kinds), not only touched types.
- For every type whose index object update succeeds, writer MUST set
  `max_indexed_commit = next_commit_id`.
- For touched types, successful updates append/replace entries for the new
  attempt-path file(s) as needed.
- For untouched types, successful updates leave `entries` unchanged and update
  only the coverage watermark.
- Exception: if `meta/schema/types.json` is unreadable at step 8, writer MUST
  skip index mutation for that commit and emit an operator-visible warning
  (detailed in §3.3 and §5).

This makes `max_indexed_commit` semantics enforceable without forcing readers to
walk manifests for sparse types on every query.

Temporal pruning uses the range fields: an `as_of(c)` query selects entries
where `min_commit_id <= c`; a `history_since(c)` query selects entries where
`max_commit_id > c`. DuckDB applies further `commit_id` filtering within each
selected file.

#### 2.4 Manifest schema

`manifest.json` contains at minimum:

- `commit_id: int`
- `parent_commit_id: int | null`
- `parent_manifest_path: str | null` (S3 key of the parent commit's manifest;
  `null` for the first commit)
- `created_at: str` (UTC ISO-8601)
- `runtime_id: str`
- `metadata: dict[str, str]`
- `files: list[CommitFile]`

`parent_manifest_path` creates a backward-walkable chain from any committed
manifest to all its ancestors. This is required for gap-reconciliation: when
indices are stale and multiple attempt directories may exist for the same
`commit_id`, the chain unambiguously identifies which attempt was committed (see
§3.5).

`CommitFile` fields:

- `kind: "entity" | "relation"`
- `type_name: str`
- `path: str` (object key)
- `row_count: int`
- `schema_version_id: int`
- `content_sha256: str`

#### 2.5 Parquet row schema

Entity Parquet rows include:

- `commit_id: int64`
- `entity_type: string`
- `entity_key: string`
- `schema_version_id: int64`
- one typed column per entity field in the writing schema version
- `fields_json: string` (canonical payload for lossless compatibility)

Relation Parquet rows include:

- `commit_id: int64`
- `relation_type: string`
- `left_key: string`
- `right_key: string`
- `instance_key: string` (empty-string sentinel for unkeyed relations)
- `schema_version_id: int64`
- one typed column per relation payload field
- `fields_json: string` (canonical payload for lossless compatibility)

Notes:

- Typed field columns are for pushdown performance.
- `fields_json` remains canonical for compatibility and migration safety.
- Readers use DuckDB `read_parquet(..., union_by_name = true)` across files with
  schema drift.

### 3. Write protocol: lock, commit files, head CAS

#### 3.1 Acquire lock

Lock object key: `meta/locks/ontology_write.json`.

Acquire algorithm:

1. Try create lock object with conditional create (`if-none-match` style
   precondition).
2. If lock exists, read lock payload.
3. If unexpired and owned by another runtime, wait/retry with backoff until
   timeout.
4. If expired, attempt takeover with conditional replace against observed lock
   version/etag.

Lock payload contains:

- `owner_id`
- `acquired_at`
- `expires_at`
- `lease_ttl_ms`

**Clock assumptions:** Lease expiry evaluation assumes clocks across writer
processes are synchronized within `lease_ttl / 3` (e.g., via NTP). Greater clock
skew may cause premature takeovers (safe — CAS still prevents corruption) or
delayed takeovers (reduces availability but not correctness). GC pauses
exceeding the safety margin have the same effect as clock skew; the lease-expiry
guard (§3.3 step 6) and head CAS together ensure no committed data is corrupted.

#### 3.2 Keep-alive

Active writer renews lock lease periodically (interval = `lease_ttl / 3`), using
conditional update that verifies current owner.

On successful renewal, writer MUST update the local `lease_expires_at` used by
the lease-expiry guard (step 6). If renewal fails (ownership lost or renewal
request failure), writer MUST treat the lease as unsafe and abort before CAS.

#### 3.3 Commit finalization with head CAS

With lock held:

1. Read `meta/head.json`, capturing `commit_id` (denote it locally as
   `head_commit_id`), `manifest_path` (may be `null` if `commit_id == 0`), and
   object version/etag.
2. Compute `next_commit_id = head_commit_id + 1`.
3. Generate a random `attempt_uuid` for this write attempt.
4. Write all Parquet commit files under
   `commits/{next_commit_id}-{attempt_uuid}/...`.
5. Write `commits/{next_commit_id}-{attempt_uuid}/manifest.json` with
   `parent_commit_id` set to the observed `head_commit_id` (or `null` if this is
   the first commit), and `parent_manifest_path` set to the `manifest_path`
   observed in step 1 (or `null` if this is the first commit).
6. **Lease-expiry guard:** compare `now()` against the locally held
   `lease_expires_at`. If the lease has expired (or remaining time is below a
   safety margin, e.g. `lease_ttl / 3`), abort the commit immediately — attempt
   owner-conditional lock release and raise `LeaseExpiredError`. If ownership
   has already changed, release is a no-op. Best-effort deletion of the orphan
   files written in steps 4–5 SHOULD be attempted, but failure to delete is
   tolerated; orphans are harmless and will be cleaned by the janitor (§3.5).
   This avoids a wasteful CAS attempt when another writer has likely already
   taken over.
7. Conditionally update `meta/head.json` to point to `next_commit_id` with
   `manifest_path` referencing the attempt-specific directory, using
   compare-and-swap against previously observed head version/etag.
8. Best-effort update per-type index objects (`meta/indices/...`) using the
   normative algorithm in §2.3: attempt updates for all known types; for each
   successful type update, set `max_indexed_commit = next_commit_id`; and for
   touched types, append/replace attempt-specific file paths (see §3.5 for
   crash-safety rationale).
9. Release lock.

**Post-CAS commit success rule:** Once head CAS (step 7) succeeds, the API MUST
report the commit as successful to the caller, regardless of whether step 8
(index update) or step 9 (lock release) fail. Index update failure is a
degraded-but-correct state: readers fall back to manifest-chain walk (§3.5), and
the next successful commit self-heals the index. Lock release failure is
similarly non-fatal — the lock will expire and be taken over by the next writer.
Implementations MUST NOT raise an error or trigger client retry logic after a
successful CAS.

Special case: if `meta/schema/types.json` is missing or malformed during step 8,
the writer MUST skip index mutation for that attempt, emit an operator-visible
warning/diagnostic, and still return commit success (CAS already linearized the
commit).

Visibility rule:

- Commit is authoritative only after successful head CAS (step 7).
- Parquet/manifest objects written before CAS are invisible to committed state
  if CAS fails.
- Index objects are updated after head CAS and are treated as advisory
  accelerators, not as the source of truth for commit membership.

#### 3.4 CAS failure behavior

If head CAS fails (stale head race):

- release lock,
- recompute from latest head,
- retry with bounded jitter/backoff,
- fail with `HeadMismatchError` when retry budget is exhausted.

#### 3.5 Crash safety and index consistency

The commit protocol is designed so that `meta/head.json` is the sole
authoritative record of the latest committed state. Indices are advisory
accelerators that may lag behind head.

**Crash scenarios:**

- Crash before head CAS (step 7): orphan Parquet/manifest objects may remain
  under `commits/{next_commit_id}-{attempt_uuid}/`, but no committed head change
  occurs. These orphans are invisible to readers (no head references them) and
  can be cleaned by a future garbage collector.
- Lease-expiry guard (step 6) triggers: same as crash before head CAS — orphan
  files remain, no head change, writer raises `LeaseExpiredError` and retries
  from lock acquisition.
- Crash after head CAS but before index update (between steps 7 and 8): the
  commit is visible and complete. Indices are stale — they do not yet reference
  the new commit. Readers tolerate this via the index-gap reconciliation
  protocol (§4.1).
- Crash after head CAS and after index update: fully consistent, no recovery
  needed.

**Write-write collision safety:** Because each attempt writes to a unique
`commits/{commit_id}-{attempt_uuid}/` prefix, two writers targeting the same
`commit_id` (e.g., after stale lease takeover) never overwrite each other's data
or manifest files. Only one writer's head CAS can succeed; the other's orphan
directory is harmless.

**Index-gap reconciliation (reader side):**

When a reader resolves files for a query (§4.1), it compares the commit range
covered by the index against the current head. If the index does not cover all
commits up to the queried head, the reader resolves the gap by walking the
manifest chain backward from head:

1. Start from `head.json → manifest_path` (the head commit's manifest).
2. Follow `parent_manifest_path` links backward until reaching a commit covered
   by the index (or the beginning of history).
3. Each manifest in the chain unambiguously identifies the committed attempt
   directory and its files for that `commit_id`, even when orphan attempts for
   the same `commit_id` exist.

This ensures correctness regardless of index freshness and resolves the
ambiguity introduced by attempt UUIDs.

**Index repair (writer side):**

On the next successful commit, the writer reads current indices before step 8
and detects any gaps between each index object's `max_indexed_commit` and the
previous head.

Repair algorithm (normative):

1. For each type in `meta/schema/types.json`, if
   `max_indexed_commit < previous_head`, mark it lagged. Missing index objects
   are treated as `max_indexed_commit = 0` with `entries = []`.
2. Reconstruct gap commit membership by walking the authoritative manifest chain
   from `previous_head` backward to `max_indexed_commit + 1`.
3. For each lagged type, derive missing entries from manifest `files` metadata
   in that gap window:
   - append per-commit entries for missing touched commits, unless those commits
     are already covered by an existing snapshot-range entry;
   - for untouched commits in the gap, update coverage watermark only.
4. Write repaired index objects and set `max_indexed_commit = next_commit_id`
   for successful updates.

This makes index staleness self-healing under normal write traffic.

**Index repair (`onto index repair --apply`) semantics:**

- The command does not create a new commit and does not mutate `meta/head.json`.
- Under the write lock, set `repair_head = head.commit_id`, build the repair
  plan from authoritative manifests, then re-check head immediately before
  persisting repaired indices.
- If head changed before index publish, abort/replan (same safety rule as
  compaction).
- For each successfully repaired type, set `max_indexed_commit = repair_head`.

**Index lag detection and warning (operator-facing):**

- A backend is considered index-lagged when any per-type index has
  `max_indexed_commit < head.commit_id`.
- A backend is considered latest-entry-missing when the head manifest touches
  type `T`, but `T`'s index has no entry whose commit range covers
  `head.commit_id` (per-commit entry or snapshot-range entry).
- For non-compacted per-commit coverage of the head commit, implementations MUST
  verify that the index entry path matches the committed attempt path from the
  head manifest. On mismatch, treat that type as latest-entry-missing.
- Runtime/CLI operations that read index metadata (including `onto info` and
  query planning paths) SHOULD emit a warning when either condition is detected.
- Warnings are operational only: reads remain correct via manifest-chain
  fallback.

A background janitor (future follow-up) may clean orphaned unreferenced commit
objects and repair stale indices independently of write traffic.

### 4. Query execution model (DuckDB pushdown)

#### 4.1 File pruning pipeline

For each query:

1. Read `meta/head.json` to establish the authoritative head commit ID.
   - If `head_commit_id == 0`, return empty result immediately for all query
     modes (no index or manifest walk).
2. Resolve temporal window and effective query head (`q_head`):
   - latest: `q_head = head`, window `[1 .. q_head]`
   - `as_of(c)`: `q_head = min(max(c, 0), head)`; if `q_head == 0`, return empty
     result; else window `[1 .. q_head]`
   - `history_since(c)`: `q_head = head`, window `(max(c, 0) .. q_head]`
   - `with_history`: `q_head = head`, window `[1 .. q_head]`
3. Resolve touched commits using **Index Objects** (`meta/indices/...`) to avoid
   scanning all manifests.
   - If the index covers all commits in the temporal window, use it directly.
   - If the index does not cover commits up to `q_head` (gap due to crash or
     incomplete index update), walk the manifest chain backward from head via
     `parent_manifest_path` to resolve the authoritative files for each gap
     commit (see §3.5). Merge with indexed entries, then prune to the temporal
     window.
   - If per-commit head coverage exists but the index path for a touched type
     does not match the authoritative head-manifest path, treat that type as
     uncovered at head and resolve it via manifest-chain fallback (same as gap
     handling in §3.5).
   - If the index is missing entirely, walk the full manifest chain from head.
4. Build DuckDB Parquet scan over selected files only.

#### 4.2 Pushdown contract

- Predicates on identity columns and typed field columns must be translated into
  DuckDB SQL where clauses.
- Projection must select only required columns for hydration/aggregation.
- Endpoint filters for relation queries (`left(...)`, `right(...)`) are handled
  via joins to entity scans with same temporal semantics.

#### 4.3 Semantic equivalence rules

Behavior must match existing query semantics:

- latest-state queries return one row per identity using max `commit_id`.
- `as_of(commit_id)` uses max `commit_id <= as_of` per identity.
- `with_history()` returns all rows ordered by `commit_id ASC` plus identity
  tie-break keys.
- `history_since(commit_id)` returns rows with `commit_id > since` using the
  same deterministic ordering.

Identity partitions:

- Entity: `(entity_type, entity_key)`
- Relation unkeyed: `(relation_type, left_key, right_key, instance_key="")`
- Relation keyed: `(relation_type, left_key, right_key, instance_key)`

Deterministic ordering contract for history-producing modes
(`with_history()`/`history_since()`):

- Primary sort key: `commit_id ASC`
- Tie-break key (entity queries): `entity_key ASC`
- Tie-break key (relation queries):
  `left_key ASC, right_key ASC, instance_key ASC`

#### 4.4 DuckDB query translation

The existing query DSL compiles filter expressions to SQL WHERE clauses using
`json_extract()` over `fields_json`. DuckDB supports the same `json_extract()`
function, so filter compilation is largely portable. Key translation rules:

**Deduplication (latest-state):** SQLite uses an inner join against a
`MAX(commit_id)` subquery grouped by identity columns. DuckDB uses window
functions:

```sql
-- Entity latest-state
SELECT entity_key, fields_json, commit_id
FROM read_parquet([file1, file2, ...], union_by_name = true)
WHERE entity_type = ?
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY entity_key ORDER BY commit_id DESC
) = 1

-- Relation latest-state
SELECT left_key, right_key, instance_key, fields_json, commit_id
FROM read_parquet([file1, file2, ...], union_by_name = true)
WHERE relation_type = ?
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY left_key, right_key, instance_key ORDER BY commit_id DESC
) = 1
```

**Temporal modes:**

- `as_of(c)`: add `AND commit_id <= ?` before the `QUALIFY` window.
- `with_history()`: omit `QUALIFY`; return rows ordered by `commit_id ASC` +
  identity tie-break keys.
- `history_since(c)`: omit `QUALIFY`; add `AND commit_id > ?`; apply the same
  ordering as `with_history()`.

**Pushdown on typed columns:** When a filter references a field that has a typed
Parquet column (e.g., `tier` as `string`), the DuckDB scan pushes the predicate
directly to the Parquet column, avoiding `json_extract()` overhead. Filters on
fields only present in `fields_json` fall back to `json_extract()`.

**Type casting:** SQLite uses `CAST(... AS REAL)` for numeric aggregations.
DuckDB uses `TRY_CAST(... AS DOUBLE)` for safe casting with NULL on failure.

**Aggregations and GROUP BY:** `COUNT`, `SUM`, `AVG`, `MIN`, `MAX` translate
directly. `GROUP BY` uses the same `json_extract()` or typed-column reference.
`HAVING` clauses translate without modification.

**Endpoint filters (relations):** Filters using `left(R).field` or
`right(R).field` compile to `EXISTS` subqueries that scan the corresponding
entity Parquet files with the same temporal window. Deduplication in endpoint
subqueries matches the outer query mode (latest/as_of use dedup;
with_history/history_since omit dedup):

```sql
WHERE relation_type = ?
  AND EXISTS (
    SELECT 1 FROM read_parquet([entity_files...], union_by_name = true) le
    WHERE le.entity_type = ?
      AND le.entity_key = rh.left_key
      AND json_extract(le.fields_json, '$.department') = ?
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY le.entity_key ORDER BY le.commit_id DESC
    ) = 1
  )
```

The SQL above shows latest/as_of-style endpoint deduplication. For
`with_history()` / `history_since()`, omit endpoint `QUALIFY` and apply the same
temporal predicate rules described above.

**Traversals (`.via()`):** Multi-hop traversals execute as sequential
scan-and-filter steps. The first hop resolves source entity keys, then each
subsequent hop scans relation Parquet files filtered by the resolved keys. This
matches the current SQLite implementation's iterative approach and avoids
complex cross-file joins.

**ORDER BY, LIMIT, OFFSET:** Translate directly to DuckDB SQL clauses.

#### 4.5 DuckDB connection management

- `S3Repository` manages a DuckDB in-memory connection internally. The
  connection is created lazily on first query and reused for the lifetime of the
  repository instance.
- `onto.query()` (out-of-session queries) and session-scoped queries both use
  the repository's connection. Out-of-session query behavior is preserved for
  API continuity with the SQLite backend.
- S3 credentials are forwarded to DuckDB via the `httpfs` extension using the
  same AWS SDK credential chain configured for the repository.
- Memory budget per connection is bounded by `s3_duckdb_memory_limit` config
  (default: 256 MB).

### 5. Schema registry and migration behavior on S3

Schema metadata persists in S3 under `meta/schema/...` using append-only version
records equivalent to current schema tables.

**Known-type catalog (normative):**

- `meta/schema/types.json` is the authoritative enumerable catalog of known
  types:
  - `entities: list[str]`
  - `relations: list[str]`
  - `updated_at: str` (UTC ISO-8601)
- Writers (step 8), `onto index verify`, and `onto index repair` MUST enumerate
  types from this catalog when it is readable.
- Any operation that adds or removes types (schema migration, explicit schema
  update, or commit-time first-write type registration) MUST update `types.json`
  in the same locked critical section as schema-registry changes.
- If `types.json` is missing or malformed during writer step 8, writer MUST skip
  index mutation for that commit and emit an operator-visible warning/diagnostic
  (commit remains successful after CAS per §3.3).
- If `types.json` is missing or malformed, `onto index verify` and
  `onto index repair` MUST fail fast with a schema-metadata error (no partial
  index mutation).

Migration behavior remains consistent:

- preview/apply token ties plan hash + head commit,
- apply recomputes plan under lock,
- token mismatch aborts unless `force=True`,
- write-path drift checks still occur under lock before persistence.

### 6. Compaction

Compaction is a maintenance operation invoked via `onto compact`.

- **Scope**: Targeted by type or global.
- **Action**: Merges per-commit Parquet files for a type into fewer, larger
  "snapshot" Parquet files covering a commit range.
- **Locking**: Compaction acquires the write lock to prevent concurrent writes
  during snapshot creation and index update, and MUST follow the same lock
  lifecycle as write commits (§3.1–§3.2): acquire, keepalive renewal, and
  owner-conditional release.
- **Head-stability check**: Compaction records `start_head_commit_id` after lock
  acquisition and MUST re-read head immediately before publishing index
  replacements. If head changed, compaction MUST abort/replan (no index write).
- **Lease-expiry guard**: Before publishing index replacements, compaction MUST
  run the same lease-expiry guard semantics as §3.3 step 6. If lease is unsafe,
  compaction aborts with `LeaseExpiredError`; snapshot files written earlier are
  treated as orphan candidates and cleaned by janitor tooling.

**Temporal invariants (must hold after compaction):**

- Snapshot Parquet files preserve the `commit_id` column for every row. No
  commit attribution is lost.
- `with_history()` returns the same rows in the same deterministic order before
  and after compaction (`commit_id ASC`, then identity tie-break keys in §4.3).
- `as_of(c)` and `history_since(c)` produce identical results because
  `commit_id` filtering still works within snapshot files.
- `latest` queries produce identical results because deduplication by
  `MAX(commit_id)` per identity partition is unchanged.

**Result:**

- Writes snapshot files to
  `snapshots/{kind}/{TypeName}-{min_cid}-{max_cid}.parquet`.
- Updates `meta/indices/...` to replace individual commit entries with a single
  snapshot entry covering the merged range.
- Original per-commit files are retained until explicitly purged (separate
  follow-up). This allows rollback and audit trail preservation.
- Manifests are never modified; they remain the immutable audit record of what
  each commit contained.

### 7. CLI surface changes

Global options:

- add `--storage-uri URI` (preferred backend selector)
- keep `--db PATH` for SQLite compatibility

Commands:

- `onto init`: New command to bootstrap a storage backend.
  - For S3: creates `meta/head.json` with
    `{commit_id: 0, manifest_path: null, updated_at: <UTC-ISO8601>, runtime_id: "onto-init"}`,
    plus `meta/schema/` and `meta/indices/` prefix structure, including
    `meta/schema/types.json` initialized as empty known-type catalog. Required
    before first use; subsequent commands fail fast on uninitialized prefix.
  - Supports `--dry-run` to validate target access and print planned objects
    without writing.
  - For SQLite: optional; database and tables are created implicitly on first
    use (existing behavior preserved).
  - Fails if the target already contains an initialized ontology. Use `--force`
    to re-initialize: this requires a confirmation token (same pattern as other
    destructive CLI operations, e.g., `onto schema drop`).
    `onto init --force --dry-run` emits the token; apply requires both `--force`
    and `--token`, and resets the backend to empty state.
- `onto compact`: New command for S3 maintenance.
  - Options: `--type NAME` (optional filter).
  - Defaults to dry-run (preview compaction plan and estimated rewrite count).
  - `--apply` executes compaction under write lock.
- `onto index verify`: New read-only command for S3 index health checks.
  - Detects index lag (`max_indexed_commit < head.commit_id`) and missing latest
    touched-type entries.
  - Exits non-zero when lag/missing coverage is detected (operator warning
    state).
- `onto index repair`: New S3 maintenance command for index recovery.
  - Defaults to dry-run (shows lagged types and planned index writes).
  - `--apply` rebuilds/patches affected index objects from authoritative
    manifest-chain data under write lock, with the same keepalive and
    lease-expiry guard requirements used by writes/compaction.
  - Operation is idempotent: rerunning after successful repair produces no
    further index changes.

Resolution rules:

- `--storage-uri` selects backend.
- `--db` implies SQLite backend.
- providing both with conflicting targets fails fast.

Existing command semantics (`info`, `verify`, `query`, `commits`, `migrate`,
`import`, `export`) remain logically identical across backends. `index verify`
and `index repair` are additive S3-specific operational commands.

### 8. Data migration between backends

Migration from SQLite to S3 (or vice versa) uses the existing `onto export` and
`onto import` commands. The workflow is:

```bash
# Export from SQLite
onto export --db onto.db --output data/

# Initialize S3 backend
onto init --storage-uri s3://my-bucket/onto-prod

# Import into S3
onto import --storage-uri s3://my-bucket/onto-prod --input data/ --models myapp.models --apply --on-conflict abort
```

No dedicated migration tool is introduced. The export format is backend-agnostic
by design (JSONL with type/key/fields records), ensuring portability between any
supported backends.

**History note:** Export/import transfers _data_, not _commit history_. The
default export produces latest-state snapshots. Using
`--history-since 0 --with-metadata` includes `commit_id` per record, but all
imported records are written as a single new commit on the target backend. Full
commit-graph metadata (manifests, timestamps, runtime IDs) is not preserved.
This is consistent with the non-goal of lossless commit-graph migration
(§Non-Goals).

## Detailed Semantics

### Head object schema

`meta/head.json` fields:

- `commit_id: int` (`0` for empty/initialized state)
- `manifest_path: str | null` (`null` when `commit_id == 0`, required non-null
  for `commit_id >= 1`)
- `updated_at: str`
- `runtime_id: str`

Optional:

- `commit_sha256: str` (manifest hash)

### Commit metadata semantics

Commit metadata stays as `dict[str, str]` and is persisted in manifest JSON.
`ctx.add_commit_meta(...)` and CLI `--meta` semantics are unchanged.

### Read isolation

Readers never observe partial commit state:

- if head has not advanced, new objects are ignored,
- after head advances, manifest/files for that commit must already exist.

### Lock timeout and error mapping

- lock acquisition timeout maps to `LockContentionError`.
- lease-expiry guard before CAS maps to `LeaseExpiredError`.
- repeated stale-head retries map to `HeadMismatchError`.
- backend/network/permission failures map to storage/runtime errors with backend
  context (bucket, prefix, operation).

### Ordering guarantees

Commit IDs are monotonic integers across the ontology prefix. No two successful
commits may share a commit ID.

## API Sketch

```python
from ontologia import Ontology, OntologiaConfig

cfg = OntologiaConfig(
    runtime_id="writer-a",
    s3_region="us-west-2",
    s3_lock_timeout_ms=5000,
    s3_lease_ttl_ms=30000,
)

onto = Ontology(
    storage_uri="s3://my-bucket/onto-prod",
    config=cfg,
    entity_types=[Customer, Product],
    relation_types=[Subscription],
)

with onto.session() as s:
    s.ensure(Customer(id="c1", name="Alice", tier="Gold"))
    s.commit()
```

CLI examples:

```bash
# Initialize S3 backend
onto init --storage-uri s3://my-bucket/onto-prod

# Inspect
onto info --storage-uri s3://my-bucket/onto-prod

# Verify index health (non-zero exit if lag/missing coverage is detected)
onto index verify --storage-uri s3://my-bucket/onto-prod

# Repair stale indices
onto index repair --storage-uri s3://my-bucket/onto-prod --apply

# Query
onto query entities Customer --storage-uri s3://my-bucket/onto-prod --models myapp.models --filter '$.tier' eq '"Gold"'

# Migrate
onto migrate --storage-uri s3://my-bucket/onto-prod --models myapp.models --upgraders myapp.migrations
```

## Alternatives Considered

1. Lock-only write safety.
   - Rejected: stale client bugs can still corrupt ordering without head CAS.
2. CAS-only write safety.
   - Rejected: higher wasted work under contention; weaker operational
     observability of active owner.
3. Single Parquet file per commit across all types.
   - Rejected: poorer type pruning and larger mixed-schema files.
4. Arrow Dataset-only query engine.
   - Rejected: more custom planning/expression work and weaker reuse of SQL
     translation path.

## Risks and Mitigations

- Risk: many small Parquet files over time degrade query performance.
  Mitigation: index-based file pruning and compaction (`onto compact`) to merge
  small files into larger snapshots.
- Risk: orphan objects from failed CAS or process crashes. Mitigation: head
  pointer is source of truth; orphan sweeper follow-up.
- Risk: schema drift across many commit files complicates scans. Mitigation:
  `union_by_name` reads, schema-version tracking, typed hydration validation.
- Risk: lock starvation under high write contention. Mitigation: bounded retry
  with jitter, timeout errors, runtime ownership diagnostics in lock payload.
- Risk: S3 IAM misconfiguration causes partial operational outages. Mitigation:
  explicit required permission set in docs (GetObject, PutObject, ListBucket for
  prefix, conditional write support), fail-fast startup checks.

## Release Impact

Ontologia is pre-release. This RFC introduces additive backend capability and a
new preferred backend-selection API (`storage_uri`) while retaining SQLite
compatibility.

Source-level compatibility targets:

- existing `Ontology(db_path=...)` code continues to work,
- existing typed models, handlers, and query DSL remain unchanged.

## Rollout Tasks

### A. Update `spec/vision.md`

1. Generalize persistence model from SQLite-specific assumptions to pluggable
   backend contract.
2. Add S3 backend normative append-only object model and commit visibility rule
   (head pointer authoritative).
3. Add lock + head CAS concurrency semantics.
4. Add query pushdown expectation for S3 backend while preserving query
   semantics.

### B. Update `spec/api.md`

1. Document `Ontology(storage_uri=..., db_path=...)` binding rules.
2. Document config additions for S3 runtime tuning.
3. Keep session, ensure/commit, handler, and query semantics unchanged.
4. Document backend-specific operational errors and mapping.

### C. Update `spec/cli.md`

1. Add global `--storage-uri` option and precedence rules with `--db`.
2. Document `onto init` command with S3-required / SQLite-optional semantics.
3. Confirm backend parity for existing commands.
4. Document backend-specific diagnostics (`onto info` output includes backend
   type, bucket/prefix for S3).
5. Add `onto index verify` and `onto index repair` command contracts, including
   dry-run/apply and non-zero warning-state exits.

### D. Implementation and Validation

1. Extract repository protocol/interface from current `Repository`.
2. Rename current implementation to `SqliteRepository`.
3. Add `S3Repository` with object layout and lock/CAS write protocol.
4. Add backend factory from `storage_uri`/`db_path`.
5. Implement DuckDB scan adapter for S3 query paths with index-first file
   pruning and manifest-chain fallback.
6. Keep migration/schema APIs backend-agnostic with S3 metadata persistence.
7. Add index-lag detection/warning surfaces (runtime + `onto info`).
8. Add `onto index verify`/`onto index repair` implementations.
9. Add end-to-end tests for backend parity and concurrency safety.

## Acceptance Criteria

- S3 backend can run all core flows (`session.ensure/commit`, query APIs, commit
  inspection, migration preview/apply) with behavior equivalent to SQLite
  semantics.
- Commit visibility is atomic via head CAS; partial commit writes are never
  visible as committed state.
- Write lock supports acquire, renew, release, stale takeover, and timeout with
  expected errors.
- Commit IDs remain monotonic and unique under concurrent writers.
- Query semantics (`latest`, `as_of`, `with_history`, `history_since`) are
  backend-equivalent.
- Pushdown filtering/projection is exercised through DuckDB scans over S3
  Parquet.
- CLI supports `--storage-uri` while preserving existing `--db` behavior.
- `onto init` bootstraps S3 backend; subsequent commands fail fast on
  uninitialized prefix.
- `onto init`/`onto compact` support documented dry-run/apply safety semantics.
- Index lag / missing-latest-entry conditions are detectable and surfaced as
  operator warnings.
- `onto index verify` reports lag/missing coverage, and `onto index repair`
  restores index coverage without changing committed data.
- When `types.json` is unreadable at writer step 8, commit still succeeds after
  CAS and emits an operator-visible warning while leaving index objects
  unchanged for that attempt.
- `onto compact` merges small Parquet files without changing query semantics.
- History-producing queries on S3 are deterministic (`commit_id ASC` plus
  identity tie-break keys).
- Existing SQLite users do not require code changes.

## Test Matrix

1. Backend parity tests (SQLite vs S3) for entities, relations (keyed/unkeyed),
   metadata hydration.
2. Concurrency tests with competing writers validating lock and CAS behavior.
3. Failure-injection tests: crash before head update, crash after head update,
   stale lock takeover.
4. Temporal query tests across commit ranges and schema-version transitions.
5. CLI integration tests with `--storage-uri` and conflict validation versus
   `--db`.
6. CLI safety tests for `onto init`/`onto compact` dry-run/apply behavior and
   `onto init --force` token flow.
7. Migration tests on S3 backend including token drift detection and lock
   semantics.
8. Commit-inspection parity tests (`onto commits`, `onto commits examine`) on S3
   backend.
9. Lock-lease tests covering renew/release behavior, including owner-conditional
   release when lease-expiry races with takeover.
10. Pushdown tests asserting typed-column predicate/projection pushdown paths
    are exercised in DuckDB query plans.
11. Uninitialized-prefix fail-fast tests for S3 command paths (`info`, `query`,
    `migrate`, `import`, `export`).
12. Index-health warning tests for lag (`max_indexed_commit < head.commit_id`)
    and missing latest touched-type coverage (including snapshot-range coverage
    after compaction), plus per-commit head-path mismatch detection with
    manifest-chain fallback.
13. `onto index verify`/`onto index repair` tests, including idempotent repair,
    post-repair zero-warning verification, `max_indexed_commit = repair_head`,
    and no head mutation/new commit creation during repair.
14. Sparse-type watermark tests: untouched types advance `max_indexed_commit`
    correctly without unnecessary manifest walks, and query fallback remains
    correct when index lag is injected.
15. Writer-step-8 schema-catalog fault tests: when `types.json` is missing or
    malformed after successful CAS, commit returns success, emits warning, and
    leaves indices unchanged for that attempt.
16. Deterministic ordering tests for `with_history()` / `history_since()` using
    same-commit multi-identity rows (verify `commit_id ASC` + identity tie-break
    ordering before/after compaction).
17. Compaction apply tests for head-stability/lease guards: abort/replan when
    head changes before index publish, and abort with `LeaseExpiredError` when
    lease safety margin is exceeded.

## Open Follow-Ups (Explicitly Deferred)

- Orphan-object garbage collection policy and tooling.
- Optional secondary indexes/statistics sidecar for faster file pruning.
- Pluggable query engines beyond DuckDB.
