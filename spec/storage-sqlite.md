# SQLite Storage Backend Specification

> **Quick Reference**: This document provides the complete technical
> specification for Ontologia's SQLite storage backend, including schema
> definitions, query patterns, write protocols, migration mechanics, and
> debugging guides.
>
> **Use Cases**: Debugging data corruption, performance issues, lock contention,
> and migration problems.

---

## 1. Architecture Overview

The SQLite backend implements an append-only commit log model where:

- **Every write creates a new history row** rather than updating in-place
- **Commits are atomic transactions** that insert into `commits` and
  `entity_history`/`relation_history`
- **Latest state is computed at query time** via `MAX(commit_id)` subqueries
- **WAL (Write-Ahead Logging) mode** enables concurrent readers during writes
- **Table-level locking** enforces single-writer semantics

### Key Design Decisions

| Decision                             | Rationale                                                                     |
| ------------------------------------ | ----------------------------------------------------------------------------- |
| **JSON fields in `fields_json`**     | Flexibility for schema evolution; no ALTER TABLE needed for new fields        |
| **Subquery deduplication**           | SQLite 3.25+ supports window functions, but subqueries are more portable      |
| **WAL mode**                         | Readers don't block writers; better concurrency than DELETE journal mode      |
| **Instance key for relations**       | Empty string default allows multiple relation instances between same entities |
| **Separate `schema_versions` table** | Tracks full schema history for migrations                                     |

---

## 2. Schema Reference

### 2.1 Complete Table Definitions

#### **commits** - Commit tracking

```sql
CREATE TABLE IF NOT EXISTS commits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    metadata_json TEXT
);
```

| Column          | Type    | Constraints               | Description                  |
| --------------- | ------- | ------------------------- | ---------------------------- |
| `id`            | INTEGER | PRIMARY KEY AUTOINCREMENT | Monotonic commit identifier  |
| `created_at`    | TEXT    | NOT NULL                  | ISO8601 UTC timestamp        |
| `metadata_json` | TEXT    | NULL                      | JSON-encoded commit metadata |

**Key Behavior**: `id` is auto-incremented; first commit is `1`, empty database
is commit `0` (head = NULL).

---

#### **entity_history** - Append-only entity versions

```sql
CREATE TABLE IF NOT EXISTS entity_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type TEXT NOT NULL,
    entity_key TEXT NOT NULL,
    fields_json TEXT NOT NULL,
    commit_id INTEGER NOT NULL,
    schema_version_id INTEGER,
    FOREIGN KEY (commit_id) REFERENCES commits(id)
);

CREATE INDEX idx_entity_history_lookup
    ON entity_history(entity_type, entity_key, commit_id DESC);
```

| Column              | Type    | Constraints  | Description                                        |
| ------------------- | ------- | ------------ | -------------------------------------------------- |
| `id`                | INTEGER | PRIMARY KEY  | Row identifier (internal use)                      |
| `entity_type`       | TEXT    | NOT NULL     | Entity type name (e.g., "Customer")                |
| `entity_key`        | TEXT    | NOT NULL     | Entity primary key value                           |
| `fields_json`       | TEXT    | NOT NULL     | JSON-encoded field values                          |
| `commit_id`         | INTEGER | NOT NULL, FK | Commit that created this version                   |
| `schema_version_id` | INTEGER | NULL         | Schema version at write time (added via migration) |

**Index Usage**:

- `entity_type = ? AND entity_key = ? ORDER BY commit_id DESC LIMIT 1` → Point
  lookup
- `entity_type = ?` → Type-scoped queries
- `commit_id DESC` → Latest version first

---

#### **relation_history** - Append-only relation versions

```sql
CREATE TABLE IF NOT EXISTS relation_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    relation_type TEXT NOT NULL,
    left_key TEXT NOT NULL,
    right_key TEXT NOT NULL,
    instance_key TEXT NOT NULL DEFAULT '',
    fields_json TEXT NOT NULL,
    commit_id INTEGER NOT NULL,
    schema_version_id INTEGER,
    FOREIGN KEY (commit_id) REFERENCES commits(id)
);

CREATE INDEX idx_relation_history_lookup
    ON relation_history(relation_type, left_key, right_key, instance_key, commit_id DESC);
```

| Column              | Type    | Constraints         | Description                                        |
| ------------------- | ------- | ------------------- | -------------------------------------------------- |
| `relation_type`     | TEXT    | NOT NULL            | Relation type name                                 |
| `left_key`          | TEXT    | NOT NULL            | Left endpoint entity key                           |
| `right_key`         | TEXT    | NOT NULL            | Right endpoint entity key                          |
| `instance_key`      | TEXT    | NOT NULL DEFAULT '' | Multi-relation discriminator (added via migration) |
| `fields_json`       | TEXT    | NOT NULL            | JSON-encoded field values                          |
| `commit_id`         | INTEGER | NOT NULL            | Commit that created this version                   |
| `schema_version_id` | INTEGER | NULL                | Schema version at write time                       |

**Key Behavior**: `instance_key` default is empty string `''`. The composite
index includes `instance_key` to support multiple relations of the same type
between the same entity pair (keyed relations).

---

#### **schema_registry** - Current schema definitions

```sql
CREATE TABLE IF NOT EXISTS schema_registry (
    type_kind TEXT NOT NULL,
    type_name TEXT NOT NULL,
    schema_json TEXT NOT NULL,
    PRIMARY KEY (type_kind, type_name)
);
```

Stores the "current" schema for each type. Used for validation and comparison
during migrations.

---

#### **schema_versions** - Schema version history

```sql
CREATE TABLE IF NOT EXISTS schema_versions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    type_kind TEXT NOT NULL,
    type_name TEXT NOT NULL,
    schema_version_id INTEGER NOT NULL,
    schema_json TEXT NOT NULL,
    schema_hash TEXT NOT NULL,
    created_at TEXT NOT NULL,
    runtime_id TEXT,
    reason TEXT,
    UNIQUE(type_kind, type_name, schema_version_id)
);
```

| Column              | Description                              |
| ------------------- | ---------------------------------------- |
| `schema_version_id` | Per-type sequential version (1, 2, 3...) |
| `schema_hash`       | SHA-256 of canonical schema JSON         |
| `reason`            | 'initial', 'migration', 'bootstrap'      |

**Key Behavior**: Each schema change creates a new row with auto-incrementing
`schema_version_id` per `(type_kind, type_name)` pair.

---

#### **locks** - Distributed write lock

```sql
CREATE TABLE IF NOT EXISTS locks (
    lock_name TEXT PRIMARY KEY,
    owner_id TEXT NOT NULL,
    acquired_at TEXT NOT NULL,
    expires_at TEXT NOT NULL
);
```

| Column        | Description                                      |
| ------------- | ------------------------------------------------ |
| `lock_name`   | Hardcoded to `'ontology_write'` (singleton lock) |
| `owner_id`    | Runtime ID of lock holder                        |
| `acquired_at` | ISO8601 timestamp                                |
| `expires_at`  | Lease expiration timestamp                       |

---

### 2.2 Physical Layout

When database `onto.db` is active:

```
onto.db           # Main database file
onto.db-wal       # Write-ahead log (uncommitted changes)
onto.db-shm       # Shared memory file (WAL index)
```

**PRAGMA Settings** (applied on connection):

```sql
PRAGMA journal_mode=WAL;    -- Enable WAL mode
PRAGMA foreign_keys=ON;     -- Enforce referential integrity
```

**Foreign Key Behavior**: No `ON DELETE CASCADE` - commits cannot be deleted
while history rows reference them. This preserves the audit trail.

---

## 3. Query Patterns

### 3.1 Entity Queries

#### Latest Entity (Point Lookup)

```sql
SELECT fields_json, commit_id
FROM entity_history
WHERE entity_type = ? AND entity_key = ?
ORDER BY commit_id DESC
LIMIT 1
```

#### Latest Entities (All Current Versions)

```sql
SELECT eh.entity_key, eh.fields_json, eh.commit_id
FROM entity_history eh
INNER JOIN (
    SELECT entity_key, MAX(commit_id) as max_cid
    FROM entity_history
    WHERE entity_type = ?
    GROUP BY entity_key
) latest ON eh.entity_key = latest.entity_key
    AND eh.commit_id = latest.max_cid
WHERE eh.entity_type = ?
```

**Pattern**: Self-join to subquery finding max commit_id per key. Filters
applied to outer query.

---

#### As Of Specific Commit (Time Travel)

```sql
SELECT eh.entity_key, eh.fields_json, eh.commit_id
FROM entity_history eh
INNER JOIN (
    SELECT entity_key, MAX(commit_id) as max_cid
    FROM entity_history
    WHERE entity_type = ? AND commit_id <= ?
    GROUP BY entity_key
) latest ON eh.entity_key = latest.entity_key
    AND eh.commit_id = latest.max_cid
WHERE eh.entity_type = ?
```

**Parameters**: `(type_name, as_of_commit_id, type_name)`

---

#### With Full History

```sql
SELECT eh.entity_key, eh.fields_json, eh.commit_id
FROM entity_history eh
WHERE eh.entity_type = ?
ORDER BY eh.commit_id ASC
```

**Note**: When `with_history=True` or `history_since` is specified, the "latest
only" subquery is skipped entirely.

---

### 3.2 Relation Queries

#### Latest Relations

```sql
SELECT rh.left_key, rh.right_key, rh.instance_key, rh.fields_json, rh.commit_id
FROM relation_history rh
INNER JOIN (
    SELECT left_key, right_key, instance_key, MAX(commit_id) as max_cid
    FROM relation_history
    WHERE relation_type = ?
    GROUP BY left_key, right_key, instance_key
) latest ON rh.left_key = latest.left_key
    AND rh.right_key = latest.right_key
    AND rh.instance_key = latest.instance_key
    AND rh.commit_id = latest.max_cid
WHERE rh.relation_type = ?
```

**Note**: Grouping includes `instance_key` to support keyed relations.

---

#### With Left Endpoint Filter

```sql
-- ... base query ...
AND EXISTS (
    SELECT 1 FROM entity_history le
    INNER JOIN (
        SELECT entity_key, MAX(commit_id) as max_cid
        FROM entity_history WHERE entity_type = ? GROUP BY entity_key
    ) le_latest ON le.entity_key = le_latest.entity_key
        AND le.commit_id = le_latest.max_cid
    WHERE le.entity_type = ? AND le.entity_key = rh.left_key
        AND json_extract(le.fields_json, '$.field') = ?
)
```

**Requirement**: `left_entity_type` must be provided when using `left.$.field`
filters.

---

### 3.3 Filter Compilation

Filters compile to SQL WHERE clauses via `_compile_filter()`:

| Filter                                 | SQL                                                                                    |
| -------------------------------------- | -------------------------------------------------------------------------------------- |
| `$.field == value`                     | `json_extract(fields_json, '$.field') = ?`                                             |
| `$.nested.field == value`              | `json_extract(fields_json, '$.nested.field') = ?`                                      |
| `left.$.nested.field == value`         | `json_extract(le.fields_json, '$.nested.field') = ?`                                   |
| `right.$.nested.field == value`        | `json_extract(re.fields_json, '$.nested.field') = ?`                                   |
| `$.field.is_null()`                    | `json_extract(fields_json, '$.field') IS NULL`                                         |
| `$.field.in_([a,b])`                   | `json_extract(fields_json, '$.field') IN (?, ?)`                                       |
| `$.field.startswith(x)`                | `json_extract(fields_json, '$.field') LIKE 'x%'`                                       |
| `$.events.any_path("kind") == "click"` | `EXISTS (SELECT 1 FROM json_each(json_extract(fields_json, '$.events')) je WHERE ...)` |
| `expr1 & expr2`                        | `(expr1_sql) AND (expr2_sql)`                                                          |
| `expr1 \| expr2`                       | `(expr1_sql) OR (expr2_sql)`                                                           |
| `~expr`                                | `NOT (expr_sql)`                                                                       |

**Aggregation expression rules**:

- SUM and AVG use `CAST(json_extract(...) AS REAL)`.
- `avg_len(field)` compiles as
  `AVG(json_array_length(json_extract(fields_json, '$.field')))`.
- path-composed fields are passed through as dotted JSON paths in
  `json_extract(...)`.

---

## 4. Write Protocol

### 4.1 Transaction Boundaries

```python
# Begin transaction (acquires database write lock)
conn.execute("BEGIN IMMEDIATE")

try:
    # 1. Create commit record
    cursor = conn.execute(
        "INSERT INTO commits (created_at, metadata_json) VALUES (?, ?)",
        (now_iso, metadata_json)
    )
    commit_id = cursor.lastrowid

    # 2. Insert history rows
    conn.execute(
        "INSERT INTO entity_history "
        "(entity_type, entity_key, fields_json, commit_id, schema_version_id) "
        "VALUES (?, ?, ?, ?, ?)",
        (type_name, key, json, commit_id, version_id)
    )

    # 3. Commit
    conn.commit()
except:
    conn.rollback()
    raise
```

**Key Points**:

- `BEGIN IMMEDIATE` acquires write lock at start (prevents writer starvation)
- Foreign key from `history.commit_id` → `commits.id` enforces referential
  integrity
- Rollback on any error ensures atomicity

---

### 4.2 Locking Protocol

#### Lock Acquisition

```python
while True:
    now = datetime.now(timezone.utc)

    # Clean up expired locks
    conn.execute(
        "DELETE FROM locks WHERE lock_name = ? AND expires_at < ?",
        (lock_name, now.isoformat())
    )

    try:
        expires = now + timedelta(milliseconds=lease_ms)
        conn.execute(
            "INSERT INTO locks (lock_name, owner_id, acquired_at, expires_at) "
            "VALUES (?, ?, ?, ?)",
            ('ontology_write', owner_id, now.isoformat(), expires.isoformat())
        )
        conn.commit()
        return True  # Lock acquired
    except IntegrityError:
        # Lock held by another owner
        conn.rollback()
        if time.monotonic() >= deadline:
            return False
        time.sleep(0.01)  # 10ms backoff
```

#### Lease Management

- **Renewal**:
  `UPDATE locks SET expires_at = ? WHERE lock_name = ? AND owner_id = ?`
- **Release**: `DELETE FROM locks WHERE lock_name = ? AND owner_id = ?`
- **Timeout**: Default 5000ms (5s) for commits, 10000ms (10s) for migrations
- **Lease TTL**: Default 30000ms (30s), renewed every `lease_ttl / 3` by
  keep-alive thread

---

### 4.3 Optimistic Concurrency

```python
current_head = repo.get_head_commit_id()
if current_head != snapshot_commit_id:
    repo.release_lock(owner_id)
    if retry_count >= max_retries:
        raise HeadMismatchError(max_retries)
    # Exponential backoff with jitter
    time.sleep(0.01 * (2 ** retry_count) + random.uniform(0, 0.01))
    return retry_commit()
```

**Behavior**: Compares expected head with actual head under lock. On mismatch,
releases lock and retries with exponential backoff (max 3 retries).

---

### 4.4 Schema Drift Detection

Before writing, the runtime validates schema versions haven't changed:

```python
def _assert_no_schema_drift(self, changes):
    for kind, type_name in touched_types:
        expected_version = self._schema_version_ids.get(type_name)
        stored = repo.get_current_schema_version(kind, type_name)
        current_version = stored["schema_version_id"]
        if current_version != expected_version:
            raise SchemaOutdatedError(diffs)
```

This prevents writes using stale schema versions.

---

## 5. Migration Mechanics

### 5.1 Schema Version Bootstrap

On repository initialization, if `schema_versions` is empty but
`schema_registry` has entries:

```python
def _bootstrap_schema_versions(self):
    rows = conn.execute(
        "SELECT type_kind, type_name, schema_json FROM schema_registry"
    ).fetchall()

    for kind, name, schema_json in rows:
        schema_hash = _schema_hash(schema_json)
        conn.execute(
            "INSERT INTO schema_versions "
            "(type_kind, type_name, schema_version_id, schema_json, "
            "schema_hash, created_at, reason) "
            "VALUES (?, ?, 1, ?, ?, ?, ?)",
            (kind, name, schema_json, schema_hash, now, "bootstrap")
        )
```

---

### 5.2 Column Migrations

The repository automatically migrates history tables on connection:

```python
def _migrate_history_columns(self):
    for table in ("entity_history", "relation_history"):
        cols = {row[1] for row in conn.execute(
            f"PRAGMA table_info({table})"
        ).fetchall()}

        if "schema_version_id" not in cols:
            conn.execute(
                f"ALTER TABLE {table} ADD COLUMN schema_version_id INTEGER"
            )
            conn.commit()

def _migrate_instance_key_column(self):
    cols = {row[1] for row in conn.execute(
        "PRAGMA table_info(relation_history)"
    ).fetchall()}

    if "instance_key" not in cols:
        conn.execute(
            "ALTER TABLE relation_history "
            "ADD COLUMN instance_key TEXT NOT NULL DEFAULT ''"
        )
        # Recreate index to include instance_key
        conn.execute("DROP INDEX IF EXISTS idx_relation_history_lookup")
        conn.execute(
            "CREATE INDEX idx_relation_history_lookup "
            "ON relation_history(relation_type, left_key, right_key, "
            "instance_key, commit_id DESC)"
        )
        conn.commit()
```

---

### 5.3 Migration Token System

Migration uses a token-based safety mechanism:

1. **Preview Phase** (`dry_run=True`):

   - Computes migration plan (diffs per type)
   - Creates SHA-256 hash of canonical plan JSON
   - Encodes `plan_hash:head_commit_id` as Base64 token
   - Returns `MigrationPreview` with token

2. **Apply Phase** (`dry_run=False`):
   - Requires token or `force=True`
   - Recomputes plan under lock
   - Verifies token matches current plan and head
   - If mismatch → `MigrationTokenError`

```python
def _compute_plan_hash(diffs):
    canonical = json.dumps(
        [serialize_diff(d) for d in diffs],
        sort_keys=True,
        separators=(",", ":")
    )
    return hashlib.sha256(canonical.encode()).hexdigest()

def _compute_migration_token(plan_hash, head_commit_id):
    raw = f"{plan_hash}:{head_commit_id if head_commit_id else 'none'}"
    return base64.urlsafe_b64encode(raw.encode()).decode()
```

---

### 5.4 Upgrader Chain

For types with existing data, upgraders transform fields:

```python
def _chain_upgraders(registry, type_name, from_version, to_version):
    chain = []
    missing = []

    for v in range(from_version, to_version):
        key = (type_name, v)
        if key not in registry:
            missing.append(v)
        else:
            chain.append(registry[key])

    if missing:
        raise MissingUpgraderError(type_name, missing)

    def composed(fields):
        result = fields
        for fn in chain:
            result = fn(result)
        return result

    return composed
```

---

## 6. Debugging Guide

### 6.1 Data Corruption Investigation

#### Check Foreign Key Integrity

```sql
PRAGMA foreign_key_check;
```

#### Find Orphaned History Rows

```sql
SELECT COUNT(*) FROM entity_history eh
WHERE NOT EXISTS (SELECT 1 FROM commits c WHERE c.id = eh.commit_id);

SELECT COUNT(*) FROM relation_history rh
WHERE NOT EXISTS (SELECT 1 FROM commits c WHERE c.id = rh.commit_id);
```

#### Verify Commit Monotonicity

```sql
SELECT id, created_at
FROM commits
ORDER BY id ASC;
-- Check for gaps or duplicate IDs
```

#### Check for Duplicate Latest Versions

```sql
SELECT entity_type, entity_key, COUNT(*) as version_count
FROM (
    SELECT eh.entity_type, eh.entity_key, eh.commit_id
    FROM entity_history eh
    INNER JOIN (
        SELECT entity_key, MAX(commit_id) as max_cid
        FROM entity_history
        WHERE entity_type = 'Customer'
        GROUP BY entity_key
    ) latest ON eh.entity_key = latest.entity_key
        AND eh.commit_id = latest.max_cid
    WHERE eh.entity_type = 'Customer'
)
GROUP BY entity_type, entity_key
HAVING version_count > 1;
```

---

### 6.2 Performance Issues

#### Analyze Query Plans

```sql
EXPLAIN QUERY PLAN
SELECT eh.entity_key, eh.fields_json, eh.commit_id
FROM entity_history eh
INNER JOIN (
    SELECT entity_key, MAX(commit_id) as max_cid
    FROM entity_history
    WHERE entity_type = 'Customer'
    GROUP BY entity_key
) latest ON eh.entity_key = latest.entity_key
    AND eh.commit_id = latest.max_cid
WHERE eh.entity_type = 'Customer';
```

Look for:

- `USING INDEX idx_entity_history_lookup` (good)
- `SCAN TABLE entity_history` without index (bad)

#### Check Index Effectiveness

```sql
SELECT name, tbl_name, sql
FROM sqlite_master
WHERE type = 'index'
  AND tbl_name IN ('entity_history', 'relation_history');
```

#### Monitor WAL Size

```bash
ls -lh onto.db*
# If onto.db-wal is very large (>100MB), consider checkpointing
```

#### Force WAL Checkpoint

```sql
PRAGMA wal_checkpoint(TRUNCATE);
```

#### Count Rows Per Type

```sql
SELECT entity_type, COUNT(*) as total_versions
FROM entity_history
GROUP BY entity_type
ORDER BY total_versions DESC;
```

---

### 6.3 Lock/Deadlock Issues

#### Check Current Lock Holder

```sql
SELECT * FROM locks WHERE lock_name = 'ontology_write';
```

#### Detect Expired Locks

```sql
SELECT *,
       datetime('now') as current_time,
       datetime(expires_at) as expiration_time
FROM locks
WHERE lock_name = 'ontology_write'
  AND datetime(expires_at) < datetime('now');
```

#### Force Lock Release (Emergency)

```sql
-- CAUTION: Only use if you're certain no writer is active
DELETE FROM locks WHERE lock_name = 'ontology_write';
```

#### Check Lock Contention History

```python
# Enable SQLite trace to see lock contention
import sqlite3
conn = sqlite3.connect('onto.db')
conn.set_trace_callback(print)
```

---

### 6.4 Migration Problems

#### Verify Schema Version Consistency

```sql
SELECT sv.type_name, sv.schema_version_id, sv.created_at
FROM schema_versions sv
WHERE sv.type_kind = 'entity'
ORDER BY sv.type_name, sv.schema_version_id;
```

#### Check for Missing Schema Versions

```sql
-- Find gaps in version sequences
SELECT type_name, schema_version_id,
       LEAD(schema_version_id) OVER (
           PARTITION BY type_name ORDER BY schema_version_id
       ) as next_version
FROM schema_versions
WHERE type_kind = 'entity';
-- Look for rows where next_version - schema_version_id > 1
```

#### Diagnose Token Mismatch

```python
# Recompute plan hash and compare
from ontologia.migration import _compute_plan_hash, _compute_migration_token

preview = session.migrate(dry_run=True)
expected_token = preview.token
print(f"Expected token: {expected_token}")

# Decode token to see plan_hash and head
import base64
decoded = base64.urlsafe_b64decode(expected_token).decode()
print(f"Token contents: {decoded}")
```

#### Find Entities with Stale Schema Versions

```sql
SELECT eh.entity_type, eh.schema_version_id, COUNT(*) as row_count
FROM entity_history eh
LEFT JOIN schema_versions sv
    ON sv.type_kind = 'entity'
    AND sv.type_name = eh.entity_type
    AND sv.schema_version_id = eh.schema_version_id
WHERE sv.id IS NULL  -- No matching schema version
GROUP BY eh.entity_type, eh.schema_version_id;
```

---

### 6.5 Schema Evolution

#### View Current Schemas

```sql
SELECT type_kind, type_name,
       substr(schema_json, 1, 100) as schema_preview
FROM schema_registry
ORDER BY type_kind, type_name;
```

#### Compare Schema Versions

```sql
SELECT type_name, schema_version_id,
       substr(schema_json, 1, 100) as schema_preview,
       created_at, reason
FROM schema_versions
WHERE type_kind = 'entity' AND type_name = 'Customer'
ORDER BY schema_version_id;
```

---

## 7. Edge Cases and Gotchas

### 7.1 Filter Edge Cases

| Scenario              | Behavior                                         |
| --------------------- | ------------------------------------------------ |
| `field == None`       | Raises `TypeError`: "Use .is_null() instead"     |
| `field != None`       | Raises `TypeError`: "Use .is_not_null() instead" |
| Empty IN list         | Works: `IN ()` (always false)                    |
| JSON null vs SQL NULL | `json_extract` returns SQL NULL for missing keys |
| `any_path` on NULL/[] | Predicate is false (no matching list elements)   |

### 7.2 Query Edge Cases

| Scenario                          | Behavior                        |
| --------------------------------- | ------------------------------- |
| `query_entities` with no results  | Returns empty list              |
| `first()` with no results         | Returns `None`                  |
| `aggregate_entities` on empty set | Returns `None`                  |
| `as_of` with commit_id 0          | Returns empty (no commits <= 0) |

### 7.3 Instance Key Behavior

- Empty string `''` is the default for unkeyed relations
- Empty string is falsy in Python; checked via `if ik:`
- Unique constraint includes instance_key: `(left_key, right_key, instance_key)`

---

## 8. Performance Characteristics

| Operation             | Complexity       | Notes                                     |
| --------------------- | ---------------- | ----------------------------------------- |
| Point lookup (latest) | O(log N)         | Index seek on (type, key, commit_id DESC) |
| Type scan (latest)    | O(N + M log M)   | N rows, M unique keys, sort for MAX       |
| Full history          | O(N)             | Sequential scan with type filter          |
| as_of query           | O(N + M log M)   | Same as latest, with commit_id filter     |
| Aggregation           | O(N)             | Full scan with computed aggregate         |
| Write commit          | O(types \* rows) | Inserts into history tables               |

### Trade-offs

**JSON Storage**:

- ✅ Schema flexibility, no ALTER TABLE
- ❌ No index on JSON fields, slower filtering

**Subquery Deduplication**:

- ✅ Portable across SQLite versions
- ❌ Two table scans (subquery + outer query)

**WAL Mode**:

- ✅ Concurrent readers during writes
- ❌ Requires cleanup (checkpoint) for disk space

---

## 9. Event Bus Persistence

This section specifies the event-bus storage layer implemented in
`src/ontologia/event_store.py` for the SQLite backend.

### 9.1 Event Bus Tables and Indexes

```sql
CREATE TABLE IF NOT EXISTS events (
    id TEXT PRIMARY KEY,
    namespace TEXT NOT NULL,
    type TEXT NOT NULL,
    payload TEXT NOT NULL,
    created_at TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 100,
    root_event_id TEXT NOT NULL,
    chain_depth INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_events_namespace_type_order
    ON events(namespace, type, priority DESC, created_at ASC, id ASC);

CREATE TABLE IF NOT EXISTS event_claims (
    event_id TEXT NOT NULL,
    handler_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    claimed_at TEXT NOT NULL,
    lease_until TEXT NOT NULL,
    ack_at TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    available_at TEXT NOT NULL,
    last_error TEXT,
    dead_lettered_at TEXT,
    PRIMARY KEY (event_id, handler_id)
);
CREATE INDEX IF NOT EXISTS idx_event_claims_handler_state
    ON event_claims(handler_id, ack_at, dead_lettered_at, lease_until, available_at);
CREATE INDEX IF NOT EXISTS idx_event_claims_event
    ON event_claims(event_id);

CREATE TABLE IF NOT EXISTS dead_letters (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL,
    handler_id TEXT NOT NULL,
    namespace TEXT NOT NULL,
    failed_at TEXT NOT NULL,
    attempts INTEGER NOT NULL,
    last_error TEXT NOT NULL,
    event_type TEXT NOT NULL,
    event_payload TEXT NOT NULL,
    root_event_id TEXT NOT NULL,
    chain_depth INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_dead_letters_namespace_failed
    ON dead_letters(namespace, failed_at DESC);

CREATE TABLE IF NOT EXISTS sessions (
    session_id TEXT PRIMARY KEY,
    namespace TEXT NOT NULL,
    started_at TEXT NOT NULL,
    last_heartbeat TEXT NOT NULL,
    metadata TEXT
);
CREATE INDEX IF NOT EXISTS idx_sessions_heartbeat
    ON sessions(last_heartbeat);
CREATE INDEX IF NOT EXISTS idx_sessions_namespace
    ON sessions(namespace);
```

### 9.2 Event Row Semantics

- `enqueue()` inserts one `events` row with generated defaults:
  - `id`: existing event ID or new UUID
  - `created_at`: existing timestamp or current UTC ISO-8601
  - `root_event_id`: existing root or `id`
  - `chain_depth`: integer depth from event object (default `0`)
- Event ordering for claims/listing is deterministic:
  `priority DESC, created_at ASC, id ASC`.
- `payload` is canonical JSON (`json.dumps(..., sort_keys=True)`).

### 9.3 Claim/Ack/Release Lifecycle

#### Claim (`claim`)

- Scope is `(namespace, handler_id, event_types[])`; claims are per-handler.
- A claim can be created/re-acquired only when:
  - there is no claim row for `(event_id, handler_id)`, or
  - existing row is not acked/dead-lettered and both:
    - `lease_until <= now`
    - `available_at <= now`
- Implementation uses `BEGIN IMMEDIATE` to serialize concurrent claimers.
- `lease_until = now + event_claim_lease_ms`.
- New claim rows start with `attempts = 0`, `available_at = now`.

#### Ack (`ack`)

- `ack_at` is set to current UTC timestamp for `(event_id, handler_id)`.
- Acknowledgement does not delete `events`; it only marks claim state.

#### Release (`release`)

- On handler failure, `attempts` increments (`attempts = attempts + 1`).
- `last_error` is updated (`error` or `"handler failure"`).
- For non-terminal retries:
  - `lease_until` is set to `now` (lease immediately released)
  - `available_at = now + backoff + jitter`
  - `backoff = min(event_backoff_base_ms * 2^attempts, event_backoff_max_ms)`
  - `jitter` is uniform random `0..100ms`
- Dead-letter threshold: `attempts >= event_max_attempts`.
  - claim row is marked with `dead_lettered_at`, `lease_until=now`,
    `available_at=now`
  - one `dead_letters` row is inserted with event snapshot fields
  - one `event.dead_letter` event is enqueued to `events`
    - `root_event_id` is preserved from original event
    - `chain_depth` becomes original `chain_depth + 1`

### 9.4 Session Lifecycle

- `register_session(session_id, namespace, metadata)` upserts session row.
  - On first insert: sets both `started_at` and `last_heartbeat` to now.
  - On conflict: updates `namespace`, `last_heartbeat`, `metadata`; keeps
    original `started_at`.
- `heartbeat()` updates `last_heartbeat` for the session.
- CLI/session liveness uses:
  `is_dead = (now - last_heartbeat) > session_ttl_ms`.

### 9.5 Replay, Inspection, Listing, Cleanup

- `replay_event(namespace, event_id)`:
  - copies original `type`, `payload`, `priority`
  - inserts a new event with new UUID and new `created_at`
  - resets lineage: `root_event_id = new_id`, `chain_depth = 0`
- `inspect_event(event_id, namespace?)` returns event + all claim rows
  (`handler_id`, `session_id`, attempts/errors, lease and availability times).
- `list_events(namespace, limit)` status precedence:
  - `dead_lettered` if any claim has `dead_lettered_at`
  - else `acked` if any claim has `ack_at`
  - else `claimed` if any active lease (`lease_until > now`)
  - else `pending`
- `cleanup_events(namespace, before)` deletes:
  - `events` rows with `created_at < before`
  - matching `event_claims` rows
  - it does **not** delete `dead_letters` or `sessions`.

### 9.6 Runtime Integration Notes

- Event processing loop (`Session.run`) registers a session, heartbeats every
  `session_heartbeat_interval_ms`, and claims per handler with
  `event_claim_limit` bounded by `max_events_per_iteration`.
- Successful handler execution must call `ack`; exceptions call `release`.
- Scheduled events and handler-emitted buffered events are persisted through the
  same `enqueue()` path, so they share ordering/claim/retry/dead-letter rules.

---

This specification serves as the authoritative reference for understanding,
debugging, and operating the SQLite storage backend.
