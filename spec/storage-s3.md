# S3 Storage Backend Specification

> **Quick Reference**: This document provides the complete technical
> specification for Ontologia's S3 storage backend, including object layout,
> Parquet schemas, write protocols, query execution, crash recovery, and
> debugging guides.
>
> **Use Cases**: Debugging data integrity issues, performance problems,
> lock/lease issues, index lag, and query execution problems.

---

## 1. Architecture Overview

The S3 backend implements a cloud-native, append-only storage layer using:

- **S3 object storage** with Parquet commit files for horizontal read
  scalability
- **DuckDB query execution** with predicate/projection pushdown for efficient
  filtering
- **Distributed locking with CAS** (Compare-And-Swap) for write serialization
- **Index-based file resolution** to avoid scanning all manifests on every query
- **Manifest chain** as the authoritative source of truth for committed history

### Key Design Decisions

| Decision                              | Rationale                                                                                           |
| ------------------------------------- | --------------------------------------------------------------------------------------------------- |
| **Per-type-per-commit Parquet files** | Balances file count with type pruning; avoids one-file-per-row and mixed-schema complexity          |
| **Attempt UUIDs in paths**            | Prevents write-write collisions when stale lease leads to concurrent attempts                       |
| **Lock + CAS (not just CAS)**         | Lock provides explicit ownership/observability; CAS ensures linearizability even with buggy clients |
| **Indices as advisory**               | Manifest chain is truth; indices accelerate but readers tolerate stale indices via fallback         |
| **DuckDB over Arrow Dataset**         | Mature Parquet pushdown, natural SQL mapping, less custom engine surface                            |
| **Typed columns + JSON**              | Typed columns enable pushdown; `fields_json` preserves compatibility across schema drift            |

---

## 2. Object Layout

### 2.1 Storage URI Format

```
s3://bucket/prefix
```

Example: `s3://my-bucket/onto-prod`

All paths below are relative to the prefix.

---

### 2.2 Control Plane Objects (Meta)

| Object Key                                    | Purpose                                                                   |
| --------------------------------------------- | ------------------------------------------------------------------------- |
| `meta/head.json`                              | **Authoritative head pointer** - single source of truth for latest commit |
| `meta/locks/ontology_write.json`              | Distributed write lock with lease semantics                               |
| `meta/schema/types.json`                      | Known-type catalog (entities, relations arrays)                           |
| `meta/schema/registry.json`                   | Schema registry (type_kind -> type_name -> schema)                        |
| `meta/schema/dropped.json`                    | Dropped type markers with purge status                                    |
| `meta/schema/versions/{kind}/{TypeName}.json` | Schema version history per type                                           |

---

### 2.3 Index Objects

| Object Key                               | Purpose                                             |
| ---------------------------------------- | --------------------------------------------------- |
| `meta/indices/entities/{TypeName}.json`  | Per-entity type index (maps commit ranges to files) |
| `meta/indices/relations/{TypeName}.json` | Per-relation type index                             |

**Coverage Semantics**: `max_indexed_commit` represents the head commit ID at
the time the index was last written, not just the highest commit that modified
this type. This tells readers: "all commits up to X have been considered for
this type."

---

### 2.4 Commit Plane Objects (Per-Commit)

For commit `123` written by attempt `a1b2c3`:

| Object Key                                          | Purpose                           |
| --------------------------------------------------- | --------------------------------- |
| `commits/123-a1b2c3/manifest.json`                  | Commit metadata and file manifest |
| `commits/123-a1b2c3/entities/Customer.parquet`      | Entity data file                  |
| `commits/123-a1b2c3/relations/Subscription.parquet` | Relation data file                |

**Attempt UUID**: Each write attempt generates a random 8-character hex UUID.
Two writers targeting the same `commit_id` (e.g., after stale lease takeover)
write to non-overlapping keys and cannot corrupt each other's data.

---

### 2.5 Snapshot Objects (Post-Compaction)

| Object Key                                        | Purpose                                             |
| ------------------------------------------------- | --------------------------------------------------- |
| `snapshots/entities/Customer-1-100.parquet`       | Compacted entity snapshot covering commits 1-100    |
| `snapshots/relations/Subscription-50-150.parquet` | Compacted relation snapshot covering commits 50-150 |

---

## 3. Object Schemas

### 3.1 Head Object (`meta/head.json`)

```json
{
  "commit_id": 123,
  "manifest_path": "commits/123-a1b2c3/manifest.json",
  "updated_at": "2026-02-10T12:34:56.789012+00:00",
  "runtime_id": "writer-a"
}
```

| Field           | Type        | Description                                             |
| --------------- | ----------- | ------------------------------------------------------- |
| `commit_id`     | int         | Monotonic integer; `0` = empty/initialized state        |
| `manifest_path` | str \| null | S3 key to commit manifest; `null` when `commit_id == 0` |
| `updated_at`    | str         | UTC ISO-8601 timestamp                                  |
| `runtime_id`    | str         | Writer runtime identifier                               |

**Invariant**: `manifest_path` MUST be non-null when `commit_id >= 1`.

---

### 3.2 Lock Object (`meta/locks/ontology_write.json`)

```json
{
  "owner_id": "writer-a",
  "acquired_at": "2026-02-10T12:34:56.789012+00:00",
  "expires_at": "2026-02-10T12:35:26.789012+00:00",
  "lease_ttl_ms": 30000
}
```

| Field          | Description                    |
| -------------- | ------------------------------ |
| `owner_id`     | Runtime ID of lock holder      |
| `acquired_at`  | UTC ISO-8601 timestamp         |
| `expires_at`   | Lease expiration timestamp     |
| `lease_ttl_ms` | Lease duration in milliseconds |

**Clock Assumptions**: Lease expiry evaluation assumes clocks are synchronized
within `lease_ttl / 3` (e.g., via NTP).

---

### 3.3 Types Catalog (`meta/schema/types.json`)

```json
{
  "entities": ["Customer", "Product"],
  "relations": ["Subscription", "Order"],
  "updated_at": "2026-02-10T12:34:56.789012+00:00"
}
```

**Purpose**: Authoritative enumerable catalog of known types. Writers MUST
update this when adding/removing types. Used by index maintenance to enumerate
types for watermark updates.

---

### 3.4 Index Object (`meta/indices/{kind}/{TypeName}.json`)

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

| Field                     | Description                                 |
| ------------------------- | ------------------------------------------- |
| `type_name`               | Type name (e.g., "Customer")                |
| `max_indexed_commit`      | Head commit ID when index was last written  |
| `entries[]`               | Array of commit range -> file path mappings |
| `entries[].min_commit_id` | Start of commit range (inclusive)           |
| `entries[].max_commit_id` | End of commit range (inclusive)             |
| `entries[].path`          | S3 key to Parquet file                      |

**Invariants**:

- Entries MUST NOT overlap
- For per-commit files: `min_commit_id == max_commit_id`
- For snapshots: `min_commit_id < max_commit_id`
- Compaction replaces multiple per-commit entries with a single snapshot entry

---

### 3.5 Manifest Object (`commits/{id}-{attempt}/manifest.json`)

```json
{
  "commit_id": 123,
  "parent_commit_id": 122,
  "parent_manifest_path": "commits/122-x9y8/manifest.json",
  "created_at": "2026-02-10T12:34:56.789012+00:00",
  "runtime_id": "writer-a",
  "metadata": { "migration": "v2" },
  "files": [
    {
      "kind": "entity",
      "type_name": "Customer",
      "path": "commits/123-a1b2c3/entities/Customer.parquet",
      "row_count": 50,
      "schema_version_id": 3,
      "content_sha256": "abc123..."
    }
  ]
}
```

| Field                       | Description                                         |
| --------------------------- | --------------------------------------------------- |
| `commit_id`                 | Commit identifier                                   |
| `parent_commit_id`          | Parent commit ID (`null` for first commit)          |
| `parent_manifest_path`      | S3 key to parent manifest (`null` for first commit) |
| `created_at`                | UTC ISO-8601 timestamp                              |
| `runtime_id`                | Writer runtime identifier                           |
| `metadata`                  | User-provided commit metadata                       |
| `files[]`                   | Array of file descriptors                           |
| `files[].kind`              | "entity" or "relation"                              |
| `files[].type_name`         | Type name                                           |
| `files[].path`              | S3 key to Parquet file                              |
| `files[].row_count`         | Number of rows in file                              |
| `files[].schema_version_id` | Schema version used                                 |
| `files[].content_sha256`    | SHA-256 hash of file content                        |

**Key Behavior**: `parent_manifest_path` creates a backward-walkable chain from
any committed manifest to all ancestors. Required for gap reconciliation when
indices are stale.

---

## 4. Parquet File Schemas

### 4.1 Entity Parquet Schema

| Column              | Type   | Description                                       |
| ------------------- | ------ | ------------------------------------------------- |
| `commit_id`         | int64  | Commit that wrote this row                        |
| `entity_type`       | string | Type name (e.g., "Customer")                      |
| `entity_key`        | string | Entity identifier                                 |
| `schema_version_id` | int64  | Schema version used                               |
| `fields_json`       | string | **Canonical JSON payload**                        |
| `{field1}`          | varies | Typed column for field1 (e.g., `tier: string`)    |
| `{field2}`          | varies | Typed column for field2 (e.g., `balance: double`) |
| ...                 | ...    | One column per field in the writing schema        |

**Design Notes**:

- Typed field columns enable DuckDB predicate pushdown
- `fields_json` remains canonical for compatibility and lossless schema
  evolution
- DuckDB uses `union_by_name=true` to handle schema drift across files

---

### 4.2 Relation Parquet Schema

| Column              | Type   | Description                                 |
| ------------------- | ------ | ------------------------------------------- |
| `commit_id`         | int64  | Commit that wrote this row                  |
| `relation_type`     | string | Type name (e.g., "Subscription")            |
| `left_key`          | string | Left endpoint entity key                    |
| `right_key`         | string | Right endpoint entity key                   |
| `instance_key`      | string | Empty-string sentinel for unkeyed relations |
| `schema_version_id` | int64  | Schema version used                         |
| `fields_json`       | string | **Canonical JSON payload**                  |
| `{field1}`          | varies | Typed column for field1                     |
| ...                 | ...    | One column per field in the writing schema  |

---

## 5. Write Protocol (Step-by-Step)

### 5.1 Complete Write Flow

```
┌─────────────────────────────────────────────────────────────┐
│ Step 1: Acquire Lock (if not held)                         │
│   - Conditional PUT meta/locks/ontology_write.json          │
│   - if-none-match="*" for create                            │
│   - If exists and expired: conditional takeover if-match    │
│   - If exists and unexpired: retry with backoff             │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│ Step 2: Read Head                                           │
│   - GET meta/head.json                                      │
│   - Capture: head_commit_id, manifest_path, head_etag       │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│ Step 3: Compute Next Commit                                 │
│   - next_commit_id = head_commit_id + 1                     │
│   - attempt_uuid = random 8-char hex                        │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│ Step 4: Write Parquet Files                                 │
│   - For each touched entity type:                           │
│     PUT commits/{next}-{attempt}/entities/{Type}.parquet    │
│   - For each touched relation type:                         │
│     PUT commits/{next}-{attempt}/relations/{Type}.parquet   │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│ Step 5: Write Manifest                                      │
│   - parent_commit_id = head_commit_id                       │
│   - parent_manifest_path = head.manifest_path               │
│   - files[] with metadata (row_count, sha256, version_id)  │
│   PUT commits/{next}-{attempt}/manifest.json                │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│ Step 6: Lease-Expiry Guard                                  │
│   - Compare now() against lease_expires_at                  │
│   - If expired or within safety margin (lease_ttl / 3):     │
│     → Raise LeaseExpiredError                               │
│     → Best-effort cleanup of orphan files                   │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│ Step 7: Head CAS (Commit Point) ★ LINEARIZATION POINT ★    │
│   - Conditional PUT meta/head.json with if-match=head_etag  │
│   - If PreconditionFailed: HeadMismatchError → retry        │
│   - SUCCESS: Commit is now visible and authoritative        │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│ Step 8: Best-Effort Index Update                            │
│   - Read meta/schema/types.json                             │
│   - For each known type (not just touched):                 │
│     - If index.lag: repair gap via manifest chain walk      │
│     - For touched types: append/replace entry               │
│     - Update max_indexed_commit = next_commit_id            │
│     - PUT index object                                      │
│   - If types.json unreadable: emit warning, skip            │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│ Step 9: Release Lock                                        │
│   - Conditional DELETE lock object (best effort)            │
└─────────────────────────────────────────────────────────────┘
```

---

### 5.2 Post-CAS Success Rule

Once Step 7 (head CAS) succeeds:

- The commit **MUST** be reported as successful to the caller
- Step 8 (index update) failure does **NOT** fail the commit
- Step 9 (lock release) failure does **NOT** fail the commit
- Implementation **MUST NOT** raise error or trigger retry after successful CAS

**Rationale**: Head CAS is the linearization point. After CAS, the commit is
authoritative and visible. Index lag is a degraded-but-correct state (readers
fall back to manifest chain).

---

### 5.3 CAS Failure Handling

```python
try:
    s3.put_object(
        Bucket=bucket,
        Key=f"{prefix}/meta/head.json",
        Body=json.dumps(next_head),
        IfMatch=head_etag
    )
except ClientError as e:
    if e.response['Error']['Code'] == 'PreconditionFailed':
        release_lock()
        if retry_count >= max_retries:
            raise HeadMismatchError(retry_count)
        # Exponential backoff with jitter
        time.sleep(0.01 * (2 ** retry_count) + random.uniform(0, 0.02))
        return retry_from_step_2()
```

---

### 5.4 Write-Write Collision Safety

Because each attempt writes to `commits/{commit_id}-{attempt_uuid}/`:

- Two writers targeting the same `commit_id` (e.g., after stale lease takeover)
  never overwrite each other's data
- Only one writer's head CAS succeeds
- The loser's orphan directory is harmless and ignored by readers

---

## 6. Lock & Lease Mechanics

### 6.1 Lock Acquisition Algorithm

```python
def acquire_lock(owner_id, timeout_ms, lease_ms):
    deadline = time.monotonic() + timeout_ms / 1000.0

    while True:
        now = datetime.now(timezone.utc)
        expires = now + timedelta(milliseconds=lease_ms)

        payload = {
            "owner_id": owner_id,
            "acquired_at": now.isoformat(),
            "expires_at": expires.isoformat(),
            "lease_ttl_ms": lease_ms
        }

        try:
            # Attempt conditional create
            s3.put_object(
                Bucket=bucket,
                Key=f"{prefix}/meta/locks/ontology_write.json",
                Body=json.dumps(payload),
                IfNoneMatch="*"  # Only succeed if object doesn't exist
            )
            return True  # Lock acquired
        except ClientError as e:
            if e.response['Error']['Code'] != 'PreconditionFailed':
                raise

            # Lock exists - inspect and maybe takeover
            try:
                resp = s3.get_object(Bucket=bucket, Key=lock_key)
                lock_obj = json.loads(resp['Body'].read())
                etag = resp['ETag'].strip('"')
            except ClientError:
                continue  # Lock disappeared, retry

            # Check if expired
            lock_expires = datetime.fromisoformat(lock_obj["expires_at"])
            if now >= lock_expires:
                try:
                    # Attempt takeover
                    s3.put_object(
                        Bucket=bucket,
                        Key=lock_key,
                        Body=json.dumps(payload),
                        IfMatch=etag
                    )
                    return True  # Lock taken over
                except ClientError:
                    pass  # Lost race, retry

            # Lock held by another, not expired
            if time.monotonic() >= deadline:
                return False  # Timeout

            time.sleep(0.01 + random.uniform(0, 0.02))  # Jittered backoff
```

---

### 6.2 Lease Keep-Alive

Active writers MUST renew lease periodically at interval `lease_ttl_ms / 3`:

```python
def renew_lock(owner_id, lease_ms):
    try:
        resp = s3.get_object(Bucket=bucket, Key=lock_key)
        lock_obj = json.loads(resp['Body'].read())
        etag = resp['ETag'].strip('"')
    except ClientError:
        return False  # Lock doesn't exist

    if lock_obj["owner_id"] != owner_id:
        return False  # Lock owned by another

    now = datetime.now(timezone.utc)
    lock_obj["expires_at"] = (now + timedelta(milliseconds=lease_ms)).isoformat()

    try:
        s3.put_object(
            Bucket=bucket,
            Key=lock_key,
            Body=json.dumps(lock_obj),
            IfMatch=etag
        )
        return True
    except ClientError:
        return False  # Lost ownership
```

**Critical**: On successful renewal, writer MUST update local
`lease_expires_at`. If renewal fails, writer MUST treat lease as unsafe and
abort before CAS.

---

### 6.3 Lease Safety Check

Before any critical operation (CAS, index write):

```python
def _ensure_lease_safe(self):
    if self._lease_unsafe:
        raise LeaseExpiredError()

    if self._lease_expires_at:
        margin = self._lease_ttl_ms / 3000.0  # Safety margin in seconds
        if time.time() + margin >= self._lease_expires_at.timestamp():
            self._lease_unsafe = True
            raise LeaseExpiredError()
```

**Safety Margin**: Uses `lease_ttl / 3` to account for clock skew and network
delays.

---

### 6.4 Lock Release

```python
def release_lock(owner_id):
    try:
        resp = s3.get_object(Bucket=bucket, Key=lock_key)
        lock_obj = json.loads(resp['Body'].read())
        etag = resp['ETag'].strip('"')

        if lock_obj["owner_id"] == owner_id:
            s3.delete_object(
                Bucket=bucket,
                Key=lock_key,
                IfMatch=etag
            )
    except ClientError:
        pass  # Best effort; if lock already gone or changed owner, that's fine
```

---

## 7. Index Structure and Maintenance

### 7.1 Index Update Algorithm (Writer Side)

On each successful commit (Step 8):

```python
def _update_indices(next_commit_id, previous_head, previous_manifest_path, touched_types):
    # Read known types
    try:
        types_catalog = read_json(f"{prefix}/meta/schema/types.json")
    except:
        # types.json unreadable - emit warning, skip index update
        log.warning("types.json unreadable; skipping index update")
        return

    all_types = [
        ("entity", t) for t in types_catalog["entities"]
    ] + [
        ("relation", t) for t in types_catalog["relations"]
    ]

    for kind, type_name in all_types:
        try:
            idx = read_index(kind, type_name)
        except NotFound:
            idx = {"type_name": type_name, "max_indexed_commit": 0, "entries": []}

        # Repair gap if lagged
        if idx["max_indexed_commit"] < previous_head:
            idx = _repair_index_gap(idx, previous_head, previous_manifest_path, kind, type_name)

        # Add entry for this commit if type was touched
        if (kind, type_name) in touched_types:
            attempt_path = f"commits/{next_commit_id}-{attempt_uuid}/{kind}s/{type_name}.parquet"

            # Remove any existing per-commit entry for next_commit_id (shouldn't exist, but safety)
            idx["entries"] = [
                e for e in idx["entries"]
                if not (e["min_commit_id"] == next_commit_id and e["max_commit_id"] == next_commit_id)
            ]

            # Append new entry
            idx["entries"].append({
                "min_commit_id": next_commit_id,
                "max_commit_id": next_commit_id,
                "path": attempt_path
            })

        # Update watermark
        idx["max_indexed_commit"] = next_commit_id

        # Write index
        write_json(f"{prefix}/meta/indices/{kind}s/{type_name}.json", idx)
```

---

### 7.2 Gap Repair Algorithm

```python
def _repair_index_gap(idx, previous_head, previous_manifest_path, kind, type_name):
    """Walk manifest chain backward to fill gap."""
    for manifest in walk_manifest_chain_backward(previous_manifest_path):
        cid = manifest["commit_id"]

        # Stop when we reach indexed commits
        if cid <= idx["max_indexed_commit"]:
            break

        # Check if this commit touched the type
        for file in manifest["files"]:
            if file["kind"] == kind and file["type_name"] == type_name:
                # Check if already covered by snapshot
                if not any(_entry_covers(e, cid) for e in idx["entries"]):
                    idx["entries"].append({
                        "min_commit_id": cid,
                        "max_commit_id": cid,
                        "path": file["path"]
                    })
                break

    return idx

def _entry_covers(entry, commit_id):
    return entry["min_commit_id"] <= commit_id <= entry["max_commit_id"]
```

---

### 7.3 Manifest Chain Walk

```python
def walk_manifest_chain_backward(manifest_path):
    """Yield manifests from head backward to genesis."""
    while manifest_path:
        manifest = read_json(f"{prefix}/{manifest_path}")
        yield manifest
        manifest_path = manifest.get("parent_manifest_path")
```

---

## 8. Query Execution with DuckDB

### 8.1 File Resolution Pipeline

```
1. Read meta/head.json → head_commit_id
   └─ If head_commit_id == 0: return empty result immediately

2. Determine temporal window:
   - latest: q_head = head, lower_exclusive = 0
   - as_of(c): q_head = min(c, head), lower_exclusive = 0
   - history_since(c): q_head = head, lower_exclusive = c
   - with_history: q_head = head, lower_exclusive = 0

3. Resolve files for type via index:
   └─ Read meta/indices/{kind}/{TypeName}.json
   └─ Check if index covers q_head (max_indexed_commit >= q_head)
   └─ If gap: walk manifest chain for missing commits
   └─ If head commit path mismatch: force manifest fallback for head

4. Build file list from index entries + gap-filled manifests

5. Execute DuckDB query over file list
```

---

### 8.2 Index Fallback Logic (Reader Side)

```python
def _resolve_type_files(kind, type_name, q_head, lower_exclusive):
    selected = set()

    # Read index
    try:
        idx = read_index(kind, type_name)
    except NotFound:
        # No index - walk full manifest chain
        return _resolve_via_manifest_chain(kind, type_name, q_head, lower_exclusive)

    # Check if head commit path matches index
    force_head_fallback = False
    if q_head == head_commit_id:
        head_manifest = read_json(f"{prefix}/{head_manifest_path}")
        touched_head_path = None

        for file in head_manifest["files"]:
            if file["kind"] == kind and file["type_name"] == type_name:
                touched_head_path = file["path"]
                break

        if touched_head_path:
            # Check if index has matching per-commit entry
            head_entries = [
                e for e in idx["entries"]
                if e["min_commit_id"] == q_head and e["max_commit_id"] == q_head
            ]

            if not head_entries or head_entries[0]["path"] != touched_head_path:
                # Path mismatch - index is stale for head
                force_head_fallback = True

    # Add index entries that intersect query window
    for entry in idx["entries"]:
        if _entry_intersects(entry, lower_exclusive, q_head):
            if not (force_head_fallback and entry["min_commit_id"] == q_head):
                selected.add(entry["path"])

    # Walk manifest chain for gap
    covered = min(idx["max_indexed_commit"], q_head)
    if force_head_fallback:
        covered = min(covered, q_head - 1)

    if covered < q_head:
        for manifest in walk_manifest_chain_backward(head_manifest_path):
            cid = manifest["commit_id"]
            if cid <= covered:
                break
            if lower_exclusive < cid <= q_head:
                for file in manifest["files"]:
                    if file["kind"] == kind and file["type_name"] == type_name:
                        selected.add(file["path"])

    return list(selected)

def _entry_intersects(entry, lower_exclusive, upper_inclusive):
    return entry["max_commit_id"] > lower_exclusive and entry["min_commit_id"] <= upper_inclusive
```

---

### 8.3 DuckDB SQL Generation

#### Entity Query (Latest State)

```sql
SELECT q.entity_key, q.fields_json, q.commit_id
FROM (
  SELECT eh.entity_key, eh.fields_json, eh.commit_id,
         ROW_NUMBER() OVER (
           PARTITION BY eh.entity_key
           ORDER BY eh.commit_id DESC
         ) AS _rn
  FROM read_parquet([
    's3://bucket/prefix/commits/105-cd34/entities/Customer.parquet',
    's3://bucket/prefix/commits/123-ef56/entities/Customer.parquet'
  ], union_by_name=true) eh
  WHERE eh.entity_type = 'Customer'
) q
WHERE q._rn = 1
```

**Pattern**: Window function `ROW_NUMBER()` with `QUALIFY` for deduplication.

---

#### Entity Query (as_of)

```sql
SELECT q.entity_key, q.fields_json, q.commit_id
FROM (
  SELECT eh.entity_key, eh.fields_json, eh.commit_id,
         ROW_NUMBER() OVER (
           PARTITION BY eh.entity_key
           ORDER BY eh.commit_id DESC
         ) AS _rn
  FROM read_parquet([...], union_by_name=true) eh
  WHERE eh.entity_type = 'Customer'
    AND eh.commit_id <= 100
) q
WHERE q._rn = 1
```

---

#### Entity Query (with_history)

```sql
SELECT eh.entity_key, eh.fields_json, eh.commit_id
FROM read_parquet([...], union_by_name=true) eh
WHERE eh.entity_type = 'Customer'
ORDER BY eh.commit_id ASC, eh.entity_key ASC
```

**Deterministic Ordering**: `commit_id ASC` + identity tie-break
(`entity_key ASC`).

---

#### Relation Query with Endpoint Filters

```sql
SELECT q.left_key, q.right_key, q.instance_key, q.fields_json, q.commit_id
FROM (
  SELECT rh.left_key, rh.right_key, rh.instance_key,
         rh.fields_json, rh.commit_id,
         ROW_NUMBER() OVER (
           PARTITION BY rh.left_key, rh.right_key, rh.instance_key
           ORDER BY rh.commit_id DESC
         ) AS _rn
  FROM read_parquet([...], union_by_name=true) rh
  WHERE rh.relation_type = 'Subscription'
) q
WHERE q._rn = 1
  AND EXISTS (
    SELECT 1 FROM (
      SELECT le.entity_key, le.fields_json,
             ROW_NUMBER() OVER (
               PARTITION BY le.entity_key
               ORDER BY le.commit_id DESC
             ) AS _rn
      FROM read_parquet([...], union_by_name=true) le
      WHERE le.entity_type = 'Customer'
    ) le
    WHERE le._rn = 1
      AND le.entity_key = q.left_key
      AND json_extract(le.fields_json, '$.tier') = 'Gold'
  )
```

---

### 8.4 Filter Evaluation Semantics

S3 query execution uses two filter-evaluation paths that must remain
semantically aligned:

- **DuckDB SQL path** for repository queries (`query_entities`,
  `query_relations`, aggregates, grouped aggregates)
- **In-process Python path** for row-wise filtering in code paths that do not
  execute full SQL plans

Both paths support:

- direct fields (`$.field`, `$.nested.field`)
- endpoint fields (`left.$.*`, `right.$.*`) in relation-query contexts
- logical composition (`AND`, `OR`, `NOT`)
- existential list predicates over list fields (`any_path(...)`)

#### Shared existential SQL pattern

Existential predicates compile through the shared filter compiler to `EXISTS`
with `json_each`:

```sql
EXISTS (
  SELECT 1
  FROM json_each(json_extract(q.fields_json, '$.events')) AS je
  WHERE json_extract(je.value, '$.kind') = 'click'
)
```

`count_where(predicate)` reuses the same filter pipeline as `.where(predicate)`
with `COUNT(*)`. `avg_len(field)` uses `AVG(json_array_length(...))`.

#### In-process nested-path and existential behavior

In-process evaluation resolves dotted paths segment-by-segment through nested
dicts. Missing segments, `NULL` intermediates, or non-dict traversal targets
resolve to `None`.

For existential predicates:

- non-list values resolve to predicate false
- `NULL` and `[]` both resolve to predicate false
- each list item is tested using the same comparison operator semantics used by
  scalar filters

---

### 8.5 DuckDB Connection Management

```python
def _duck_conn(self):
    if self._duck is None:
        self._duck = duckdb.connect(database=":memory:")

        # Set memory limit
        self._duck.execute(
            f"SET memory_limit='{self._config.s3_duckdb_memory_limit}'"
        )

        # Install and load httpfs extension
        self._duck.execute("INSTALL httpfs")
        self._duck.execute("LOAD httpfs")

        # Configure S3 credentials
        creds = self._get_aws_credentials()
        self._duck.execute(f"SET s3_region='{creds['region']}'")
        self._duck.execute(f"SET s3_access_key_id='{creds['key']}'")
        self._duck.execute(f"SET s3_secret_access_key='{creds['secret']}'")

        if self._config.s3_endpoint_url:
            endpoint = urlparse(self._config.s3_endpoint_url)
            self._duck.execute(f"SET s3_endpoint='{endpoint.netloc}'")
            self._duck.execute(
                f"SET s3_use_ssl='{'true' if endpoint.scheme == 'https' else 'false'}'"
            )
            self._duck.execute("SET s3_url_style='path'")

    return self._duck
```

---

## 9. Compaction Mechanics

### 9.1 Algorithm

```python
def compact(type_name=None, apply=False):
    # Plan phase (no lock needed)
    types = select_types_to_compact(type_name)
    plan = []

    for kind, name in types:
        idx = _rebuild_index_from_manifests(kind, name, head_commit_id)
        per_commit_entries = [
            e for e in idx["entries"]
            if e["min_commit_id"] == e["max_commit_id"]
        ]

        if len(per_commit_entries) > 1:
            plan.append({
                "kind": kind,
                "type_name": name,
                "entry_count": len(per_commit_entries),
                "min_commit_id": min(e["min_commit_id"] for e in per_commit_entries),
                "max_commit_id": max(e["max_commit_id"] for e in per_commit_entries)
            })

    if not apply:
        return {"planned": plan, "applied": False}

    # Execute phase (requires lock)
    acquire_lock(owner, timeout, lease)
    try:
        with LeaseKeepAlive(owner, lease_ttl):
            head_start = get_head_commit_id()

            rewrites = []
            for item in plan:
                # Read and merge files
                files = _get_files_for_range(item["kind"], item["type_name"],
                                             item["min_commit_id"], item["max_commit_id"])
                tables = [pq.read_table(download_s3(f)) for f in files]
                merged = pa.concat_tables(tables, promote_options='default')

                # Sort to maintain deterministic ordering
                merged = merged.sort_by([
                    ("commit_id", "ascending"),
                    ("entity_key" if item["kind"] == "entity" else "left_key", "ascending")
                ])

                # Write snapshot
                snap_path = (
                    f"snapshots/{item['kind']}s/{item['type_name']}"
                    f"-{item['min_commit_id']}-{item['max_commit_id']}.parquet"
                )
                write_parquet_s3(snap_path, merged)

                rewrites.append({
                    "kind": item["kind"],
                    "type_name": item["type_name"],
                    "min_commit_id": item["min_commit_id"],
                    "max_commit_id": item["max_commit_id"],
                    "snap_path": snap_path
                })

            # Verify head stability and lease safety
            ensure_lease_safe()
            if get_head_commit_id() != head_start:
                raise HeadMismatchError("Head changed during compaction")

            # Update indices atomically
            for rw in rewrites:
                idx = read_index(rw["kind"], rw["type_name"])

                # Remove old per-commit entries
                idx["entries"] = [
                    e for e in idx["entries"]
                    if not (rw["min_commit_id"] <= e["max_commit_id"] <= rw["max_commit_id"])
                ]

                # Add snapshot entry
                idx["entries"].append({
                    "min_commit_id": rw["min_commit_id"],
                    "max_commit_id": rw["max_commit_id"],
                    "path": rw["snap_path"]
                })

                write_index(rw["kind"], rw["type_name"], idx)

            return {"planned": plan, "applied": True, "rewrites": rewrites}
    finally:
        release_lock(owner)
```

---

### 9.2 Compaction Invariants

- **Temporal preservation**: `commit_id` column preserved for every row
- **Query equivalence**: All query modes produce identical results before/after
- **Deterministic ordering**: `commit_id ASC` + identity tie-break maintained in
  snapshot files
- **Index atomicity**: Index update happens under lock after head stability
  check

---

## 10. Crash Recovery and Consistency

### 10.1 Crash Scenarios

| Crash Point                         | State                                                            | Recovery                                                                  |
| ----------------------------------- | ---------------------------------------------------------------- | ------------------------------------------------------------------------- |
| Before head CAS (Step 7)            | Orphan Parquet/manifest files exist in `commits/{id}-{attempt}/` | Orphans ignored by readers (no head reference); cleaned by future janitor |
| Lease-expiry guard (Step 6)         | Same as above                                                    | `LeaseExpiredError` raised; retry from lock acquisition                   |
| After head CAS, before index update | Commit visible; index stale                                      | Readers use manifest fallback; next writer repairs index                  |
| After head CAS and index update     | Fully consistent                                                 | No recovery needed                                                        |

---

### 10.2 Consistency Guarantees

- **Atomic visibility**: Commits become visible atomically via head CAS
- **No partial reads**: Readers never observe partial commit state
- **Monotonic commits**: Commit IDs are monotonic integers
- **Linearizability**: Head CAS ensures total order of commits
- **Index as advisory**: Manifest chain is source of truth; indices accelerate
  but are not required for correctness

---

### 10.3 Index Lag Detection

| Condition                | Definition                                                                     |
| ------------------------ | ------------------------------------------------------------------------------ |
| **Index Lag**            | `max_indexed_commit < head.commit_id`                                          |
| **Missing Latest Entry** | Head manifest touches type T, but index has no entry covering `head.commit_id` |
| **Path Mismatch**        | Index entry path for head commit doesn't match authoritative manifest path     |

**Warnings**: Surfaced via `storage_info()["last_index_warning"]`,
`onto index verify` (non-zero exit), and runtime diagnostics.

---

## 11. Debugging Guide

### 11.1 Data Integrity Issues

#### Verify head.json Points to Existing Manifest

```bash
# Get head
aws s3 cp s3://bucket/prefix/meta/head.json - | jq .

# Check manifest exists
aws s3 ls s3://bucket/prefix/commits/123-a1b2c3/manifest.json
```

#### Check Manifest Chain Integrity

```python
def verify_manifest_chain(head_manifest_path):
    visited = set()
    path = head_manifest_path

    while path:
        if path in visited:
            print(f"ERROR: Cycle detected at {path}")
            return False
        visited.add(path)

        manifest = read_json(f"{prefix}/{path}")
        parent = manifest.get("parent_manifest_path")

        if parent:
            # Verify parent exists
            try:
                read_json(f"{prefix}/{parent}")
            except NotFound:
                print(f"ERROR: Parent manifest not found: {parent}")
                return False

        path = parent

    print(f"Manifest chain verified: {len(visited)} commits")
    return True
```

#### Detect Orphan Commit Directories

```bash
# List all commit directories
aws s3 ls s3://bucket/prefix/commits/ --recursive | grep manifest.json

# Compare with manifest chain (only those referenced by chain are valid)
```

---

### 11.2 Performance Issues

#### Analyze File Count Per Type

```bash
# Count files for a type
aws s3 ls s3://bucket/prefix/commits/ --recursive | \
  grep entities/Customer.parquet | wc -l
```

If count is high (>100), consider compaction.

#### Check Index Coverage

```bash
aws s3 cp s3://bucket/prefix/meta/indices/entities/Customer.json - | jq .
```

Look for:

- `max_indexed_commit` close to head (good)
- Large number of per-commit entries (consider compaction)

#### Profile DuckDB Query

```python
# Enable profiling
duck_conn.execute("PRAGMA enable_profiling")
duck_conn.execute("PRAGMA profiling_output='query_profile.json'")

# Run query
result = duck_conn.execute(query).fetchall()

# View profile
import json
with open("query_profile.json") as f:
    print(json.dumps(json.load(f), indent=2))
```

Look for:

- `PARQUET_SCAN` with `Filter Pushdown: true` (good)
- Large number of files scanned (consider compaction)

---

### 11.3 Lock/Lease Problems

#### Check Current Lock Holder

```bash
aws s3 cp s3://bucket/prefix/meta/locks/ontology_write.json - | jq .
```

#### Detect Expired Lock

```python
import json
from datetime import datetime, timezone

lock = json.loads(...)
expires = datetime.fromisoformat(lock["expires_at"])
now = datetime.now(timezone.utc)

if now >= expires:
    print(f"Lock expired {(now - expires).total_seconds()}s ago")
else:
    print(f"Lock valid for {(expires - now).total_seconds()}s more")
```

#### Force Lock Release (Emergency)

```bash
# CAUTION: Only if you're certain no writer is active
aws s3 rm s3://bucket/prefix/meta/locks/ontology_write.json
```

---

### 11.4 Index Issues

#### Verify Index Lag

```bash
# Get head commit
HEAD=$(aws s3 cp s3://bucket/prefix/meta/head.json - | jq -r .commit_id)

# Check index coverage
aws s3 cp s3://bucket/prefix/meta/indices/entities/Customer.json - | \
  jq -r ".max_indexed_commit, \"Head: $HEAD\""
```

#### Diagnose Missing Latest Entry

```python
head = read_json(f"{prefix}/meta/head.json")
head_manifest = read_json(f"{prefix}/{head['manifest_path']}")
idx = read_json(f"{prefix}/meta/indices/entities/Customer.json")

# Check if head touched Customer
customer_files = [
    f for f in head_manifest["files"]
    if f["kind"] == "entity" and f["type_name"] == "Customer"
]

if customer_files:
    head_cid = head["commit_id"]
    covering_entries = [
        e for e in idx["entries"]
        if e["min_commit_id"] <= head_cid <= e["max_commit_id"]
    ]

    if not covering_entries:
        print(f"ERROR: No index entry covers head commit {head_cid}")
    else:
        # Check path match
        per_commit = [e for e in covering_entries if e["min_commit_id"] == e["max_commit_id"]]
        if per_commit and per_commit[0]["path"] != customer_files[0]["path"]:
            print(f"ERROR: Path mismatch - index stale")
```

#### Manually Repair Index

```bash
onto index repair --storage-uri s3://bucket/prefix --apply
```

---

### 11.5 Query Problems

#### Verify File Resolution for Query

```python
files = _resolve_type_files("entity", "Customer", head_commit_id, 0)
print(f"Resolved {len(files)} files:")
for f in files:
    print(f"  {f}")
```

#### Check DuckDB Query Plan

```sql
EXPLAIN
SELECT q.entity_key, q.fields_json
FROM (
  SELECT eh.entity_key, eh.fields_json,
         ROW_NUMBER() OVER (PARTITION BY eh.entity_key ORDER BY eh.commit_id DESC) AS _rn
  FROM read_parquet([...], union_by_name=true) eh
  WHERE eh.entity_type = 'Customer'
) q
WHERE q._rn = 1;
```

Look for:

- `PARQUET_SCAN` (good)
- `Filter: entity_type = 'Customer'` (pushdown working)

#### Diagnose S3 Credential Issues

```python
import boto3

s3 = boto3.client('s3')
try:
    s3.head_object(Bucket=bucket, Key=f"{prefix}/meta/head.json")
    print("S3 credentials valid")
except Exception as e:
    print(f"S3 error: {e}")
```

---

## 12. Configuration Options

| Option                   | Default | Description                   |
| ------------------------ | ------- | ----------------------------- |
| `s3_region`              | None    | AWS region for S3             |
| `s3_endpoint_url`        | None    | Custom endpoint (MinIO, etc.) |
| `s3_lock_timeout_ms`     | 5000    | Lock acquisition timeout      |
| `s3_lease_ttl_ms`        | 30000   | Lock lease duration           |
| `s3_request_timeout_s`   | 10.0    | S3 API timeout                |
| `s3_duckdb_memory_limit` | "256MB" | DuckDB memory budget          |

---

## 13. Performance Characteristics

| Operation     | Complexity                   | Notes                                            |
| ------------- | ---------------------------- | ------------------------------------------------ |
| Commit write  | O(types \* rows)             | Per-type Parquet write + manifest + index update |
| Latest query  | O(files for type)            | Index reduces file count; DuckDB dedup           |
| History query | O(files intersecting window) | Temporal pruning via index                       |
| as_of query   | O(files up to commit)        | Index + commit_id filter                         |
| Compaction    | O(total rows for type)       | Reads all, writes merged                         |

### File Count Bounds

- **Without compaction**: O(commits) files per type
- **With compaction**: O(log commits) snapshot files (configurable strategy)

---

## 14. Trade-offs & Rationale

### Why Per-Type-Per-Commit Parquet Files?

- ✅ Enables type pruning (don't scan unrelated types)
- ✅ Simpler than rolling append-bucket strategies
- ❌ More files over time (mitigated by compaction)

### Why Attempt UUIDs in Paths?

- ✅ Prevents write-write collisions under stale lease
- ✅ Orphan detection is unambiguous

### Why Lock + CAS (Not Just CAS)?

- ✅ Lock provides explicit ownership and observability
- ✅ CAS ensures linearizability even with buggy/stale clients
- ❌ More complex protocol

### Why Indices Are Advisory?

- ✅ Manifest chain is authoritative; indices are optimization
- ✅ Readers tolerate stale indices gracefully via fallback
- ❌ Index lag increases query latency

### Why DuckDB Over Arrow Dataset?

- ✅ Mature Parquet pushdown with SQL planner
- ✅ Natural mapping from existing query DSL
- ❌ Additional dependency

---

## 15. Event Bus Persistence

This section specifies the event-bus storage layer implemented in
`src/ontologia/event_store.py` for the S3 backend.

### 15.1 Key Layout

All event-bus objects are under the configured storage prefix:

| Key Pattern                                             | Purpose                                      |
| ------------------------------------------------------- | -------------------------------------------- |
| `events/{namespace}/{created_at_colon_safe}_{id}.json`  | Event envelope payload                       |
| `claims/{namespace}/{event_id}/{handler_id}.json`       | Per-handler claim/lease state                |
| `dead_letters/{namespace}/{event_id}/{handler_id}.json` | Dead-letter audit row for handler/event pair |
| `sessions/{namespace}/{session_id}.json`                | Session heartbeat and metadata               |

`created_at_colon_safe` is `created_at` with `:` replaced by `-` for S3 key
compatibility and lexical sorting in list output.

### 15.2 Object Shapes

#### Event object (`events/...`)

```json
{
  "id": "uuid",
  "namespace": "default",
  "type": "order.created",
  "payload": { "...": "..." },
  "created_at": "2026-02-11T12:00:00.000000+00:00",
  "priority": 100,
  "root_event_id": "uuid",
  "chain_depth": 0
}
```

#### Claim object (`claims/...`)

```json
{
  "event_id": "uuid",
  "handler_id": "billing.handle_order",
  "session_id": "session-uuid",
  "claimed_at": "2026-02-11T12:00:00.000000+00:00",
  "lease_until": "2026-02-11T12:00:30.000000+00:00",
  "ack_at": null,
  "attempts": 0,
  "available_at": "2026-02-11T12:00:00.000000+00:00",
  "last_error": null,
  "dead_lettered_at": null
}
```

#### Dead-letter object (`dead_letters/...`)

```json
{
  "event_id": "uuid",
  "handler_id": "billing.handle_order",
  "namespace": "default",
  "failed_at": "2026-02-11T12:05:00.000000+00:00",
  "attempts": 10,
  "last_error": "handler failure"
}
```

#### Session object (`sessions/...`)

```json
{
  "session_id": "session-uuid",
  "namespace": "default",
  "started_at": "2026-02-11T12:00:00.000000+00:00",
  "last_heartbeat": "2026-02-11T12:00:05.000000+00:00",
  "metadata": { "hostname": "...", "pid": 12345 }
}
```

### 15.3 Claim/Ack/Release Semantics (CAS-Based)

#### Claim (`claim`)

- Candidate events are discovered by listing `events/{namespace}/`, loading each
  object, filtering by requested event types, then sorting by:
  `priority DESC, created_at ASC, id ASC`.
- Claim conflict control uses S3 conditional writes:
  - first claim: `PUT` with `If-None-Match: *`
  - reclaim existing lease: `PUT` with `If-Match: <etag>`
- Reclaim is allowed only when existing claim is:
  - not acked
  - not dead-lettered
  - `lease_until <= now`
  - `available_at <= now`
- On successful claim/reclaim:
  - `session_id`, `claimed_at`, and `lease_until` are refreshed
  - `ack_at`, `last_error`, `dead_lettered_at` are cleared (`null`)
  - existing `attempts` is preserved for reclaimed rows
- Any conditional write precondition failure means another worker won the race;
  that event is skipped.

#### Ack (`ack`)

- Reads `claims/{namespace}/{event_id}/{handler_id}.json`.
- If claim object and ETag exist, sets `ack_at = now` and writes with
  `If-Match`.
- Missing claim or CAS conflict is treated as no-op.

#### Release (`release`)

- Reads claim, increments `attempts`, sets:
  - `last_error = error or "handler failure"`
  - `lease_until = now`
- Non-terminal retry:
  - `available_at = now + backoff + jitter`
  - `backoff = min(event_backoff_base_ms * 2^attempts, event_backoff_max_ms)`
  - `jitter` is uniform random `0..100ms`
- Dead-letter threshold: `attempts >= event_max_attempts`.
  - sets `dead_lettered_at = now`
  - writes `dead_letters/{namespace}/{event_id}/{handler_id}.json`
  - enqueues one `event.dead_letter` event into `events/{namespace}/...`
- Final claim update is CAS-protected (`If-Match`); CAS conflict is treated as
  no-op.

### 15.4 Sessions, Listing, Replay, Cleanup

- `register_session()` writes `sessions/{namespace}/{session_id}.json` with
  `started_at` and `last_heartbeat` set to now.
- `heartbeat()` refreshes `last_heartbeat` with CAS; if session object is
  missing, it re-registers the session using empty metadata.
- `list_sessions(namespace, session_ttl_ms)` computes
  `is_dead = (now - last_heartbeat) > session_ttl_ms`.
- `list_namespaces()` discovers namespaces from `events/` and `sessions/`
  prefixes (not `dead_letters/`), then reports counts for
  events/sessions/dead_letters per discovered namespace.
- `list_events(namespace, limit)` evaluates status from claim objects with
  precedence: `dead_lettered` > `acked` > `claimed` > `pending`.
- `list_dead_letters(namespace, limit)` reads dead-letter objects and returns
  `type` from optional `event_type`; current writer objects do not set
  `event_type`, so `type` is typically empty.
- `inspect_event(event_id, namespace?)` scans matching event objects and returns
  raw claim objects under `claims/{namespace}/{event_id}/`.
- `replay_event(namespace, event_id)` re-enqueues a copy with new UUID and
  timestamp, preserving `type`, `payload`, and `priority`, and resetting lineage
  to `root_event_id=new_id`, `chain_depth=0`.
- `cleanup_events(namespace, before)` deletes event objects with
  `created_at < before` and removes claim objects for those deleted events; it
  does **not** delete dead-letter or session objects.

### 15.5 Operational Notes

- S3 event-bus operations are object-per-record and rely on
  list/read/conditional put semantics rather than SQL transactions.
- Retry/dead-letter behavior is aligned at the control-flow level (attempt
  counters, exponential backoff, dead-letter threshold), while persistence is
  represented by JSON objects and CAS updates.
- `list_namespaces(session_ttl_ms=...)` currently ignores the TTL argument and
  reports raw session object counts.
- The emitted `event.dead_letter` in S3 release flow does not currently copy the
  failed event lineage (`root_event_id` / `chain_depth`).

---

This specification serves as the authoritative reference for understanding,
debugging, and operating the S3 storage backend.
