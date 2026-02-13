# CLI Reference

Operator reference for the `onto` command-line interface.

## Mission

The CLI is an **operator console** for Ontologia data stores.

- Inspect health, schema, commits, and data.
- Perform controlled operational data movement (import/export).

Business logic, handler execution, and scheduling belong in application runtime
code.

## Safety Model

- `query`, `info`, `verify`, `schema export`, `schema history`, `commits`, and
  `export` are read-only.
- `migrate`, `import`, and `schema drop` are CLI write paths.
- CLI writes must support `--dry-run`.
- `migrate --apply` should require an explicit safety token (`--token`) unless
  `--force` is used.
- `migrate --force` skips token verification but must still recompute and
  validate under lock before apply.
- `import` must require explicit conflict policy and can enforce preconditions.
- `import --apply` is always atomic (single commit) for predictable outcomes.
- `schema drop --apply` must require an explicit safety token (`--token`).
- `schema drop` is type-level only; no row-level `delete`/`retract` command.
- No general-purpose surgical `update`/`patch` command is part of the default
  CLI.

## Global Options

- `--db PATH`: SQLite database file path (default: `onto.db`)
- `--storage-uri URI`: Backend storage URI (for example `sqlite:///onto.db` or
  `s3://bucket/prefix`)
- `--config PATH`, `-c PATH`: Config file path
- `--json`: JSON output when supported
- `--help`: Show help
- `--version`: Show version

## Command Tree

```text
onto
├─ init
├─ info
├─ verify
├─ compact
├─ index
│  ├─ verify
│  └─ repair
├─ schema
│  ├─ export
│  ├─ history
│  └─ drop
├─ migrate
├─ query
│  ├─ entities
│  ├─ relations
│  └─ traverse
├─ commits
│  └─ examine
├─ events
│  ├─ list-namespaces
│  ├─ sessions
│  ├─ show
│  ├─ dead-letters
│  ├─ cleanup
│  ├─ replay
│  └─ inspect
├─ export
└─ import
```

## Commands

### `onto init`

Initialize storage backend metadata/state.

```bash
# SQLite lazy-init metadata bootstrap
onto init

# Force v1 engine for explicit backward-compat setup
onto init --engine-version v1

# S3 bootstrap (dry-run plan)
onto --storage-uri s3://my-bucket/my-prefix init --dry-run
```

Options:

- `--dry-run`: Preview initialization only
- `--force`: Force re-initialization (S3)
- `--token TOKEN`: Confirmation token for `--force` (S3)
- `--engine-version v1|v2`: Engine version to initialize (default: latest
  supported)

Engine defaults:

- New SQLite storages default to `v2` (including `:memory:`).
- Existing SQLite storages without engine metadata are treated as `v1`.
- S3 storages without `meta/engine.json` are treated as `v1`.

### `onto info`

Show DB status and high-level metadata.

```bash
onto info
onto info --stats
onto info --schema
```

Options:

- `--stats`: Counts and storage stats
- `--schema`: Entity/relation schema summary

Output includes:

- `backend`
- `engine_version`
- backend-specific location fields (`db_path` or `storage_uri`/bucket/prefix)
- optional `type_layouts` for v2 engines

### `onto verify`

Verify stored schema matches code-defined schema. Read-only.

```bash
onto verify
onto verify --diff
```

Options:

- `--diff`: Show mismatches only
- `--strict`: Non-zero exit on any mismatch

Notes:

- Runtime startup is fail-fast on schema mismatch.
- Apply schema changes with explicit tooling (`onto migrate`), using dry-run
  first.

### `onto schema export`

Export schema artifacts for review, diffing, and CI artifacts.

```bash
# Code-defined schema snapshot
onto schema export --models myapp.models --output schema.json
onto schema export --models myapp.models --format yaml --output schema.yaml

# Stored schema version snapshot
onto schema export --kind entity --type Customer --version 2 --output customer-v2.json
```

Options:

- `--models PYTHON_IMPORT_PATH` (preferred schema source)
- `--models-path PATH` (alternative schema source)
- `--kind entity|relation` (stored schema source)
- `--type NAME` (stored schema source)
- `--version ID` (stored schema source; default: latest version for the type)
- `--output PATH`
- `--format json|yaml` (default: `json`)
- `--with-hash` (include stable schema fingerprint)

Notes:

- Export source mode is exclusive:
  - Code mode requires one of `--models` or `--models-path`.
  - Stored-version mode requires both `--kind` and `--type`.
- `--version` is valid only in stored-version mode.
- `--models` is preferred because it matches runtime import behavior.
- Missing type/version in stored-version mode exits non-zero and prints a
  not-found error.

### `onto schema history`

Inspect stored schema version lineage for a single type.

```bash
onto schema history --kind entity --type Customer --last 20
onto schema history --kind entity --type Customer --since-version 2
onto schema history --kind relation --type Purchase --version 3
```

Options:

- `--kind entity|relation`
- `--type NAME`
- `--last N` (default: `20`)
- `--since-version ID`
- `--version ID`

Output contract:

- List mode (`--last`, `--since-version`) returns one row/object per version
  with:
  - `type_kind`
  - `type_name`
  - `schema_version_id`
  - `schema_hash`
  - `created_at`
  - `runtime_id`
  - `reason`
- Detail mode (`--version`) returns one version object with all list fields
  plus:
  - `schema_json`
- `--json` must emit the same fields as structured JSON.
- When `--version` targets a missing version, command exits non-zero and prints
  a not-found error.

Notes:

- `--version` is mutually exclusive with `--last` and `--since-version`.

### `onto schema drop`

Drop one or more schema types for operator cleanup and mass design cleanup. This
is an administrative destructive path for removing obsolete types, not a
row-level lifecycle operation.

```bash
# Dry-run (default): drop one relation type
onto schema drop relation Subscription

# Dry-run (default): drop one entity and explicit dependent relation types
onto schema drop entity Customer --drop-relation Subscription --drop-relation Purchase

# Dry-run (default): drop one entity and all dependent relation types
onto schema drop entity Customer --cascade-relations

# Apply destructive drop (purges history rows for affected types)
onto schema drop entity Customer --cascade-relations --purge-history --apply --token <TOKEN>
```

Target:

- `entity TYPE`
- `relation TYPE`

Options:

- `--drop-relation TYPE` (repeatable; valid only when target is `entity`)
- `--cascade-relations` (include all relation types whose left/right endpoint
  references the target entity)
- `--dry-run` (default; show plan and token only)
- `--apply` (execute drop)
- `--token TOKEN` (required for apply; value from dry-run output)
- `--purge-history` (required when any affected type currently has data rows)
- `--meta KEY=VALUE` (repeatable commit/audit metadata for schema-drop apply)

Behavior:

1. Resolve affected types from target + explicit/cascaded relation drops.
2. Validate dependency rules and row-count safety rules.
3. Emit dry-run plan with token, or apply atomically under write lock.
4. On apply, execute one atomic global schema-drop admin commit and attach
   metadata from `--meta` (plus system metadata such as operation kind and
   affected types).
5. In that same atomic operation, remove schema registry and schema version
   records for affected types; with `--purge-history`, also remove affected type
   rows from `entity_history`/`relation_history`.

Notes:

- `--dry-run` and `--apply` are mutually exclusive. If neither is provided,
  command runs in dry-run mode.
- `--apply` without `--token` must fail.
- `--token` without `--apply` must fail.
- Entity drop must fail when dependent relation types exist unless they are
  included via `--drop-relation` or `--cascade-relations`.
- In `entity TYPE` mode, `--drop-relation` and `--cascade-relations` are
  mutually exclusive.
- In `relation TYPE` mode, `--drop-relation` and `--cascade-relations` are
  invalid.
- Without `--purge-history`, drop succeeds only when all affected types have
  zero rows in current materialized state.
- If any affected type has rows, command must fail and require `--purge-history`
  for apply.
- Dry-run token must bind to target and affected types, `--purge-history` mode,
  current head commit, and current schema-version heads for affected types.
- On apply, runtime must recompute the plan under lock and verify token match.
  If token mismatches, command aborts as stale.
- Successful apply MUST create exactly one global schema-drop admin commit.
- Dry-run and failed apply must create no commit.
- `--meta` keys/values are persisted on the schema-drop admin commit metadata.
- Dry-run output must include: target type, affected types, dependency warnings,
  per-type row counts, whether `--purge-history` is required, and `token`.

### `onto migrate`

Plan and optionally apply schema migration from code models to database. This
command reuses the same planner/executor as `session.migrate(...)`.

Upgrader functions are discovered from a Python module via `@upgrader`
decorators. The CLI uses `load_upgraders()` internally to scan the module and
collect upgraders keyed by `(type_name, from_version)` for chain execution.

```bash
# Dry-run (default): show migration plan, output token
onto migrate --models myapp.models --upgraders myapp.migrations

# Apply with token (from dry-run output)
onto migrate --models myapp.models --upgraders myapp.migrations --apply --token <TOKEN>

# Force apply (skip token, still recompute and validate under lock)
onto migrate --models myapp.models --upgraders myapp.migrations --apply --force

# With metadata
onto migrate --models myapp.models --upgraders myapp.migrations --apply --token <TOKEN> --meta reason=add-email-field
```

Options:

- `--models PYTHON_IMPORT_PATH` (preferred schema source)
- `--models-path PATH` (alternative schema source)
- `--upgraders PYTHON_IMPORT_PATH` (module containing `@upgrader`-decorated
  functions)
- `--dry-run` (default; show migration plan only)
- `--apply` (execute migration)
- `--token TOKEN` (required for safe apply unless `--force`; value from dry-run
  output)
- `--force` (skip token verification; recompute latest plan under lock and
  apply)
- `--meta KEY=VALUE` (repeatable migration metadata)

Notes:

- One of `--models` or `--models-path` is required.
- Without `--apply`, command runs in dry-run mode by default.
- `--force` is mutually exclusive with `--token`.
- `--apply` without `--token` must fail unless `--force` is provided.
- `--upgraders` is required when any type with schema diff has existing data
  rows. If omitted and upgraders are needed, command fails with a message
  listing which types require upgraders.
- `--upgraders` is optional when all schema diffs are schema-only (types with
  zero data rows).
- Dry-run output must include: `token`, per-type schema diffs, estimated row
  counts, types requiring upgraders, and types that are schema-only.
- Token is an opaque plain-encoded string (base64 of plan hash + head commit)
  printed to stdout. User passes it back with `--token` for apply.
- On apply, runtime recomputes plan under lock and verifies token matches. If
  token mismatches (stale preview), command aborts.
- Migration executor holds an ontology-wide lease lock with background
  keep-alive renewal. Lock timeout and lease TTL use runtime defaults
  (`lock_timeout_s=30`, `lease_ttl_s=600`).
- Concurrent readers see pre-migration state during migration.

Upgrader module convention:

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

### `onto query`

Run manual reads for entities, relations, traversals, and history.

```bash
# Entity lookup
onto query entities Customer --filter '$.tier' eq '"Gold"'

# Relation lookup
onto query relations Purchase --filter '$.left.tier' eq '"Gold"'

# Traversal lookup
onto query traverse Customer --via Purchase --via PartOf

# Historical snapshot
onto query entities Customer --as-of 120

# Changes since commit
onto query entities Customer --history-since 120
```

Aggregations should use the Python typed query builder API. Advanced path/list
query helpers (`path(...)`, `any_path(...)`, `count_where`, `avg_len`) are part
of the Python query DSL; the CLI filter flags intentionally remain a smaller,
tokenized subset.

Modes:

- `entities TYPE`
- `relations TYPE`
- `traverse ROOT_TYPE --via RELATION [--via RELATION ...]`

Common options:

- `--filter PATH OP VALUE_JSON` (repeatable, AND semantics)
- `--limit N`, `--offset N`
- `--as-of ID`
- `--with-history`
- `--history-since ID`
- `--without-relations` (traversal output)

Filter contract:

- `PATH` uses JSONPath (restricted CLI subset): `$.field`, `$.nested.field`,
  `$.left.field`, `$.right.field`.
- `OP` is one of: `eq`, `ne`, `gt`, `gte`, `lt`, `lte`, `in`, `is_null`.
- `VALUE_JSON` is JSON-typed input (`"Gold"`, `42`, `true`, `["A","B"]`).
- `--filter` consumes exactly 3 tokens: `PATH OP VALUE_JSON`.
- `VALUE_JSON` must be passed as one shell token (quote when needed).
- `--filter` can be repeated; filters are combined with logical AND.
- CLI does not support arbitrary boolean expression strings (no Python-like
  `--where` DSL). Use Python typed query API for complex OR/parentheses logic.

Boundary diagnostics:

- For v2 storages, `--as-of` earlier than a type's activation boundary returns
  empty results and emits a warning in text output.
- JSON output schema is unchanged (diagnostics are not embedded in result rows).

### `onto commits`

Inspect commit history (summary mode).

```bash
onto commits --last 20
onto commits --since 100
```

Options:

- `--last N` (default: `10`)
- `--since ID`
- `--meta KEY=VALUE` (repeatable metadata filter)

Notes:

- `--last` and `--since` can be combined. Command applies `--since` filtering
  first, then limits to `--last`.
- `--meta` applies to summary/list mode only.
- Legacy `--id` on `onto commits` switches to examine mode and is mutually
  exclusive with `--last`, `--since`, and `--meta`.

### `onto commits examine`

Inspect one commit in detail.

```bash
onto commits examine --id 245

# Backward-compatible alias
onto commits --id 245
```

Options:

- `--id ID`

Output contract:

- Summary mode (`onto commits --last|--since`) returns one row/object per commit
  with:
  - `commit_id`
  - `timestamp`
  - `operations` (number of persisted changes in the commit)
  - `meta` (key/value metadata map)
- Examine mode (`onto commits examine --id`) returns one commit object with all
  summary fields plus:
  - `changes` (array of changed identities)
  - each `changes[]` item includes:
    - `kind` (`entity` or `relation`)
    - `type_name`
    - identity keys (`key` for entities; `left_key` and `right_key` for
      relations)
    - `operation` (`insert` or `update_version`)
- `--json` must emit the same fields as structured JSON.
- Legacy `onto commits --id` behavior is equivalent to
  `onto commits examine --id`; mixing `--id` with summary-mode flags is a usage
  error.
- When `--id` targets a missing commit, command exits non-zero and prints a
  not-found error.

### `onto events`

Inspect and manage event bus namespaces, sessions, queue rows, and dead-letter
state.

```bash
onto events list-namespaces
onto events sessions --namespace billing
onto events show --namespace billing --limit 10
onto events dead-letters --namespace billing --limit 100
onto events cleanup --namespace billing --before 7d
onto events replay --namespace billing --event-id evt_123
onto events inspect --event-id evt_123
onto events inspect --event-id evt_123 --namespace billing
```

Subcommands:

- `list-namespaces`
  - No command-specific options.
- `sessions`
  - `--namespace NAME` (required)
- `show`
  - `--namespace NAME` (required)
  - `--limit N` (default: `10`)
- `dead-letters`
  - `--namespace NAME` (required)
  - `--limit N` (default: `100`)
- `cleanup`
  - `--namespace NAME` (required)
  - `--before DURATION` (required; `d`/`h`/`m` suffix, for example `7d`, `24h`,
    `30m`)
- `replay`
  - `--namespace NAME` (required)
  - `--event-id ID` (required)
- `inspect`
  - `--event-id ID` (required)
  - `--namespace NAME` (optional namespace filter)

Output contract:

- `--json` emits structured JSON for the same records shown in table/object
  mode.
- `list-namespaces`, `sessions`, `show`, and `dead-letters` return arrays.
- `cleanup` returns one object with: `namespace`, `before` (UTC timestamp), and
  `deleted`.
- `replay` returns one object with: `replayed_event_id`, `new_event_id`, and
  `namespace`.
- `inspect` returns one object for the target event and claim history; when the
  event is not found, command exits non-zero and prints a not-found error.

### `onto export`

Export ontology data as JSONL.

```bash
onto export --output out/
onto export --output out/ --as-of 200
onto export --output out/ --history-since 200 --with-metadata
```

Options:

- `--output PATH`
- `--type NAME` (optional type filter)
- `--as-of ID`
- `--history-since ID`
- `--with-metadata`

Boundary diagnostics:

- For v2 storages, `--as-of` earlier than a type's activation boundary produces
  empty exports for that type and emits a warning in text output.

### `onto import`

Controlled operational ingest. This is the main CLI write path.

```bash
# Validate only
onto import --input data/ --dry-run

# Safe apply (abort on any conflict)
onto import --input data/ --apply --on-conflict abort --meta reason=migration-2026-02-08

# Upsert apply with precondition
onto import --input data/ --apply --on-conflict upsert --precondition must_exist --meta reason=backfill
```

Options:

- `--input PATH` (file or directory)
- `--dry-run` (show planned delta and conflicts)
- `--apply` (execute write)
- `--on-conflict abort|skip|upsert` (required for apply)
- `--precondition must_exist|must_not_exist|if_commit_id:ID`
- `--meta KEY=VALUE` (repeatable commit metadata)

Behavior:

1. Load rows and validate schema/types.
2. Build typed `Ensure(...)` intents.
3. Check preconditions and conflict policy.
4. Attach commit metadata from `--meta`.
5. Compute delta and show plan (`--dry-run`) or persist one atomic commit
   (`--apply`).

## Recommended Workflows

### Schema change

```bash
onto verify --diff
onto schema export --models myapp.models --output schema.json
onto schema history --kind entity --type Customer --last 5
onto migrate --models myapp.models --upgraders myapp.migrations
onto migrate --models myapp.models --upgraders myapp.migrations --apply --token <TOKEN>
```

### Schema cleanup

```bash
onto schema drop entity Customer --cascade-relations
onto schema drop entity Customer --cascade-relations --purge-history --apply --token <TOKEN>
```

### Inspect and query

```bash
onto info --stats --schema
onto commits --last 20
onto commits examine --id 245
onto query entities Customer --filter '$.tier' eq '"Gold"'
```

### Ops data move

```bash
onto export --output backup-$(date +%Y%m%d)/
onto import --input backup-20260208/ --dry-run --on-conflict abort
onto import --input backup-20260208/ --apply --on-conflict abort --meta source=restore --meta ticket=INC-1234
```

## Exit Codes

- `0`: Success
- `1`: General error
- `2`: Usage error
- `3`: Verification/schema mismatch
- `4`: Command execution failure
- `5`: Database or transaction error
- `6`: Import precondition/conflict failure
- `7`: Schema drop dependency/safety failure

## Environment Variables

- `ONTOLOGIA_DB`
- `ONTOLOGIA_CONFIG`
- `ONTOLOGIA_LOG_LEVEL`
