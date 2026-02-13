# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.4.0] - 2026-02-12

### Added

- Storage engine versioning and backend-aware dispatch for SQLite and S3 with
  explicit `v1`/`v2` engine metadata and compatibility fallback for legacy
  storages.
- SQLite v2 repository implementation with type layout catalog, activation
  tracking, and current-schema-only temporal typed reads.
- S3 v2 engine support with engine metadata, versioned commit file paths, and
  type layout catalog support.
- Operator-facing engine controls and visibility:
  - `onto init --engine-version`
  - `storage_info()` engine metadata and type layout reporting
  - `onto info` engine version output
- New v2-focused test coverage for dispatch, activation-boundary semantics, and
  write-path enforcement.

### Changed

- Migration apply flow now writes a structured migration commit payload,
  rewrites rows under the new schema version id, and activates the new schema
  version layout when supported by the backend.
- Default engine for new SQLite storages is now `v2`, including `:memory:`.
- `as_of` reads before activation boundaries now return empty results with
  operator-visible diagnostics in CLI query/export text output.

## [0.2.0] - 2026-02-11

### Added

- Core data model: typed Entity and Relation schemas with Field descriptors,
  primary/instance keys
- Intent-based reconciliation: declarative state via ensure(), automatic delta
  computation
- Append-only storage: atomic commits with monotonic IDs, full history
  retention, schema versioning
- Dual backends: SQLite with table-based locking, S3 with Parquet files and
  distributed lease locks
- Type-safe queries: entity/relation lookups, endpoint filtering, multi-step
  traversals, aggregations, temporal reads
- Filter DSL: comparisons, string operations, collections, null/boolean checks
  with logical composition
- Reactive handlers: @on_schedule, @on_commit, @on_commit_entity,
  @on_commit_relation with priority ordering, self-trigger control, and commit
  metadata
- Schema migration: preview-then-apply workflow, @upgrader data transforms,
  deterministic tokens
- Batch safety: configurable max_batch_size limits and max_commit_chain_depth
  loop prevention
- CLI operator console: query, verify, migrate, export, import, schema
  introspection, and index maintenance
- Safety-first design: dry-run defaults, token verification, lifecycle-based
  soft deletes
