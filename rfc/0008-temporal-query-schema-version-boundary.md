# RFC 0008: Temporal Query Schema Version Boundary

## Status

Implemented (2026-02-12)

## Created

2026-02-12

## Summary

Restrict temporal query APIs (`as_of()`, `with_history()`, `history_since()`) to
return only rows written under the **current schema version**. This prevents
Pydantic validation failures when historical rows lack required fields
introduced by later schema migrations.

## Motivation

Temporal queries always hydrate results using the current code schema class.
When a schema version change has occurred (e.g., a mandatory field was added),
querying historical rows from older schema versions causes hydration failures —
the old JSON lacks required fields defined in the new schema.

**Example:** `Customer` v1 has `name: str`. A migration adds `age: int`
(mandatory), creating v2. Commit 1 stored `{"name": "Joe"}` under v1.
`as_of(commit_id=1)` retrieves that JSON and tries to construct
`Customer(name="Joe")` against the v2 schema → fails because `age` is required.

The append-only history tables must retain all historical rows (they are needed
for future migrations), but the query API must not expose rows it cannot
hydrate.

## Non-Goals

- Removing the temporal API entirely.
- Implementing an upgrader-chain that transforms old rows to the current schema
  at query time — complexity outweighs the benefit.
- Generating dynamic Pydantic models from stored schema fingerprints — fragile
  and undermines type safety.
- Modifying or deleting old history rows during migration.

## Decision

Temporal queries (`with_history()`, `as_of()`, `history_since()`) MUST add a
`schema_version_id` filter so that only rows written under the current schema
version for the queried type are returned.

### Mechanism

1. `Ontology` already maintains `_schema_version_ids: dict[str, int]` mapping
   each type name to its current schema version ID.
2. `QueryBuilder` receives this mapping and threads it to `EntityQuery` /
   `RelationQuery`.
3. When a temporal query mode is active, the storage layer appends
   `AND schema_version_id = ?` (SQLite) or an equivalent DuckDB predicate (S3
   backend) to the SQL query.
4. Non-temporal (latest-state) queries are unaffected — after migration, all
   latest rows have been rewritten under the current schema version.

### Implications

- `as_of(commit_id)` returns an empty result if the specified commit predates
  the current schema version for the queried type.
- `with_history()` only shows history within the current version's lifetime.
- `history_since(commit_id)` only returns current-version rows, even if older
  rows have higher commit IDs from a different type's perspective.
- The underlying append-only history tables are unchanged; historical rows
  remain available for future migrations.

## Alternatives Considered

| Alternative                                | Reason Rejected                                                                                                            |
| ------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------- |
| Remove temporal API entirely               | Discards valuable audit/diff capability within a schema version's lifetime                                                 |
| Upgrader-chain hydration at query time     | High complexity; requires running all intermediate upgraders on every historical row; performance and correctness concerns |
| Dynamic Pydantic models from stored schema | Fragile; loses static type checking; complex to implement for nested types                                                 |

## Changes

### Storage Layer

- `RepositoryProtocol.query_entities()` and `query_relations()` gain a new
  `schema_version_id: int | None = None` parameter.
- SQLite `Repository` and S3 `S3Repository` add the filter to temporal SQL
  branches when the parameter is provided.

### Query DSL

- `EntityQuery` and `RelationQuery` accept `current_schema_version_id` in their
  constructors and pass it through to the storage layer.
- `QueryBuilder` accepts `schema_version_ids: dict[str, int] | None` and looks
  up the appropriate version ID when constructing queries.

### Runtime

- `Ontology.query()` and `Session.query()` pass `schema_version_ids` to
  `QueryBuilder`.
