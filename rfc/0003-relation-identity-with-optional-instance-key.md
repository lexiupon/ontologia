# RFC 0003: Relation Identity with Optional Instance Key

## Status

Implemented (2026-02-11)

## Created

2026-02-10

## Summary

Extend relation identity to support multiplicity per endpoint pair using an
optional instance key field.

Current identity:

- `(relation_name, left_key, right_key)`

Proposed identity:

- If relation declares an instance key field:
  `(relation_name, left_key, right_key, instance_key)`
- If relation declares no instance key field:
  `(relation_name, left_key, right_key)`

No null fallback mode exists for keyed relations. If a relation has an instance
key field, that value is required, cannot be null, and cannot be empty string.

This preserves current unkeyed behavior by default and enables multiple relation
instances between the same endpoints when the domain needs it (for example,
multiple employment stints between one person and one company).

## Motivation

The current model allows only one current relation instance per endpoint pair.
This is too restrictive for repeatable or multi-instance facts, even when those
facts are naturally relations:

- Employment stints (leave and rejoin)
- Multiple contracts between the same parties
- Repeated enrollments/registrations over time

Today, the only workaround is to model each record as an entity and then connect
it with relations. That pattern is valid, but forces extra modeling overhead
when the domain still wants relation semantics and relation query ergonomics.

## Non-Goals

- Changing entity identity semantics.
- Introducing built-in hard delete/retract operations.
- Supporting composite instance keys in this RFC.
- Allowing instance key mutation semantics.

## Proposal

### 1. Relation schema rule

Relations may declare zero or one instance key attribute using
`Field(instance_key=True)`.

- Entities remain unchanged: exactly one `Field(primary_key=True)` is required.
  Entity fields must not use `instance_key`.
- Relations: `Field(instance_key=True)` is optional, and at most one such field
  may exist. Relation fields must not use `primary_key`.
- If a relation defines an instance key field:
  - field type must be `str` (consistent with entity key storage),
  - value is required on every relation write intent,
  - value cannot be `None`,
  - value cannot be empty string `""` or consist only of whitespace,
  - field type must be non-optional.

`Field(instance_key=True)` is intentionally named differently from entity
`Field(primary_key=True)` because the semantics differ: an entity primary key is
the sole identity of the record, while an instance key is an additional
discriminator alongside the endpoint pair.

If a relation declares no instance key field, behavior is exactly the same as
current spec.

### 2. Identity rule

For relation type `R`:

- If `R` is keyed: identity is `(type, left_key, right_key, instance_key)`.
- If `R` is unkeyed: identity is `(type, left_key, right_key)`.

Implications:

- Keyed relation types support multiple instances for one endpoint pair (one per
  distinct `instance_key`).
- Unkeyed relation types allow at most one instance per endpoint pair.
- A keyed relation intent missing `instance_key` is invalid.

### 3. Write/upsert semantics

`session.ensure(...)` and reconciliation continue to be upsert-by-identity.

- Match existing relation by effective identity.
- Same attrs: no-op.
- Different attrs: append new version row for that identity.
- No match: insert new relation identity.

For keyed relation types, missing, null, or empty-string `instance_key` raises
validation error at intent construction time.

Instance key is immutable. Changing it creates a different identity.

#### Field serialization

The instance key field is excluded from `model_dump()` / `fields_json`, the same
way `left_key` and `right_key` are excluded. It is stored in its own identity
column (see §4). This avoids redundant storage and keeps delta comparison scoped
to mutable attributes only.

#### Dispatch identity

Commit handlers deduplicate dispatched changes by identity. For relations, the
dispatch identity key must be a structured tuple, not a string concatenation:

- Keyed: `(type_name, left_key, right_key, instance_key)`
- Unkeyed: `(type_name, left_key, right_key)`

This avoids ambiguity from delimiter collisions in key values.

### 4. Storage/index semantics

#### Column layout

The `relation_history` table gains an `instance_key` column (`TEXT NOT NULL`).

- Keyed relation rows: `instance_key` stores the user-supplied value.
- Unkeyed relation rows: `instance_key` stores the sentinel value `""` (empty
  string).

Using a sentinel instead of `NULL` is required because SQL `NULL != NULL` in
UNIQUE constraints, which would break uniqueness enforcement for unkeyed
relations.

#### Uniqueness and indexing

A single uniqueness basis covers both keyed and unkeyed relations:

`(relation_type, left_key, right_key, instance_key)`

- Unkeyed relations: all rows share `instance_key = ""`, so the 4-tuple
  collapses to effective 3-tuple uniqueness.
- Keyed relations: distinct `instance_key` values allow multiplicity.

Primary lookup index:

`idx_relation_history_lookup(relation_type, left_key, right_key, instance_key, commit_id DESC)`

#### GROUP BY correctness (implementation note)

All existing relation queries use `GROUP BY left_key, right_key` to deduplicate
to the latest version per relation identity. Every such site must be updated to
`GROUP BY left_key, right_key, instance_key`. Failure to update any single site
would silently collapse multiple keyed instances into one arbitrary row. This is
the highest-risk implementation concern and must be verified exhaustively.

Known GROUP BY sites (as of current codebase): `query_relations`,
`group_by_relations`, `get_relations_for_entity`, `count_latest_relations`,
`iter_latest_relations`, and all `as_of` query variants.

### 5. Query and traversal semantics

#### Relation queries

`.relations(R).collect()` returns all relation instances matching the query
filters. For keyed relation types, this may include multiple instances for the
same endpoint pair. No deduplication by endpoint pair is applied — each distinct
identity is a separate result row.

#### Specific instance lookup

To retrieve a single specific relation instance, use query filters on the
instance key field. No new dedicated `get()` method is introduced for relations;
the existing query builder pattern is sufficient.

#### Traversal queries

`.via(R)` traversals resolve through relation instances. For keyed relation
types, a single endpoint pair may produce multiple traversal paths (one per
instance). The final entity collection is deduplicated by entity key, so the
same destination entity appears at most once even if reachable through multiple
relation instances.

Example: if Person p1 has two Employment stints with Company c1, then
`query().entities(Person).via(Employment).entities(Company).collect()` returns
c1 once, not twice.

#### Grouping and aggregation

`group_by_relations` and `count_latest_relations` must include `instance_key` in
their grouping/distinct clauses. For keyed relations, each instance is counted
and grouped independently.

### 6. Temporal query semantics

`as_of(commit_id)` and `with_history` queries must respect the full identity
tuple for keyed relations.

- `as_of(commit_id)`: returns the latest version of each distinct identity
  `(type, left_key, right_key, instance_key)` as of the given commit. For keyed
  relations this may return multiple rows per endpoint pair.
- `with_history`: returns all version rows. Each row's identity includes
  `instance_key`, so history for distinct instances is not interleaved.

### 7. Metadata surface

Relation metadata adds optional `instance_key`:

- `relation.meta().left_key: str`
- `relation.meta().right_key: str`
- `relation.meta().instance_key: str | None`

For keyed relation types, `instance_key` is always non-null. For unkeyed types,
it is `None`.

### 8. Schema evolution rule

Instance key definition is immutable.

Disallowed schema changes for a relation type:

- add/remove instance key field
- rename instance key field
- change instance key field type

Allowed schema changes:

- add/remove/rename/change non-instance-key relation fields

#### Migration path for unkeyed → keyed

Because adding an instance key to an existing relation type is disallowed, the
migration path for a relation that needs multiplicity after initial deployment
is:

1. Define a new keyed relation type (e.g., `EmploymentV2` with instance key).
2. Write a data migration that copies existing relation rows into the new type,
   assigning instance key values.
3. Retire the old relation type.

This is intentionally strict to preserve identity stability.

## Example

```python
class Employment(Relation[Person, Company]):
    stint_id: Field[str] = Field(instance_key=True)
    role: Field[str]
    started_at: Field[datetime]
    ended_at: Field[datetime | None] = Field(default=None)

# Same person/company, different stints -> distinct relation identities
session.ensure(
    Employment(
        left_key="person-1",
        right_key="company-1",
        stint_id="2018-2020",
        role="Engineer",
        started_at=datetime(2018, 1, 1),
    )
)
session.ensure(
    Employment(
        left_key="person-1",
        right_key="company-1",
        stint_id="2022-",
        role="Staff Engineer",
        started_at=datetime(2022, 7, 1),
    )
)
```

## Release Impact

Ontologia is pre-release. This RFC does not promise migration compatibility for
external deployed databases.

Source-level behavior impact:

- Existing unkeyed relation behavior is unchanged.
- Domains needing multiplicity can opt in by declaring one instance key field.

## Alternatives Considered

1. Keep current model and always use junction entities.
   - Rejected: forces extra entity types in common relation-shaped domains.
2. Require instance key for all relations.
   - Rejected: breaks current ergonomics and existing schemas.
3. Include all relation attributes in identity.
   - Rejected: unstable identity and poor update semantics.
4. Reuse `Field(primary_key=True)` for both entities and relations.
   - Rejected: entity PK is the sole identity; relation instance key is an
     additional discriminator alongside endpoints. Same annotation with
     different semantics causes confusion.

## Risks and Mitigations

- Risk: confusion between entity PK and relation instance key semantics.
  Mitigation: distinct annotation names (`primary_key` vs `instance_key`) and
  explicit spec language separating entity and relation constraints.
- Risk: inconsistent key encoding across validation/storage/query paths.
  Mitigation: instance key type is constrained to `str`; stored in a dedicated
  column with the same encoding as `left_key`/`right_key`.
- Risk: accidental instance key mutation attempts. Mitigation: explicit
  immutability rule and schema-evolution guardrails.
- Risk: GROUP BY sites that omit `instance_key` silently collapse keyed
  instances into one row. Mitigation: exhaustive audit of all GROUP BY /
  DISTINCT sites in storage layer during implementation. Add regression tests
  that assert correct row count for keyed relations with same endpoint pair.
- Risk: dispatch identity collisions from string-concatenated keys containing
  delimiter characters. Mitigation: dispatch identity uses structured tuples,
  not string concatenation.
- Risk: empty-string or whitespace-only instance key creates accidental identity
  collisions or confusion. Mitigation: validation rejects `""` and
  whitespace-only strings at intent construction time; sentinel `""` is reserved
  for unkeyed relations in storage.

## Rollout Tasks

### A. Update `spec/vision.md`

1. Update relation identity statements to be type-level:
   - keyed relation: `(relation_name, left_key, right_key, instance_key)`
   - unkeyed relation: `(relation_name, left_key, right_key)`
2. Update behavior rules for relation upsert identity and multiplicity support.
3. Clarify instance key nullability/empty-string rule:
   - keyed relation instance key required, non-null, non-empty
   - unkeyed relation has no instance key
4. Update metadata section to include optional relation `instance_key`.
5. Add schema-evolution rule that instance key definition is immutable.
6. Add query/traversal semantics for keyed relations (dedup at entity level).
7. Add temporal query semantics for keyed relations.

### B. Update `spec/api.md`

1. Update Relation class docs to allow optional `Field(instance_key=True)` on
   relation attributes (max one, must be `str` type).
2. Document keyed relation validation: instance key required/non-null/non-empty
   when declared.
3. Update identity definition and write-intent semantics:
   - keyed relation identity includes instance key
   - unkeyed relation identity remains `(relation_type, left_key, right_key)`
4. Document instance key exclusion from `model_dump()` / `fields_json`.
5. Update `ensure/commit` reconciliation text and examples to show multiple
   keyed relation instances for same endpoint pair.
6. Update metadata API docs to add `relation.meta().instance_key`.
7. Add normative Employment-stint example using keyed relation.
8. Add migration/schema API note: instance key definition changes are rejected.
9. Document traversal dedup behavior for keyed relations.

### C. Implementation and Validation (follow-up after spec updates)

1. Add `instance_key` column to `relation_history` table (`TEXT NOT NULL`,
   default `""`).
2. Update lookup index to include `instance_key`.
3. Add `instance_key` to `insert_relation` and `get_latest_relation` signatures.
4. Update all GROUP BY / DISTINCT sites to include `instance_key` (exhaustive
   audit required — see §4 implementation note).
5. Exclude instance key from `model_dump()` and `fields_json`, analogous to
   `left_key`/`right_key`.
6. Update dispatch identity to use structured tuple instead of string
   concatenation.
7. Add `Field(instance_key=True)` support to schema extraction and validation.
8. Add `instance_key` to Meta dataclass and hydration paths.
9. Add keyed/unkeyed uniqueness enforcement in write path and storage indexes.
10. Update migration upgrader interface to pass `instance_key` for keyed
    relations.
11. Update temporal query paths (`as_of`, `with_history`) for 4-tuple identity.
12. Add tests for:
    - backward-compatible unkeyed relation behavior
    - keyed multiplicity for same endpoint pair
    - keyed intent validation error when instance key is missing, null, or empty
      string
    - no-op/update semantics per effective identity
    - trigger dispatch dedupe behavior with keyed relations
    - schema evolution rejection when instance key definition changes
    - traversal dedup (same entity not duplicated across instances)
    - temporal queries with keyed relations (`as_of`, `with_history`)
    - `count_latest_relations` correctness for keyed relations
    - GROUP BY correctness regression (multiple instances not collapsed)

## Acceptance Criteria

- Specs (`spec/vision.md`, `spec/api.md`) consistently define keyed vs unkeyed
  relation identities.
- `Field(instance_key=True)` is the annotation for relation instance keys;
  `Field(primary_key=True)` is reserved for entities.
- Keyed relation instance key is required, non-null, and non-empty when
  declared.
- Instance key type is constrained to `str`.
- Instance key is excluded from `model_dump()` / `fields_json`.
- Instance key is stored in a dedicated column with sentinel `""` for unkeyed
  relations.
- Existing unkeyed relation behavior remains unchanged.
- Same endpoint pair can store multiple keyed relation instances.
- All GROUP BY / DISTINCT sites include `instance_key`.
- Dispatch identity uses structured tuples, not string concatenation.
- Traversal queries deduplicate destination entities across relation instances.
- Temporal queries (`as_of`, `with_history`) respect 4-tuple identity.
- Instance key definition changes are explicitly disallowed by schema evolution
  rules.
