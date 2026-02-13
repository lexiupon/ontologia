# RFC 0006: Structured Typed Fields with Safe Path Queries and Aggregations

## Status

Draft

## Created

2026-02-12

## Summary

This RFC extends Ontologia field typing and query ergonomics for structured JSON
payloads while staying compatible with the architecture in `spec/vision.md`.

The proposal adds:

- First-class support for `Field[TypedDict]` and `Field[list[TypedDict]]`.
- Safe nested-path query syntax from top-level `Field` anchors:
  - `field.path("a.b.c")`
  - `field["a"]["b"]["c"]` (sugar)
- Deterministic schema serialization for nested type drift detection.
- Path-aware scalar aggregations over nested fields.
- A clearly-scoped existential list predicate API (`any_path`) with explicit
  rollout gates for backend parity.

This RFC does **not** change traversal shape, relation identity semantics, or
append-only history semantics.

## Motivation

Ontologia already stores JSON-serializable field values, but query ergonomics
are mostly flat-field oriented. Teams currently choose between:

- strong top-level query DSL (`Field[...]`), or
- rich nested payloads (`dict`) with weak query ergonomics and weaker static
  contracts.

`TypedDict` gives compile-time shape guarantees. Path-based query operators keep
execution explicit and backend-compilable.

## Compatibility with Vision

This RFC is designed to align with `spec/vision.md`:

- **Typed Query Construction** remains schema-driven and type anchored
  (`spec/vision.md:186`).
- **Traversal** remains lookup-only and path-result based; no traversal API
  shape changes (`spec/vision.md:195`).
- **Aggregation availability** remains on entity/relation queries only
  (`spec/vision.md:204`).
- **Schema drift and migration guarantees** are preserved by introducing
  deterministic nested type schema representation (`spec/vision.md:106`,
  `spec/vision.md:114`).

## Non-Goals

- Replacing relation modeling for facts with independent identity/history.
- Compiling arbitrary Python lambdas to SQL.
- Supporting arbitrary class-object values inside field payloads.
- Introducing backend-specific behavior without parity requirements.

## Proposal

### 1. Type Model

#### 1.1 Supported top-level field shapes

New supported shapes:

- `Field[MyTypedDict]`
- `Field[list[MyTypedDict]]`

Already-supported shapes remain valid:

- primitive-like scalar fields (`str`, `int`, `float`, `bool`, `None`,
  `datetime`, unions/optionals already accepted by current type system)
- `Field[list[primitive]]`
- `Field[dict[str, primitive]]`

Inside `TypedDict`, members are plain Python types (not `Field[...]`).

```python
from typing import TypedDict

class Geo(TypedDict):
    lat: float
    lng: float

class Address(TypedDict):
    city: str
    geo: Geo

class Profile(TypedDict):
    address: Address

class User(Entity):
    id: Field[str] = Field(primary_key=True)
    profile: Field[Profile]
```

#### 1.2 Nested and recursive types

Nested `TypedDict` is supported to arbitrary depth.

Recursive definitions are allowed at type level, but query operators only target
explicit user-selected scalar paths. Schema serialization MUST use reference
nodes (`ref`) to avoid infinite recursion in the `type_spec` tree (see §2).

**Recursive type example:**

```python
class TreeNode(TypedDict):
    label: str
    children: list["TreeNode"]
```

The canonical `type_spec` for `Field[TreeNode]`:

```json
{
  "kind": "typed_dict",
  "name": "TreeNode",
  "total": true,
  "fields": {
    "label": { "kind": "primitive", "name": "str" },
    "children": {
      "kind": "list",
      "item": { "kind": "ref", "name": "TreeNode" }
    }
  }
}
```

**`ref` resolution rules:**

- The `ref` identifier is the `TypedDict.__name__` attribute. If the name is
  ambiguous (i.e. two distinct `TypedDict` classes share the same `__name__` in
  the same schema), the identifier MUST be qualified as
  `"{module}.{qualname}"` (e.g., `"myapp.models.TreeNode"`).
- Schema comparison (drift detection) MUST resolve `ref` nodes structurally:
  two types are considered equal if and only if expanding all `ref` nodes
  produces structurally identical infinite trees. In practice this is
  implemented by comparing `type_spec` trees with a visited-set that treats
  back-edges to already-seen `ref` names as equal.

#### 1.3 `dict[str, T]` path queries

Fields typed as `dict[str, T]` (where `T` is a supported primitive or nested
type) already support path queries. Path expressions on `dict`-typed fields
compile identically to `TypedDict` paths — `json_extract(fields_json,
'$.metrics.spend')` works regardless of whether `metrics` is a `TypedDict` or
`dict[str, float]`.

The key difference: **`dict[str, T]` paths are NOT schema-validated.** The
schema records the field's `type_spec` as `{"kind": "dict", "key": ...,
"value": ...}` without enumerating allowed keys. Path correctness is the
caller's responsibility. This is consistent with Python's `dict` semantics —
key existence is a runtime property, not a type-level one.

### 2. Schema Serialization and Drift Detection (Critical)

Current schema serialization (`str(annotation)`) is insufficient to detect
nested `TypedDict` drift reliably. This RFC requires canonical nested type
specs.

For each field, schema metadata MUST include:

- `type`: legacy string (kept for readability/backward compatibility)
- `type_spec`: canonical structured type tree

Canonical `type_spec` node kinds:

- `primitive` (e.g. `str`, `int`, `float`, `bool`, `datetime`, `none`, `any`)
- `list`
- `dict`
- `union`
- `typed_dict`
- `ref` (for recursive/named type reuse)

`typed_dict` nodes MUST include deterministic field maps and required/optional
key metadata.

Canonicalization requirements:

- object keys MUST be serialized in sorted order for hashing
- union member specs MUST be normalized to deterministic order
- recursive references MUST use stable `ref` identifiers

Illustrative `type_spec` for `Field[Profile]`:

```json
{
  "kind": "typed_dict",
  "name": "Profile",
  "total": true,
  "fields": {
    "address": {
      "kind": "typed_dict",
      "name": "Address",
      "total": true,
      "fields": {
        "city": { "kind": "primitive", "name": "str" },
        "geo": {
          "kind": "typed_dict",
          "name": "Geo",
          "total": true,
          "fields": {
            "lat": { "kind": "primitive", "name": "float" },
            "lng": { "kind": "primitive", "name": "float" }
          }
        }
      }
    }
  }
}
```

Schema hashing and drift checks MUST include `type_spec`. Nested changes MUST
produce schema diffs and follow the existing explicit migration path.

#### 2.1 Recursive type `ref` identifiers

The `type_spec` serializer MUST track a visited-set of `TypedDict` classes
during traversal:

1. **First visit** to a `TypedDict` class: emit the full `typed_dict` node
   (including all fields) and record the class name in the visited-set.
2. **Subsequent visits** to the same class (back-edge): emit
   `{"kind": "ref", "name": "<TypedDict.__name__>"}`.

This ensures that the `type_spec` tree is always finite and JSON-serializable,
even for mutually recursive types. Deserialization reverses the process by
maintaining a name→node registry built during the first-pass expansion.

#### 2.2 Union member ordering

For `Union[A, B, C]` types, the `union` node's `members` array MUST contain
member `type_spec` objects sorted by their canonical JSON serialization
(deterministic string comparison). This guarantees that `Union[str, int]` and
`Union[int, str]` produce identical `type_spec` trees and therefore identical
schema hashes.

Sorting is performed on the serialized JSON string of each member node (using
sorted keys) to ensure a total, stable order.

#### 2.3 Schema upgrade migration for `type_spec`

Existing schemas stored before this RFC will lack `type_spec` entries. The
following rules govern the upgrade path:

1. **Synthesis on validation:** When the validator encounters a stored field
   that has a `"type"` string but no `"type_spec"`, it MUST attempt to
   synthesize a `type_spec` from the stored `"type"` string.

2. **Synthesis heuristic:** The synthesizer recognizes common patterns:
   - `"<class 'str'>"` → `{"kind": "primitive", "name": "str"}`
   - `"<class 'int'>"` → `{"kind": "primitive", "name": "int"}`
   - `"<class 'float'>"` → `{"kind": "primitive", "name": "float"}`
   - `"<class 'bool'>"` → `{"kind": "primitive", "name": "bool"}`
   - `"list[str]"` → `{"kind": "list", "item": {"kind": "primitive", "name": "str"}}`
   - `"typing.Optional[str]"` → `{"kind": "union", "members": [{"kind": "primitive", "name": "str"}, {"kind": "primitive", "name": "none"}]}`
   - (similar patterns for other primitive wrappers)

3. **Synthesis succeeds and matches:** If the synthesized `type_spec` is
   structurally equal to the code-defined `type_spec` → no drift is reported.
   This prevents false schema change alarms on first upgrade.

4. **Synthesis fails:** If the stored `"type"` string does not match any
   recognized pattern → the field is considered changed → drift is reported.
   This is the safe-by-default behavior — unrecognized types require explicit
   migration.

5. **Permanent storage:** The first successful schema storage after upgrade
   writes `type_spec` permanently alongside `type`. No implicit data rewrite
   of entity payloads occurs — only schema metadata is updated.

### 3. Safe Path API and Grammar

#### 3.1 Query API

Add path composition on `FieldProxy`:

- `field.path("a.b.c")` — returns a **new `FieldProxy`** instance
- `field["a"]["b"]["c"]` — syntactic sugar, also returns a **new `FieldProxy`**

Both produce a `FieldProxy` whose `_field_path` is extended with the
sub-path segments:

```python
# FieldProxy.path implementation sketch
def path(self, sub_path: str) -> "FieldProxy":
    _validate_path(sub_path)
    proxy = FieldProxy(f"{self._field_path}.{sub_path}")
    return proxy

# FieldProxy.__getitem__ implementation sketch
def __getitem__(self, segment: str) -> "FieldProxy":
    _validate_segment(segment)
    proxy = FieldProxy(f"{self._field_path}.{segment}")
    return proxy
```

The returned proxy inherits all comparison operators from `FieldProxy`
(`__eq__`, `__ne__`, `__gt__`, `__ge__`, `__lt__`, `__le__`, `in_`,
`is_null`, `is_not_null`). Each operator produces a `ComparisonExpression`
with the extended `_field_path`.

**Why this works through existing compilation:** The extended path
(e.g., `"$.profile.address.city"`) flows into `_compile_comparison` which
injects `field_name` into `json_extract(fields_json, '$.{field_name}')`.
Since `field_name` is extracted by stripping the `$.` prefix, a path like
`"profile.address.city"` compiles to
`json_extract(eh.fields_json, '$.profile.address.city')` — which SQLite
and DuckDB both evaluate as nested JSON extraction with no code changes
to the compilation layer.

#### 3.2 Path grammar and validation scope

To keep compilation safe and predictable, typed DSL paths MUST use a restricted
segment grammar:

- non-empty path string
- segment regex: `[A-Za-z_][A-Za-z0-9_]*`
- separator: `.`
- no wildcards
- no filters
- no quoted JSONPath expressions

Invalid segments MUST raise `ValueError` at query-build time.

**Validation scope in Phase 1: grammar-only.**

Path validation checks syntactic correctness (regex conformance) but does NOT
verify that the path exists in the field's `TypedDict` schema. This is
consistent with existing `FieldProxy` behavior — the current implementation does
not validate that field names referenced in queries actually exist on the entity
class. Schema-aware path validation (checking that `"address.city"` is a valid
path through the `Profile` TypedDict) is out of scope for this RFC and may be
proposed separately.

**Ready-to-implement validation helpers:**

```python
import re

_SEGMENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

def _validate_segment(segment: str) -> None:
    """Validate a single path segment. Raises ValueError if invalid."""
    if not segment or not _SEGMENT_RE.match(segment):
        raise ValueError(
            f"Invalid path segment: {segment!r}. "
            f"Segments must match [A-Za-z_][A-Za-z0-9_]*"
        )

def _validate_path(path: str) -> None:
    """Validate a dotted path string. Raises ValueError if invalid."""
    if not path:
        raise ValueError("Path must be non-empty")
    for segment in path.split("."):
        _validate_segment(segment)
```

#### 3.3 Path compilation safety

Path strings MUST be parsed into validated segments and compiled from segments,
not injected verbatim into SQL path literals.

### 4. Query Semantics

#### 4.1 Nested scalar filters

Examples:

```python
User.profile.path("address.city") == "SF"
User.profile["address"]["geo"]["lat"] > 37.0
```

These compile to nested JSON extraction for entity, relation, and endpoint
contexts.

#### 4.2 List existential filters

For `Field[list[TypedDict]]`, add existential path predicate:

```python
User.events.any_path("kind") == "click"
User.events.any_path("payload.geo.lat") > 37.0
```

Semantics:

- true when **at least one** array element satisfies the comparison.

##### 4.2.1 `AnyPathProxy`

`AnyPathProxy` is a **distinct class** (not a subclass of `FieldProxy`) because
the underlying SQL compilation pattern is fundamentally different — existential
predicates compile to `EXISTS` subqueries (or `unnest`-based joins), not scalar
`json_extract` expressions.

`AnyPathProxy` stores two path components:
- `_list_field_path: str` — the JSON path to the list field (e.g., `"$.events"`)
- `_item_path: str` — the path within each list item (e.g., `"kind"` or `"payload.geo.lat"`)

**Full interface:**

```python
class AnyPathProxy:
    _list_field_path: str   # e.g., "$.events"
    _item_path: str         # e.g., "kind"

    def __eq__(self, value) -> ExistsComparisonExpression: ...
    def __ne__(self, value) -> ExistsComparisonExpression: ...
    def __gt__(self, value) -> ExistsComparisonExpression: ...
    def __ge__(self, value) -> ExistsComparisonExpression: ...
    def __lt__(self, value) -> ExistsComparisonExpression: ...
    def __le__(self, value) -> ExistsComparisonExpression: ...
    def in_(self, values) -> ExistsComparisonExpression: ...
    def is_null(self) -> ExistsComparisonExpression: ...
    def is_not_null(self) -> ExistsComparisonExpression: ...
```

Each operator returns an `ExistsComparisonExpression` (see §4.2.2).

**`FieldProxy.any_path` implementation sketch:**

```python
# On FieldProxy
def any_path(self, sub_path: str) -> AnyPathProxy:
    _validate_path(sub_path)
    return AnyPathProxy(
        list_field_path=self._field_path,
        item_path=sub_path,
    )
```

**Error behavior:** Calling `any_path` on a field that is not typed as a list
MUST raise `TypeError` at query-build time with a message indicating that
existential predicates require list-typed fields.

##### 4.2.2 `ExistsComparisonExpression`

A new AST node extending `FilterExpression` for existential list predicates:

```python
@dataclass
class ExistsComparisonExpression(FilterExpression):
    list_field_path: str  # e.g., "$.events"
    item_path: str        # e.g., "kind"
    op: str               # "==", "!=", ">", ">=", "<", "<=", "IN", "IS_NULL", "IS_NOT_NULL"
    value: Any = None
```

Because `ExistsComparisonExpression` extends `FilterExpression`, it inherits
`__and__`, `__or__`, and `__invert__` for logical composition (see §7.1):

```python
# Composable predicates
has_click = User.events.any_path("kind") == "click"
has_high_score = User.events.any_path("score") > 90
combined = has_click & has_high_score  # LogicalExpression(AND, [...])
```

**Null and empty list semantics:**

- If the list field is `NULL` (field not set): the existential predicate
  evaluates to **false**.
- If the list field is an empty `[]`: the existential predicate evaluates to
  **false** (no elements to satisfy the condition).
- Non-array values in the list field position: treated as `NULL` (predicate
  evaluates to false).

### 5. Aggregation Semantics

#### 5.1 Path-aware scalar aggregations

Existing scalar/grouped aggregations accept path-derived scalar references:

```python
session.query().entities(User).sum(User.profile.path("metrics.spend"))
session.query().entities(User).avg(User.profile.path("metrics.score"))
session.query().entities(User).group_by(User.profile.path("address.city")).agg(n=count())
```

**Aggregation pipeline note:** Nested paths flow through the existing
aggregation pipeline with no changes required. `_extract_field_name` strips the
`$.` prefix from the `FieldProxy._field_path`, producing a dotted path like
`"profile.address.city"`. This string is passed unchanged to
`aggregate_entities`, `group_by_entities`, and `AggBuilder.to_sql_expr`, all of
which inject it into `json_extract(eh.fields_json, '$.{field_name}')`. Since
SQLite and DuckDB both support nested JSON path extraction natively, no
aggregation pipeline changes are needed for Phase 1.

#### 5.2 List-aware aggregations

Two explicit list aggregations are introduced in Phase 2:

##### 5.2.1 `count_where`

`count_where` counts entities (rows) where an existential list predicate is
true. It accepts an `ExistsComparisonExpression` directly — the same expression
type produced by `any_path` comparisons (see §4.2.2). This reuses the predicate
AST rather than accepting raw operator strings.

```python
# Count users who have at least one click event
click_count = (
    session.query()
    .entities(User)
    .count_where(User.events.any_path("kind") == "click")
)
```

`count_where(predicate)` is semantically equivalent to
`.where(predicate).count()` but is provided as a convenience for aggregation
pipelines.

##### 5.2.2 `avg_len`

`avg_len` computes the average array length for a list field across matching
entities.

```python
avg_event_count = session.query().entities(User).avg_len(User.events)
```

**Null semantics for `avg_len`:**

- `NULL` field (field not set on entity) → **excluded** from the average
  (SQLite/DuckDB: `json_array_length(NULL)` returns `NULL`, `AVG` ignores
  `NULL` values).
- Empty array `[]` → length **0**, **included** in the average.
- All entities have `NULL` list field → returns `None` (SQL `AVG` over all
  NULLs returns NULL).
- Non-array value in list field position → treated as `NULL` (excluded).

### 6. Traversal Semantics

Traversal remains unchanged:

- `.via(...)` behavior is unchanged.
- traversal result shape (`Path[source, relations]`) is unchanged.
- traversal remains lookup-only (no traversal aggregations).

Only filter expressiveness is extended by allowing nested path predicates in
source and endpoint filters.

#### 6.1 Endpoint nested filter composition

Nested path queries compose naturally with endpoint filter proxies:

```python
# Endpoint filter with nested path
subscriptions = (
    session.query()
    .relations(Subscription)
    .where(left(Subscription).profile.path("city") == "SF")
    .collect()
)
```

**Compilation path:** `left(Subscription).profile.path("city")` produces a
`FieldProxy` with `_field_path = "left.$.profile.city"`. The existing
`_compile_comparison` function strips the `"left.$."` prefix (7 characters) to
extract `"profile.city"`, then compiles to
`json_extract(le.fields_json, '$.profile.city')`. This works naturally with no
changes to the endpoint compilation layer.

**`any_path` on endpoint proxies:** Using `any_path` on endpoint-derived field
proxies (e.g., `left(Subscription).events.any_path(...)`) is **NOT supported in
Phase 2**. Calling `any_path` on an endpoint proxy MUST raise `ValueError` with
a message indicating that existential list predicates on endpoint fields require
Phase 3+ backend support.

### 7. Backend Compilation and Parity Rules

#### 7.1 Filter AST dispatch

The `_compile_filter` function MUST dispatch on the new
`ExistsComparisonExpression` AST node in addition to existing
`ComparisonExpression` and `LogicalExpression` nodes:

```python
def _compile_filter(expr: FilterExpression, ...) -> str:
    if isinstance(expr, ExistsComparisonExpression):
        return _compile_exists(expr, ...)
    elif isinstance(expr, ComparisonExpression):
        return _compile_comparison(expr, ...)
    elif isinstance(expr, LogicalExpression):
        return _compile_logical(expr, ...)
    else:
        raise TypeError(f"Unknown filter expression type: {type(expr)}")
```

Cross-references: `ExistsComparisonExpression` is defined in §4.2.2.

#### 7.2 SQL compilation targets

##### 7.2.1 SQLite nested scalar (Phase 1)

Nested scalar path queries compile to `json_extract` with a dotted path:

```sql
-- User.profile.path("address.city") == "SF"
json_extract(eh.fields_json, '$.profile.address.city') = ?
```

This works today with no changes to `_compile_comparison`. The dotted path
segment (e.g., `"profile.address.city"`) is extracted by stripping the `$.`
prefix from `FieldProxy._field_path` and injected into the `json_extract` call.

##### 7.2.2 SQLite existential (Phase 2)

Existential list predicates compile to `EXISTS` subqueries using `json_each`:

```sql
-- User.events.any_path("kind") == "click"
EXISTS (
    SELECT 1
    FROM json_each(json_extract(eh.fields_json, '$.events')) AS je
    WHERE json_extract(je.value, '$.kind') = ?
)
```

**Compiler helper sketch:**

```python
def _compile_exists_sqlite(
    expr: ExistsComparisonExpression,
    table_alias: str = "eh",
) -> Tuple[str, List[Any]]:
    list_path = expr.list_field_path  # e.g., "$.events"
    item_path = expr.item_path        # e.g., "kind"
    json_each_src = f"json_extract({table_alias}.fields_json, '{list_path}')"
    item_extract = f"json_extract(je.value, '$.{item_path}')"
    condition, params = _compile_op(item_extract, expr.op, expr.value)
    sql = f"EXISTS (SELECT 1 FROM json_each({json_each_src}) AS je WHERE {condition})"
    return sql, params
```

##### 7.2.3 SQLite `avg_len`

```sql
-- avg_len(User.events)
AVG(json_array_length(json_extract(eh.fields_json, '$.events')))
```

SQLite `json_array_length(NULL)` returns `NULL`; `AVG` ignores `NULL` values.
This naturally implements the null semantics specified in §5.2.2.

##### 7.2.4 DuckDB nested scalar (Phase 1)

DuckDB uses the same `json_extract` syntax for nested paths:

```sql
json_extract(fields_json, '$.profile.address.city') = ?
```

No backend-specific changes required for Phase 1.

##### 7.2.5 DuckDB existential (Phase 2)

DuckDB uses `unnest` with `CAST` to expand JSON arrays:

```sql
-- User.events.any_path("kind") == "click"
EXISTS (
    SELECT 1
    FROM unnest(CAST(json_extract(fields_json, '$.events') AS JSON[])) AS item
    WHERE json_extract_string(item, '$.kind') = ?
)
```

Note: DuckDB uses `json_extract_string` for string comparisons (returns text
rather than JSON-quoted value). The compiler MUST use the appropriate extraction
function based on comparison type.

##### 7.2.6 DuckDB `avg_len`

```sql
AVG(json_array_length(json_extract(fields_json, '$.events')))
```

Semantics match SQLite: `json_array_length(NULL)` → `NULL`, ignored by `AVG`.

#### 7.3 In-process filter — nested path traversal

The S3 backend (`storage_s3.py`) and runtime (`runtime.py`) evaluate filters
in-process using Python dictionary operations. The current implementation uses
`row_fields.get(path[2:])` for flat field lookup — this is **broken for nested
paths** because `"profile.address.city"` is not a key in the flat dict.

**`_resolve_nested_path` helper (Phase 1, required):**

```python
def _resolve_nested_path(data: dict, dotted_path: str) -> Any:
    """Traverse nested dicts using a dotted path string.

    Returns the value at the path, or None if any segment is missing.

    Examples:
        _resolve_nested_path({"a": {"b": 1}}, "a.b") → 1
        _resolve_nested_path({"a": {"b": 1}}, "a.c") → None
        _resolve_nested_path({"a": None}, "a.b") → None
    """
    current = data
    for segment in dotted_path.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(segment)
        if current is None:
            return None
    return current
```

**Required changes:**

- `storage_s3.py`: Replace `row_fields.get(path[2:])` with
  `_resolve_nested_path(row_fields, path[2:])` in `_matches_filter`.
- `runtime.py`: Apply same replacement in any in-process filter evaluation.

**Phase 2: In-process `ExistsComparisonExpression` evaluation:**

```python
def _evaluate_exists_in_process(
    expr: ExistsComparisonExpression,
    row_fields: dict,
) -> bool:
    list_path = expr.list_field_path[2:]  # strip "$."
    items = _resolve_nested_path(row_fields, list_path)
    if not isinstance(items, list):
        return False
    for item in items:
        if not isinstance(item, dict):
            continue
        value = _resolve_nested_path(item, expr.item_path)
        if _compare(value, expr.op, expr.value):
            return True
    return False
```

#### 7.4 Compilation parity requirements

All backends (SQL-compiled AND in-process) MUST pass identical semantic parity
tests for each feature before it is considered GA. Specifically:

- Phase 1: SQLite, DuckDB, and in-process backends must return identical results
  for nested scalar path queries.
- Phase 2: All three must return identical results for existential predicates,
  `count_where`, and `avg_len`.

#### 7.5 Rollout gating

- Nested scalar paths (`path`, `[...]`) are **Phase 1 GA**.
- Existential list predicates (`any_path`, `count_where`, `avg_len`) are
  **Phase 2**, and MUST be gated behind backend parity completion.
- Non-query contexts that evaluate filters in-process (for example handler-side
  filter evaluation) MUST either implement nested/existential semantics or
  reject such filters with explicit errors until parity is implemented.

Until parity is complete, runtime MUST fail fast with a clear
`NotImplementedError`-style backend capability error for unsupported operations.

### 8. Modeling Guidance: Embedded vs Relation

This RFC does not change core modeling guidance:

- Use relations/entities when data needs independent identity, lifecycle, or
  history queries.
- Use embedded `TypedDict` fields for owned attributes that do not require
  standalone identity.

For repeatable facts across entities, relation identity rules in `vision.md`
remain primary.

### 9. Compatibility and Migration

- Existing flat-field query API remains unchanged.
- Existing JSON payload fields remain valid.
- Existing schema versions without `type_spec` are treated as legacy.

On first validation after upgrade:

- validator MUST normalize legacy stored type metadata into canonical
  `type_spec` before diffing for all supported legacy forms;
- semantically equivalent legacy and canonical forms MUST NOT trigger drift;
- true nested shape differences MUST trigger drift;
- unparsable legacy type metadata MUST fail with an explicit schema validation
  error that identifies the affected type/field;
- migrations MUST still follow preview/apply semantics;
- no implicit data rewrite is allowed.

### 10. Public API Additions

**Phase 1 — Core path query API:**

- `FieldProxy.path(path: str) -> FieldProxy` — nested path composition
- `FieldProxy.__getitem__(segment: str) -> FieldProxy` — bracket sugar
- `_validate_segment(segment: str) -> None` — single segment grammar check
- `_validate_path(path: str) -> None` — dotted path grammar check
- `_resolve_nested_path(data: dict, dotted_path: str) -> Any` — in-process
  nested dict traversal (used by S3 and runtime backends)

**Phase 2 — Existential predicates and list aggregations:**

- `AnyPathProxy` — proxy class for existential list predicates (§4.2.1)
- `ExistsComparisonExpression` — filter AST node (§4.2.2)
- `FieldProxy.any_path(path: str) -> AnyPathProxy`
- `EntityQuery.count_where(predicate: ExistsComparisonExpression) -> int`
- `EntityQuery.avg_len(field_ref) -> float | None`
- Relation-query equivalents for `count_where` and `avg_len` where applicable

### 11. Error Behavior

- invalid path grammar: `ValueError` at query build time
- `any_path` on non-list field: `TypeError`/`ValueError` at query build time
- unsupported backend feature: explicit capability error
- non-JSON-serializable payload: write-time validation/serialization error

### 12. Examples

```python
from typing import TypedDict
from ontologia import Entity, Field, count

class EventPayload(TypedDict):
    kind: str
    score: float

class Profile(TypedDict):
    city: str
    metrics: dict[str, float]

class User(Entity):
    id: Field[str] = Field(primary_key=True)
    profile: Field[Profile]
    events: Field[list[EventPayload]] = Field(default_factory=list)

# Nested scalar filter
sf_users = (
    session.query()
    .entities(User)
    .where(User.profile.path("city") == "SF")
    .collect()
)

# Sugar form
sf_users2 = (
    session.query()
    .entities(User)
    .where(User.profile["city"] == "SF")
    .collect()
)

# Path-aware scalar aggregation
avg_spend = session.query().entities(User).avg(User.profile.path("metrics.spend"))
by_city = (
    session.query()
    .entities(User)
    .group_by(User.profile.path("city"))
    .agg(n=count())
)

# Phase 2 existential filter
click_users = (
    session.query()
    .entities(User)
    .where(User.events.any_path("kind") == "click")
    .collect()
)

# Phase 2 count_where — count entities matching an existential predicate
click_count = (
    session.query()
    .entities(User)
    .count_where(User.events.any_path("kind") == "click")
)

# Phase 2 composed existential predicate
high_score_clicks = (
    session.query()
    .entities(User)
    .where(
        (User.events.any_path("kind") == "click")
        & (User.events.any_path("score") > 90)
    )
    .collect()
)

# Phase 2 list aggregation
avg_event_count = session.query().entities(User).avg_len(User.events)

# Endpoint nested filter (Phase 1 — works with nested path composition)
from ontologia import Relation, left

class Subscription(Relation):
    id: Field[str] = Field(primary_key=True)
    tier: Field[str]

sf_subscriptions = (
    session.query()
    .relations(Subscription)
    .where(left(Subscription).profile.path("city") == "SF")
    .collect()
)
```

### 13. Testing Plan

#### 13.1 Schema and migration correctness

- nested `TypedDict` shape changes are detected in schema diffs
- drift includes `type_spec`-based changes, not only annotation strings
- preview/apply migration flow remains required for nested shape changes

#### 13.2 Path safety

- valid segment paths compile
- invalid segments fail fast
- no raw path injection through compiled SQL fragments

#### 13.3 Query parity

- SQLite and S3/DuckDB backends produce equivalent results for nested scalar
  paths (e.g., `User.profile.path("address.city") == "SF"`)
- In-process `_matches_filter` in both `runtime.py` and `storage_s3.py`
  correctly resolves nested paths using `_resolve_nested_path`
- Endpoint nested filters compile correctly (e.g.,
  `left(Subscription).profile.path("city")` → `json_extract(le.fields_json, '$.profile.city')`)
- Nested path aggregations (`sum`, `avg`, `group_by` on nested paths) return
  correct results across all backends
- `_resolve_nested_path` edge cases:
  - missing intermediate key → `None`
  - intermediate value is `None` → `None`
  - intermediate value is not a dict → `None`
  - empty path segment → `ValueError` from `_validate_path`

#### 13.4 Existential/list parity (Phase 2)

- `ExistsComparisonExpression` composes with `&`, `|`, `~` (inherits from
  `FilterExpression`)
- `any_path` equivalence across SQLite, DuckDB, and in-process backends
- `count_where` correctness:
  - empty list `[]` → predicate false → not counted
  - `NULL` list field → predicate false → not counted
  - missing list field → predicate false → not counted
  - mixed list (some items match, some don't) → counted if any match
- `avg_len` null semantics:
  - `NULL` field → excluded from average
  - empty `[]` → length 0, included
  - all `NULL` → returns `None`
  - non-array value → treated as `NULL`, excluded
- `any_path` on non-list field → `TypeError` at query-build time
- `any_path` on endpoint proxy → `ValueError` at query-build time

#### 13.5 Traversal invariants

- traversal output shape unchanged
- nested filters affect source/endpoint selection only

#### 13.6 Schema migration

- **False drift prevention:** existing schemas with `"type": "<class 'str'>"`
  but no `type_spec` MUST NOT trigger false drift when code defines `type_spec`
  equivalent to `{"kind": "primitive", "name": "str"}`
- **Recursive `ref` round-trip:** serializing and deserializing a recursive
  `TypedDict` (e.g., `TreeNode`) produces structurally identical `type_spec`
  trees, with `ref` nodes in the same positions
- **Union ordering determinism:** `Union[str, int]` and `Union[int, str]`
  produce identical `type_spec` hashes
- **Unrecognized type string failure:** stored `"type"` strings that don't
  match any synthesis pattern → drift reported (not silently ignored)
- **First-write permanence:** after first successful schema storage post-upgrade,
  `type_spec` is persisted and subsequent validations use it directly

### 14. Alternatives Considered

- **Raw JSONPath strings everywhere**: rejected due to safety and portability
  risks.
- **Lambda predicates (`any(lambda ...)`)**: rejected for initial rollout due to
  opaque compilation and type-checking complexity.
- **No schema serialization upgrade**: rejected because nested drift would be
  under-detected, violating schema governance guarantees.

### 15. Decision Defaults

- Nested scalar path querying is in-scope and prioritized first (Phase 1).
- Path validation is **grammar-only** (regex check, not schema-aware) —
  consistent with existing `FieldProxy` behavior (§3.2).
- `FieldProxy.path()` and `FieldProxy.__getitem__()` return new `FieldProxy`
  instances with extended `_field_path` (§3.1).
- `AnyPathProxy` is a **distinct type** from `FieldProxy` because it compiles
  to `EXISTS` subqueries rather than scalar `json_extract` (§4.2.1).
- `ExistsComparisonExpression` is a new **AST node** extending
  `FilterExpression`, enabling logical composition via `&`/`|`/`~` (§4.2.2).
- `count_where` (not `count_any`) reuses the `ExistsComparisonExpression`
  predicate directly rather than accepting raw operator strings (§5.2.1).
- `avg_len` null semantics: `NULL` fields excluded, empty `[]` included as
  length 0, all-NULL returns `None` (§5.2.2).
- Schema serialization upgrade (`type_spec`) is mandatory for correctness;
  upgrade uses **synthesis heuristic** to prevent false drift on legacy
  schemas (§2.3).
- `_resolve_nested_path` is introduced in **Phase 1** for in-process filter
  backends (`storage_s3.py`, `runtime.py`) to fix broken nested lookups (§7.3).
- `dict[str, T]` path queries compile identically to `TypedDict` paths but
  are **not schema-validated** — key correctness is the caller's
  responsibility (§1.3).
- `any_path` on endpoint proxies is not supported in Phase 2 (§6.1).
