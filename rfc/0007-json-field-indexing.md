# RFC 0007: JSON Field Indexing with SQLite Generated Columns

## Status

Proposed

## Summary

Enable efficient querying of JSON-stored entity and relation fields by declaring
index intent via `Field(index=True)` and implementing SQLite generated columns
with B-tree indexes on frequently-accessed JSON paths.

## Motivation

Current implementation stores all entity and relation fields as JSON text and
accesses them via `json_extract()` at query time. While this provides schema
flexibility, queries on indexed fields perform full table scans instead of
leveraging database indexes.

**Current gaps:**

- `Field(index=True)` declares intent but has no effect
- All field access uses `json_extract()` without indexing
- Schema has hand-written indexes on entity type/key only, not field values
- Schema changes cannot evolve indexes without manual intervention

**Benefits of this RFC:**

- Improve query performance on frequently-filtered fields
- Leverage SQLite's JSON1 generated column feature for zero-migration schema
  evolution
- Automatic index creation aligned with model definitions

## Non-Goals

- Change the append-only history storage model
- Modify filter expression AST or query builder
- Add multi-field composite indexes (single-field only)
- Automatic index recommendations or cardinality analysis

## Proposal

### 1. Schema Evolution

For entity/relation tables, add support for creating generated columns:

```sql
ALTER TABLE entity_history
ADD COLUMN IF NOT EXISTS gen_name GENERATED ALWAYS AS
  json_extract(fields_json, '$.name') STORED;

CREATE INDEX IF NOT EXISTS idx_entity_gen_name
  ON entity_history(gen_name, commit_id DESC);
```

### 2. Index Creation Flow

**Repository initialization** (`_create_tables`):

1. Create base tables as before (entity_history, relation_history, etc.)
2. After table creation, introspect all entity and relation classes
3. For each class with `Field(index=True)`:
   - Generate a generated column name: `gen_{field_name}`
   - Execute `ALTER TABLE ... ADD COLUMN ... GENERATED ALWAYS AS ...`
   - Create B-tree index on the generated column
4. Handle gracefully if columns already exist (idempotent)

**Schema migration** (when models change):

- If a field gains `index=True`, add the generated column + index on next init
- If a field loses `index=True`, no action needed (indexes are harmless
  overhead)
- Generated columns are virtual (no storage cost) until marked STORED

### 3. Query Plan Integration

Query builder (`_compile_filter`, `_entity_sql`, `_relation_sql`) should:

1. Check if a queried field has `index=True`
2. If yes, rewrite `json_extract(fields_json, '$.fieldname')` to use the
   generated column
3. Let SQLite query planner choose between generated column index or table scan

**Example optimization:**

```python
# Before: full scan
WHERE json_extract(eh.fields_json, '$.status') = 'active'

# After: uses generated column index
WHERE eh.gen_status = 'active'
```

### 4. API

No public API changes. `Field(index=True)` continues to work as is.

```python
class Order(Entity):
    __entity_name__ = "Order"
    id: Field[str]
    status: Field[str] = Field(default="pending", index=True)  # creates index
    created_at: Field[str] = Field(index=True)
```

### 5. Implementation Steps

1. **Add index tracking to Repository**:

   - Store mapping of (entity_type, field_name) → index_created
   - Check on init to avoid duplicate index creation

2. **Create index generation function**:

   - `_create_field_indexes(entity_classes, relation_classes) -> None`
   - Called after table creation in `_create_tables`
   - Iterate over field definitions, check `index=True`, execute ALTER + CREATE
     INDEX

3. **Update query compilation**:

   - In `_build_filter_predicate`, detect indexed fields
   - Map `json_extract(fields_json, '$.fieldname')` → `gen_fieldname`

4. **Add tests**:
   - Verify generated columns are created
   - Verify indexes are created
   - Verify query behavior is identical (correctness, not just performance)
   - Verify EXPLAIN QUERY PLAN shows index usage

## Migration Path

Existing databases:

- Running schema init on an existing db will detect missing generated columns
- ALTER TABLE IF NOT EXISTS prevents errors on retry
- Indexes are created incrementally as table grows
- Zero data migration needed (virtual columns in ALWAYS mode)

## Examples

```python
class Product(Entity):
    __entity_name__ = "Product"
    sku: Field[str]
    category: Field[str] = Field(index=True)
    stock: Field[int] = Field(index=True)
    archived: Field[bool] = Field(index=True)

class Purchase(Relation):
    __relation_name__ = "purchase"
    quantity: Field[int] = Field(index=True)
    shipped: Field[bool] = Field(index=True)
```

After init, these queries will benefit from indexes:

```python
# Uses idx_product_gen_category
ontology.query(Product).filter("$.category", "=", "electronics").all()

# Uses idx_product_gen_stock
ontology.query(Product).filter("$.stock", ">", 100).all()

# Uses idx_purchase_gen_quantity
ontology.query(Purchase).filter("$.quantity", ">", 5).all()
```

## Trade-offs

| Aspect           | Trade-off                                                                       |
| ---------------- | ------------------------------------------------------------------------------- |
| Storage          | Generated columns (VIRTUAL) add ~32 bytes schema overhead per field, negligible |
| Schema evolution | Adds ALTER TABLE calls on init; idempotent and safe                             |
| Query complexity | Query builder must check `Field.index` when compiling; minimal overhead         |
| Benefit          | Significant speedup on indexed field queries; enables index-driven optimization |
