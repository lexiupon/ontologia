# Ontologia Examples

This directory contains executable examples demonstrating key features of
Ontologia. Each example is a self-contained Python script that showcases
specific capabilities.

## Quick Start

All examples create their database files in a local `tmp/` directory (not
`/tmp`). To run an example:

```bash
python examples/example_01_basic_usage.py
```

## Learning Paths

### 🎓 Beginner Path

Start here if you're new to Ontologia:

1. **example_01_basic_usage** - Learn the fundamentals
2. **example_02_complete_ecommerce** - See a complete workflow
3. **example_03_advanced_querying** - Master complex queries

### 🔧 Intermediate Path

After you know the basics: 4. **example_04_relations** - Deep dive into
relations 5. **example_05_keyed_relations** - Multiple relation instances 6.
**example_06_time_travel** - Temporal queries 7. **example_07_publishers** -
Data export

### 🚀 Advanced Path

For production use cases: 8. **example_08_handlers** - Reactive programming 9.
**example_09_schema_migration** - Schema evolution 10.
**example_10_complete_queries** - All query operators 11.
**example_11_introspection** - Schema inspection 12.
**example_12_export_import** - Data portability

### 📦 Production Features (New!)

Essential for production deployments: 13. **example_13_pagination_search** -
Pagination and search 14. **example_14_error_handling** - Error handling and
retries 15. **example_15_cli_workflows** - CLI operations 16.
**example_16_s3_storage** - Cloud-native S3 backend 17.
**example_17_typed_paths_and_list_aggregations** - Structured paths and list
aggregations

## Available Examples

### Core Features

#### 1. Basic Usage (`example_01_basic_usage.py`)

**Learn**: Fundamental operations with Ontologia

**Demonstrates**:

- Defining entities with `Entity` base class and `Field[T]` annotations
- Defining relations with `Relation[Left, Right]` base class
- Loading data with `session.ensure()`
- Creating relations via `session.ensure()`
- Basic queries with `onto.query().entities()`
- Simple filtering with `.where()`
- Working with typed Entity instances

**Prerequisites**: None

**Run**: `python examples/example_01_basic_usage.py`

---

#### 2. Complete E-Commerce Pipeline (`example_02_complete_ecommerce.py`)

**Learn**: Complete end-to-end workflow combining multiple features

This example demonstrates the complete Ontologia feature set in a single
cohesive example. Unlike other examples that focus on individual features, this
shows how they work together.

**Demonstrates**:

- Entity definitions (Customer, Order, Product)
- Relation definitions (PlacedOrder, Contains)
- Data loading with `session.ensure()`
- Querying with filters and aggregations
- Multi-hop traversals via relations
- Complete e-commerce scenario

**Prerequisites**: example_01_basic_usage

**Run**: `python examples/example_02_complete_ecommerce.py`

---

#### 3. Advanced Querying (`example_03_advanced_querying.py`)

**Learn**: Complex queries, traversals, and aggregations

**Demonstrates**:

- Multi-hop traversals with `.via()`
- Complex filtering with `.where()` and compound conditions
- Grouping data with `.group_by()`
- Filtering groups with `.having()`
- Combining multiple query operations
- Edge queries for relation attributes

**Prerequisites**: example_01_basic_usage

**Run**: `python examples/example_03_advanced_querying.py`

---

#### 4. Relations (`example_04_relations.py`)

**Learn**: Relation features and edge queries

**Demonstrates**:

- Directed `left -> right` relations with `Relation[L, R]`
- Undirected/bidirectional relations
- Relation attributes with `Field[T]` annotations
- Edge-centric queries with `.relations()`
- Filtering relations by attributes

**Prerequisites**: example_01_basic_usage, example_03_advanced_querying

**Run**: `python examples/example_04_relations.py`

---

#### 5. Keyed Relations (`example_05_keyed_relations.py`) 🆕

**Learn**: Multiple relation instances per endpoint pair

**Demonstrates**:

- `Field(instance_key=True)` for keyed relations
- Multiple concurrent instances between same endpoints
- Employment stints scenario (rehires, promotions)
- Querying keyed relations by instance key
- Comparing keyed vs unkeyed relations

**Prerequisites**: example_01_basic_usage, example_04_relations

**Run**: `python examples/example_05_keyed_relations.py`

---

#### 6. Time Travel (`example_06_time_travel.py`)

**Learn**: Temporal querying and audit trails

**Demonstrates**:

- Creating and updating entities over time
- Querying historical state with `.as_of(commit_id)`
- Tracking commit history
- Document lifecycle: draft → published → archived
- Use case: Audit trails and debugging

**Prerequisites**: example_01_basic_usage

**Run**: `python examples/example_06_time_travel.py`

---

#### 7. Publishers (`example_07_publishers.py`)

**Learn**: Exporting query results to external formats

**Demonstrates**:

- Manual CSV export with Python csv module
- Manual NDJSON export with Python json module
- Exporting filtered query results
- Exporting aggregated data
- Use case: Data integration (NDJSON for APIs)
- Use case: Reporting (CSV for spreadsheets)

**Prerequisites**: example_01_basic_usage, example_03_advanced_querying

**Run**: `python examples/example_07_publishers.py`

---

### Advanced Features

#### 8. Handlers (`example_08_handlers.py`) 🆕

**Learn**: Reactive programming with event handlers

**Demonstrates**:

- Custom typed `Event` classes
- `@on_event(EventType)` handlers
- `HandlerContext` for intents, metadata, and event emission
- Explicit handler commits with `ctx.commit()`
- Optional periodic scheduling with `Schedule`
- `session.run([handlers])` for execution
- Event chaining with `ctx.emit(...)`

**Prerequisites**: example_01_basic_usage through example_04_relations

**Run**: `python examples/example_08_handlers.py`

---

#### 9. Schema Migration (`example_09_schema_migration.py`) 🆕

**Learn**: Schema evolution and data transformation

**Demonstrates**: (Planned feature - API specification)

- `onto.migrate(dry_run=True)` for previewing changes
- `@upgrader` decorator for data transformation
- Token-based apply workflow for safety
- Multi-version jumps with chained upgraders
- Schema versioning and compatibility

**Prerequisites**: example_01_basic_usage through example_04_relations

**Run**: `python examples/example_09_schema_migration.py`

---

#### 10. Complete Queries (`example_10_complete_queries.py`) 🆕

**Learn**: All query operators and patterns

**Demonstrates**:

- All comparison operators: `==`, `!=`, `>`, `<`, `>=`, `<=`
- String operations: `startswith()`, `endswith()`, `contains()`
- Collection operations: `in_()`
- Null checks: `is_null()`, `is_not_null()`
- Boolean checks: `is_true()`, `is_false()`
- Logical operators: `&`, `|`, `~` (AND, OR, NOT)
- Complete aggregation suite: `avg()`, `min()`, `max()`
- Metadata access with `obj.meta()`

**Prerequisites**: example_01_basic_usage, example_03_advanced_querying

**Run**: `python examples/example_10_complete_queries.py`

---

### Observability & Operations

#### 11. Introspection (`example_11_introspection.py`)

**Learn**: Schema inspection and drift detection

**Demonstrates**: (Planned feature - API specification)

- Schema fingerprinting with `schema_hash()`
- Drift detection with `diff_registry_vs_db()`
- Comparing schemas across environments
- Use case: CI/CD validation
- Use case: Environment verification
- Use case: Schema version control

**Prerequisites**: example_01_basic_usage through example_07_publishers

**Run**: `python examples/example_11_introspection.py`

---

#### 12. Export/Import (`example_12_export_import.py`)

**Learn**: Backup, migration, and data sharing

**Demonstrates**: (Planned feature - API specification)

- Exporting ontology with `export()`
- Understanding manifest format (JSON metadata)
- JSONL format (newline-delimited JSON)
- Importing data with `import_data()`
- Data integrity verification
- Use case: Automated backups
- Use case: Test fixtures
- Use case: Cross-environment migration

**Prerequisites**: example_01_basic_usage through example_07_publishers

**Run**: `python examples/example_12_export_import.py`

---

#### 13. Pagination & Text Search (`example_13_pagination_search.py`)

**Learn**: Paginated queries and full-text search operations

**Demonstrates**:

- Basic pagination with `limit()` and `offset()`
- Result ordering with `order_by()` (ascending and descending)
- String search operators: `startswith()`, `endswith()`, `contains()`
- Multi-value filtering with `in_()`
- Cursor-based pagination for performance on large datasets
- Field indexing with `Field(index=True)` for search performance
- Comparing indexed vs non-indexed query performance

**Prerequisites**: example_01_basic_usage, example_03_advanced_querying

**Run**: `python examples/example_13_pagination_search.py`

---

#### 14. Error Handling (`example_14_error_handling.py`)

**Learn**: Production-grade error handling and session management

**Demonstrates**:

- `ConcurrentWriteError` catching and retry logic with exponential backoff
- `ValidationError` handling for constraint violations
- `Field(unique=True)` constraints for enforcing uniqueness
- `session.rollback()` for transaction abort and recovery
- `session.clear()` for memory management in long-running sessions
- Context manager pattern for safe session handling
- Graceful error handling patterns for production use

**Prerequisites**: example_01_basic_usage

**Run**: `python examples/example_14_error_handling.py`

---

#### 15. CLI Workflows (`example_15_cli_workflows.sh`)

**Learn**: Command-line tools for operations and data management

**Demonstrates**:

- `onto init sqlite` - Initialize SQLite storage
- `onto show-schema` - Display entity/relation schema in YAML/JSON
- `onto export` - Export data to NDJSON and CSV formats
- `onto import` - Import data with validation
- Storage URL formats: `sqlite:///path` and `s3://bucket/prefix?options`
- Exit codes and error handling
- S3 URL documentation with AWS regions and profiles

**Prerequisites**: None (standalone shell script)

**Run**: `bash examples/example_15_cli_workflows.sh`

---

#### 16. S3 Storage Backend (`example_16_s3_storage.py`)

**Learn**: Cloud-native storage for distributed systems

**Demonstrates**:

- `S3StorageConfig` initialization with bucket and prefix
- `endpoint_url` parameter for MinIO/LocalStack testing
- `lease_duration_seconds` for lock tuning in distributed systems
- Multi-writer scenarios with simulated worker threads
- Distributed lock mechanics and lock contention handling
- Production deployment patterns for serverless/Kubernetes
- SQLite vs S3 storage comparison

**Prerequisites**: example_01_basic_usage, example_14_error_handling

**Run**: `python examples/example_16_s3_storage.py`

**Note**: Requires MinIO for S3 testing (optional):

```bash
docker run -p 9000:9000 minio/minio server /data
```

---

#### 17. Structured Paths and List Aggregations (`example_17_typed_paths_and_list_aggregations.py`) 🆕

**Learn**: Querying structured payloads with nested paths and list predicates

**Demonstrates**:

- Structured field annotations with `TypedDict` and `list[TypedDict]`
- Nested path filters with `.path("a.b")` and bracket sugar `["a"]["b"]`
- Existential list predicates with `.any_path(...)`
- Path-aware scalar aggregations (for example `.avg(field.path(...))`)
- List-aware helpers: `.count_where(...)` and `.avg_len(...)`
- Endpoint nested filtering (for example `left(RelationType).field.path(...)`)

**Prerequisites**: example_03_advanced_querying, example_10_complete_queries

**Run**:
`uv run python examples/example_17_typed_paths_and_list_aggregations.py`

---

## Feature Status

### ✅ Currently Available

- Entity and Relation definitions
- Session API with `ensure()`
- Event runtime with `Event`, `@on_event`, and `Schedule`
- Query API (entities, relations, traversals, filters, aggregations)
- Structured nested path filters and list existential predicates
- Path/list aggregation helpers (`count_where`, `avg_len`)
- Pagination with `limit()` and `offset()`
- Ordering with `order_by()`
- Text search: `startswith()`, `endswith()`, `contains()`
- Field constraints: `index=True`, `unique=True`, `nullable=True`
- Time travel with `as_of()`
- Manual data export (CSV, NDJSON)
- Error handling: `ConcurrentWriteError`, `ValidationError`
- Session management: `rollback()`, `clear()`
- Field indexing for query performance

### 🚧 Planned Features (API Specified)

The following features have specified APIs but are not yet implemented:

- Schema migration (`onto.migrate()`, `@upgrader`)
- Query introspection (`query.explain()`)
- Schema introspection (`onto.schema_hash()`, `onto.diff_registry_vs_db()`)
- Export/Import (`onto.export()`, `onto.import_data()`)

Examples for planned features demonstrate the intended API and use cases.

## Tips

1. **Start Simple**: Begin with example_01 and work your way up
2. **Check Prerequisites**: Each example lists what you should know first
3. **Clean Database**: Examples reuse database files in `tmp/` - delete them to
   start fresh
4. **Read the Spec**: For deep dives, see `spec/api.md` and `spec/cli.md`
5. **Check WRKs**: Implementation details are in `docs/wrk/` and `rfc/`

## Troubleshooting

**ImportError: No module named 'ontologia'**

- Make sure you're in the virtual environment: `source .venv/bin/activate`
- Or use: `uv run python examples/example_01_basic_usage.py`

**Database locked errors**

- Delete the `tmp/` directory to reset all examples
- Or delete specific database files: `rm tmp/*.db`

**Examples seem to run slowly**

- First run creates database schemas
- Subsequent runs are faster
- Each example is independent
