"""Tests for schema versioning and migration API (RFC 0002)."""

from __future__ import annotations

import json
import sqlite3

import pytest

from ontologia import Entity, Field, Relation, Session
from ontologia.errors import (
    MigrationError,
    MigrationTokenError,
    MissingUpgraderError,
    SchemaOutdatedError,
    SchemaValidationError,
    TypeSchemaDiff,
)
from ontologia.migration import (
    MigrationPreview,
    MigrationResult,
    _compute_migration_token,
    _compute_plan_hash,
    _verify_token,
    upgrader,
)
from ontologia.storage import Repository

# --- Test entity/relation types ---


class User(Entity):
    id: Field[str] = Field(primary_key=True)
    name: Field[str]
    age: Field[int]


class Tag(Entity):
    id: Field[str] = Field(primary_key=True)
    label: Field[str]


class Tagged(Relation[User, Tag]):
    weight: Field[float] = Field(default=1.0)


# --- Phase 1: Database foundation tests ---


class TestSchemaVersionsTable:
    def test_table_created(self, tmp_db):
        repo = Repository(tmp_db)
        row = repo._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='schema_versions'"
        ).fetchone()
        assert row is not None
        repo.close()

    def test_history_tables_have_schema_version_id(self, tmp_db):
        repo = Repository(tmp_db)
        for table in ("entity_history", "relation_history"):
            cols = {r[1] for r in repo._conn.execute(f"PRAGMA table_info({table})").fetchall()}
            assert "schema_version_id" in cols
        repo.close()

    def test_create_schema_version(self, tmp_db):
        repo = Repository(tmp_db)
        vid = repo.create_schema_version(
            "entity", "User", '{"fields":{}}', "abc123", runtime_id="r1", reason="test"
        )
        assert vid == 1

        vid2 = repo.create_schema_version(
            "entity", "User", '{"fields":{"x":1}}', "def456", reason="update"
        )
        assert vid2 == 2
        repo.close()

    def test_get_current_schema_version(self, tmp_db):
        repo = Repository(tmp_db)
        assert repo.get_current_schema_version("entity", "User") is None

        repo.create_schema_version("entity", "User", '{"a":1}', "h1")
        repo.create_schema_version("entity", "User", '{"a":2}', "h2")

        current = repo.get_current_schema_version("entity", "User")
        assert current is not None
        assert current["schema_version_id"] == 2
        assert current["schema_hash"] == "h2"
        repo.close()

    def test_get_specific_schema_version(self, tmp_db):
        repo = Repository(tmp_db)
        repo.create_schema_version("entity", "User", '{"v":1}', "h1")
        repo.create_schema_version("entity", "User", '{"v":2}', "h2")

        v1 = repo.get_schema_version("entity", "User", 1)
        assert v1 is not None
        assert v1["schema_hash"] == "h1"

        assert repo.get_schema_version("entity", "User", 99) is None
        repo.close()

    def test_list_schema_versions(self, tmp_db):
        repo = Repository(tmp_db)
        repo.create_schema_version("entity", "User", '{"v":1}', "h1")
        repo.create_schema_version("entity", "User", '{"v":2}', "h2")

        versions = repo.list_schema_versions("entity", "User")
        assert len(versions) == 2
        assert versions[0]["schema_version_id"] == 1
        assert versions[1]["schema_version_id"] == 2
        repo.close()

    def test_bootstrap_from_schema_registry(self, tmp_db):
        """Existing DB with schema_registry but no schema_versions bootstraps correctly."""
        # Create a DB with schema_registry entries but no schema_versions rows
        conn = sqlite3.connect(tmp_db)
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS commits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                metadata_json TEXT
            );
            CREATE TABLE IF NOT EXISTS entity_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entity_type TEXT NOT NULL,
                entity_key TEXT NOT NULL,
                fields_json TEXT NOT NULL,
                commit_id INTEGER NOT NULL,
                FOREIGN KEY (commit_id) REFERENCES commits(id)
            );
            CREATE TABLE IF NOT EXISTS relation_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                relation_type TEXT NOT NULL,
                left_key TEXT NOT NULL,
                right_key TEXT NOT NULL,
                fields_json TEXT NOT NULL,
                commit_id INTEGER NOT NULL,
                FOREIGN KEY (commit_id) REFERENCES commits(id)
            );
            CREATE TABLE IF NOT EXISTS schema_registry (
                type_kind TEXT NOT NULL,
                type_name TEXT NOT NULL,
                schema_json TEXT NOT NULL,
                PRIMARY KEY (type_kind, type_name)
            );
            CREATE TABLE IF NOT EXISTS locks (
                lock_name TEXT PRIMARY KEY,
                owner_id TEXT NOT NULL,
                acquired_at TEXT NOT NULL,
                expires_at TEXT NOT NULL
            );
        """)
        conn.execute(
            "INSERT INTO schema_registry VALUES (?, ?, ?)",
            ("entity", "User", '{"entity_name":"User","fields":{"id":{"primary_key":true}}}'),
        )
        conn.commit()
        conn.close()

        # Open via Repository — should bootstrap schema_versions
        repo = Repository(tmp_db)
        versions = repo.list_schema_versions("entity", "User")
        assert len(versions) == 1
        assert versions[0]["schema_version_id"] == 1
        assert versions[0]["reason"] == "bootstrap"
        repo.close()

    def test_insert_entity_with_schema_version_id(self, tmp_db):
        repo = Repository(tmp_db)
        cid = repo.create_commit()
        repo.insert_entity("User", "u1", {"id": "u1", "name": "Alice"}, cid, schema_version_id=1)
        repo.commit_transaction()

        row = repo._conn.execute(
            "SELECT schema_version_id FROM entity_history WHERE entity_key = 'u1'"
        ).fetchone()
        assert row[0] == 1
        repo.close()

    def test_insert_relation_with_schema_version_id(self, tmp_db):
        repo = Repository(tmp_db)
        cid = repo.create_commit()
        repo.insert_relation("Tagged", "u1", "t1", {"weight": 1.0}, cid, schema_version_id=2)
        repo.commit_transaction()

        row = repo._conn.execute(
            "SELECT schema_version_id FROM relation_history WHERE left_key = 'u1'"
        ).fetchone()
        assert row[0] == 2
        repo.close()

    def test_count_latest_entities(self, tmp_db):
        repo = Repository(tmp_db)
        assert repo.count_latest_entities("User") == 0

        cid = repo.create_commit()
        repo.insert_entity("User", "u1", {"id": "u1"}, cid)
        repo.insert_entity("User", "u2", {"id": "u2"}, cid)
        repo.commit_transaction()

        assert repo.count_latest_entities("User") == 2
        repo.close()

    def test_count_latest_relations(self, tmp_db):
        repo = Repository(tmp_db)
        assert repo.count_latest_relations("Tagged") == 0

        cid = repo.create_commit()
        repo.insert_relation("Tagged", "u1", "t1", {}, cid)
        repo.insert_relation("Tagged", "u1", "t2", {}, cid)
        repo.commit_transaction()

        assert repo.count_latest_relations("Tagged") == 2
        repo.close()

    def test_iter_latest_entities(self, tmp_db):
        repo = Repository(tmp_db)
        cid = repo.create_commit()
        for i in range(5):
            repo.insert_entity("User", f"u{i}", {"id": f"u{i}", "name": f"User{i}"}, cid)
        repo.commit_transaction()

        all_rows = []
        for batch in repo.iter_latest_entities("User", batch_size=2):
            all_rows.extend(batch)
        assert len(all_rows) == 5
        repo.close()

    def test_iter_latest_relations(self, tmp_db):
        repo = Repository(tmp_db)
        cid = repo.create_commit()
        for i in range(3):
            repo.insert_relation("Tagged", f"u{i}", f"t{i}", {"weight": float(i)}, cid)
        repo.commit_transaction()

        all_rows = []
        for batch in repo.iter_latest_relations("Tagged", batch_size=2):
            all_rows.extend(batch)
        assert len(all_rows) == 3
        repo.close()


# --- Phase 2: Error types ---


class TestErrorTypes:
    def test_type_schema_diff(self):
        diff = TypeSchemaDiff(
            type_kind="entity",
            type_name="User",
            stored_version=1,
            added_fields=["email"],
            removed_fields=["age"],
            changed_fields={"name": {"stored": {"type": "str"}, "code": {"type": "str | None"}}},
        )
        assert diff.type_name == "User"
        assert diff.added_fields == ["email"]

    def test_schema_outdated_error(self):
        diffs = [TypeSchemaDiff(type_kind="entity", type_name="User", stored_version=1)]
        err = SchemaOutdatedError(diffs)
        assert err.diffs == diffs
        assert "User" in str(err)

    def test_schema_validation_error_is_deprecated_alias(self):
        err = SchemaValidationError(["Entity 'User' schema mismatch"])
        assert isinstance(err, SchemaOutdatedError)
        assert len(err.diffs) == 1

    def test_missing_upgrader_error(self):
        err = MissingUpgraderError({"User": [1, 2]})
        assert err.missing == {"User": [1, 2]}
        assert "User" in str(err)


# --- Phase 2: Token helpers ---


class TestTokenHelpers:
    def test_token_determinism(self):
        diffs = [
            TypeSchemaDiff(
                type_kind="entity", type_name="User", stored_version=1, added_fields=["email"]
            )
        ]
        h1 = _compute_plan_hash(diffs)
        h2 = _compute_plan_hash(diffs)
        assert h1 == h2

    def test_token_roundtrip(self):
        diffs = [TypeSchemaDiff(type_kind="entity", type_name="User", stored_version=1)]
        plan_hash = _compute_plan_hash(diffs)
        token = _compute_migration_token(plan_hash, 42)
        assert _verify_token(token, plan_hash, 42)
        assert not _verify_token(token, plan_hash, 43)  # different head

    def test_token_with_none_head(self):
        diffs = [TypeSchemaDiff(type_kind="entity", type_name="User", stored_version=1)]
        plan_hash = _compute_plan_hash(diffs)
        token = _compute_migration_token(plan_hash, None)
        assert _verify_token(token, plan_hash, None)

    def test_different_diffs_different_hash(self):
        d1 = [
            TypeSchemaDiff(
                type_kind="entity", type_name="User", stored_version=1, added_fields=["a"]
            )
        ]
        d2 = [
            TypeSchemaDiff(
                type_kind="entity", type_name="User", stored_version=1, added_fields=["b"]
            )
        ]
        assert _compute_plan_hash(d1) != _compute_plan_hash(d2)


# --- Phase 2: Upgrader decorator ---


class TestUpgrader:
    def test_decorator_attaches_metadata(self):
        @upgrader("User", from_version=1)
        def upgrade_user_v1(fields):
            return fields

        assert upgrade_user_v1._ontologia_upgrader == {
            "type_name": "User",
            "from_version": 1,
        }


# --- Phase 3: Schema validation lifecycle ---


class TestSchemaValidationLifecycle:
    def test_schema_auto_created_on_explicit_validate(self, tmp_db):
        onto = Session(tmp_db, entity_types=[User])
        onto.validate()
        assert "User" in onto._schema_version_ids
        assert onto._schema_version_ids["User"] == 1

        ver = onto.repo.get_current_schema_version("entity", "User")
        assert ver is not None
        assert ver["schema_version_id"] == 1
        onto.close()

    def test_schema_auto_created_on_first_session(self, tmp_db):
        onto = Session(tmp_db, entity_types=[User])
        # No error at construction
        onto.session()
        assert "User" in onto._schema_version_ids
        assert onto._schema_version_ids["User"] == 1

        # Verify stored
        ver = onto.repo.get_current_schema_version("entity", "User")
        assert ver is not None
        assert ver["schema_version_id"] == 1
        onto.close()

    def test_schema_outdated_error_on_mismatch(self, tmp_db):
        # Create initial schema
        onto1 = Session(tmp_db, entity_types=[User])
        onto1.session()
        onto1.close()

        # Define a different User
        class UserV2(Entity, name="User"):
            id: Field[str] = Field(primary_key=True)
            name: Field[str]
            age: Field[int]
            email: Field[str | None] = Field(default=None)

        onto2 = Session(tmp_db, entity_types=[UserV2])
        with pytest.raises(SchemaOutdatedError) as exc_info:
            onto2.session()

        err = exc_info.value
        assert len(err.diffs) == 1
        assert err.diffs[0].type_name == "User"
        assert "email" in err.diffs[0].added_fields
        onto2.close()

    def test_no_error_at_constructor(self, tmp_db):
        """Constructor should NOT validate schema (moved to session())."""
        onto1 = Session(tmp_db, entity_types=[User])
        onto1.session()
        onto1.close()

        class UserV2(Entity, name="User"):
            id: Field[str] = Field(primary_key=True)
            name: Field[str]
            age: Field[int]
            email: Field[str | None] = Field(default=None)

        # This should not raise
        onto2 = Session(tmp_db, entity_types=[UserV2])
        # Only session() raises
        with pytest.raises(SchemaOutdatedError):
            onto2.session()
        onto2.close()

    def test_matching_schema_no_error(self, tmp_db):
        onto1 = Session(tmp_db, entity_types=[User])
        onto1.session()
        onto1.close()

        # Same schema — no error
        onto2 = Session(tmp_db, entity_types=[User])
        onto2.session()
        assert onto2._schema_version_ids["User"] == 1
        onto2.close()

    def test_schema_version_id_written_on_data_rows(self, tmp_db):
        onto = Session(tmp_db, entity_types=[User, Tag], relation_types=[Tagged])
        with onto.session() as s:
            s.ensure(User(id="u1", name="Alice", age=30))
            s.ensure(Tag(id="t1", label="python"))
            s.ensure(Tagged(left_key="u1", right_key="t1", weight=2.0))

        # Check entity_history has schema_version_id
        row = onto.repo._conn.execute(
            "SELECT schema_version_id FROM entity_history WHERE entity_key = 'u1'"
        ).fetchone()
        assert row[0] == onto._schema_version_ids["User"]

        # Check relation_history
        row = onto.repo._conn.execute(
            "SELECT schema_version_id FROM relation_history WHERE left_key = 'u1'"
        ).fetchone()
        assert row[0] == onto._schema_version_ids["Tagged"]
        onto.close()


class TestSchemaDriftGuards:
    def test_commit_aborts_when_touched_type_schema_drifted(self, tmp_db):
        onto_seed = Session(tmp_db, entity_types=[User, Tag])
        onto_seed.validate()
        onto_seed.close()

        onto_writer = Session(tmp_db, entity_types=[User, Tag])
        onto_writer.validate()

        class UserV2(Entity, name="User"):
            id: Field[str] = Field(primary_key=True)
            name: Field[str]
            age: Field[int]
            email: Field[str | None] = Field(default=None)

        onto_migrator = Session(tmp_db, entity_types=[UserV2, Tag])
        migrate_result = onto_migrator.migrate(dry_run=False, force=True)
        assert migrate_result.success
        onto_migrator.close()

        with pytest.raises(SchemaOutdatedError):
            with onto_writer.session() as s:
                s.ensure(User(id="u1", name="Alice", age=30))

        assert onto_writer.repo.count_latest_entities("User") == 0
        onto_writer.close()

    def test_commit_checks_only_touched_types(self, tmp_db):
        onto_seed = Session(tmp_db, entity_types=[User, Tag])
        onto_seed.validate()
        onto_seed.close()

        onto_writer = Session(tmp_db, entity_types=[User, Tag])
        onto_writer.validate()

        class TagV2(Entity, name="Tag"):
            id: Field[str] = Field(primary_key=True)
            label: Field[str]
            color: Field[str | None] = Field(default=None)

        onto_migrator = Session(tmp_db, entity_types=[User, TagV2])
        migrate_result = onto_migrator.migrate(dry_run=False, force=True)
        assert migrate_result.success
        onto_migrator.close()

        with onto_writer.session() as s:
            s.ensure(User(id="u1", name="Alice", age=30))

        users = onto_writer.session().query().entities(User).collect()
        assert len(users) == 1
        assert users[0].id == "u1"
        onto_writer.close()


# --- Phase 4: Migration API ---


class TestMigrationPreview:
    def test_no_changes_preview(self, tmp_db):
        onto = Session(tmp_db, entity_types=[User])
        onto.session()

        preview = onto.migrate(dry_run=True)
        assert isinstance(preview, MigrationPreview)
        assert not preview.has_changes
        assert preview.token == ""
        onto.close()

    def test_preview_with_changes(self, tmp_db):
        onto1 = Session(tmp_db, entity_types=[User])
        onto1.session()
        onto1.close()

        class UserV2(Entity, name="User"):
            id: Field[str] = Field(primary_key=True)
            name: Field[str]
            age: Field[int]
            email: Field[str | None] = Field(default=None)

        onto2 = Session(tmp_db, entity_types=[UserV2])
        preview = onto2.migrate(dry_run=True)
        assert isinstance(preview, MigrationPreview)
        assert preview.has_changes
        assert len(preview.diffs) == 1
        assert preview.diffs[0].type_name == "User"
        assert "email" in preview.diffs[0].added_fields
        assert preview.token != ""
        onto2.close()

    def test_preview_with_data_shows_row_counts(self, tmp_db):
        onto1 = Session(tmp_db, entity_types=[User])
        with onto1.session() as s:
            s.ensure(User(id="u1", name="Alice", age=30))
            s.ensure(User(id="u2", name="Bob", age=25))
        onto1.close()

        class UserV2(Entity, name="User"):
            id: Field[str] = Field(primary_key=True)
            name: Field[str]
            age: Field[int]
            email: Field[str | None] = Field(default=None)

        onto2 = Session(tmp_db, entity_types=[UserV2])
        preview = onto2.migrate(dry_run=True)
        assert preview.estimated_rows["User"] == 2
        assert "User" in preview.types_requiring_upgraders
        assert "User" in preview.missing_upgraders  # no upgraders provided
        onto2.close()

    def test_preview_schema_only_type(self, tmp_db):
        """Type with schema change but zero rows is schema_only."""
        onto1 = Session(tmp_db, entity_types=[User])
        onto1.session()  # store schema, no data
        onto1.close()

        class UserV2(Entity, name="User"):
            id: Field[str] = Field(primary_key=True)
            name: Field[str]
            age: Field[int]
            email: Field[str | None] = Field(default=None)

        onto2 = Session(tmp_db, entity_types=[UserV2])
        preview = onto2.migrate(dry_run=True)
        assert "User" in preview.types_schema_only
        assert "User" not in preview.types_requiring_upgraders
        onto2.close()

    def test_preview_missing_upgraders_reported(self, tmp_db):
        onto1 = Session(tmp_db, entity_types=[User])
        with onto1.session() as s:
            s.ensure(User(id="u1", name="Alice", age=30))
        onto1.close()

        class UserV2(Entity, name="User"):
            id: Field[str] = Field(primary_key=True)
            name: Field[str]
            age: Field[int]
            email: Field[str | None] = Field(default=None)

        @upgrader("User", from_version=1)
        def up_user(fields):
            fields["email"] = None
            return fields

        onto2 = Session(tmp_db, entity_types=[UserV2])
        # With upgraders
        preview = onto2.migrate(dry_run=True, upgraders={(("User", 1)): up_user})
        assert preview.missing_upgraders == []

        # Without upgraders
        preview2 = onto2.migrate(dry_run=True)
        assert "User" in preview2.missing_upgraders
        onto2.close()


class TestMigrationApply:
    def test_schema_only_migration(self, tmp_db):
        """Zero data rows → no upgrader needed."""
        onto1 = Session(tmp_db, entity_types=[User])
        onto1.session()
        onto1.close()

        class UserV2(Entity, name="User"):
            id: Field[str] = Field(primary_key=True)
            name: Field[str]
            age: Field[int]
            email: Field[str | None] = Field(default=None)

        onto2 = Session(tmp_db, entity_types=[UserV2])
        result = onto2.migrate(dry_run=False, force=True)
        assert isinstance(result, MigrationResult)
        assert result.success
        assert "User" in result.types_migrated
        assert result.rows_migrated["User"] == 0
        assert result.new_schema_versions["User"] == 2

        # Now session() should work
        onto2.session()
        onto2.close()

    def test_upgrader_migration_with_data(self, tmp_db):
        onto1 = Session(tmp_db, entity_types=[User])
        with onto1.session() as s:
            s.ensure(User(id="u1", name="Alice", age=30))
            s.ensure(User(id="u2", name="Bob", age=25))
        onto1.close()

        class UserV2(Entity, name="User"):
            id: Field[str] = Field(primary_key=True)
            name: Field[str]
            age: Field[int]
            email: Field[str | None] = Field(default=None)

        @upgrader("User", from_version=1)
        def up_user(fields):
            fields["email"] = f"{fields['name'].lower()}@example.com"
            return fields

        onto2 = Session(tmp_db, entity_types=[UserV2])

        # Get token via preview
        preview = onto2.migrate(dry_run=True, upgraders={("User", 1): up_user})
        assert preview.has_changes

        # Apply with token
        result = onto2.migrate(dry_run=False, token=preview.token, upgraders={("User", 1): up_user})
        assert result.success
        assert result.rows_migrated["User"] == 2

        # Verify data was migrated
        session = onto2.session()
        users = session.query().entities(UserV2).collect()
        emails = {u.email for u in users}
        assert "alice@example.com" in emails
        assert "bob@example.com" in emails
        onto2.close()

    def test_stale_token_rejected(self, tmp_db):
        onto1 = Session(tmp_db, entity_types=[User])
        onto1.session()
        onto1.close()

        class UserV2(Entity, name="User"):
            id: Field[str] = Field(primary_key=True)
            name: Field[str]
            age: Field[int]
            email: Field[str | None] = Field(default=None)

        onto2 = Session(tmp_db, entity_types=[UserV2])
        preview = onto2.migrate(dry_run=True)

        # Simulate a change by inserting a commit
        onto2.repo.create_commit({"stale": "true"})

        with pytest.raises(MigrationTokenError, match="stale"):
            onto2.migrate(dry_run=False, token=preview.token)
        onto2.close()

    def test_force_skips_token(self, tmp_db):
        onto1 = Session(tmp_db, entity_types=[User])
        onto1.session()
        onto1.close()

        class UserV2(Entity, name="User"):
            id: Field[str] = Field(primary_key=True)
            name: Field[str]
            age: Field[int]
            email: Field[str | None] = Field(default=None)

        onto2 = Session(tmp_db, entity_types=[UserV2])
        result = onto2.migrate(dry_run=False, force=True)
        assert result.success
        onto2.close()

    def test_cannot_specify_both_token_and_force(self, tmp_db):
        onto = Session(tmp_db, entity_types=[User])
        onto.session()
        with pytest.raises(MigrationError, match="Cannot specify both"):
            onto.migrate(dry_run=False, token="abc", force=True)
        onto.close()

    def test_must_specify_token_or_force(self, tmp_db):
        onto = Session(tmp_db, entity_types=[User])
        onto.session()
        with pytest.raises(MigrationError, match="Either token or force"):
            onto.migrate(dry_run=False)
        onto.close()

    def test_missing_upgrader_abort_on_apply(self, tmp_db):
        onto1 = Session(tmp_db, entity_types=[User])
        with onto1.session() as s:
            s.ensure(User(id="u1", name="Alice", age=30))
        onto1.close()

        class UserV2(Entity, name="User"):
            id: Field[str] = Field(primary_key=True)
            name: Field[str]
            age: Field[int]
            email: Field[str | None] = Field(default=None)

        onto2 = Session(tmp_db, entity_types=[UserV2])
        with pytest.raises(MissingUpgraderError):
            onto2.migrate(dry_run=False, force=True)
        onto2.close()

    def test_upgrader_error_reporting(self, tmp_db):
        onto1 = Session(tmp_db, entity_types=[User])
        with onto1.session() as s:
            s.ensure(User(id="u1", name="Alice", age=30))
        onto1.close()

        class UserV2(Entity, name="User"):
            id: Field[str] = Field(primary_key=True)
            name: Field[str]
            age: Field[int]
            email: Field[str]  # Required field — upgrader doesn't provide it

        @upgrader("User", from_version=1)
        def bad_upgrader(fields):
            # Doesn't add required 'email' field
            return fields

        onto2 = Session(tmp_db, entity_types=[UserV2])
        with pytest.raises(MigrationError, match="Upgrader failed.*User.*u1"):
            onto2.migrate(dry_run=False, force=True, upgraders={("User", 1): bad_upgrader})
        onto2.close()

    def test_multi_type_atomic_migration(self, tmp_db):
        onto1 = Session(tmp_db, entity_types=[User, Tag])
        onto1.session()
        onto1.close()

        class UserV2(Entity, name="User"):
            id: Field[str] = Field(primary_key=True)
            name: Field[str]
            age: Field[int]
            email: Field[str | None] = Field(default=None)

        class TagV2(Entity, name="Tag"):
            id: Field[str] = Field(primary_key=True)
            label: Field[str]
            color: Field[str | None] = Field(default=None)

        onto2 = Session(tmp_db, entity_types=[UserV2, TagV2])
        result = onto2.migrate(dry_run=False, force=True)
        assert result.success
        assert set(result.types_migrated) == {"User", "Tag"}
        assert result.new_schema_versions["User"] == 2
        assert result.new_schema_versions["Tag"] == 2

        # Both types now pass validation
        onto2.session()
        onto2.close()

    def test_lock_timeout_no_partial_writes(self, tmp_db):
        """If lock can't be acquired, no partial migration happens."""
        onto1 = Session(tmp_db, entity_types=[User])
        onto1.session()

        class UserV2(Entity, name="User"):
            id: Field[str] = Field(primary_key=True)
            name: Field[str]
            age: Field[int]
            email: Field[str | None] = Field(default=None)

        # Hold the lock
        onto1.repo.acquire_lock("blocker", timeout_ms=100, lease_ms=60000)

        onto2 = Session(tmp_db, entity_types=[UserV2])
        with pytest.raises(MigrationError, match="Could not acquire"):
            onto2.migrate(dry_run=False, force=True)

        # Schema_versions should still be at v1
        ver = onto1.repo.get_current_schema_version("entity", "User")
        assert ver is not None
        assert ver["schema_version_id"] == 1

        onto1.repo.release_lock("blocker")
        onto1.close()
        onto2.close()

    def test_backward_compat_existing_db_bootstraps(self, tmp_db):
        """Existing DB without schema_versions table gets bootstrapped on Repository init."""
        # Setup: create old-style DB
        conn = sqlite3.connect(tmp_db)
        conn.executescript("""
            CREATE TABLE commits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                metadata_json TEXT
            );
            CREATE TABLE entity_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entity_type TEXT NOT NULL,
                entity_key TEXT NOT NULL,
                fields_json TEXT NOT NULL,
                commit_id INTEGER NOT NULL
            );
            CREATE TABLE relation_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                relation_type TEXT NOT NULL,
                left_key TEXT NOT NULL,
                right_key TEXT NOT NULL,
                fields_json TEXT NOT NULL,
                commit_id INTEGER NOT NULL
            );
            CREATE TABLE schema_registry (
                type_kind TEXT NOT NULL,
                type_name TEXT NOT NULL,
                schema_json TEXT NOT NULL,
                PRIMARY KEY (type_kind, type_name)
            );
            CREATE TABLE locks (
                lock_name TEXT PRIMARY KEY,
                owner_id TEXT NOT NULL,
                acquired_at TEXT NOT NULL,
                expires_at TEXT NOT NULL
            );
        """)
        schema = json.dumps(
            {
                "entity_name": "User",
                "fields": {
                    "id": {"primary_key": True, "index": False},
                    "name": {"primary_key": False, "index": False, "type": "<class 'str'>"},
                    "age": {"primary_key": False, "index": False, "type": "<class 'int'>"},
                },
            }
        )
        conn.execute(
            "INSERT INTO schema_registry VALUES (?, ?, ?)",
            ("entity", "User", schema),
        )
        conn.commit()
        conn.close()

        # Open with Repository — bootstraps
        onto = Session(tmp_db, entity_types=[User])
        # Schema should be version 1 (bootstrapped)
        ver = onto.repo.get_current_schema_version("entity", "User")
        assert ver is not None
        assert ver["schema_version_id"] == 1
        onto.close()


class TestMigrationNoChanges:
    def test_apply_no_changes(self, tmp_db):
        onto = Session(tmp_db, entity_types=[User])
        onto.session()
        result = onto.migrate(dry_run=False, force=True)
        assert result.success
        assert result.types_migrated == []
        onto.close()
