"""Tests for RFC 0003: Relation instance_key support."""

from __future__ import annotations

import pytest

from ontologia import (
    Entity,
    Event,
    Field,
    HandlerContext,
    OntologiaConfig,
    Relation,
    Session,
    meta,
    on_event,
)

from .conftest import (
    Company,
    Customer,
    Employment,
    Person,
    Product,
    Subscription,
)

# --- Type definition tests ---


class TestTypeDefinition:
    def test_entity_rejects_instance_key(self):
        with pytest.raises(TypeError, match="cannot use Field\\(instance_key=True\\)"):

            class BadEntity(Entity):  # pyright: ignore[reportUnusedClass]
                id: Field[str] = Field(primary_key=True)
                ik: Field[str] = Field(instance_key=True)

    def test_relation_rejects_primary_key(self):
        with pytest.raises(TypeError, match="cannot use Field\\(primary_key=True\\)"):

            class BadRelation1(Relation[Person, Company]):  # pyright: ignore[reportUnusedClass]
                pk: Field[str] = Field(primary_key=True)

    def test_at_most_one_instance_key(self):
        with pytest.raises(TypeError, match="multiple instance_key fields"):

            class BadRelation2(Relation[Person, Company]):  # pyright: ignore[reportUnusedClass]
                ik1: Field[str] = Field(instance_key=True)
                ik2: Field[str] = Field(instance_key=True)

    def test_instance_key_must_be_str(self):
        with pytest.raises(TypeError, match="must be of type str"):

            class BadRelation3(Relation[Person, Company]):  # pyright: ignore[reportUnusedClass]
                ik: Field[int] = Field(instance_key=True)

    def test_instance_key_must_not_have_default(self):
        with pytest.raises(TypeError, match="must not have a default"):

            class BadRelation4(Relation[Person, Company]):  # pyright: ignore[reportUnusedClass]
                ik: Field[str] = Field(instance_key=True, default="x")

    def test_keyed_relation_class_attrs(self):
        assert Employment._instance_key_field == "stint_id"
        assert "stint_id" not in Employment.__relation_fields__
        assert "role" in Employment.__relation_fields__
        assert "started_at" in Employment.__relation_fields__

    def test_unkeyed_relation_class_attrs(self):
        assert Subscription._instance_key_field is None


class TestRelationInit:
    def test_keyed_relation_requires_field(self):
        with pytest.raises(ValueError, match="requires 'stint_id'"):
            Employment(left_key="p1", right_key="c1", role="eng", started_at="2024")

    def test_keyed_relation_empty_field(self):
        with pytest.raises(ValueError, match="must not be empty"):
            Employment(left_key="p1", right_key="c1", stint_id="", role="eng", started_at="2024")

    def test_keyed_relation_whitespace_field(self):
        with pytest.raises(ValueError, match="must not be empty"):
            Employment(left_key="p1", right_key="c1", stint_id="  ", role="eng", started_at="2024")

    def test_keyed_relation_none_field(self):
        with pytest.raises(ValueError, match="requires 'stint_id'"):
            Employment(left_key="p1", right_key="c1", stint_id=None, role="eng", started_at="2024")

    def test_keyed_relation_valid(self):
        emp = Employment(
            left_key="p1", right_key="c1", stint_id="stint-1", role="eng", started_at="2024"
        )
        assert emp.instance_key == "stint-1"
        assert emp.stint_id == "stint-1"
        assert emp.role == "eng"

    def test_instance_key_excluded_from_model_dump(self):
        emp = Employment(
            left_key="p1", right_key="c1", stint_id="stint-1", role="eng", started_at="2024"
        )
        d = emp.model_dump()
        assert "stint_id" not in d
        assert "instance_key" not in d
        assert d == {"role": "eng", "started_at": "2024"}

    def test_keyed_relation_repr(self):
        emp = Employment(
            left_key="p1", right_key="c1", stint_id="stint-1", role="eng", started_at="2024"
        )
        r = repr(emp)
        assert "instance_key='stint-1'" in r

    def test_keyed_relation_equality(self):
        a = Employment(
            left_key="p1", right_key="c1", stint_id="stint-1", role="eng", started_at="2024"
        )
        b = Employment(
            left_key="p1", right_key="c1", stint_id="stint-1", role="eng", started_at="2024"
        )
        c = Employment(
            left_key="p1", right_key="c1", stint_id="stint-2", role="eng", started_at="2024"
        )
        assert a == b
        assert a != c

    def test_unkeyed_relation_backward_compat(self):
        sub = Subscription(left_key="c1", right_key="p1", seat_count=5, started_at="2024")
        assert sub.instance_key == ""
        assert sub.left_key == "c1"
        assert sub.right_key == "p1"


# --- Storage & runtime integration tests ---


class TestInstanceKeyStorage:
    def _make_ontology(self, tmp_db):
        return Session(
            tmp_db,
            entity_types=[Person, Company],
            relation_types=[Employment],
        )

    def test_multiple_instances_same_endpoints(self, tmp_db):
        onto = self._make_ontology(tmp_db)
        with onto.session() as s:
            s.ensure(Person(id="p1", name="Alice"))
            s.ensure(Company(id="c1", name="Acme"))
            s.ensure(
                Employment(
                    left_key="p1",
                    right_key="c1",
                    stint_id="stint-1",
                    role="eng",
                    started_at="2020",
                )
            )
            s.ensure(
                Employment(
                    left_key="p1",
                    right_key="c1",
                    stint_id="stint-2",
                    role="mgr",
                    started_at="2023",
                )
            )

        rels = onto.query().relations(Employment).collect()
        assert len(rels) == 2
        roles = {r.role for r in rels}
        assert roles == {"eng", "mgr"}
        onto.close()

    def test_noop_same_identity_same_fields(self, tmp_db):
        onto = self._make_ontology(tmp_db)
        with onto.session() as s:
            s.ensure(Person(id="p1", name="Alice"))
            s.ensure(Company(id="c1", name="Acme"))
            s.ensure(
                Employment(
                    left_key="p1",
                    right_key="c1",
                    stint_id="stint-1",
                    role="eng",
                    started_at="2020",
                )
            )

        commits_before = len(onto.list_commits(limit=100))

        with onto.session() as s:
            s.ensure(
                Employment(
                    left_key="p1",
                    right_key="c1",
                    stint_id="stint-1",
                    role="eng",
                    started_at="2020",
                )
            )

        commits_after = len(onto.list_commits(limit=100))
        assert commits_after == commits_before
        onto.close()

    def test_update_same_identity_different_fields(self, tmp_db):
        onto = self._make_ontology(tmp_db)
        with onto.session() as s:
            s.ensure(Person(id="p1", name="Alice"))
            s.ensure(Company(id="c1", name="Acme"))
            s.ensure(
                Employment(
                    left_key="p1",
                    right_key="c1",
                    stint_id="stint-1",
                    role="eng",
                    started_at="2020",
                )
            )

        with onto.session() as s:
            s.ensure(
                Employment(
                    left_key="p1",
                    right_key="c1",
                    stint_id="stint-1",
                    role="senior-eng",
                    started_at="2020",
                )
            )

        rels = onto.query().relations(Employment).collect()
        assert len(rels) == 1
        assert rels[0].role == "senior-eng"
        onto.close()

    def test_count_latest_relations_keyed(self, tmp_db):
        onto = self._make_ontology(tmp_db)
        with onto.session() as s:
            s.ensure(Person(id="p1", name="Alice"))
            s.ensure(Company(id="c1", name="Acme"))
            s.ensure(
                Employment(
                    left_key="p1",
                    right_key="c1",
                    stint_id="stint-1",
                    role="eng",
                    started_at="2020",
                )
            )
            s.ensure(
                Employment(
                    left_key="p1",
                    right_key="c1",
                    stint_id="stint-2",
                    role="mgr",
                    started_at="2023",
                )
            )

        count = onto.query().relations(Employment).count()
        assert count == 2
        onto.close()

    def test_instance_key_on_meta(self, tmp_db):
        onto = self._make_ontology(tmp_db)
        with onto.session() as s:
            s.ensure(Person(id="p1", name="Alice"))
            s.ensure(Company(id="c1", name="Acme"))
            s.ensure(
                Employment(
                    left_key="p1",
                    right_key="c1",
                    stint_id="stint-1",
                    role="eng",
                    started_at="2020",
                )
            )

        rels = onto.query().relations(Employment).collect()
        assert len(rels) == 1
        m = meta(rels[0])
        assert m.instance_key == "stint-1"
        onto.close()

    def test_group_by_not_collapsed(self, tmp_db):
        onto = self._make_ontology(tmp_db)
        with onto.session() as s:
            s.ensure(Person(id="p1", name="Alice"))
            s.ensure(Company(id="c1", name="Acme"))
            s.ensure(
                Employment(
                    left_key="p1",
                    right_key="c1",
                    stint_id="stint-1",
                    role="eng",
                    started_at="2020",
                )
            )
            s.ensure(
                Employment(
                    left_key="p1",
                    right_key="c1",
                    stint_id="stint-2",
                    role="eng",
                    started_at="2023",
                )
            )

        from ontologia.query import count

        groups = onto.query().relations(Employment).group_by(Employment.role).agg(n=count())
        eng_group = [g for g in groups if g["role"] == "eng"]
        assert len(eng_group) == 1
        assert eng_group[0]["n"] == 2
        onto.close()

    def test_as_of_with_keyed_relations(self, tmp_db):
        onto = self._make_ontology(tmp_db)
        with onto.session() as s:
            s.ensure(Person(id="p1", name="Alice"))
            s.ensure(Company(id="c1", name="Acme"))
            s.ensure(
                Employment(
                    left_key="p1",
                    right_key="c1",
                    stint_id="stint-1",
                    role="eng",
                    started_at="2020",
                )
            )

        commits = onto.list_commits(limit=100)
        commit_after_first = commits[0]["id"]

        with onto.session() as s:
            s.ensure(
                Employment(
                    left_key="p1",
                    right_key="c1",
                    stint_id="stint-2",
                    role="mgr",
                    started_at="2023",
                )
            )

        # as_of the first commit should only show stint-1
        rels = onto.query().relations(Employment).as_of(commit_after_first).collect()
        assert len(rels) == 1
        assert rels[0].instance_key == "stint-1"
        onto.close()

    def test_with_history_keyed_relations(self, tmp_db):
        onto = self._make_ontology(tmp_db)
        with onto.session() as s:
            s.ensure(Person(id="p1", name="Alice"))
            s.ensure(Company(id="c1", name="Acme"))
            s.ensure(
                Employment(
                    left_key="p1",
                    right_key="c1",
                    stint_id="stint-1",
                    role="eng",
                    started_at="2020",
                )
            )

        with onto.session() as s:
            s.ensure(
                Employment(
                    left_key="p1",
                    right_key="c1",
                    stint_id="stint-1",
                    role="senior-eng",
                    started_at="2020",
                )
            )

        rels = onto.query().relations(Employment).with_history().collect()
        assert len(rels) == 2
        roles = [r.role for r in rels]
        assert "eng" in roles
        assert "senior-eng" in roles
        onto.close()

    def test_traversal_with_keyed_relations(self, tmp_db):
        onto = self._make_ontology(tmp_db)
        with onto.session() as s:
            s.ensure(Person(id="p1", name="Alice"))
            s.ensure(Company(id="c1", name="Acme"))
            s.ensure(Company(id="c2", name="Beta"))
            s.ensure(
                Employment(
                    left_key="p1",
                    right_key="c1",
                    stint_id="stint-1",
                    role="eng",
                    started_at="2020",
                )
            )
            s.ensure(
                Employment(
                    left_key="p1",
                    right_key="c1",
                    stint_id="stint-2",
                    role="mgr",
                    started_at="2023",
                )
            )
            s.ensure(
                Employment(
                    left_key="p1",
                    right_key="c2",
                    stint_id="stint-3",
                    role="cto",
                    started_at="2024",
                )
            )

        # Traverse from Person -> Employment -> Company
        paths = onto.query().entities(Person).via(Employment).collect()
        assert len(paths) == 1  # one source person
        # Should have 3 relations (two to Acme, one to Beta)
        assert len(paths[0].relations) == 3

        # without_relations should deduplicate destination entities
        companies = onto.query().entities(Person).via(Employment).without_relations()
        company_names = {c.name for c in companies}
        # Both Acme (2 stints) and Beta (1 stint) should appear
        assert "Acme" in company_names
        assert "Beta" in company_names
        onto.close()

    def test_unkeyed_backward_compat(self, tmp_db):
        """Existing unkeyed relations should work unchanged."""
        onto = Session(
            tmp_db,
            entity_types=[Customer, Product],
            relation_types=[Subscription],
        )
        with onto.session() as s:
            s.ensure(Customer(id="c1", name="Alice", age=30))
            s.ensure(Product(sku="p1", name="Widget", price=10.0))
            s.ensure(Subscription(left_key="c1", right_key="p1", seat_count=5, started_at="2024"))

        rels = onto.query().relations(Subscription).collect()
        assert len(rels) == 1
        assert rels[0].seat_count == 5
        m = meta(rels[0])
        assert m.instance_key is None
        onto.close()


class TestInstanceKeyDispatch:
    def test_dispatch_dedup_keyed(self, tmp_db):
        """Two keyed-relation events are dispatched and persisted independently."""

        class EmploymentImported(Event):
            left_key: Field[str]
            right_key: Field[str]
            stint_id: Field[str]
            role: Field[str]
            started_at: Field[str]

        call_log: list[str] = []

        @on_event(EmploymentImported)
        def track_employment(ctx: HandlerContext[EmploymentImported]) -> None:
            evt = ctx.event
            call_log.append(f"{evt.stint_id}:{evt.role}")
            ctx.ensure(
                Employment(
                    left_key=evt.left_key,
                    right_key=evt.right_key,
                    stint_id=evt.stint_id,
                    role=evt.role,
                    started_at=evt.started_at,
                )
            )
            ctx.commit()

        onto = Session(
            tmp_db,
            config=OntologiaConfig(event_poll_interval_ms=10),
            entity_types=[Person, Company],
            relation_types=[Employment],
        )
        with onto.session() as s:
            s.ensure(Person(id="p1", name="Alice"))
            s.ensure(Company(id="c1", name="Acme"))
            s.commit()
            s.commit(
                event=EmploymentImported(
                    left_key="p1",
                    right_key="c1",
                    stint_id="stint-1",
                    role="eng",
                    started_at="2020",
                )
            )
            s.commit(
                event=EmploymentImported(
                    left_key="p1",
                    right_key="c1",
                    stint_id="stint-2",
                    role="mgr",
                    started_at="2023",
                )
            )
            s.run([track_employment], max_iterations=10)

        # Both instances should trigger the handler separately
        assert len(call_log) == 2
        assert "stint-1:eng" in call_log
        assert "stint-2:mgr" in call_log
        rels = onto.query().relations(Employment).collect()
        assert len(rels) == 2
        onto.close()


class TestSchemaEvolution:
    def test_instance_key_in_schema(self, tmp_db):
        onto = Session(
            tmp_db,
            entity_types=[Person, Company],
            relation_types=[Employment],
        )
        _ = onto.session()  # triggers schema validation

        schema = onto.repo.get_schema("relation", "Employment")
        assert schema is not None
        assert schema["instance_key_field"] == "stint_id"
        onto.close()

    def test_unkeyed_schema_has_null_instance_key_field(self, tmp_db):
        onto = Session(
            tmp_db,
            entity_types=[Customer, Product],
            relation_types=[Subscription],
        )
        _ = onto.session()

        schema = onto.repo.get_schema("relation", "Subscription")
        assert schema is not None
        assert schema["instance_key_field"] is None
        onto.close()
