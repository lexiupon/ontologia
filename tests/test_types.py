"""Tests for the type system: Entity, Relation, Field, Meta."""

from __future__ import annotations

import re
from typing import Any, cast

import pytest

from ontologia import Entity, Field, Meta, Relation, meta
from ontologia.errors import MetadataUnavailableError
from ontologia.filters import NULL_EQ_ERROR, NULL_NE_ERROR, ComparisonExpression

# --- Field descriptor tests ---


class TestField:
    def test_field_default(self):
        f = Field(default="hello")
        assert f.has_default()
        assert f.get_default() == "hello"

    def test_field_default_factory(self):
        f = Field(default_factory=list)
        assert f.has_default()
        assert f.get_default() == []
        # Each call returns a new list
        assert f.get_default() is not f.get_default()

    def test_field_no_default(self):
        f = Field()
        assert not f.has_default()
        with pytest.raises(ValueError):
            f.get_default()

    def test_field_primary_key(self):
        f = Field(primary_key=True)
        assert f.primary_key is True

    def test_field_index(self):
        f = Field(index=True)
        assert f.index is True

    def test_field_comparison_returns_filter_expr(self):
        """Class-level field access should return FieldProxy for query building."""

        class TestEntity(Entity):
            id: Field[str] = Field(primary_key=True)
            name: Field[str]
            age: Field[int]

        # These should return FilterExpression-compatible objects
        expr = TestEntity.name == "Alice"
        assert isinstance(expr, ComparisonExpression)
        assert expr.field_path == "$.name"
        assert expr.op == "=="
        assert expr.value == "Alice"

    def test_field_string_methods(self):
        class TestEntity(Entity):
            id: Field[str] = Field(primary_key=True)
            name: Field[str]

        expr = TestEntity.name.startswith("A")
        assert isinstance(expr, ComparisonExpression)
        assert expr.op == "LIKE"
        assert expr.value == "A%"

        expr = TestEntity.name.endswith("z")
        assert expr.value == "%z"

        expr = TestEntity.name.contains("mid")
        assert expr.value == "%mid%"

    def test_field_in_method(self):
        class TestEntity(Entity):
            id: Field[str] = Field(primary_key=True)
            tier: Field[str]

        expr = TestEntity.tier.in_(["Gold", "Platinum"])
        assert isinstance(expr, ComparisonExpression)
        assert expr.op == "IN"
        assert expr.value == ["Gold", "Platinum"]

    def test_field_null_checks(self):
        class TestEntity(Entity):
            id: Field[str] = Field(primary_key=True)
            email: Field[str | None] = Field(default=None)

        expr = TestEntity.email.is_null()
        assert isinstance(expr, ComparisonExpression)
        assert expr.op == "IS_NULL"

        expr = TestEntity.email.is_not_null()
        assert expr.op == "IS_NOT_NULL"

    def test_field_none_comparison_raises(self):
        class TestEntity(Entity):
            id: Field[str] = Field(primary_key=True)
            email: Field[str | None] = Field(default=None)

        field_desc = TestEntity.__dict__["email"]
        with pytest.raises(TypeError, match=re.escape(NULL_EQ_ERROR)):
            field_desc.__eq__(None)
        with pytest.raises(TypeError, match=re.escape(NULL_NE_ERROR)):
            field_desc.__ne__(None)

    def test_field_numeric_comparisons(self):
        class TestEntity(Entity):
            id: Field[str] = Field(primary_key=True)
            age: Field[int]

        for op_name, op_str in [
            ("__gt__", ">"),
            ("__ge__", ">="),
            ("__lt__", "<"),
            ("__le__", "<="),
        ]:
            expr = getattr(TestEntity.age, op_name)(30)
            assert isinstance(expr, ComparisonExpression)
            assert expr.op == op_str
            assert expr.value == 30


# --- Entity tests ---


class TestEntity:
    def test_entity_creation(self):
        from tests.conftest import Customer

        c = Customer(id="c1", name="Alice", age=30)
        assert c.id == "c1"
        assert c.name == "Alice"
        assert c.age == 30
        assert c.email is None
        assert c.tier == "Standard"
        assert c.active is True

    def test_entity_all_fields(self):
        from tests.conftest import Customer

        c = Customer(id="c1", name="Bob", age=25, email="bob@test.com", tier="Gold", active=False)
        assert c.email == "bob@test.com"
        assert c.tier == "Gold"
        assert c.active is False

    def test_entity_name_default(self):
        from tests.conftest import Customer

        assert Customer.__entity_name__ == "Customer"

    def test_entity_name_explicit(self):
        class MyEntity(Entity, name="custom_name"):
            id: Field[str] = Field(primary_key=True)

        assert MyEntity.__entity_name__ == "custom_name"

    def test_entity_fields_tuple(self):
        from tests.conftest import Customer

        assert "id" in Customer.__entity_fields__
        assert "name" in Customer.__entity_fields__
        assert "age" in Customer.__entity_fields__
        assert "email" in Customer.__entity_fields__
        assert "tier" in Customer.__entity_fields__
        assert "active" in Customer.__entity_fields__

    def test_entity_no_primary_key_raises(self):
        with pytest.raises(TypeError, match="primary_key"):

            class _BadEntity(Entity):
                name: Field[str]

            _ = _BadEntity

    def test_entity_multiple_primary_keys_raises(self):
        with pytest.raises(TypeError, match="multiple primary keys"):

            class _BadEntity(Entity):
                id: Field[str] = Field(primary_key=True)
                other_id: Field[str] = Field(primary_key=True)

            _ = _BadEntity

    def test_entity_model_dump(self):
        from tests.conftest import Customer

        c = Customer(id="c1", name="Alice", age=30)
        d = c.model_dump()
        assert d == {
            "id": "c1",
            "name": "Alice",
            "age": 30,
            "email": None,
            "tier": "Standard",
            "active": True,
        }

    def test_entity_model_validate(self):
        from tests.conftest import Customer

        c = Customer.model_validate({"id": "c1", "name": "Alice", "age": 30})
        assert c.id == "c1"
        assert c.name == "Alice"
        assert c.age == 30

    def test_entity_validation_error(self):
        from tests.conftest import Customer

        with pytest.raises(Exception):  # Pydantic ValidationError
            Customer(id="c1", name="Alice")  # missing age

    def test_entity_meta_not_hydrated(self):
        from tests.conftest import Customer

        c = Customer(id="c1", name="Alice", age=30)
        with pytest.raises(MetadataUnavailableError):
            c.meta()

    def test_entity_meta_hydrated(self):
        from tests.conftest import Customer

        c = Customer(id="c1", name="Alice", age=30)
        cast(Any, c).__onto_meta__ = Meta(commit_id=1, type_name="Customer", key="c1")
        m = c.meta()
        assert m.commit_id == 1
        assert m.type_name == "Customer"
        assert m.key == "c1"

    def test_entity_meta_utility(self):
        from tests.conftest import Customer

        c = Customer(id="c1", name="Alice", age=30)
        cast(Any, c).__onto_meta__ = Meta(commit_id=1, type_name="Customer", key="c1")
        m = meta(c)
        assert m.commit_id == 1

    def test_entity_equality(self):
        from tests.conftest import Customer

        c1 = Customer(id="c1", name="Alice", age=30)
        c2 = Customer(id="c1", name="Alice", age=30)
        c3 = Customer(id="c1", name="Bob", age=30)
        assert c1 == c2
        assert c1 != c3

    def test_entity_repr(self):
        from tests.conftest import Customer

        c = Customer(id="c1", name="Alice", age=30)
        r = repr(c)
        assert "Customer" in r
        assert "c1" in r

    def test_entity_with_list_field(self):
        class TaggedItem(Entity):
            id: Field[str] = Field(primary_key=True)
            tags: Field[list[str]] = Field(default_factory=list)

        item = TaggedItem(id="t1")
        assert item.tags == []

        item2 = TaggedItem(id="t2", tags=["a", "b"])
        assert item2.tags == ["a", "b"]


# --- Relation tests ---


class TestRelation:
    def test_relation_creation(self):
        from tests.conftest import Subscription

        s = Subscription(left_key="c1", right_key="p1", seat_count=5, started_at="2024-01-15")
        assert s.left_key == "c1"
        assert s.right_key == "p1"
        assert s.seat_count == 5
        assert s.started_at == "2024-01-15"
        assert s.active is True

    def test_relation_name_default(self):
        from tests.conftest import Subscription

        assert Subscription.__relation_name__ == "Subscription"

    def test_relation_name_explicit(self):
        from tests.conftest import Customer, Product

        class MyRel(Relation[Customer, Product], name="custom_rel"):
            pass

        assert MyRel.__relation_name__ == "custom_rel"

    def test_relation_generic_types(self):
        from tests.conftest import Customer, Product, Subscription

        assert Subscription._left_type is Customer
        assert Subscription._right_type is Product

    def test_relation_fields_tuple(self):
        from tests.conftest import Subscription

        assert "seat_count" in Subscription.__relation_fields__
        assert "started_at" in Subscription.__relation_fields__
        assert "active" in Subscription.__relation_fields__
        # left_key and right_key are NOT in __relation_fields__
        assert "left_key" not in Subscription.__relation_fields__

    def test_relation_no_attributes(self):
        from tests.conftest import Follows

        f = Follows(left_key="c1", right_key="c2")
        assert f.left_key == "c1"
        assert f.right_key == "c2"
        assert f.model_dump() == {}

    def test_relation_model_dump(self):
        from tests.conftest import Subscription

        s = Subscription(left_key="c1", right_key="p1", seat_count=5, started_at="2024-01-15")
        d = s.model_dump()
        assert d == {"seat_count": 5, "started_at": "2024-01-15", "active": True}
        # model_dump should NOT include left_key/right_key
        assert "left_key" not in d

    def test_relation_model_validate(self):
        from tests.conftest import Subscription

        s = Subscription.model_validate(
            {
                "left_key": "c1",
                "right_key": "p1",
                "seat_count": 5,
                "started_at": "2024-01-15",
            }
        )
        assert s.left_key == "c1"
        assert s.seat_count == 5

    def test_relation_meta_not_hydrated(self):
        from tests.conftest import Subscription

        s = Subscription(left_key="c1", right_key="p1", seat_count=5, started_at="2024-01-15")
        with pytest.raises(MetadataUnavailableError):
            s.meta()

    def test_relation_meta_hydrated(self):
        from tests.conftest import Subscription

        s = Subscription(left_key="c1", right_key="p1", seat_count=5, started_at="2024-01-15")
        cast(Any, s).__onto_meta__ = Meta(
            commit_id=1, type_name="Subscription", left_key="c1", right_key="p1"
        )
        m = s.meta()
        assert m.commit_id == 1
        assert m.left_key == "c1"
        assert m.right_key == "p1"

    def test_relation_equality(self):
        from tests.conftest import Subscription

        s1 = Subscription(left_key="c1", right_key="p1", seat_count=5, started_at="2024")
        s2 = Subscription(left_key="c1", right_key="p1", seat_count=5, started_at="2024")
        s3 = Subscription(left_key="c1", right_key="p1", seat_count=10, started_at="2024")
        assert s1 == s2
        assert s1 != s3

    def test_relation_endpoint_accessors_default_none(self):
        from tests.conftest import Subscription

        s = Subscription(left_key="c1", right_key="p1", seat_count=5, started_at="2024")
        assert s.left is None
        assert s.right is None
