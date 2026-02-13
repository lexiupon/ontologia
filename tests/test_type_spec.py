"""Tests for RFC 0006: type_spec canonical serialization."""

import json
from typing import Optional, Union

import pytest

from ontologia import Entity, Field
from ontologia.runtime import Ontology
from ontologia.storage import Repository, _schema_hash
from ontologia.type_spec import build_type_spec, synthesize_type_spec_from_legacy

try:
    from typing import TypedDict
except ImportError:
    from typing_extensions import TypedDict


# --- build_type_spec tests ---


class TestBuildTypeSpecPrimitives:
    def test_str(self):
        assert build_type_spec(str) == {"kind": "primitive", "name": "str"}

    def test_int(self):
        assert build_type_spec(int) == {"kind": "primitive", "name": "int"}

    def test_float(self):
        assert build_type_spec(float) == {"kind": "primitive", "name": "float"}

    def test_bool(self):
        assert build_type_spec(bool) == {"kind": "primitive", "name": "bool"}

    def test_none_type(self):
        assert build_type_spec(type(None)) == {"kind": "primitive", "name": "NoneType"}

    def test_any(self):
        import typing

        assert build_type_spec(typing.Any) == {"kind": "primitive", "name": "any"}


class TestBuildTypeSpecContainers:
    def test_list_str(self):
        assert build_type_spec(list[str]) == {
            "kind": "list",
            "item": {"kind": "primitive", "name": "str"},
        }

    def test_dict_str_int(self):
        assert build_type_spec(dict[str, int]) == {
            "kind": "dict",
            "key": {"kind": "primitive", "name": "str"},
            "value": {"kind": "primitive", "name": "int"},
        }

    def test_list_of_list(self):
        spec = build_type_spec(list[list[int]])
        assert spec == {
            "kind": "list",
            "item": {"kind": "list", "item": {"kind": "primitive", "name": "int"}},
        }


class TestBuildTypeSpecUnion:
    def test_optional_str(self):
        spec = build_type_spec(Optional[str])
        assert spec["kind"] == "union"
        assert len(spec["members"]) == 2
        # Check members contain str and NoneType
        names = {m["name"] for m in spec["members"]}
        assert names == {"str", "NoneType"}

    def test_union_ordering_determinism(self):
        spec1 = build_type_spec(Union[str, int])
        spec2 = build_type_spec(Union[int, str])
        assert spec1 == spec2

    def test_union_three_types(self):
        spec = build_type_spec(Union[str, int, float])
        assert spec["kind"] == "union"
        assert len(spec["members"]) == 3


class TestBuildTypeSpecTypedDict:
    def test_simple_typed_dict(self):
        class Address(TypedDict):
            city: str
            zip_code: str

        spec = build_type_spec(Address)
        assert spec["kind"] == "typed_dict"
        assert spec["name"] == "Address"
        assert spec["total"] is True
        assert spec["fields"]["city"] == {"kind": "primitive", "name": "str"}
        assert spec["fields"]["zip_code"] == {"kind": "primitive", "name": "str"}

    def test_nested_typed_dict(self):
        class Inner(TypedDict):
            x: int

        class Outer(TypedDict):
            inner: Inner

        spec = build_type_spec(Outer)
        assert spec["kind"] == "typed_dict"
        assert spec["fields"]["inner"]["kind"] == "typed_dict"
        assert spec["fields"]["inner"]["name"] == "Inner"

    def test_recursive_typed_dict(self):
        """TypedDict with self-reference produces ref nodes."""

        class Node(TypedDict):
            value: int
            children: list[object]  # Can't do list[Node] easily, use list as proxy

        spec = build_type_spec(Node)
        assert spec["kind"] == "typed_dict"
        assert spec["name"] == "Node"
        # children is list[object], should produce a list spec
        assert spec["fields"]["children"]["kind"] == "list"


# --- synthesize_type_spec_from_legacy tests ---


class TestSynthesizeFromLegacy:
    def test_class_str(self):
        assert synthesize_type_spec_from_legacy("<class 'str'>") == {
            "kind": "primitive",
            "name": "str",
        }

    def test_class_int(self):
        assert synthesize_type_spec_from_legacy("<class 'int'>") == {
            "kind": "primitive",
            "name": "int",
        }

    def test_class_float(self):
        assert synthesize_type_spec_from_legacy("<class 'float'>") == {
            "kind": "primitive",
            "name": "float",
        }

    def test_class_bool(self):
        assert synthesize_type_spec_from_legacy("<class 'bool'>") == {
            "kind": "primitive",
            "name": "bool",
        }

    def test_simple_name(self):
        assert synthesize_type_spec_from_legacy("str") == {
            "kind": "primitive",
            "name": "str",
        }

    def test_optional_str(self):
        spec = synthesize_type_spec_from_legacy("typing.Optional[str]")
        assert spec is not None
        assert spec["kind"] == "union"
        names = {m["name"] for m in spec["members"]}
        assert names == {"str", "NoneType"}

    def test_list_str(self):
        spec = synthesize_type_spec_from_legacy("list[str]")
        assert spec == {"kind": "list", "item": {"kind": "primitive", "name": "str"}}

    def test_typing_list(self):
        spec = synthesize_type_spec_from_legacy("typing.List[int]")
        assert spec == {"kind": "list", "item": {"kind": "primitive", "name": "int"}}

    def test_unrecognized_returns_none(self):
        assert synthesize_type_spec_from_legacy("SomeComplexType[A, B]") is None

    def test_dict_returns_none(self):
        # dict types are not currently synthesized from legacy
        assert synthesize_type_spec_from_legacy("dict[str, int]") is None


# --- Schema upgrade: legacy DB without type_spec ---


class SimpleEntity(Entity):
    id: Field[str] = Field(primary_key=True)
    name: Field[str]
    count: Field[int]


class TestSchemaUpgradeLegacy:
    def test_no_false_drift_on_legacy_schema(self, tmp_path):
        """A DB with pre-type_spec schema should not trigger false drift."""
        db_path = str(tmp_path / "legacy.db")

        # Step 1: Manually create a legacy schema (without type_spec)
        repo = Repository(db_path)
        legacy_schema = {
            "entity_name": "SimpleEntity",
            "fields": {
                "id": {"primary_key": True, "index": False, "type": "<class 'str'>"},
                "name": {"primary_key": False, "index": False, "type": "<class 'str'>"},
                "count": {"primary_key": False, "index": False, "type": "<class 'int'>"},
            },
        }
        legacy_json = json.dumps(legacy_schema, sort_keys=True)
        legacy_hash = _schema_hash(legacy_json)
        repo.create_schema_version(
            "entity",
            "SimpleEntity",
            legacy_json,
            legacy_hash,
            runtime_id="test",
            reason="initial",
        )
        repo.store_schema("entity", "SimpleEntity", legacy_schema)
        repo.close()

        # Step 2: Open with new code that includes type_spec — should NOT raise
        ont = Ontology(db_path=db_path, entity_types=[SimpleEntity])
        ont.validate()  # Should succeed without SchemaOutdatedError
        ont.close()

    def test_real_drift_still_detected(self, tmp_path):
        """A field type change should still trigger drift even with type_spec upgrade."""
        db_path = str(tmp_path / "drift.db")

        repo = Repository(db_path)
        # Legacy schema has count as str instead of int
        legacy_schema = {
            "entity_name": "SimpleEntity",
            "fields": {
                "id": {"primary_key": True, "index": False, "type": "<class 'str'>"},
                "name": {"primary_key": False, "index": False, "type": "<class 'str'>"},
                "count": {"primary_key": False, "index": False, "type": "<class 'str'>"},
            },
        }
        legacy_json = json.dumps(legacy_schema, sort_keys=True)
        legacy_hash = _schema_hash(legacy_json)
        repo.create_schema_version(
            "entity",
            "SimpleEntity",
            legacy_json,
            legacy_hash,
            runtime_id="test",
            reason="initial",
        )
        repo.store_schema("entity", "SimpleEntity", legacy_schema)
        repo.close()

        # Code has count as int — should detect drift
        from ontologia.errors import SchemaOutdatedError

        ont = Ontology(db_path=db_path, entity_types=[SimpleEntity])
        with pytest.raises(SchemaOutdatedError):
            ont.validate()
        ont.close()

    def test_unrecognized_legacy_type_triggers_drift(self, tmp_path):
        """Legacy type string that can't be synthesized should trigger drift."""
        db_path = str(tmp_path / "unrec.db")

        repo = Repository(db_path)
        legacy_schema = {
            "entity_name": "SimpleEntity",
            "fields": {
                "id": {"primary_key": True, "index": False, "type": "SomeWeirdType"},
                "name": {"primary_key": False, "index": False, "type": "<class 'str'>"},
                "count": {"primary_key": False, "index": False, "type": "<class 'int'>"},
            },
        }
        legacy_json = json.dumps(legacy_schema, sort_keys=True)
        legacy_hash = _schema_hash(legacy_json)
        repo.create_schema_version(
            "entity",
            "SimpleEntity",
            legacy_json,
            legacy_hash,
            runtime_id="test",
            reason="initial",
        )
        repo.store_schema("entity", "SimpleEntity", legacy_schema)
        repo.close()

        from ontologia.errors import SchemaOutdatedError

        ont = Ontology(db_path=db_path, entity_types=[SimpleEntity])
        with pytest.raises(SchemaOutdatedError):
            ont.validate()
        ont.close()
