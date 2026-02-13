"""Tests for CLI model loader."""

import textwrap

import pytest

from ontologia.cli._loader import load_models


def test_load_models_from_path(tmp_path):
    """Load models from a Python file path."""
    models_file = tmp_path / "test_models.py"
    models_file.write_text(
        textwrap.dedent("""\
        from ontologia import Entity, Field, Relation

        class Widget(Entity):
            id: Field[str] = Field(primary_key=True)
            name: Field[str]

        class Gizmo(Entity):
            id: Field[str] = Field(primary_key=True)
            size: Field[int]

        class WidgetGizmo(Relation[Widget, Gizmo]):
            weight: Field[float]
    """)
    )

    entity_types, relation_types = load_models(models_path=str(models_file))
    assert "Widget" in entity_types
    assert "Gizmo" in entity_types
    assert "WidgetGizmo" in relation_types


def test_load_models_from_import():
    """Load models from Python import path (using the test conftest module)."""
    entity_types, relation_types = load_models(models="tests.conftest")
    assert "Customer" in entity_types
    assert "Product" in entity_types
    assert "Subscription" in relation_types


def test_load_models_missing_path():
    with pytest.raises(FileNotFoundError):
        load_models(models_path="/nonexistent/models.py")


def test_load_models_no_args():
    with pytest.raises(ValueError, match="One of"):
        load_models()
