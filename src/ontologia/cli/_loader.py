"""Model loader: import Python modules and discover Entity/Relation types."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import Any

from ontologia.types import Entity, Relation


def load_models(
    models: str | None = None,
    models_path: str | None = None,
) -> tuple[dict[str, type[Entity]], dict[str, type[Relation[Any, Any]]]]:
    """Load Entity and Relation classes from a Python module.

    Args:
        models: Dotted Python import path (e.g. 'myapp.models')
        models_path: Filesystem path to a Python file

    Returns:
        (entity_types, relation_types) dicts keyed by type name
    """
    if models_path:
        path = Path(models_path).resolve()
        if not path.exists():
            raise FileNotFoundError(f"Models path not found: {models_path}")
        # Add parent to sys.path so import works
        parent = str(path.parent)
        if parent not in sys.path:
            sys.path.insert(0, parent)
        module_name = path.stem
        module = importlib.import_module(module_name)
    elif models:
        module = importlib.import_module(models)
    else:
        raise ValueError("One of --models or --models-path is required")

    entity_types: dict[str, type[Entity]] = {}
    relation_types: dict[str, type[Relation[Any, Any]]] = {}

    for attr_name in dir(module):
        obj = getattr(module, attr_name)
        if not isinstance(obj, type):
            continue
        if issubclass(obj, Entity) and obj is not Entity:
            entity_types[obj.__entity_name__] = obj
        elif issubclass(obj, Relation) and obj is not Relation:
            relation_types[obj.__relation_name__] = obj

    return entity_types, relation_types
