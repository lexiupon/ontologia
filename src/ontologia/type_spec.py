"""Canonical type_spec serialization for schema drift detection (RFC 0006)."""

from __future__ import annotations

import json
import re
import typing
from typing import Any, get_args, get_origin, get_type_hints


def build_type_spec(annotation: Any, *, _visited: set[str] | None = None) -> dict[str, Any]:
    """Recursively serialize a type annotation to a canonical type_spec dict.

    Handles: primitives, list[T], dict[K,V], Union/Optional, TypedDict, Any.
    Detects cycles in TypedDict references via a visited set.
    """
    if _visited is None:
        _visited = set()

    # Handle None / NoneType
    if annotation is type(None):
        return {"kind": "primitive", "name": "NoneType"}

    # Handle Any
    if annotation is typing.Any:
        return {"kind": "primitive", "name": "any"}

    # Handle string annotations (forward refs)
    if isinstance(annotation, str):
        return {"kind": "primitive", "name": annotation}

    origin = get_origin(annotation)
    args = get_args(annotation)

    # Handle Union (including Optional[T] which is Union[T, None])
    if origin is typing.Union:
        members = [build_type_spec(a, _visited=_visited) for a in args]
        # Sort members by canonical JSON for determinism
        members.sort(key=lambda m: json.dumps(m, sort_keys=True))
        return {"kind": "union", "members": members}

    # Handle list[T]
    if origin is list:
        item_spec = (
            build_type_spec(args[0], _visited=_visited)
            if args
            else {"kind": "primitive", "name": "any"}
        )
        return {"kind": "list", "item": item_spec}

    # Handle dict[K, V]
    if origin is dict:
        key_spec = (
            build_type_spec(args[0], _visited=_visited)
            if args
            else {"kind": "primitive", "name": "any"}
        )
        val_spec = (
            build_type_spec(args[1], _visited=_visited)
            if len(args) > 1
            else {"kind": "primitive", "name": "any"}
        )
        return {"kind": "dict", "key": key_spec, "value": val_spec}

    # Handle TypedDict classes
    if _is_typed_dict(annotation):
        name = annotation.__name__
        if name in _visited:
            return {"kind": "ref", "name": name}
        _visited = _visited | {name}  # copy to allow sibling branches
        try:
            hints = get_type_hints(annotation)
        except Exception:
            # Fallback to __annotations__ if get_type_hints fails
            # (e.g., when from __future__ annotations is active and forward refs can't resolve)
            hints = annotation.__annotations__
        total = getattr(annotation, "__total__", True)
        fields = {}
        for field_name, field_type in sorted(hints.items()):
            fields[field_name] = build_type_spec(field_type, _visited=_visited)
        return {"kind": "typed_dict", "name": name, "total": total, "fields": fields}

    # Handle plain types (str, int, float, bool, etc.)
    if isinstance(annotation, type):
        return {"kind": "primitive", "name": annotation.__name__}

    # Fallback for unrecognized annotations
    return {"kind": "primitive", "name": str(annotation)}


def _is_typed_dict(annotation: Any) -> bool:
    """Check if annotation is a TypedDict class."""
    if not isinstance(annotation, type):
        return False
    # TypedDict classes have __annotations__ and inherit from dict
    # They also have __required_keys__ and __optional_keys__ in Python 3.9+
    return (
        hasattr(annotation, "__annotations__")
        and hasattr(annotation, "__required_keys__")
        and hasattr(annotation, "__optional_keys__")
    )


# --- Legacy synthesis for schema upgrade path (RFC 0006 §2.3) ---

_LEGACY_CLASS_RE = re.compile(r"^<class '(\w+)'>$")
_LEGACY_TYPING_RE = re.compile(r"^typing\.(\w+)\[(.+)\]$")


def synthesize_type_spec_from_legacy(type_str: str) -> dict[str, Any] | None:
    """Attempt to parse a legacy type string like "<class 'str'>" into a type_spec dict.

    Returns None if the string cannot be parsed.
    """
    # Handle "<class 'str'>" style
    m = _LEGACY_CLASS_RE.match(type_str)
    if m:
        name = m.group(1)
        return {"kind": "primitive", "name": name}

    # Handle "typing.Optional[str]" -> union with NoneType
    if type_str.startswith("typing.Optional[") and type_str.endswith("]"):
        inner = type_str[len("typing.Optional[") : -1]
        inner_spec = synthesize_type_spec_from_legacy(inner)
        if inner_spec is None:
            inner_spec = {"kind": "primitive", "name": inner}
        none_spec = {"kind": "primitive", "name": "NoneType"}
        members = sorted([inner_spec, none_spec], key=lambda m: json.dumps(m, sort_keys=True))
        return {"kind": "union", "members": members}

    # Handle "typing.List[X]" or "list[X]"
    for prefix in ("typing.List[", "list["):
        if type_str.startswith(prefix) and type_str.endswith("]"):
            inner = type_str[len(prefix) : -1]
            inner_spec = synthesize_type_spec_from_legacy(inner)
            if inner_spec is None:
                inner_spec = {"kind": "primitive", "name": inner}
            return {"kind": "list", "item": inner_spec}

    # Handle simple type names
    if type_str in ("str", "int", "float", "bool", "NoneType"):
        return {"kind": "primitive", "name": type_str}

    return None
