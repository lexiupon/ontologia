"""Intents: declarative state assertions for handlers."""

from __future__ import annotations

import warnings
from dataclasses import dataclass
from typing import Any

from ontologia.types import Entity, Relation


@dataclass
class Intent:
    """Opaque wrapper holding a typed Entity or Relation for state reconciliation."""

    obj: Entity | Relation[Any, Any]

    @property
    def is_entity(self) -> bool:
        return isinstance(self.obj, Entity)

    @property
    def is_relation(self) -> bool:
        return isinstance(self.obj, Relation)


def Ensure(obj: Entity | Relation[Any, Any]) -> Intent:
    """Declare expected state using a typed Entity or Relation object.

    .. deprecated::
        Use `session.ensure()` or `ctx.ensure()` instead.
        This function will be removed in a future version.
    """
    warnings.warn(
        "Ensure() is deprecated. Use session.ensure() or ctx.ensure() instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    if not isinstance(obj, (Entity, Relation)):
        raise TypeError(
            f"Ensure() requires an Entity or Relation instance, got {type(obj).__name__}"
        )
    return Intent(obj=obj)
