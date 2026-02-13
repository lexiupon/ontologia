"""Handler decorators and HandlerContext for event-driven state management."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any, Callable, Literal, TypeVar

from ontologia.filters import FilterExpression
from ontologia.types import Entity, Relation

EntityT = TypeVar("EntityT", bound=Entity)
RelationT = TypeVar("RelationT", bound=Relation[Any, Any])


@dataclass
class HandlerMeta:
    """Metadata stored on decorated handler functions."""

    event_type: str  # "ON_COMMIT", "ON_SCHEDULE"
    priority: int = 100
    target_kind: Literal["entity", "relation"] | None = None
    target_type: type[Any] | None = None
    when: FilterExpression | None = None  # Condition filter for ON_COMMIT
    allow_self_trigger: bool = False
    cron: str | None = None  # Cron expression for ON_SCHEDULE


@dataclass
class HandlerContext:
    """Context object provided to all handlers."""

    event: str
    commit_id: int | None
    root_event_id: str
    chain_depth: int
    session: Any  # Session instance (Any to avoid circular import)
    _commit_meta: dict[str, str] = field(default_factory=dict)

    def ensure(
        self, obj: Entity | Relation[Any, Any] | Iterable[Entity | Relation[Any, Any]]
    ) -> None:  # type: ignore[name-defined]
        """Declare expected state (single object or iterable of objects).

        Args:
            obj: Single Entity/Relation or an iterable of Entity/Relation instances
        """
        self.session.ensure(obj)

    def add_commit_meta(self, key: str, value: str) -> None:
        """Attach metadata to the commit produced by this handler run/chunk.

        Last write wins for the same key.
        """
        self._commit_meta[key] = value


def on_commit(
    when: FilterExpression | None = None,
    allow_self_trigger: bool = False,
    priority: int = 100,
) -> Callable[..., Any]:
    """Decorator for unfiltered commit event handlers."""

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        func._ontologia_handler = HandlerMeta(  # type: ignore[attr-defined]
            event_type="ON_COMMIT",
            priority=priority,
            when=when,
            allow_self_trigger=allow_self_trigger,
        )
        return func

    return decorator


def on_commit_entity(
    entity_type: type[EntityT],
    when: FilterExpression | None = None,
    allow_self_trigger: bool = False,
    priority: int = 100,
) -> Callable[
    [Callable[[HandlerContext, EntityT], Any]],
    Callable[[HandlerContext, EntityT], Any],
]:
    """Decorator for commit handlers filtered to one entity type."""

    def decorator(
        func: Callable[[HandlerContext, EntityT], Any],
    ) -> Callable[[HandlerContext, EntityT], Any]:
        func._ontologia_handler = HandlerMeta(  # type: ignore[attr-defined]
            event_type="ON_COMMIT",
            priority=priority,
            target_kind="entity",
            target_type=entity_type,
            when=when,
            allow_self_trigger=allow_self_trigger,
        )
        return func

    return decorator


def on_commit_relation(
    relation_type: type[RelationT],
    when: FilterExpression | None = None,
    allow_self_trigger: bool = False,
    priority: int = 100,
) -> Callable[
    [Callable[[HandlerContext, RelationT], Any]],
    Callable[[HandlerContext, RelationT], Any],
]:
    """Decorator for commit handlers filtered to one relation type."""

    def decorator(
        func: Callable[[HandlerContext, RelationT], Any],
    ) -> Callable[[HandlerContext, RelationT], Any]:
        func._ontologia_handler = HandlerMeta(  # type: ignore[attr-defined]
            event_type="ON_COMMIT",
            priority=priority,
            target_kind="relation",
            target_type=relation_type,
            when=when,
            allow_self_trigger=allow_self_trigger,
        )
        return func

    return decorator


def on_schedule(cron: str, priority: int = 100) -> Callable[..., Any]:
    """Decorator for scheduled event handlers."""

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        func._ontologia_handler = HandlerMeta(  # type: ignore[attr-defined]
            event_type="ON_SCHEDULE",
            priority=priority,
            cron=cron,
        )
        return func

    return decorator
