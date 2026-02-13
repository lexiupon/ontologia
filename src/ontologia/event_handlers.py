"""Event handler decorators and context for the RFC 0005 runtime."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any, Callable, Generic, TypeVar

from ontologia.events import Event
from ontologia.types import Entity, Relation

if TYPE_CHECKING:
    from ontologia.session import Session

TEvent = TypeVar("TEvent", bound=Event)


@dataclass(frozen=True)
class EventHandlerMeta:
    """Metadata for handlers decorated with @on_event."""

    event_cls: type[Event]
    priority: int = 100
    handler_id: str = ""


@dataclass
class HandlerContext(Generic[TEvent]):
    """Context object supplied to event handlers."""

    event: TEvent
    session: Session
    lease_until: datetime | None = None
    _commit_meta: dict[str, str] = field(default_factory=dict)
    _buffered_events: list[Event] = field(default_factory=list)

    def ensure(
        self,
        obj: Entity | Relation[Any, Any] | Iterable[Entity | Relation[Any, Any]],
    ) -> None:
        self.session.ensure(obj)

    def emit(self, event: Event) -> None:
        self._buffered_events.append(event)

    def add_commit_meta(self, key: str, value: str) -> None:
        self._commit_meta[key] = value

    def commit(
        self,
        *,
        event: Event | None = None,
    ) -> int | None:
        commit_id = self.session._commit_from_handler(
            self,
            event=event,
            commit_meta=self._commit_meta,
        )
        self._commit_meta = {}
        return commit_id


def _handler_id(func: Callable[..., Any]) -> str:
    return f"{func.__module__}.{func.__qualname__}"


def on_event(
    event_cls: type[TEvent],
    *,
    priority: int = 100,
) -> Callable[[Callable[[HandlerContext[TEvent]], None]], Callable[[HandlerContext[TEvent]], None]]:
    """Register a handler for a typed Event subclass."""

    def decorator(
        func: Callable[[HandlerContext[TEvent]], None],
    ) -> Callable[[HandlerContext[TEvent]], None]:
        meta = EventHandlerMeta(
            event_cls=event_cls,
            priority=priority,
            handler_id=_handler_id(func),
        )
        setattr(func, "_ontologia_event_handler", meta)
        return func

    return decorator
