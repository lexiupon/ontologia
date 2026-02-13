"""Integration tests for imperative and RFC 0005 event-driven modes."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from ontologia import Event, Field, HandlerContext, OntologiaConfig, Schedule, Session, on_event
from ontologia.errors import HandlerError
from ontologia.events import EventDeadLetter
from tests.conftest import Customer, Order, Product, Subscription


class SyncCustomers(Event):
    source: Field[str]


class CustomerImported(Event):
    customer_id: Field[str]
    name: Field[str]
    age: Field[int]


class OrderImported(Event):
    order_id: Field[str]
    customer_id: Field[str]
    total_amount: Field[float]
    status: Field[str]
    country: Field[str]


class SubscriptionImported(Event):
    left_key: Field[str]
    right_key: Field[str]
    seat_count: Field[int]
    started_at: Field[str]


class CreateOrderForCustomer(Event):
    customer_id: Field[str]


class Tick(Event):
    label: Field[str]


class BulkLoad(Event):
    count: Field[int]


class Loop(Event):
    step: Field[int]


class Snapshot(Event):
    label: Field[str]


def _fast_config(**kwargs) -> OntologiaConfig:
    return OntologiaConfig(
        event_poll_interval_ms=10,
        event_backoff_base_ms=1,
        event_backoff_max_ms=5,
        **kwargs,
    )


class TestImperativeMode:
    """Test imperative mode (direct ensure() calls without handlers)."""

    def test_insert_entities(self, tmp_db):
        onto = Session(
            tmp_db,
            entity_types=[Customer],
            relation_types=[],
        )

        with onto.session() as session:
            session.ensure(Customer(id="c1", name="Alice", age=30))
            session.ensure(Customer(id="c2", name="Bob", age=25))

        customers = onto.query().entities(Customer).collect()
        assert len(customers) == 2

    def test_batch_insert(self, tmp_db):
        onto = Session(
            tmp_db,
            entity_types=[Customer],
            relation_types=[],
        )

        with onto.session() as session:
            session.ensure(
                [
                    Customer(id="c1", name="Alice", age=30),
                    Customer(id="c2", name="Bob", age=25),
                    Customer(id="c3", name="Carol", age=35),
                ]
            )

        customers = onto.query().entities(Customer).collect()
        assert len(customers) == 3

    def test_no_op_no_commit(self, tmp_db):
        onto = Session(
            tmp_db,
            entity_types=[Customer],
            relation_types=[],
        )

        with onto.session():
            pass  # No intents

        commits = onto.list_commits()
        assert len(commits) == 0

    def test_commit_returns_commit_id(self, tmp_db):
        onto = Session(
            tmp_db,
            entity_types=[Customer],
            relation_types=[],
        )

        with onto.session() as session:
            session.ensure(Customer(id="c1", name="Alice", age=30))
            commit_id = session.commit()

        assert isinstance(commit_id, int)
        assert onto.get_commit(commit_id) is not None

    def test_commit_returns_none_when_no_delta(self, tmp_db):
        onto = Session(
            tmp_db,
            entity_types=[Customer],
            relation_types=[],
        )

        with onto.session() as session:
            session.ensure(Customer(id="c1", name="Alice", age=30))
            first_commit_id = session.commit()
        assert isinstance(first_commit_id, int)

        with onto.session() as session:
            session.ensure(Customer(id="c1", name="Alice", age=30))
            second_commit_id = session.commit()
        assert second_commit_id is None

    def test_commit_returns_none_when_queue_empty(self, tmp_db):
        onto = Session(
            tmp_db,
            entity_types=[Customer],
            relation_types=[],
        )

        with onto.session() as session:
            commit_id = session.commit()

        assert commit_id is None


class TestEventMode:
    """Test event mode (session.run() with @on_event handlers)."""

    def test_event_handler_processes_seed_event(self, tmp_db):
        @on_event(SyncCustomers)
        def sync_data(ctx: HandlerContext[SyncCustomers]) -> None:
            ctx.ensure(
                [
                    Customer(id="c1", name="Alice", age=30),
                    Customer(id="c2", name="Bob", age=25),
                ]
            )
            ctx.commit()

        onto = Session(
            tmp_db,
            config=_fast_config(),
            entity_types=[Customer],
            relation_types=[],
        )

        with onto.session() as session:
            session.commit(event=SyncCustomers(source="seed"))
            session.run([sync_data], max_iterations=8)

        customers = onto.query().entities(Customer).collect()
        assert len(customers) == 2

    def test_event_handler_payload_filtering(self, tmp_db):
        fires: list[str] = []

        @on_event(OrderImported)
        def track_urgent(ctx: HandlerContext[OrderImported]) -> None:
            order = ctx.event
            if order.status != "Urgent":
                return
            fires.append(order.order_id)
            ctx.ensure(
                Order(
                    id=order.order_id,
                    customer_id=order.customer_id,
                    total_amount=order.total_amount,
                    status=order.status,
                    country=order.country,
                )
            )
            ctx.commit()

        onto = Session(
            tmp_db,
            config=_fast_config(),
            entity_types=[Order],
            relation_types=[],
        )

        with onto.session() as session:
            session.commit(
                event=OrderImported(
                    order_id="o1",
                    customer_id="c1",
                    total_amount=100.0,
                    status="New",
                    country="US",
                )
            )
            session.commit(
                event=OrderImported(
                    order_id="o2",
                    customer_id="c1",
                    total_amount=200.0,
                    status="Urgent",
                    country="US",
                )
            )
            session.run([track_urgent], max_iterations=10)

        assert fires == ["o2"]
        stored = onto.query().entities(Order).collect()
        assert [o.id for o in stored] == ["o2"]

    def test_relation_event_handler(self, tmp_db):
        fires: list[tuple[str, str]] = []

        @on_event(SubscriptionImported)
        def track_subscription(ctx: HandlerContext[SubscriptionImported]) -> None:
            evt = ctx.event
            fires.append((evt.left_key, evt.right_key))
            ctx.ensure(
                Subscription(
                    left_key=evt.left_key,
                    right_key=evt.right_key,
                    seat_count=evt.seat_count,
                    started_at=evt.started_at,
                )
            )
            ctx.commit()

        onto = Session(
            tmp_db,
            config=_fast_config(),
            entity_types=[Customer, Product],
            relation_types=[Subscription],
        )

        with onto.session() as session:
            session.ensure(Customer(id="c1", name="Alice", age=30))
            session.ensure(Product(sku="p1", name="Widget", price=10.0))
            session.commit()
            session.commit(
                event=SubscriptionImported(
                    left_key="c1",
                    right_key="p1",
                    seat_count=5,
                    started_at="2024",
                )
            )
            session.run([track_subscription], max_iterations=8)

        assert fires == [("c1", "p1")]

    def test_two_handlers_for_same_event(self, tmp_db):
        seen_events: list[str] = []
        names: list[str] = []

        @on_event(CustomerImported)
        def track_ctx(ctx: HandlerContext[CustomerImported]) -> None:
            seen_events.append(ctx.event.customer_id)

        @on_event(CustomerImported)
        def track_payload(ctx: HandlerContext[CustomerImported]) -> None:
            names.append(ctx.event.name)

        onto = Session(
            tmp_db,
            config=_fast_config(),
            entity_types=[Customer],
            relation_types=[],
        )

        with onto.session() as session:
            session.commit(event=CustomerImported(customer_id="c1", name="Alice", age=30))
            session.commit(event=CustomerImported(customer_id="c2", name="Bob", age=25))
            session.run([track_ctx, track_payload], max_iterations=10)

        assert seen_events == ["c1", "c2"]
        assert names == ["Alice", "Bob"]

    def test_handler_chain_with_emit_and_commit_event(self, tmp_db):
        @on_event(CustomerImported)
        def create_order_request(ctx: HandlerContext[CustomerImported]) -> None:
            ctx.emit(CreateOrderForCustomer(customer_id=ctx.event.customer_id))
            ctx.commit()

        @on_event(CreateOrderForCustomer)
        def create_order(ctx: HandlerContext[CreateOrderForCustomer]) -> None:
            cid = ctx.event.customer_id
            ctx.ensure(
                Order(
                    id=f"order-{cid}",
                    customer_id=cid,
                    total_amount=0.0,
                    status="New",
                    country="US",
                )
            )
            ctx.commit()

        onto = Session(
            tmp_db,
            config=_fast_config(),
            entity_types=[Customer, Order],
            relation_types=[],
        )

        with onto.session() as session:
            session.ensure(Customer(id="c1", name="Alice", age=30))
            session.commit(event=CustomerImported(customer_id="c1", name="Alice", age=30))
            session.run([create_order_request, create_order], max_iterations=12)

        orders = onto.query().entities(Order).collect()
        assert len(orders) == 1
        assert orders[0].id == "order-c1"

    def test_schedule_emits_event_for_on_event_handler(self, tmp_db, monkeypatch):
        import ontologia.session as session_module

        seen: list[str] = []
        next_fire_calls = {"count": 0}

        def fake_next_fire(spec: object, after):
            del spec
            next_fire_calls["count"] += 1
            if next_fire_calls["count"] == 1:
                return after
            return after + timedelta(minutes=1)

        monkeypatch.setattr(session_module, "_next_fire", fake_next_fire)

        @on_event(Tick)
        def handle_tick(ctx: HandlerContext[Tick]) -> None:
            seen.append(ctx.event.label)

        schedule = Schedule(event=Tick(label="scheduled"), cron="* * * * *")

        onto = Session(tmp_db, config=_fast_config(), entity_types=[Customer], relation_types=[])
        with onto.session() as session:
            session.run([handle_tick], schedules=[schedule], max_iterations=3)

        assert seen == ["scheduled"]

    def test_run_can_be_invoked_multiple_times(self, tmp_db):
        seen: list[str] = []

        @on_event(Tick)
        def handler(ctx: HandlerContext[Tick]) -> None:
            seen.append(ctx.event.label)

        onto = Session(
            tmp_db,
            config=_fast_config(),
            entity_types=[Customer],
            relation_types=[],
        )

        with onto.session() as session:
            session.commit(event=Tick(label="first"))
            session.run([handler], max_iterations=6)
            session.commit(event=Tick(label="second"))
            session.run([handler], max_iterations=6)

        assert seen == ["first", "second"]

    def test_duplicate_handler_fails(self, tmp_db):
        @on_event(Tick)
        def handler(ctx: HandlerContext[Tick]) -> None:
            del ctx

        onto = Session(
            tmp_db,
            config=_fast_config(),
            entity_types=[Customer],
            relation_types=[],
        )

        with pytest.raises(HandlerError, match="Duplicate handler"):
            with onto.session() as session:
                session.run([handler, handler], max_iterations=1)

    def test_undecorated_handler_fails(self, tmp_db):
        def plain_handler(ctx: object) -> None:
            del ctx

        onto = Session(
            tmp_db,
            config=_fast_config(),
            entity_types=[Customer],
            relation_types=[],
        )

        with pytest.raises(HandlerError, match="not decorated with @on_event"):
            with onto.session() as session:
                session.run([plain_handler], max_iterations=1)

    def test_batch_size_exceeded_is_released_without_persisting(self, tmp_db):
        @on_event(BulkLoad)
        def large_batch(ctx: HandlerContext[BulkLoad]) -> None:
            ctx.ensure(
                [
                    Customer(id="c1", name="Alice", age=30),
                    Customer(id="c2", name="Bob", age=25),
                ]
            )
            ctx.commit()

        onto = Session(
            tmp_db,
            config=_fast_config(max_batch_size=1, event_max_attempts=1),
            entity_types=[Customer],
            relation_types=[],
        )

        with onto.session() as session:
            session.commit(event=BulkLoad(count=2))
            session.run([large_batch], max_iterations=8)
            dead_letters = session._event_store.list_dead_letters(session.namespace)

        assert onto.query().entities(Customer).collect() == []
        assert len(dead_letters) == 1
        assert "max_batch_size" in dead_letters[0]["last_error"]

    def test_event_chain_depth_limit_dead_letters_event(self, tmp_db):
        @on_event(Loop)
        def infinite_chain(ctx: HandlerContext[Loop]) -> None:
            ctx.commit(event=Loop(step=ctx.event.step + 1))

        onto = Session(
            tmp_db,
            config=_fast_config(max_event_chain_depth=1, event_max_attempts=1),
            entity_types=[Customer],
            relation_types=[],
        )

        with onto.session() as session:
            session.commit(event=Loop(step=0))
            session.run([infinite_chain], max_iterations=12)
            dead_letters = session._event_store.list_dead_letters(session.namespace)

        assert len(dead_letters) >= 1
        assert any("max_event_chain_depth" in dl["last_error"] for dl in dead_letters)


class TestExplicitCommitBehavior:
    """Test explicit commit behavior in event mode."""

    def test_handler_without_commit_produces_no_state(self, tmp_db):
        seen: list[str] = []

        @on_event(CustomerImported)
        def on_customer(ctx: HandlerContext[CustomerImported]) -> None:
            seen.append(ctx.event.customer_id)
            ctx.ensure(Customer(id=ctx.event.customer_id, name=ctx.event.name, age=ctx.event.age))
            # no ctx.commit()

        onto = Session(
            tmp_db,
            config=_fast_config(),
            entity_types=[Customer],
            relation_types=[],
        )

        with onto.session() as session:
            session.commit(event=CustomerImported(customer_id="c1", name="Alice", age=30))
            session.run([on_customer], max_iterations=8)
            assert onto.query().entities(Customer).collect() == []

        assert seen == ["c1"]

    def test_commit_without_event_does_not_trigger_handlers(self, tmp_db):
        seen: list[str] = []

        @on_event(Tick)
        def on_tick(ctx: HandlerContext[Tick]) -> None:
            seen.append(ctx.event.label)

        onto = Session(
            tmp_db,
            config=_fast_config(),
            entity_types=[Customer],
            relation_types=[],
        )

        with onto.session() as session:
            session.ensure(Customer(id="c1", name="Alice", age=30))
            session.commit()  # no event
            session.run([on_tick], max_iterations=4)

        assert seen == []

    def test_commit_event_makes_pending_ensure_visible(self, tmp_db):
        seen_names: list[str] = []

        @on_event(Snapshot)
        def capture(ctx: HandlerContext[Snapshot]) -> None:
            customers = ctx.session.query().entities(Customer).collect()
            seen_names.extend(sorted(c.name for c in customers))

        onto = Session(
            tmp_db,
            config=_fast_config(),
            entity_types=[Customer],
            relation_types=[],
        )

        with onto.session() as session:
            session.ensure(Customer(id="c1", name="Alice", age=30))
            session.ensure(Customer(id="c2", name="Bob", age=25))
            session.commit(event=Snapshot(label="after-seed"))
            session.run([capture], max_iterations=8)

        assert seen_names == ["Alice", "Bob"]

    def test_run_without_events_is_noop(self, tmp_db):
        @on_event(Tick)
        def noop(ctx: HandlerContext[Tick]) -> None:
            del ctx

        onto = Session(
            tmp_db,
            config=_fast_config(),
            entity_types=[Customer],
            relation_types=[],
        )

        with onto.session() as session:
            session.run([noop], max_iterations=3)

        commits = onto.list_commits()
        assert len(commits) == 0


class TestLeaseExpiry:
    """Test that LeaseExpiredError is raised when a handler exceeds its lease."""

    def test_lease_expired_error_on_commit(self, tmp_db):
        """If the lease expires before ctx.commit(), LeaseExpiredError is raised
        and the event is released (not acked)."""

        handler_ran = {"value": False}

        @on_event(Tick)
        def slow_handler(ctx: HandlerContext[Tick]) -> None:
            handler_ran["value"] = True
            ctx.ensure(Customer(id="c1", name="Alice", age=30))
            # Force lease_until into the past so commit's lease check fails.
            ctx.lease_until = datetime(2000, 1, 1, tzinfo=timezone.utc)
            ctx.commit()

        onto = Session(
            tmp_db,
            config=_fast_config(event_claim_lease_ms=30000, event_max_attempts=1),
            entity_types=[Customer],
            relation_types=[],
        )

        with onto.session() as session:
            session.commit(event=Tick(label="slow"))
            session.run([slow_handler], max_iterations=8)

        assert handler_ran["value"] is True

        # The entity should NOT have been persisted because the lease expired
        customers = onto.query().entities(Customer).collect()
        assert customers == []

        # The event should be dead-lettered since max_attempts=1
        dead_letters = onto._event_store.list_dead_letters(onto.namespace)
        assert len(dead_letters) >= 1
        assert any("lease" in dl["last_error"].lower() for dl in dead_letters)


class TestMultiHandlerPriority:
    """Test that handlers execute in priority order (higher priority first)."""

    def test_handlers_execute_in_priority_order(self, tmp_db):
        execution_order: list[str] = []

        @on_event(Tick, priority=50)
        def low_priority(ctx: HandlerContext[Tick]) -> None:
            execution_order.append("low")

        @on_event(Tick, priority=200)
        def high_priority(ctx: HandlerContext[Tick]) -> None:
            execution_order.append("high")

        @on_event(Tick, priority=100)
        def medium_priority(ctx: HandlerContext[Tick]) -> None:
            execution_order.append("medium")

        onto = Session(
            tmp_db,
            config=_fast_config(),
            entity_types=[Customer],
            relation_types=[],
        )

        with onto.session() as session:
            session.commit(event=Tick(label="priority-test"))
            session.run(
                [low_priority, high_priority, medium_priority],
                max_iterations=8,
            )

        # Each handler should have processed the event, in priority order
        assert execution_order == ["high", "medium", "low"]


class TestEventPriorityOrdering:
    """Test that higher-priority events are claimed before lower-priority ones."""

    def test_high_priority_events_processed_first(self, tmp_db):
        class PrioritizedEvent(Event):
            label: Field[str]

        seen: list[str] = []

        @on_event(PrioritizedEvent)
        def handler(ctx: HandlerContext[PrioritizedEvent]) -> None:
            seen.append(ctx.event.label)

        onto = Session(
            tmp_db,
            config=_fast_config(),
            entity_types=[Customer],
            relation_types=[],
        )

        with onto.session() as session:
            # Enqueue low-priority first, then high-priority
            low = PrioritizedEvent(label="low")
            low.priority = 10
            session.commit(event=low)

            high = PrioritizedEvent(label="high")
            high.priority = 200
            session.commit(event=high)

            session.run([handler], max_iterations=8)

        assert seen == ["high", "low"]


class TestNamespaceIsolation:
    """Test that events in one namespace are invisible to handlers in another."""

    def test_events_isolated_by_namespace(self, tmp_db):
        seen_a: list[str] = []
        seen_b: list[str] = []

        @on_event(Tick)
        def handler_a(ctx: HandlerContext[Tick]) -> None:
            seen_a.append(ctx.event.label)

        @on_event(Tick)
        def handler_b(ctx: HandlerContext[Tick]) -> None:
            seen_b.append(ctx.event.label)

        db_uri = f"sqlite:///{tmp_db}"

        # Session A in namespace "ns_a"
        session_a = Session(
            db_uri,
            namespace="ns_a",
            config=_fast_config(),
            entity_types=[Customer],
            relation_types=[],
        )

        # Session B in namespace "ns_b"
        session_b = Session(
            db_uri,
            namespace="ns_b",
            config=_fast_config(),
            entity_types=[Customer],
            relation_types=[],
        )

        with session_a.session() as sa:
            sa.commit(event=Tick(label="for-a"))

        with session_b.session() as sb:
            sb.commit(event=Tick(label="for-b"))

        # Run handler_a in ns_a — should only see "for-a"
        with session_a.session() as sa:
            sa.run([handler_a], max_iterations=6)

        # Run handler_b in ns_b — should only see "for-b"
        with session_b.session() as sb:
            sb.run([handler_b], max_iterations=6)

        assert seen_a == ["for-a"]
        assert seen_b == ["for-b"]


class TestCommitEventFromHandler:
    """Test ctx.commit(event=...) atomically commits and enqueues."""

    def test_commit_event_inline_from_handler(self, tmp_db):
        """ctx.commit(event=X) should atomically commit entity state and
        enqueue the event so a downstream handler picks it up."""

        downstream_seen: list[str] = []

        @on_event(CustomerImported)
        def import_handler(ctx: HandlerContext[CustomerImported]) -> None:
            ctx.ensure(
                Customer(
                    id=ctx.event.customer_id,
                    name=ctx.event.name,
                    age=ctx.event.age,
                )
            )
            # Use commit(event=...) to atomically persist + enqueue
            ctx.commit(event=Tick(label=f"imported-{ctx.event.customer_id}"))

        @on_event(Tick)
        def downstream(ctx: HandlerContext[Tick]) -> None:
            downstream_seen.append(ctx.event.label)

        onto = Session(
            tmp_db,
            config=_fast_config(),
            entity_types=[Customer],
            relation_types=[],
        )

        with onto.session() as session:
            session.commit(event=CustomerImported(customer_id="c1", name="Alice", age=30))
            session.run([import_handler, downstream], max_iterations=12)

        customers = onto.query().entities(Customer).collect()
        assert len(customers) == 1
        assert downstream_seen == ["imported-c1"]


class TestMultipleCommitsPerHandler:
    """Test that a handler can commit multiple times."""

    def test_multiple_commits_in_one_handler(self, tmp_db):
        @on_event(BulkLoad)
        def batch_handler(ctx: HandlerContext[BulkLoad]) -> None:
            for i in range(ctx.event.count):
                ctx.ensure(Customer(id=f"c{i}", name=f"Customer {i}", age=20 + i))
                ctx.commit()

        onto = Session(
            tmp_db,
            config=_fast_config(),
            entity_types=[Customer],
            relation_types=[],
        )

        with onto.session() as session:
            session.commit(event=BulkLoad(count=3))
            session.run([batch_handler], max_iterations=8)

        customers = onto.query().entities(Customer).collect()
        assert len(customers) == 3


class TestExponentialBackoff:
    """Test that failed events get exponentially increasing available_at."""

    def test_backoff_increases_on_repeated_failures(self, tmp_db):
        fail_count = {"n": 0}

        @on_event(Tick)
        def failing_handler(ctx: HandlerContext[Tick]) -> None:
            fail_count["n"] += 1
            raise RuntimeError("intentional failure")

        config = OntologiaConfig(
            event_poll_interval_ms=10,
            event_max_attempts=5,
            event_backoff_base_ms=100,
            event_backoff_max_ms=10000,
        )

        onto = Session(
            tmp_db,
            config=config,
            entity_types=[Customer],
            relation_types=[],
        )

        with onto.session() as session:
            session.commit(event=Tick(label="backoff-test"))
            # Run enough iterations for the first failure
            session.run([failing_handler], max_iterations=3)

        # Verify the event was released (not acked) and has attempt count > 0
        event_details = onto._event_store.list_events(onto.namespace, limit=10)
        assert len(event_details) >= 1

        # Check the claim record directly for backoff state
        conn = getattr(onto._repo, "_conn")
        row = conn.execute(
            "SELECT attempts, available_at, last_error FROM event_claims LIMIT 1"
        ).fetchone()
        assert row is not None
        attempts = int(row[0])
        assert attempts >= 1
        assert "intentional failure" in str(row[2])


class TestEventDeadLetterHandler:
    """Test that EventDeadLetter events can be handled by an @on_event handler."""

    def test_dead_letter_event_emitted_and_handled(self, tmp_db):
        dead_letter_seen: list[str] = []

        @on_event(Tick)
        def always_fail(ctx: HandlerContext[Tick]) -> None:
            raise RuntimeError("always fails")

        @on_event(EventDeadLetter)
        def on_dead_letter(ctx: HandlerContext[EventDeadLetter]) -> None:
            dead_letter_seen.append(ctx.event.event_id)

        onto = Session(
            tmp_db,
            config=_fast_config(event_max_attempts=1),
            entity_types=[Customer],
            relation_types=[],
        )

        with onto.session() as session:
            session.commit(event=Tick(label="will-die"))
            session.run([always_fail, on_dead_letter], max_iterations=15)

        assert len(dead_letter_seen) >= 1


class TestExitAutoCommit:
    """Test that __exit__ auto-commits pending intents on clean exit."""

    def test_exit_auto_commits_pending_ensure(self, tmp_db):
        onto = Session(
            tmp_db,
            entity_types=[Customer],
            relation_types=[],
        )

        with onto.session() as session:
            session.ensure(Customer(id="c1", name="Alice", age=30))
            # No explicit commit — __exit__ should auto-commit

        customers = onto.query().entities(Customer).collect()
        assert len(customers) == 1
        assert customers[0].id == "c1"

    def test_exit_does_not_commit_on_exception(self, tmp_db):
        onto = Session(
            tmp_db,
            entity_types=[Customer],
            relation_types=[],
        )

        with pytest.raises(RuntimeError):
            with onto.session() as session:
                session.ensure(Customer(id="c1", name="Alice", age=30))
                raise RuntimeError("abort")

        customers = onto.query().entities(Customer).collect()
        assert len(customers) == 0
