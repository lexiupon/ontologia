"""Example 08: Event Handlers with RFC 0005 runtime.

This example demonstrates:
- Custom typed events with ``Event``
- ``@on_event(EventType)`` handlers
- Explicit handler commits via ``ctx.commit()``
- Event chaining via ``ctx.emit(...)``
- Optional periodic scheduling via ``Schedule``
"""

from __future__ import annotations

from datetime import datetime, timezone

from ontologia import (
    Entity,
    Event,
    Field,
    HandlerContext,
    OntologiaConfig,
    Schedule,
    Session,
    on_event,
)


class Customer(Entity):
    customer_id: Field[str] = Field(primary_key=True)
    name: Field[str]
    tier: Field[str] = Field(default="standard")


class Order(Entity):
    order_id: Field[str] = Field(primary_key=True)
    customer_id: Field[str] = Field(index=True)
    amount: Field[float]
    status: Field[str] = Field(default="pending")


class Alert(Entity):
    alert_id: Field[str] = Field(primary_key=True)
    order_id: Field[str]
    message: Field[str]
    severity: Field[str]
    created_at: Field[str]


class ImportDemoData(Event):
    batch_id: Field[str]


class OrderPlaced(Event):
    order_id: Field[str]
    customer_id: Field[str]
    amount: Field[float]
    status: Field[str]


class ReportSnapshot(Event):
    label: Field[str]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@on_event(ImportDemoData)
def import_demo_data(ctx: HandlerContext[ImportDemoData]) -> None:
    """Seed demo entities and emit an OrderPlaced event per order."""
    customers = [
        Customer(customer_id="c1", name="Alice Johnson", tier="gold"),
        Customer(customer_id="c2", name="Bob Smith", tier="standard"),
    ]
    orders = [
        Order(order_id="ord-001", customer_id="c1", amount=1500.0, status="pending"),
        Order(order_id="ord-002", customer_id="c1", amount=5000.0, status="urgent"),
        Order(order_id="ord-003", customer_id="c2", amount=250.0, status="pending"),
    ]

    ctx.ensure(customers)
    ctx.ensure(orders)

    for order in orders:
        ctx.emit(
            OrderPlaced(
                order_id=order.order_id,
                customer_id=order.customer_id,
                amount=order.amount,
                status=order.status,
            )
        )

    ctx.add_commit_meta("batch_id", ctx.event.batch_id)
    ctx.add_commit_meta("source", "example_08")
    ctx.commit()


@on_event(OrderPlaced)
def create_alert_for_urgent_orders(ctx: HandlerContext[OrderPlaced]) -> None:
    """Generate alerts for urgent or high-value orders."""
    event = ctx.event
    if event.status != "urgent" and event.amount < 2000:
        return

    severity = "critical" if event.status == "urgent" else "warning"
    ctx.ensure(
        Alert(
            alert_id=f"alert-{event.order_id}",
            order_id=event.order_id,
            message=f"Order {event.order_id} needs attention (${event.amount:.2f})",
            severity=severity,
            created_at=_now_iso(),
        )
    )
    ctx.add_commit_meta("handler", "create_alert_for_urgent_orders")
    ctx.commit()


@on_event(ReportSnapshot)
def print_report(ctx: HandlerContext[ReportSnapshot]) -> None:
    """Read committed state and print a compact report."""
    customers = ctx.session.query().entities(Customer).collect()
    orders = ctx.session.query().entities(Order).collect()
    alerts = ctx.session.query().entities(Alert).collect()

    print(f"\n[{ctx.event.label}]")
    print(f"  Customers: {len(customers)}")
    print(f"  Orders:    {len(orders)}")
    print(f"  Alerts:    {len(alerts)}")


def main() -> None:
    print("=" * 80)
    print("ONTOLOGIA EVENT HANDLERS EXAMPLE")
    print("=" * 80)

    config = OntologiaConfig(event_poll_interval_ms=50)
    session = Session(
        datastore_uri="tmp/handlers_example.db",
        config=config,
        entity_types=[Customer, Order, Alert],
    )

    report_schedule = Schedule(
        event=ReportSnapshot(label="scheduled-report"),
        cron="*/5 * * * *",
    )

    with session:
        session.commit(event=ImportDemoData(batch_id="demo-batch-1"))
        session.commit(event=ReportSnapshot(label="manual-report"))
        session.run(
            [import_demo_data, create_alert_for_urgent_orders, print_report],
            schedules=[report_schedule],
            max_iterations=20,
        )

        alerts = session.query().entities(Alert).collect()
        print("\nGenerated alerts:")
        for alert in sorted(alerts, key=lambda a: a.alert_id):
            print(f"  - {alert.alert_id}: {alert.message} [{alert.severity}]")

    print("\nDone. Database file: tmp/handlers_example.db")


if __name__ == "__main__":
    main()
