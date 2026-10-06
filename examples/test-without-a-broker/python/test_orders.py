from mq_bridge import Consumer, Publisher

from orders import orders_route

INPUT = {"memory": {"topic": "orders.in", "capacity": 100}}
OUTPUT = {"memory": {"topic": "orders.out", "capacity": 100}}


def test_valid_orders_are_checked_and_invalid_ones_dropped():
    publisher = Publisher.from_config(INPUT)
    results = Consumer.from_config(OUTPUT)

    with orders_route({"input": INPUT, "output": OUTPUT}):
        publisher.send_json({"id": 1, "amount": 25.0}, {"kind": "order.created"})
        publisher.send_json({"id": 2, "amount": 0}, {"kind": "order.created"})
        publisher.send_json({"id": 3, "amount": 5.0}, {"kind": "order.created"})
        received = []
        while len(received) < 2:
            batch = results.poll(max=10, timeout_ms=5000)
            assert batch, "timed out waiting for the route"
            received.extend(batch)

    assert [m.json() for m in received] == [
        {"id": 1, "amount": 25.0, "checked": True},
        {"id": 3, "amount": 5.0, "checked": True},
    ]
