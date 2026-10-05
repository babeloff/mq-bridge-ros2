"""A route whose transports come from config: Kafka in production, memory in tests."""

from mq_bridge import Route


def check_order(order):
    """Business logic: drop orders without a positive amount, mark the rest as checked."""
    if order.get("amount", 0) <= 0:
        return None
    return {**order, "checked": True}


def orders_route(config: dict) -> Route:
    """The route used in production and in tests. Only `config` differs."""
    return Route.from_config(config).add_handler("order.created", check_order)
