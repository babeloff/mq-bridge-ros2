//! A route whose transports are chosen by the caller: Kafka in production,
//! in-memory channels in the tests below.

use mq_bridge::{models::Endpoint, CanonicalMessage, Handled, HandlerError, Route};
use serde_json::{json, Value};

/// Business logic: reject orders without a positive amount, mark the rest as checked.
async fn check_order(msg: CanonicalMessage) -> Result<Handled, HandlerError> {
    let mut order: Value = msg
        .parse()
        .map_err(|e| HandlerError::NonRetryable(e.into()))?;
    if order["amount"].as_f64().unwrap_or(0.0) <= 0.0 {
        return Ok(Handled::Ack);
    }
    order["checked"] = json!(true);
    let out = CanonicalMessage::from_json(order).map_err(|e| HandlerError::NonRetryable(e.into()))?;
    Ok(Handled::Publish(out))
}

/// The route used in production and in tests. Only the endpoints differ.
pub fn orders_route(input: Endpoint, output: Endpoint) -> Route {
    Route::new(input, output).with_handler(check_order)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test(flavor = "multi_thread")]
    async fn valid_orders_are_checked_and_invalid_ones_dropped() {
        let input = Endpoint::new_memory("orders.in", 100);
        let output = Endpoint::new_memory("orders.out", 100);
        let (orders_in, orders_out) = (input.channel().unwrap(), output.channel().unwrap());

        orders_route(input, output).deploy("orders").await.unwrap();

        for order in [json!({"id": 1, "amount": 25.0}), json!({"id": 2, "amount": 0})] {
            let msg = CanonicalMessage::from_json(order).unwrap();
            orders_in.send_message(msg).await.unwrap();
        }

        let mut received = Vec::new();
        for _ in 0..100 {
            received.extend(orders_out.drain_messages());
            if !received.is_empty() && orders_in.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Route::stop("orders").await;

        assert_eq!(received.len(), 1);
        let order: Value = received[0].parse().unwrap();
        assert_eq!(order, json!({"id": 1, "amount": 25.0, "checked": true}));
    }
}
