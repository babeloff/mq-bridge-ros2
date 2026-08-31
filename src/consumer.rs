use std::{
    any::Any,
    collections::VecDeque,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, MutexGuard,
    },
    time::Duration,
};

use anyhow::anyhow;
use async_trait::async_trait;
use mq_bridge::{
    errors::ConsumerError as BridgeConsumerError,
    traits::{BatchCommitFunc, BoxFuture, MessageConsumer},
    CanonicalMessage, ReceivedBatch,
};
use rclrs::{
    DynamicMessage, DynamicMessageMetadata, DynamicSubscription, MessageInfo, MessageTypeName,
    SubscriptionOptions,
};
use tokio::sync::Notify;

use crate::{
    config::{self, History},
    message,
    runtime::{self, Ros2Runtime},
};

/// Only applied while draining, so an idle topic yields an empty batch and lets
/// `exit_on_empty` fire. Live consumption blocks until a message arrives.
const FIRST_MESSAGE_WAIT: Duration = Duration::from_millis(250);
const NEXT_MESSAGE_WAIT: Duration = Duration::from_millis(5);

/// The hand-off between the ROS callback, which pushes, and `receive_batch`,
/// which pulls. `rclrs` exposes no way to take a message from a subscription on
/// demand, so a queue in between is unavoidable.
///
/// Its bound is the route's own history policy rather than a number of this
/// endpoint's choosing: `keep_last: depth` holds at most `depth` messages and
/// discards the oldest beyond that, which is what a KEEP_LAST reader queue
/// upstream already does, and `keep_all` holds everything. So the policy a
/// route asked ROS for is the policy it also gets on this side of the callback.
struct Inbox {
    queue: Mutex<VecDeque<CanonicalMessage>>,
    arrived: Notify,
    capacity: Option<usize>,
    discarded: AtomicU64,
}

impl Inbox {
    fn new(capacity: Option<usize>) -> Self {
        Self {
            queue: Mutex::new(VecDeque::new()),
            arrived: Notify::new(),
            capacity,
            discarded: AtomicU64::new(0),
        }
    }

    fn push(&self, message: CanonicalMessage) {
        {
            let mut queue = lock(&self.queue);
            if let Some(capacity) = self.capacity {
                while queue.len() >= capacity {
                    queue.pop_front();
                    self.discarded.fetch_add(1, Ordering::Relaxed);
                }
            }
            queue.push_back(message);
        }
        // Exactly one consumer waits on this, and `notify_one` leaves a permit
        // behind when nobody is waiting yet, so a message that arrives between
        // a failed `pop` and the `await` below cannot be missed.
        self.arrived.notify_one();
    }

    fn pop(&self) -> Option<CanonicalMessage> {
        lock(&self.queue).pop_front()
    }

    async fn wait(&self) {
        self.arrived.notified().await;
    }

    fn discarded(&self) -> u64 {
        self.discarded.load(Ordering::Relaxed)
    }
}

/// The queue only ever holds messages, so a poisoned lock still holds a usable
/// queue and recovering beats turning one panic into two.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

struct Ros2Consumer {
    /// Dropping this stops delivery, which is the only reason it is held.
    #[allow(dead_code)]
    subscription: DynamicSubscription,
    inbox: Arc<Inbox>,
    /// Declared after the subscription so the executor stops before the
    /// subscription it is pumping is torn down.
    runtime: Ros2Runtime,
    exit_on_empty: bool,
}

pub(crate) async fn create(
    route_name: &str,
    value: &serde_json::Value,
) -> anyhow::Result<Box<dyn MessageConsumer>> {
    let endpoint = config::resolve_for_consumer(route_name, value)?;

    // The type support library is loaded, and the payload field checked against
    // the real message definition, before this node joins the ROS graph: an
    // unknown message type or an unusable field is a mistake no amount of
    // reconnecting will fix.
    let message_type = MessageTypeName::try_from(endpoint.config.message_type.as_str())
        .map_err(|error| permanent(anyhow::Error::new(error)))?;
    let metadata = DynamicMessageMetadata::new(message_type.clone())
        .map_err(|error| permanent(anyhow::Error::new(error)))?;
    message::carrier_for(metadata.structure(), &endpoint.config.payload_field)
        .map_err(permanent)?;

    let inbox = Arc::new(Inbox::new(match endpoint.config.qos.history {
        History::KeepLast => Some(endpoint.config.qos.depth as usize),
        History::KeepAll => None,
    }));

    let field = endpoint.config.payload_field.clone();
    let topic = endpoint.topic.clone();
    let type_name = endpoint.config.message_type.clone();
    let sink = Arc::clone(&inbox);
    let callback = move |message: DynamicMessage, _info: MessageInfo| {
        match message::payload_of(&message, &field) {
            Ok(payload) => sink.push(message::to_canonical(payload, &topic, &type_name)),
            // Unreachable for a field that passed `carrier_for` above; reading a
            // validated field cannot fail. Reported rather than counted so that
            // a wrong assumption here is visible instead of silent.
            Err(error) => eprintln!("mq-bridge-ros2: dropping a message on {topic}: {error:#}"),
        }
    };

    let qos = endpoint.config.qos.profile();
    let (runtime, subscription) = Ros2Runtime::start(&endpoint, |node| {
        let mut options = SubscriptionOptions::new(&endpoint.topic);
        options.qos = qos;
        node.create_dynamic_subscription(message_type.clone(), options, callback)
    })
    .map_err(|error| {
        setup_error(error).context(format!(
            "failed to subscribe to ROS 2 topic {}",
            endpoint.topic
        ))
    })?;

    Ok(Box::new(Ros2Consumer {
        subscription,
        inbox,
        runtime,
        exit_on_empty: false,
    }))
}

#[async_trait]
impl MessageConsumer for Ros2Consumer {
    fn commit_requires_order(&self) -> bool {
        false
    }

    fn set_exit_on_empty(&mut self, exit_on_empty: bool) {
        self.exit_on_empty = exit_on_empty;
    }

    /// Stops the executor and leaves the ROS graph. The trait's `close()` awaits
    /// this hook, so both route shutdown and an explicit `close()` release the
    /// node. The join inside is bounded: halting wakes every wait set rather
    /// than waiting for the next message.
    fn on_disconnect_hook(&self) -> Option<BoxFuture<'_, anyhow::Result<()>>> {
        Some(Box::pin(async move {
            self.runtime.shutdown();
            let discarded = self.inbox.discarded();
            if discarded > 0 {
                // Worth saying once, at the end: a route that consistently
                // overflows its own `qos.depth` is losing data by configuration.
                eprintln!(
                    "mq-bridge-ros2: discarded {discarded} message(s) that overflowed \
                     `qos.depth` on node {}",
                    self.runtime.node().name()
                );
            }
            Ok(())
        }))
    }

    async fn receive_batch(
        &mut self,
        max_messages: usize,
    ) -> Result<ReceivedBatch, BridgeConsumerError> {
        if max_messages == 0 {
            return Ok(ReceivedBatch::empty());
        }

        let mut messages = Vec::with_capacity(max_messages);
        while messages.len() < max_messages {
            if let Some(message) = self.inbox.pop() {
                messages.push(message);
                continue;
            }
            // A ROS topic never ends, so unlike a queue with a closed stream
            // this can never report `EndOfStream`; a drained route stops on the
            // empty batch below instead.
            match message_wait(messages.len(), self.exit_on_empty) {
                Some(wait) => {
                    if tokio::time::timeout(wait, self.inbox.wait()).await.is_err() {
                        break;
                    }
                }
                None => self.inbox.wait().await,
            }
        }

        if messages.is_empty() {
            return Ok(ReceivedBatch::empty());
        }

        let expected = messages.len();
        // DDS has no consumer acknowledgement: nothing is sent back to the
        // publisher and nothing can be redelivered. The callback therefore only
        // holds the route to its side of the contract — one disposition per
        // message — and a `Nack` is accepted rather than honoured, because the
        // message is already gone. Routes that need redelivery need a broker.
        let commit: BatchCommitFunc = Box::new(move |dispositions| {
            Box::pin(async move {
                validate_disposition_count(expected, dispositions.len())?;
                Ok(())
            })
        });
        Ok(ReceivedBatch { messages, commit })
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// How long to wait for the message at `index`, or `None` to wait indefinitely.
/// A live route blocks for its first message; a draining one gives up after
/// [`FIRST_MESSAGE_WAIT`] so the empty batch can end the route.
fn message_wait(index: usize, exit_on_empty: bool) -> Option<Duration> {
    match index {
        0 if !exit_on_empty => None,
        0 => Some(FIRST_MESSAGE_WAIT),
        _ => Some(NEXT_MESSAGE_WAIT),
    }
}

fn permanent(error: anyhow::Error) -> anyhow::Error {
    anyhow::Error::new(BridgeConsumerError::Permanent(error))
}

/// Leaving a transient failure unclassified is deliberate: the route then treats
/// it as a connection failure and retries on its reconnect interval, which is
/// the right response to a middleware that is not up yet.
fn setup_error(error: rclrs::RclrsError) -> anyhow::Error {
    if runtime::is_permanent(&error) {
        permanent(anyhow::Error::new(error))
    } else {
        anyhow::Error::new(error)
    }
}

fn validate_disposition_count(expected: usize, actual: usize) -> anyhow::Result<()> {
    if actual == expected {
        Ok(())
    } else {
        Err(anyhow!(
            "ROS 2 batch commit received {actual} dispositions for {expected} messages"
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(payload: &str) -> CanonicalMessage {
        CanonicalMessage::from(payload.as_bytes().to_vec())
    }

    fn payloads(inbox: &Inbox) -> Vec<String> {
        let mut drained = Vec::new();
        while let Some(message) = inbox.pop() {
            drained.push(message.get_payload_str().to_string());
        }
        drained
    }

    #[test]
    fn batch_commit_requires_one_disposition_per_message() {
        assert!(validate_disposition_count(2, 2).is_ok());
        assert!(validate_disposition_count(2, 1).is_err());
    }

    #[test]
    fn live_consumption_waits_for_the_first_message() {
        assert_eq!(message_wait(0, false), None);
        assert_eq!(message_wait(1, false), Some(NEXT_MESSAGE_WAIT));
    }

    #[test]
    fn draining_gives_up_on_an_idle_topic() {
        assert_eq!(message_wait(0, true), Some(FIRST_MESSAGE_WAIT));
        assert_eq!(message_wait(1, true), Some(NEXT_MESSAGE_WAIT));
    }

    #[test]
    fn the_inbox_hands_messages_over_in_order() {
        let inbox = Inbox::new(Some(4));
        inbox.push(message("one"));
        inbox.push(message("two"));

        assert_eq!(payloads(&inbox), ["one", "two"]);
        assert_eq!(inbox.discarded(), 0);
        assert!(inbox.pop().is_none());
    }

    #[test]
    fn keep_last_drops_the_oldest_message_like_a_reader_queue_does() {
        let inbox = Inbox::new(Some(2));
        for payload in ["one", "two", "three"] {
            inbox.push(message(payload));
        }

        assert_eq!(payloads(&inbox), ["two", "three"]);
        assert_eq!(inbox.discarded(), 1);
    }

    #[test]
    fn keep_all_never_discards() {
        let inbox = Inbox::new(None);
        for index in 0..1_000 {
            inbox.push(message(&index.to_string()));
        }

        assert_eq!(payloads(&inbox).len(), 1_000);
        assert_eq!(inbox.discarded(), 0);
    }

    #[tokio::test]
    async fn a_waiter_is_woken_by_a_message_that_arrived_before_it_waited() {
        // The race `notify_one`'s stored permit exists to close: the callback
        // fires between `pop` returning `None` and `wait` being awaited.
        let inbox = Inbox::new(Some(4));
        inbox.push(message("early"));

        tokio::time::timeout(Duration::from_millis(50), inbox.wait())
            .await
            .expect("a message already in the inbox must not block its consumer");
    }

    #[tokio::test]
    async fn a_waiter_is_woken_by_a_later_push() {
        let inbox = Arc::new(Inbox::new(Some(4)));
        let sink = Arc::clone(&inbox);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            sink.push(message("late"));
        });

        tokio::time::timeout(Duration::from_millis(500), inbox.wait())
            .await
            .expect("a push must wake the consumer");
        assert_eq!(payloads(&inbox), ["late"]);
    }
}
