use anyhow::{anyhow, Context};
use mq_bridge::errors::{ConsumerError, PublisherError};
use rclrs::{QoSDurabilityPolicy, QoSHistoryPolicy, QoSProfile, QoSReliabilityPolicy};
use serde::Deserialize;

/// The message type a route carries when its configuration does not name one.
/// `std_msgs/msg/String` is what most ROS 2 graphs use for opaque text.
const DEFAULT_MESSAGE_TYPE: &str = "std_msgs/msg/String";
/// The field of that message the payload is read from and written to.
const DEFAULT_PAYLOAD_FIELD: &str = "data";
/// Matches the depth of `rmw`'s own default profile.
const DEFAULT_DEPTH: u32 = 10;

/// The `RELIABILITY` policy, as a route spells it.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Reliability {
    /// Retry until every matched subscription has the sample. The ROS default.
    #[default]
    Reliable,
    /// Send once and accept loss. A `best_effort` publisher is **incompatible**
    /// with a `reliable` subscription, and the two will not match at all.
    BestEffort,
    /// Whatever the RMW implementation defaults to.
    SystemDefault,
}

/// The `DURABILITY` policy, as a route spells it. This is the ROS 2 equivalent
/// of asking where a late-joining reader starts.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Durability {
    /// Only samples published while this endpoint is matched are delivered.
    #[default]
    Volatile,
    /// A publisher retains its last `depth` samples and delivers them to
    /// readers that match later, so a subscription created after the fact can
    /// still see them. Both sides must ask for it.
    TransientLocal,
    /// Whatever the RMW implementation defaults to.
    SystemDefault,
}

/// The `HISTORY` policy, as a route spells it.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum History {
    /// Queue the most recent `depth` samples.
    #[default]
    KeepLast,
    /// Queue everything the middleware's resource limits allow. `depth` is
    /// then ignored.
    KeepAll,
}

/// The subset of DDS quality of service a route can set. The remaining
/// policies — deadline, lifespan, liveliness — are left at the ROS defaults,
/// because a bridge has no basis on which to pick them.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, default)]
pub struct QosConfig {
    pub reliability: Reliability,
    pub durability: Durability,
    pub history: History,
    pub depth: u32,
}

impl Default for QosConfig {
    fn default() -> Self {
        Self {
            reliability: Reliability::default(),
            durability: Durability::default(),
            history: History::default(),
            depth: DEFAULT_DEPTH,
        }
    }
}

impl QosConfig {
    /// Builds the profile handed to `rclrs`. Publisher and subscription use the
    /// same one, so a route that talks to itself always matches.
    pub(crate) fn profile(&self) -> QoSProfile {
        let mut profile = QoSProfile::topics_default();
        profile.history = match self.history {
            History::KeepLast => QoSHistoryPolicy::KeepLast { depth: self.depth },
            History::KeepAll => QoSHistoryPolicy::KeepAll,
        };
        profile.reliability = match self.reliability {
            Reliability::Reliable => QoSReliabilityPolicy::Reliable,
            Reliability::BestEffort => QoSReliabilityPolicy::BestEffort,
            Reliability::SystemDefault => QoSReliabilityPolicy::SystemDefault,
        };
        profile.durability = match self.durability {
            Durability::Volatile => QoSDurabilityPolicy::Volatile,
            Durability::TransientLocal => QoSDurabilityPolicy::TransientLocal,
            Durability::SystemDefault => QoSDurabilityPolicy::SystemDefault,
        };
        profile
    }
}

/// Configuration accepted by an endpoint named `ros2`.
///
/// Every field has a default, so `config: {}` is valid: the route name supplies
/// the node and topic names, and the payload travels as the `data` field of a
/// `std_msgs/msg/String`.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Ros2Config {
    /// Node name this endpoint appears under in the ROS graph. Defaults to
    /// `mq_bridge_<route>`.
    #[serde(default)]
    pub node: Option<String>,
    /// Absolute namespace for the node, e.g. `/ingest`. Defaults to `/`.
    #[serde(default)]
    pub namespace: Option<String>,
    /// Topic to subscribe to or publish on. Defaults to the route name.
    #[serde(default)]
    pub topic: Option<String>,
    /// Message type, as `<package>/msg/<Type>`.
    #[serde(default = "default_message_type")]
    pub message_type: String,
    /// Field of that message the payload occupies. It has to be a string or a
    /// byte sequence; see `message.rs` for the exact set.
    #[serde(default = "default_payload_field")]
    pub payload_field: String,
    #[serde(default)]
    pub qos: QosConfig,
    /// `ROS_DOMAIN_ID` override. Defaults to whatever the environment says.
    #[serde(default)]
    pub domain_id: Option<usize>,
}

fn default_message_type() -> String {
    DEFAULT_MESSAGE_TYPE.to_owned()
}

fn default_payload_field() -> String {
    DEFAULT_PAYLOAD_FIELD.to_owned()
}

/// A configuration with every default filled in and every name checked, so
/// nothing downstream has to ask whether a name is usable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Resolved {
    pub config: Ros2Config,
    pub node: String,
    pub namespace: String,
    pub topic: String,
}

/// A rejected configuration cannot heal by reconnecting, so both constructors
/// below hand the route an error classified as permanent. An unclassified
/// `anyhow::Error` reaches the route as a connection failure, which it retries
/// on its reconnect interval forever.
pub(crate) fn resolve_for_consumer(
    route_name: &str,
    value: &serde_json::Value,
) -> anyhow::Result<Resolved> {
    resolve(route_name, value).map_err(|error| anyhow::Error::new(ConsumerError::Permanent(error)))
}

pub(crate) fn resolve_for_publisher(
    route_name: &str,
    value: &serde_json::Value,
) -> anyhow::Result<Resolved> {
    resolve(route_name, value)
        .map_err(|error| anyhow::Error::new(PublisherError::NonRetryable(error)))
}

fn resolve(route_name: &str, value: &serde_json::Value) -> anyhow::Result<Resolved> {
    let config: Ros2Config =
        serde_json::from_value(value.clone()).context("invalid ROS 2 endpoint configuration")?;

    // A route name is free-form, but a ROS name is not, so a *derived* name is
    // sanitised while an *explicit* one is only checked: a user who spells a
    // topic out wants that topic or an error, not a silent rewrite.
    let node = match &config.node {
        Some(node) => {
            validate_name_token(node).context("ROS 2 `node` name is not a valid ROS 2 name")?;
            node.clone()
        }
        None => format!("mq_bridge_{}", sanitize(route_name)),
    };
    let namespace = match &config.namespace {
        Some(namespace) => {
            validate_namespace(namespace)?;
            namespace.clone()
        }
        None => "/".to_owned(),
    };
    let topic = match &config.topic {
        Some(topic) => {
            validate_topic(topic)?;
            topic.clone()
        }
        None => sanitize(route_name),
    };

    validate_message_type(&config.message_type)?;
    if config.payload_field.trim().is_empty() {
        return Err(anyhow!("ROS 2 `payload_field` must not be empty"));
    }
    if matches!(config.qos.history, History::KeepLast) && config.qos.depth == 0 {
        return Err(anyhow!(
            "ROS 2 `qos.depth` must be at least 1 for keep_last"
        ));
    }

    Ok(Resolved {
        config,
        node,
        namespace,
        topic,
    })
}

/// Rewrites an arbitrary route name into something a ROS node or topic can be
/// called: `[A-Za-z_][A-Za-z0-9_]*`. Route names routinely contain hyphens and
/// dots, and a uuid-suffixed one starts with a digit often enough to matter.
fn sanitize(route_name: &str) -> String {
    let mut name: String = route_name
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character
            } else {
                '_'
            }
        })
        .collect();
    if !name
        .chars()
        .next()
        .is_some_and(|first| first.is_ascii_alphabetic() || first == '_')
    {
        name.insert(0, '_');
    }
    name
}

/// One segment of a ROS name.
fn is_name_token(token: &str) -> bool {
    let mut characters = token.chars();
    characters
        .next()
        .is_some_and(|first| first.is_ascii_alphabetic() || first == '_')
        && characters.all(|character| character.is_ascii_alphanumeric() || character == '_')
}

fn validate_name_token(name: &str) -> anyhow::Result<()> {
    if is_name_token(name) {
        Ok(())
    } else {
        Err(anyhow!(
            "{name:?} must match [A-Za-z_][A-Za-z0-9_]* to be a ROS 2 name"
        ))
    }
}

fn validate_namespace(namespace: &str) -> anyhow::Result<()> {
    if namespace == "/" {
        return Ok(());
    }
    let Some(path) = namespace.strip_prefix('/') else {
        return Err(anyhow!(
            "ROS 2 `namespace` {namespace:?} must be absolute, so it has to start with `/`"
        ));
    };
    if path.split('/').all(is_name_token) {
        Ok(())
    } else {
        Err(anyhow!(
            "ROS 2 `namespace` {namespace:?} is not a valid ROS 2 namespace"
        ))
    }
}

fn validate_topic(topic: &str) -> anyhow::Result<()> {
    // `/name` is absolute and `~/name` is private to the node; anything else is
    // relative to the namespace. `rcl` expands all three, this only checks the
    // segments it will expand.
    let path = topic
        .strip_prefix("~/")
        .or_else(|| topic.strip_prefix('/'))
        .unwrap_or(topic);
    if !path.is_empty() && path.split('/').all(is_name_token) {
        Ok(())
    } else {
        Err(anyhow!(
            "ROS 2 `topic` {topic:?} is not a valid ROS 2 topic name"
        ))
    }
}

/// The same shape `rclrs::MessageTypeName` parses. Checking it here makes a
/// typo a permanent error instead of a connection failure the route retries.
fn validate_message_type(message_type: &str) -> anyhow::Result<()> {
    let parts: Vec<&str> = message_type.split('/').collect();
    if let [package, "msg", type_name] = parts[..] {
        if is_name_token(package) && is_name_token(type_name) {
            return Ok(());
        }
    }
    Err(anyhow!(
        "ROS 2 `message_type` {message_type:?} must have the form <package>/msg/<Type>, \
         for example std_msgs/msg/String"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resolved(route: &str, value: serde_json::Value) -> Resolved {
        resolve(route, &value).unwrap()
    }

    #[test]
    fn defaults_node_topic_and_message_type_from_the_route_name() {
        let endpoint = resolved("orders", serde_json::json!({}));
        assert_eq!(endpoint.node, "mq_bridge_orders");
        assert_eq!(endpoint.namespace, "/");
        assert_eq!(endpoint.topic, "orders");
        assert_eq!(endpoint.config.message_type, "std_msgs/msg/String");
        assert_eq!(endpoint.config.payload_field, "data");
    }

    #[test]
    fn a_derived_name_is_sanitized_because_route_names_are_not_ros_names() {
        // mq-bridge's own tests build route names like `round-trip-<uuid>`,
        // which no ROS node could be called.
        let endpoint = resolved("round-trip-9f1c.2", serde_json::json!({}));
        assert_eq!(endpoint.node, "mq_bridge_round_trip_9f1c_2");
        assert_eq!(endpoint.topic, "round_trip_9f1c_2");
    }

    #[test]
    fn a_derived_name_never_starts_with_a_digit() {
        assert_eq!(sanitize("7up"), "_7up");
        assert_eq!(sanitize("-lead"), "_lead");
        assert_eq!(sanitize(""), "_");
    }

    #[test]
    fn explicit_names_win_and_are_not_rewritten() {
        let endpoint = resolved(
            "route",
            serde_json::json!({
                "node": "ingest_bridge",
                "namespace": "/ingest/edge",
                "topic": "/orders/new",
                "message_type": "std_msgs/msg/UInt8MultiArray",
                "payload_field": "data"
            }),
        );
        assert_eq!(endpoint.node, "ingest_bridge");
        assert_eq!(endpoint.namespace, "/ingest/edge");
        assert_eq!(endpoint.topic, "/orders/new");
        assert_eq!(endpoint.config.message_type, "std_msgs/msg/UInt8MultiArray");
    }

    #[test]
    fn relative_and_private_topics_are_accepted() {
        assert!(validate_topic("orders").is_ok());
        assert!(validate_topic("/orders/new").is_ok());
        assert!(validate_topic("~/orders").is_ok());
    }

    #[test]
    fn an_invalid_ros_name_is_rejected_rather_than_repaired() {
        // Hyphens are the common trap: legal in a route name, illegal in ROS.
        assert!(resolve("route", &serde_json::json!({"topic": "order-new"})).is_err());
        assert!(resolve("route", &serde_json::json!({"node": "mq-bridge"})).is_err());
        assert!(resolve("route", &serde_json::json!({"topic": "orders//new"})).is_err());
        assert!(resolve("route", &serde_json::json!({"topic": ""})).is_err());
        // A namespace has to be absolute.
        assert!(resolve("route", &serde_json::json!({"namespace": "ingest"})).is_err());
    }

    #[test]
    fn message_type_must_name_a_msg() {
        assert!(validate_message_type("std_msgs/msg/String").is_ok());
        assert!(validate_message_type("std_msgs/String").is_err());
        assert!(validate_message_type("std_msgs/srv/Trigger").is_err());
        assert!(validate_message_type("String").is_err());
        assert!(validate_message_type("std_msgs/msg/String/extra").is_err());
    }

    #[test]
    fn invalid_configuration_is_rejected_before_connecting() {
        assert!(resolve("route", &serde_json::json!({"extra": true})).is_err());
        assert!(resolve("route", &serde_json::json!({"payload_field": " "})).is_err());
        assert!(resolve("route", &serde_json::json!({"qos": {"depth": 0}})).is_err());
        assert!(resolve(
            "route",
            &serde_json::json!({"qos": {"reliability": "eventual"}})
        )
        .is_err());
    }

    #[test]
    fn keep_all_ignores_depth_so_a_zero_depth_is_not_an_error() {
        let endpoint = resolved(
            "route",
            serde_json::json!({"qos": {"history": "keep_all", "depth": 0}}),
        );
        assert_eq!(endpoint.config.qos.history, History::KeepAll);
        assert_eq!(
            endpoint.config.qos.profile().history,
            QoSHistoryPolicy::KeepAll
        );
    }

    #[test]
    fn qos_defaults_match_the_ros_topic_defaults() {
        let profile = QosConfig::default().profile();
        assert_eq!(profile.reliability, QoSReliabilityPolicy::Reliable);
        assert_eq!(profile.durability, QoSDurabilityPolicy::Volatile);
        assert_eq!(
            profile.history,
            QoSHistoryPolicy::KeepLast {
                depth: DEFAULT_DEPTH
            }
        );
    }

    #[test]
    fn transient_local_is_how_a_late_joiner_reads_what_it_missed() {
        let endpoint = resolved(
            "route",
            serde_json::json!({
                "qos": {"durability": "transient_local", "depth": 100}
            }),
        );
        let profile = endpoint.config.qos.profile();
        assert_eq!(profile.durability, QoSDurabilityPolicy::TransientLocal);
        assert_eq!(profile.history, QoSHistoryPolicy::KeepLast { depth: 100 });
    }

    #[test]
    fn a_rejected_configuration_is_permanent_so_the_route_stops_reconnecting() {
        let value = serde_json::json!({"extra": true});

        let consumer_error = resolve_for_consumer("route", &value).unwrap_err();
        assert!(matches!(
            consumer_error.downcast_ref::<ConsumerError>(),
            Some(ConsumerError::Permanent(_))
        ));

        let publisher_error = resolve_for_publisher("route", &value).unwrap_err();
        assert!(matches!(
            publisher_error.downcast_ref::<PublisherError>(),
            Some(PublisherError::NonRetryable(_))
        ));
    }
}
