use anyhow::{anyhow, Context};
use serde::Deserialize;

/// Where a newly created subscription starts reading. Pulsar applies this only
/// when it creates the subscription; one that already exists keeps its own
/// cursor, so switching an existing route to `earliest` does not replay it.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum InitialPosition {
    /// Pulsar's own default: only messages published after the subscription
    /// exists are delivered.
    #[default]
    Latest,
    /// Start at the oldest retained message, so an existing backlog is readable.
    Earliest,
}

/// Configuration accepted by an endpoint named `pulsar`.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PulsarConfig {
    pub url: String,
    #[serde(default)]
    pub topic: Option<String>,
    #[serde(default)]
    pub subscription: Option<String>,
    /// Input only; ignored by a publisher.
    #[serde(default)]
    pub initial_position: InitialPosition,
}

/// Only the settings Pulsar can be asked for; a rejected one is returned as
/// [`mq_bridge::errors::InvalidConfig`] by the constructors, so the route stops.
pub(crate) fn resolve(
    route_name: &str,
    value: &serde_json::Value,
) -> anyhow::Result<(PulsarConfig, String, String)> {
    let config: PulsarConfig =
        serde_json::from_value(value.clone()).context("invalid Pulsar endpoint configuration")?;
    if config.url.trim().is_empty() {
        return Err(anyhow!("Pulsar `url` must not be empty"));
    }
    let topic = config
        .topic
        .clone()
        .unwrap_or_else(|| route_name.to_owned());
    if topic.trim().is_empty() {
        return Err(anyhow!("Pulsar `topic` must not be empty"));
    }
    let subscription = config
        .subscription
        .clone()
        .unwrap_or_else(|| format!("mq-bridge-{route_name}"));
    if subscription.trim().is_empty() {
        return Err(anyhow!("Pulsar `subscription` must not be empty"));
    }
    Ok((config, topic, subscription))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_topic_and_subscription_from_route_name() {
        let value = serde_json::json!({"url": "pulsar://localhost:6650"});
        let (config, topic, subscription) = resolve("orders", &value).unwrap();
        assert_eq!(topic, "orders");
        assert_eq!(subscription, "mq-bridge-orders");
        assert_eq!(config.initial_position, InitialPosition::Latest);
    }

    #[test]
    fn explicit_topic_and_subscription_win() {
        let value = serde_json::json!({
            "url": "pulsar://localhost:6650",
            "topic": "persistent://public/default/input",
            "subscription": "workers"
        });
        let (_, topic, subscription) = resolve("route", &value).unwrap();
        assert_eq!(topic, "persistent://public/default/input");
        assert_eq!(subscription, "workers");
    }

    #[test]
    fn initial_position_selects_where_a_new_subscription_starts() {
        let earliest = serde_json::json!({
            "url": "pulsar://localhost:6650",
            "initial_position": "earliest"
        });
        let (config, _, _) = resolve("route", &earliest).unwrap();
        assert_eq!(config.initial_position, InitialPosition::Earliest);

        let latest = serde_json::json!({
            "url": "pulsar://localhost:6650",
            "initial_position": "latest"
        });
        let (config, _, _) = resolve("route", &latest).unwrap();
        assert_eq!(config.initial_position, InitialPosition::Latest);

        let unknown = serde_json::json!({
            "url": "pulsar://localhost:6650",
            "initial_position": "beginning"
        });
        assert!(resolve("route", &unknown).is_err());
    }

    #[test]
    fn invalid_configuration_is_rejected_before_connecting() {
        assert!(resolve("route", &serde_json::json!({})).is_err());
        assert!(resolve("route", &serde_json::json!({"url": ""})).is_err());
        assert!(resolve(
            "route",
            &serde_json::json!({"url": "pulsar://localhost:6650", "extra": true})
        )
        .is_err());
    }
}
