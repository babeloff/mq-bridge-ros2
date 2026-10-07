use anyhow::{anyhow, Context, Result};
use mq_bridge::CanonicalMessage;
use rclrs::{
    ArrayValue, ArrayValueMut, BaseType, BoundedSequenceValue, BoundedSequenceValueMut,
    DynamicMessage, MessageStructure, SequenceValue, SequenceValueMut, SimpleValue, SimpleValueMut,
    Value, ValueKind, ValueMut,
};
use rosidl_runtime_rs::Sequence;

/// Metadata a received message carries, so a downstream route can tell where a
/// payload came from. A ROS 2 message has no property map of its own, so this
/// is the whole of it, and metadata cannot survive a round trip through ROS.
pub(crate) const TOPIC_KEY: &str = "ros2_topic";
pub(crate) const MESSAGE_TYPE_KEY: &str = "ros2_message_type";

/// How a message field can carry an opaque payload.
///
/// Derived from the field's *declared* type, so a route aimed at a field that
/// cannot hold bytes fails when the endpoint is created rather than when its
/// first message arrives.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Carrier {
    /// A `string` or `string<=N` field. The payload has to be valid UTF-8.
    Text,
    /// A `uint8`, `byte` or `char` collection, in any of its shapes.
    Bytes,
}

/// Resolves the payload field of a message type, reporting what else was on
/// offer when the field does not exist or cannot hold a payload.
pub(crate) fn carrier_for(structure: &MessageStructure, field: &str) -> Result<Carrier> {
    let Some(info) = structure.get_field_info(field) else {
        let available = structure
            .fields
            .iter()
            .map(|field| field.name.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        return Err(anyhow!(
            "message type {} has no field {field:?}; its fields are: {available}",
            structure.type_name
        ));
    };
    carrier(&info.base_type, info.value_kind).with_context(|| {
        format!(
            "`payload_field` {field:?} of message type {} cannot carry a payload",
            structure.type_name
        )
    })
}

/// The rule itself, over a field's declared type alone.
fn carrier(base_type: &BaseType, value_kind: ValueKind) -> Result<Carrier> {
    match (base_type, value_kind) {
        (BaseType::String | BaseType::BoundedString { .. }, ValueKind::Simple) => Ok(Carrier::Text),
        // A single `uint8` is deliberately not accepted: a one-byte payload is
        // far more likely to be a misconfiguration than an intent.
        (BaseType::Uint8 | BaseType::Octet | BaseType::Char, kind) if kind != ValueKind::Simple => {
            Ok(Carrier::Bytes)
        }
        _ => Err(anyhow!(
            "the field is declared {} {}, but a payload can only travel in a string field \
             or in a uint8/byte/char array, sequence or bounded sequence",
            describe(base_type),
            describe_kind(value_kind),
        )),
    }
}

fn describe(base_type: &BaseType) -> String {
    match base_type {
        // The nested structure's `Debug` would print the whole message tree.
        BaseType::Message(structure) => format!("nested {}", structure.type_name),
        other => format!("{other:?}"),
    }
}

fn describe_kind(value_kind: ValueKind) -> &'static str {
    match value_kind {
        ValueKind::Simple => "single value",
        ValueKind::Array { .. } => "array",
        ValueKind::Sequence => "sequence",
        ValueKind::BoundedSequence { .. } => "bounded sequence",
    }
}

/// Reads the payload field of a received message.
pub(crate) fn payload_of(message: &DynamicMessage, field: &str) -> Result<Vec<u8>> {
    let Some(value) = message.get(field) else {
        return Err(anyhow!("received message has no field {field:?}"));
    };
    match value {
        Value::Simple(SimpleValue::String(text)) => Ok(text_bytes(text)),
        Value::Simple(SimpleValue::BoundedString(text)) => Ok(text_bytes(&text)),
        Value::Sequence(
            SequenceValue::Uint8Sequence(bytes)
            | SequenceValue::OctetSequence(bytes)
            | SequenceValue::CharSequence(bytes),
        ) => Ok(bytes.as_slice().to_vec()),
        Value::BoundedSequence(
            BoundedSequenceValue::Uint8BoundedSequence(bytes)
            | BoundedSequenceValue::OctetBoundedSequence(bytes)
            | BoundedSequenceValue::CharBoundedSequence(bytes),
        ) => Ok(bytes.as_slice().to_vec()),
        Value::Array(
            ArrayValue::Uint8Array(bytes)
            | ArrayValue::OctetArray(bytes)
            | ArrayValue::CharArray(bytes),
        ) => Ok(bytes.to_vec()),
        // Unreachable for a field `carrier_for` accepted, which every endpoint
        // checks before its first message.
        _ => Err(anyhow!("field {field:?} cannot carry a payload")),
    }
}

/// Writes a payload into the payload field of a message about to be published.
pub(crate) fn set_payload(message: &mut DynamicMessage, field: &str, payload: &[u8]) -> Result<()> {
    let Some(value) = message.get_mut(field) else {
        return Err(anyhow!("message has no field {field:?}"));
    };
    match value {
        ValueMut::Simple(SimpleValueMut::String(text)) => {
            *text = rosidl_runtime_rs::String::from(as_text(field, payload)?);
            Ok(())
        }
        ValueMut::Simple(SimpleValueMut::BoundedString(mut text)) => {
            let bound = text.upper_bound();
            text.try_assign(as_text(field, payload)?).map_err(|_| {
                anyhow!(
                    "payload of {} characters does not fit the bound of {bound} on field {field:?}",
                    payload.len()
                )
            })
        }
        ValueMut::Sequence(
            SequenceValueMut::Uint8Sequence(bytes)
            | SequenceValueMut::OctetSequence(bytes)
            | SequenceValueMut::CharSequence(bytes),
        ) => {
            *bytes = Sequence::from(payload.to_vec());
            Ok(())
        }
        ValueMut::BoundedSequence(
            BoundedSequenceValueMut::Uint8BoundedSequence(mut bytes)
            | BoundedSequenceValueMut::OctetBoundedSequence(mut bytes)
            | BoundedSequenceValueMut::CharBoundedSequence(mut bytes),
        ) => {
            let bound = bytes.upper_bound();
            bytes.try_reset(payload.len()).map_err(|_| {
                anyhow!(
                    "payload of {} bytes does not fit the bound of {bound} on field {field:?}",
                    payload.len()
                )
            })?;
            bytes.as_mut_slice().copy_from_slice(payload);
            Ok(())
        }
        ValueMut::Array(
            ArrayValueMut::Uint8Array(slot)
            | ArrayValueMut::OctetArray(slot)
            | ArrayValueMut::CharArray(slot),
        ) => {
            // A fixed-length array has no length of its own to send, so a
            // shorter payload would arrive padded and indistinguishable.
            if slot.len() != payload.len() {
                return Err(anyhow!(
                    "field {field:?} is a fixed array of {} bytes, so it cannot carry a payload \
                     of {} bytes",
                    slot.len(),
                    payload.len()
                ));
            }
            slot.copy_from_slice(payload);
            Ok(())
        }
        _ => Err(anyhow!("field {field:?} cannot carry a payload")),
    }
}

/// Copies a ROS string out byte for byte. `to_string()` is lossy for bytes that
/// are not valid UTF-8 and `to_cstr()` stops at an interior nul, so neither is
/// safe for a payload this endpoint is only passing through.
fn text_bytes(text: &rosidl_runtime_rs::String) -> Vec<u8> {
    text.iter().map(|&character| character as u8).collect()
}

fn as_text<'a>(field: &str, payload: &'a [u8]) -> Result<&'a str> {
    std::str::from_utf8(payload).map_err(|error| {
        anyhow!("field {field:?} is a ROS 2 string field, so the payload must be UTF-8: {error}")
    })
}

/// Builds the canonical message a route sees, tagged with where it came from.
pub(crate) fn to_canonical(payload: Vec<u8>, topic: &str, message_type: &str) -> CanonicalMessage {
    let mut message = CanonicalMessage::from(payload);
    message
        .metadata
        .insert(TOPIC_KEY.to_owned(), topic.to_owned());
    message
        .metadata
        .insert(MESSAGE_TYPE_KEY.to_owned(), message_type.to_owned());
    message
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_string_field_carries_a_payload_as_text() {
        assert_eq!(
            carrier(&BaseType::String, ValueKind::Simple).unwrap(),
            Carrier::Text
        );
        let bounded = BaseType::BoundedString {
            upper_bound: 32.try_into().unwrap(),
        };
        assert_eq!(carrier(&bounded, ValueKind::Simple).unwrap(), Carrier::Text);
    }

    #[test]
    fn every_shape_of_byte_collection_carries_a_payload() {
        for base_type in [BaseType::Uint8, BaseType::Octet, BaseType::Char] {
            for kind in [
                ValueKind::Sequence,
                ValueKind::BoundedSequence { upper_bound: 64 },
                ValueKind::Array { length: 4 },
            ] {
                assert_eq!(
                    carrier(&base_type, kind).unwrap(),
                    Carrier::Bytes,
                    "{base_type:?} {kind:?} should carry bytes"
                );
            }
        }
    }

    #[test]
    fn a_single_byte_is_rejected_as_a_likely_misconfiguration() {
        assert!(carrier(&BaseType::Uint8, ValueKind::Simple).is_err());
        assert!(carrier(&BaseType::Octet, ValueKind::Simple).is_err());
    }

    #[test]
    fn a_field_that_cannot_hold_bytes_is_rejected_before_the_first_message() {
        assert!(carrier(&BaseType::Double, ValueKind::Sequence).is_err());
        assert!(carrier(&BaseType::Boolean, ValueKind::Simple).is_err());
        assert!(carrier(&BaseType::Int8, ValueKind::Sequence).is_err());
        // A string sequence is a list of strings, not a string.
        assert!(carrier(&BaseType::String, ValueKind::Sequence).is_err());
        // WString is UTF-16; passing opaque bytes through it would not survive.
        assert!(carrier(&BaseType::WString, ValueKind::Simple).is_err());
    }

    #[test]
    fn the_rejection_names_the_type_it_saw_so_the_config_can_be_fixed() {
        let error = carrier(&BaseType::Double, ValueKind::Sequence)
            .unwrap_err()
            .to_string();
        assert!(error.contains("Double"), "{error}");
        assert!(error.contains("sequence"), "{error}");
    }

    #[test]
    fn a_payload_bound_for_a_string_field_must_be_utf8() {
        assert_eq!(as_text("data", b"payload").unwrap(), "payload");
        assert!(as_text("data", &[0xff, 0xfe]).is_err());
    }
}
