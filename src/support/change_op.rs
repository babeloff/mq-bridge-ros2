//  mq-bridge
//  © Copyright 2026, by Marco Mengelkoch
//  Licensed under MIT OR Apache-2.0, see LICENSE file for more details
//  git clone https://github.com/marcomq/mq-bridge

//! What a change message asks a sink to do, for sinks fed from a CDC source.

/// The operation name sources use for a table truncation.
pub const TRUNCATE: &str = "truncate";

/// What one change message asks for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChangeOp {
    /// Write the row: an insert, an update, a snapshot row, or no operation at all.
    Upsert,
    /// Remove the row.
    Delete,
    /// Empty the whole target.
    Truncate,
}

impl ChangeOp {
    /// Classifies a message's operation value, as rendered from a template such
    /// as `${metadata:postgres.operation}`. Matching ignores ASCII case. A
    /// missing or unknown value is an upsert, so a snapshot row is still written.
    pub fn classify<S: AsRef<str>>(operation: Option<&str>, delete_values: &[S]) -> Self {
        let Some(operation) = operation else {
            return ChangeOp::Upsert;
        };
        if operation.eq_ignore_ascii_case(TRUNCATE) {
            ChangeOp::Truncate
        } else if delete_values
            .iter()
            .any(|value| value.as_ref().eq_ignore_ascii_case(operation))
        {
            ChangeOp::Delete
        } else {
            ChangeOp::Upsert
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_listed_value_deletes_and_only_truncate_truncates() {
        let deletes = ["delete", "d"];
        assert_eq!(ChangeOp::classify(None, &deletes), ChangeOp::Upsert);
        assert_eq!(
            ChangeOp::classify(Some("insert"), &deletes),
            ChangeOp::Upsert
        );
        assert_eq!(ChangeOp::classify(Some(""), &deletes), ChangeOp::Upsert);
        assert_eq!(
            ChangeOp::classify(Some("DELETE"), &deletes),
            ChangeOp::Delete
        );
        assert_eq!(ChangeOp::classify(Some("d"), &deletes), ChangeOp::Delete);
        assert_eq!(
            ChangeOp::classify(Some("Truncate"), &deletes),
            ChangeOp::Truncate
        );
    }
}
