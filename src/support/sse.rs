//  mq-bridge
//  © Copyright 2026, by Marco Mengelkoch
//  Licensed under MIT OR Apache-2.0, see LICENSE file for more details
//  git clone https://github.com/marcomq/mq-bridge

//! Server-sent event framing shared by the `http` and `http_bulk` endpoints.

use bytes::Bytes;

pub(crate) struct ParsedSseEvent {
    pub(crate) payload: Bytes,
    pub(crate) event_id: Option<String>,
    pub(crate) event_name: Option<String>,
}

/// Byte offset of the blank-line terminator ending the first complete SSE event.
/// Scans raw bytes so a multi-byte UTF-8 character split across body frames is never
/// inspected mid-sequence; decoding happens only once a full event has been framed.
pub(crate) fn find_sse_event_end(buffer: &[u8]) -> Option<usize> {
    let lf = buffer.windows(2).position(|w| w == b"\n\n");
    let crlf = buffer.windows(4).position(|w| w == b"\r\n\r\n");
    match (lf, crlf) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

pub(crate) fn parse_sse_event(raw: &str) -> Option<ParsedSseEvent> {
    let mut data_lines = Vec::new();
    let mut event_id = None;
    let mut event_name = None;

    for line in raw.lines() {
        let line = line.trim_end_matches('\r');
        if line.is_empty() || line.starts_with(':') {
            continue;
        }
        let (field, value) = line.split_once(':').unwrap_or((line, ""));
        let value = value.strip_prefix(' ').unwrap_or(value);
        match field {
            "data" => data_lines.push(value.to_string()),
            "id" => event_id = Some(value.to_string()),
            "event" => event_name = Some(value.to_string()),
            _ => {}
        }
    }

    if data_lines.is_empty() {
        return None;
    }

    Some(ParsedSseEvent {
        payload: Bytes::from(data_lines.join("\n").into_bytes()),
        event_id,
        event_name,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_sse_event_collects_data_id_and_event() {
        let event = parse_sse_event(": keepalive\nid: evt-7\nevent: update\ndata: one\ndata: two")
            .expect("sse event");

        assert_eq!(event.payload, Bytes::from_static(b"one\ntwo"));
        assert_eq!(event.event_id.as_deref(), Some("evt-7"));
        assert_eq!(event.event_name.as_deref(), Some("update"));
    }
}
