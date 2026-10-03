//  mq-bridge
//  © Copyright 2026, by Marco Mengelkoch
//  Licensed under MIT OR Apache-2.0, see LICENSE file for more details
//  git clone https://github.com/marcomq/mq-bridge

//! Which HTTP response statuses a sink should retry.

use crate::errors::PublisherError;

/// Whether a request that got `status` may succeed when sent again: 408, 429
/// and the 5xx range except 501 and 505, which a retry cannot change.
pub fn is_retryable(status: u16) -> bool {
    matches!(status, 408 | 429) || (matches!(status, 500..=599) && !matches!(status, 501 | 505))
}

/// Wraps `error` as retryable or permanent according to `status`.
pub fn publisher_error(status: u16, error: anyhow::Error) -> PublisherError {
    if is_retryable(status) {
        PublisherError::Retryable(error)
    } else {
        PublisherError::NonRetryable(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn throttling_and_server_failures_are_retried_and_rejections_are_not() {
        for status in [408, 429, 500, 502, 503, 504] {
            assert!(is_retryable(status), "{status}");
        }
        for status in [200, 400, 401, 404, 413, 501, 505] {
            assert!(!is_retryable(status), "{status}");
        }
        assert!(matches!(
            publisher_error(503, anyhow::anyhow!("busy")),
            PublisherError::Retryable(_)
        ));
        assert!(matches!(
            publisher_error(400, anyhow::anyhow!("bad")),
            PublisherError::NonRetryable(_)
        ));
    }
}
