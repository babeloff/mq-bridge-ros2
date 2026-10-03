//  mq-bridge
//  © Copyright 2026, by Marco Mengelkoch
//  Licensed under MIT OR Apache-2.0, see LICENSE file for more details
//  git clone https://github.com/marcomq/mq-bridge

//! Waiting for a job a remote system runs asynchronously: an indexing task, a
//! bulk import, a query that answers with a job id.

use std::future::Future;
use std::time::{Duration, Instant};

/// How often a job is asked for its state, and when a slow one is reported.
#[derive(Debug, Clone, Copy)]
pub struct PollSchedule {
    /// Delay before the second poll; it doubles after every poll.
    pub first: Duration,
    /// Upper bound of the delay.
    pub max: Duration,
    /// How long the job may run between two calls of the `slow` callback.
    pub notice_every: Duration,
}

impl Default for PollSchedule {
    fn default() -> Self {
        Self {
            first: Duration::from_millis(10),
            max: Duration::from_millis(250),
            notice_every: Duration::from_secs(60),
        }
    }
}

/// Calls `poll` until it answers `Some`, sleeping between calls as `schedule`
/// says. There is no deadline: `slow` is called with the time waited each
/// `notice_every`, so the caller can log it. Return `Some(Err(..))` from `poll`
/// to give up, and `None` for a failed poll that is worth repeating.
pub async fn poll_until<T, Fut>(
    schedule: PollSchedule,
    mut slow: impl FnMut(Duration),
    mut poll: impl FnMut() -> Fut,
) -> T
where
    Fut: Future<Output = Option<T>>,
{
    let started = Instant::now();
    let mut notice_at = schedule.notice_every;
    let mut delay = schedule.first;
    loop {
        if let Some(done) = poll().await {
            return done;
        }
        let waited = started.elapsed();
        if waited >= notice_at {
            slow(waited);
            notice_at += schedule.notice_every;
        }
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(schedule.max);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_slow_job_is_reported_and_still_awaited() {
        let schedule = PollSchedule {
            first: Duration::from_millis(1),
            max: Duration::from_millis(5),
            notice_every: Duration::from_millis(10),
        };
        let mut polls = 0;
        let mut notices = 0;
        let done = poll_until(
            schedule,
            |_| notices += 1,
            || {
                polls += 1;
                let finished = polls >= 12;
                async move { finished.then_some("succeeded") }
            },
        )
        .await;
        assert_eq!(done, "succeeded");
        assert_eq!(polls, 12);
        assert!(notices >= 1, "a job slower than `notice_every` is reported");
    }
}
