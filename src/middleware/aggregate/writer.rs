//  mq-bridge
//  © Copyright 2026, by Marco Mengelkoch
//  Licensed under MIT OR Apache-2.0, see LICENSE file for more details
//  git clone https://github.com/marcomq/mq-bridge

//! `consistency: single_writer`: the states stay in memory and are written behind.

use super::states::States;
use super::store::{StateStore, StateWrite};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

/// Failed flushes remembered, so a late commit still learns its batch was lost.
const LOST_KEPT: usize = 64;

pub(super) struct Inner {
    pub(super) states: Vec<States>,
    /// Version each loaded or written key has in the store; absent means 0.
    pub(super) versions: Vec<HashMap<String, i64>>,
    /// Keys changed since the last flush took its snapshot.
    pub(super) dirty: Vec<HashSet<String>>,
    /// Number of the last folded batch.
    seq: u64,
    /// Every batch up to this number is in the store, unless it is in `lost`.
    flushed: u64,
    /// Batch ranges (exclusive, inclusive] whose states were dropped by a failed flush.
    lost: Vec<(u64, u64)>,
}

/// Owns the states of one middleware and writes the changed ones in a loop: as soon as a
/// flush returns, the next one takes everything changed in the meantime.
pub(super) struct Writer {
    pub(super) store: Arc<dyn StateStore>,
    /// Store key prefix of each entry.
    prefixes: Vec<String>,
    /// Held while a batch loads and folds, and while a failed flush drops the states.
    pub(super) gate: tokio::sync::Mutex<()>,
    inner: Mutex<Inner>,
    wake: tokio::sync::Notify,
    progress: tokio::sync::watch::Sender<()>,
    closed: AtomicBool,
}

impl Writer {
    pub(super) fn start(
        store: Arc<dyn StateStore>,
        prefixes: Vec<String>,
        max_keys: usize,
    ) -> Arc<Self> {
        let n = prefixes.len();
        let writer = Arc::new(Self {
            store,
            prefixes,
            gate: tokio::sync::Mutex::new(()),
            inner: Mutex::new(Inner {
                states: (0..n).map(|_| States::new(max_keys)).collect(),
                versions: vec![HashMap::new(); n],
                dirty: vec![HashSet::new(); n],
                seq: 0,
                flushed: 0,
                lost: Vec::new(),
            }),
            wake: tokio::sync::Notify::new(),
            progress: tokio::sync::watch::Sender::new(()),
            closed: AtomicBool::new(false),
        });
        tokio::spawn(writer.clone().run());
        writer
    }

    pub(super) fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Numbers the batch that was just folded and wakes the flush loop.
    pub(super) fn folded(&self, inner: &mut Inner) -> u64 {
        inner.seq += 1;
        self.wake.notify_one();
        inner.seq
    }

    /// Lets the flush loop end once everything is written.
    pub(super) fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.wake.notify_one();
    }

    /// Returns once the states of batch `seq` are in the store; fails when they were dropped.
    pub(super) async fn flushed(&self, seq: u64) -> anyhow::Result<()> {
        let mut progress = self.progress.subscribe();
        loop {
            {
                let inner = self.lock();
                if inner.lost.iter().any(|(lo, hi)| *lo < seq && seq <= *hi) {
                    anyhow::bail!("aggregate: the states of this batch could not be stored");
                }
                if seq <= inner.flushed {
                    return Ok(());
                }
            }
            progress.changed().await?;
        }
    }

    /// The changed states as writes, their owners, and the batch number they cover.
    #[allow(clippy::type_complexity)]
    fn take(&self) -> anyhow::Result<Option<(Vec<StateWrite>, Vec<(usize, String)>, u64)>> {
        let mut inner = self.lock();
        let inner = &mut *inner;
        if inner.dirty.iter().all(HashSet::is_empty) {
            if inner.flushed < inner.seq {
                inner.flushed = inner.seq;
                self.progress.send_replace(());
            }
            return Ok(None);
        }
        let mut writes = Vec::new();
        let mut owners = Vec::new();
        for (i, dirty) in inner.dirty.iter_mut().enumerate() {
            for key in dirty.drain() {
                let Some(state) = inner.states[i].get(&key) else {
                    continue;
                };
                let mut json = Vec::new();
                state.write_json(&mut json)?;
                writes.push(StateWrite {
                    key: format!("{}:{key}", self.prefixes[i]),
                    state: String::from_utf8(json)?,
                    expected: inner.versions[i].get(&key).copied().unwrap_or(0),
                });
                owners.push((i, key));
            }
        }
        Ok(Some((writes, owners, inner.seq)))
    }

    async fn flush(&self) -> anyhow::Result<bool> {
        let Some((writes, owners, upto)) = self.take()? else {
            return Ok(false);
        };
        if !self.store.store_many(&writes).await?.is_empty() {
            anyhow::bail!("another writer changed the states; `single_writer` allows one instance");
        }
        let mut inner = self.lock();
        for (i, key) in owners {
            *inner.versions[i].entry(key).or_insert(0) += 1;
        }
        inner.flushed = upto;
        Ok(true)
    }

    /// Drops the stored states used least recently once an entry holds `max_keys`; they are
    /// loaded again when their key returns. Runs between two flushes and not during a fold.
    async fn trim(&self) {
        if !self.lock().states.iter().any(States::is_full) {
            return;
        }
        let _gate = self.gate.lock().await;
        let mut inner = self.lock();
        let Inner {
            states,
            versions,
            dirty,
            ..
        } = &mut *inner;
        for (i, states) in states.iter_mut().enumerate() {
            if states.turn(|key| dirty[i].contains(key)) > 0 {
                versions[i].retain(|key, _| states.contains_key(key));
            }
        }
    }

    async fn run(self: Arc<Self>) {
        loop {
            match self.flush().await {
                Ok(true) => {
                    self.progress.send_replace(());
                    self.trim().await;
                    continue;
                }
                Ok(false) => {
                    if self.closed.load(Ordering::Acquire) {
                        break;
                    }
                    self.wake.notified().await;
                    continue;
                }
                Err(e) => {
                    tracing::error!("aggregate: writing states failed, reloading them: {e:#}");
                    // Memory is ahead of the store: drop it, the nacked batches fold again.
                    let _gate = self.gate.lock().await;
                    let mut inner = self.lock();
                    let range = (inner.flushed, inner.seq);
                    if inner.lost.len() == LOST_KEPT {
                        inner.lost.remove(0);
                    }
                    inner.lost.push(range);
                    inner.flushed = inner.seq;
                    for i in 0..inner.states.len() {
                        inner.states[i].clear();
                        inner.versions[i].clear();
                        inner.dirty[i].clear();
                    }
                }
            }
            self.progress.send_replace(());
        }
    }
}
