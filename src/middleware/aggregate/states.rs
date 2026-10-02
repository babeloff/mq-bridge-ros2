//  mq-bridge
//  © Copyright 2026, by Marco Mengelkoch
//  Licensed under MIT OR Apache-2.0, see LICENSE file for more details
//  git clone https://github.com/marcomq/mq-bridge

//! The states of one entry in memory, bounded by `max_keys`.

use super::Stored;
use hashbrown::hash_map::EntryRef;

type Map = hashbrown::HashMap<String, Stored>;

/// Two generations of states. A state that is read for an update or written moves to the
/// young one; once that holds half of `max_keys`, `turn` drops the old one. So the states
/// used least recently go, without a timestamp or a list per key.
pub(super) struct States {
    young: Map,
    old: Map,
    /// Size of the young generation that makes a turn due.
    half: usize,
}

impl States {
    /// `max_keys` of 0 keeps every state.
    pub(super) fn new(max_keys: usize) -> Self {
        Self {
            young: Map::new(),
            old: Map::new(),
            half: match max_keys {
                0 => usize::MAX,
                max => (max / 2).max(1),
            },
        }
    }

    pub(super) fn get(&self, key: &str) -> Option<&Stored> {
        self.young.get(key).or_else(|| self.old.get(key))
    }

    pub(super) fn contains_key(&self, key: &str) -> bool {
        self.young.contains_key(key) || self.old.contains_key(key)
    }

    pub(super) fn insert(&mut self, key: String, state: Stored) {
        if !self.old.is_empty() {
            self.old.remove(&key);
        }
        self.young.insert(key, state);
    }

    pub(super) fn remove(&mut self, key: &str) {
        self.young.remove(key);
        self.old.remove(key);
    }

    pub(super) fn clear(&mut self) {
        self.young.clear();
        self.old.clear();
    }

    /// The state of `key` to update in place, and whether it is new (then `Null`).
    pub(super) fn slot(&mut self, key: &str) -> (&mut Stored, bool) {
        match self.young.entry_ref(key) {
            EntryRef::Occupied(slot) => (slot.into_mut(), false),
            EntryRef::Vacant(slot) => match self.old.remove(key) {
                Some(state) => (slot.insert(state), false),
                None => (slot.insert(Stored::Null), true),
            },
        }
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.young.len() + self.old.len()
    }

    /// Whether `turn` would drop the old generation.
    pub(super) fn is_full(&self) -> bool {
        self.young.len() >= self.half
    }

    /// Drops the old generation when the young one is full, except the states `keep`
    /// names. Returns how many states were dropped.
    pub(super) fn turn(&mut self, keep: impl Fn(&str) -> bool) -> usize {
        if !self.is_full() {
            return 0;
        }
        let young = std::mem::take(&mut self.young);
        let mut dropped = 0;
        for (key, state) in std::mem::replace(&mut self.old, young) {
            match keep(&key) {
                true => drop(self.old.insert(key, state)),
                false => dropped += 1,
            }
        }
        dropped
    }
}
