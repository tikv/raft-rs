//! A representation of not-yet-committed log entries and state.

// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use alloc::vec;
use alloc::vec::Vec;

use crate::eraftpb::{Entry, Snapshot};
use crate::util::entry_approximate_size;
use slog::Logger;

/// The `unstable.entries[i]` has raft log position `i+unstable.offset`.
/// Note that `unstable.offset` may be less than the highest log
/// position in storage; this means that the next write to storage
/// might need to truncate the log before persisting unstable.entries.
#[derive(Debug)]
pub struct Unstable {
    /// The incoming unstable snapshot, if any.
    pub snapshot: Option<Snapshot>,

    /// All entries that have not yet been written to storage.
    pub entries: Vec<Entry>,

    /// The size of entries.
    pub entries_size: usize,

    /// The offset from the vector index.
    pub offset: u64,

    /// The tag to use when logging.
    pub logger: Logger,
}

impl Unstable {
    /// Creates a new log of unstable entries.
    pub fn new(offset: u64, logger: Logger) -> Unstable {
        Unstable {
            offset,
            snapshot: None,
            entries: vec![],
            entries_size: 0,
            logger,
        }
    }

    /// Returns the index of the first possible entry in entries
    /// if it has a snapshot.
    pub fn maybe_first_index(&self) -> Option<u64> {
        self.snapshot
            .as_ref()
            .map(|snap| snap.get_metadata().index + 1)
    }

    /// Returns the last index if it has at least one unstable entry or snapshot.
    pub fn maybe_last_index(&self) -> Option<u64> {
        match self.entries.len() {
            0 => self.snapshot.as_ref().map(|snap| snap.get_metadata().index),
            len => Some(self.offset + len as u64 - 1),
        }
    }

    /// Returns the term of the entry at index idx, if there is any.
    pub fn maybe_term(&self, idx: u64) -> Option<u64> {
        if idx < self.offset {
            let snapshot = self.snapshot.as_ref()?;
            let meta = snapshot.get_metadata();
            if idx == meta.index {
                Some(meta.term)
            } else {
                None
            }
        } else {
            self.maybe_last_index().and_then(|last| {
                if idx > last {
                    return None;
                }
                Some(self.entries[(idx - self.offset) as usize].term)
            })
        }
    }

    /// Clears the unstable entries and moves the stable offset up to the
    /// last index, if there is any.
    pub fn stable_entries(&mut self, index: u64, term: u64) {
        // The snapshot must be stabled before entries
        assert!(self.snapshot.is_none());
        if let Some(entry) = self.entries.last() {
            if entry.get_index() != index || entry.get_term() != term {
                fatal!(
                    self.logger,
                    "the last one of unstable.slice has different index {} and term {}, expect {} {}",
                    entry.get_index(),
                    entry.get_term(),
                    index,
                    term
                );
            }
            self.offset = entry.get_index() + 1;
            self.entries.clear();
            self.entries_size = 0;
        } else {
            fatal!(
                self.logger,
                "unstable.slice is empty, expect its last one's index and term are {} and {}",
                index,
                term
            );
        }
    }

    /// Clears the unstable snapshot.
    pub fn stable_snap(&mut self, index: u64) {
        if let Some(snap) = &self.snapshot {
            if snap.get_metadata().index != index {
                fatal!(
                    self.logger,
                    "unstable.snap has different index {}, expect {}",
                    snap.get_metadata().index,
                    index
                );
            }
            self.snapshot = None;
        } else {
            fatal!(
                self.logger,
                "unstable.snap is none, expect a snapshot with index {}",
                index
            );
        }
    }

    /// From a given snapshot, restores the snapshot to self, but doesn't unpack.
    pub fn restore(&mut self, snap: Snapshot) {
        self.entries.clear();
        self.entries_size = 0;
        self.offset = snap.get_metadata().index + 1;
        self.snapshot = Some(snap);
    }

    /// Append entries to unstable, truncate local block first if overlapped.
    ///
    /// # Panics
    ///
    /// Panics if truncate logs to the entry before snapshot
    pub fn truncate_and_append(&mut self, ents: &[Entry]) {
        let after = ents[0].index;
        if after == self.offset + self.entries.len() as u64 {
            // after is the next index in the self.entries, append directly
        } else if after <= self.offset {
            // The log is being truncated to before our current offset
            // portion, so set the offset and replace the entries
            self.offset = after;
            self.entries.clear();
            self.entries_size = 0;
        } else {
            // truncate to after and copy to self.entries then append
            let off = self.offset;
            self.must_check_outofbounds(off, after);
            for e in &self.entries[(after - off) as usize..] {
                self.entries_size -= entry_approximate_size(e);
            }
            self.entries.truncate((after - off) as usize);
        }
        self.entries.extend_from_slice(ents);
        self.entries_size += ents.iter().map(entry_approximate_size).sum::<usize>();
    }

    /// Returns a slice of entries between the high and low.
    ///
    /// # Panics
    ///
    /// Panics if the `lo` or `hi` are out of bounds.
    /// Panics if `lo > hi`.
    pub fn slice(&self, lo: u64, hi: u64) -> &[Entry] {
        self.must_check_outofbounds(lo, hi);
        let l = lo as usize;
        let h = hi as usize;
        let off = self.offset as usize;
        &self.entries[l - off..h - off]
    }

    /// Asserts the `hi` and `lo` values against each other and against the
    /// entries themselves.
    pub fn must_check_outofbounds(&self, lo: u64, hi: u64) {
        if lo > hi {
            fatal!(self.logger, "invalid unstable.slice {} > {}", lo, hi)
        }
        let upper = self.offset + self.entries.len() as u64;
        if lo < self.offset || hi > upper {
            fatal!(
                self.logger,
                "unstable.slice[{}, {}] out of bound[{}, {}]",
                lo,
                hi,
                self.offset,
                upper
            )
        }
    }
}

#[cfg(test)]
mod test {
    use crate::eraftpb::{Entry, Snapshot, SnapshotMetadata};
    use crate::log_unstable::Unstable;
    use crate::util::entry_approximate_size;
    use alloc::vec;

    fn new_entry(index: u64, term: u64) -> Entry {
        let mut e = Entry::default();
        e.term = term;
        e.index = index;
        e
    }

    fn new_snapshot(index: u64, term: u64) -> Snapshot {
        let mut snap = Snapshot::default();
        let mut meta = SnapshotMetadata::default();
        meta.index = index;
        meta.term = term;
        snap.set_metadata(meta);
        snap
    }

    #[test]
    fn test_maybe_first_index() {
        // entry, offset, snap, wok, windex,
        let tests = vec![
            // no snapshot
            (Some(new_entry(5, 1)), 5, None, false, 0),
            (None, 0, None, false, 0),
            // has snapshot
            (Some(new_entry(5, 1)), 5, Some(new_snapshot(4, 1)), true, 5),
            (None, 5, Some(new_snapshot(4, 1)), true, 5),
        ];

        for (e, offset, snapshot, wok, windex) in tests {
            let mut entries_size = 0;
            let mut entries = vec![];
            if let Some(entry) = e {
                entries_size = entry_approximate_size(&entry);
                entries = vec![entry];
            }
            let u = Unstable {
                entries,
                entries_size,
                offset,
                snapshot,
                logger: crate::default_logger(),
            };
            let index = u.maybe_first_index();
            match index {
                None => assert!(!wok),
                Some(index) => assert_eq!(index, windex),
            }
        }
    }

    #[test]
    fn test_maybe_last_index() {
        // entry, offset, snap, wok, windex,
        let tests = vec![
            (Some(new_entry(5, 1)), 5, None, true, 5),
            (Some(new_entry(5, 1)), 5, Some(new_snapshot(4, 1)), true, 5),
            // last in snapshot
            (None, 5, Some(new_snapshot(4, 1)), true, 4),
            // empty unstable
            (None, 0, None, false, 0),
        ];

        for (e, offset, snapshot, wok, windex) in tests {
            let mut entries_size = 0;
            let mut entries = vec![];
            if let Some(entry) = e {
                entries_size = entry_approximate_size(&entry);
                entries = vec![entry];
            }
            let u = Unstable {
                entries,
                entries_size,
                offset,
                snapshot,
                logger: crate::default_logger(),
            };
            let index = u.maybe_last_index();
            match index {
                None => assert!(!wok),
                Some(index) => assert_eq!(index, windex),
            }
        }
    }

    #[test]
    fn test_maybe_term() {
        // entry, offset, snap, index, wok, wterm
        let tests = vec![
            // term from entries
            (Some(new_entry(5, 1)), 5, None, 5, true, 1),
            (Some(new_entry(5, 1)), 5, None, 6, false, 0),
            (Some(new_entry(5, 1)), 5, None, 4, false, 0),
            (
                Some(new_entry(5, 1)),
                5,
                Some(new_snapshot(4, 1)),
                5,
                true,
                1,
            ),
            (
                Some(new_entry(5, 1)),
                5,
                Some(new_snapshot(4, 1)),
                6,
                false,
                0,
            ),
            // term from snapshot
            (
                Some(new_entry(5, 1)),
                5,
                Some(new_snapshot(4, 1)),
                4,
                true,
                1,
            ),
            (
                Some(new_entry(5, 1)),
                5,
                Some(new_snapshot(4, 1)),
                3,
                false,
                0,
            ),
            (None, 5, Some(new_snapshot(4, 1)), 5, false, 0),
            (None, 5, Some(new_snapshot(4, 1)), 4, true, 1),
            (None, 0, None, 5, false, 0),
        ];

        for (e, offset, snapshot, index, wok, wterm) in tests {
            let mut entries_size = 0;
            let mut entries = vec![];
            if let Some(entry) = e {
                entries_size = entry_approximate_size(&entry);
                entries = vec![entry];
            }
            let u = Unstable {
                entries,
                entries_size,
                offset,
                snapshot,
                logger: crate::default_logger(),
            };
            let term = u.maybe_term(index);
            match term {
                None => assert!(!wok),
                Some(term) => assert_eq!(term, wterm),
            }
        }
    }

    #[test]
    fn test_restore() {
        let mut u = Unstable {
            entries: vec![new_entry(5, 1)],
            entries_size: entry_approximate_size(&new_entry(5, 1)),
            offset: 5,
            snapshot: Some(new_snapshot(4, 1)),
            logger: crate::default_logger(),
        };

        let s = new_snapshot(6, 2);
        u.restore(s.clone());

        assert_eq!(u.offset, s.get_metadata().index + 1);
        assert!(u.entries.is_empty());
        assert_eq!(u.entries_size, 0);
        assert_eq!(u.snapshot.unwrap(), s);
    }

    #[test]
    fn test_stable_snapshot_and_entries() {
        let ents = vec![new_entry(5, 1), new_entry(5, 2), new_entry(6, 3)];
        let entries_size = ents.iter().map(entry_approximate_size).sum::<usize>();
        let mut u = Unstable {
            entries: ents.clone(),
            entries_size,
            offset: 5,
            snapshot: Some(new_snapshot(4, 1)),
            logger: crate::default_logger(),
        };
        assert_eq!(ents, u.entries);
        u.stable_snap(4);
        u.stable_entries(6, 3);
        assert!(u.entries.is_empty());
        assert_eq!(u.entries_size, 0);
        assert_eq!(u.offset, 7);
    }

    #[test]
    fn test_truncate_and_append() {
        // entries, offset, snap, to_append, woffset, wentries
        let tests = vec![
            // replace to the end
            (
                vec![new_entry(5, 1)],
                5,
                None,
                vec![new_entry(6, 1), new_entry(7, 1)],
                5,
                vec![new_entry(5, 1), new_entry(6, 1), new_entry(7, 1)],
            ),
            // replace to unstable entries
            (
                vec![new_entry(5, 1)],
                5,
                None,
                vec![new_entry(5, 2), new_entry(6, 2)],
                5,
                vec![new_entry(5, 2), new_entry(6, 2)],
            ),
            (
                vec![new_entry(5, 1)],
                5,
                None,
                vec![new_entry(4, 2), new_entry(5, 2), new_entry(6, 2)],
                4,
                vec![new_entry(4, 2), new_entry(5, 2), new_entry(6, 2)],
            ),
            // truncate existing entries and append
            (
                vec![new_entry(5, 1), new_entry(6, 1), new_entry(7, 1)],
                5,
                None,
                vec![new_entry(6, 2)],
                5,
                vec![new_entry(5, 1), new_entry(6, 2)],
            ),
            (
                vec![new_entry(5, 1), new_entry(6, 1), new_entry(7, 1)],
                5,
                None,
                vec![new_entry(7, 2), new_entry(8, 2)],
                5,
                vec![
                    new_entry(5, 1),
                    new_entry(6, 1),
                    new_entry(7, 2),
                    new_entry(8, 2),
                ],
            ),
        ];

        for (entries, offset, snapshot, to_append, woffset, wentries) in tests {
            let entries_size = entries.iter().map(entry_approximate_size).sum::<usize>();
            let mut u = Unstable {
                entries,
                entries_size,
                offset,
                snapshot,
                logger: crate::default_logger(),
            };
            u.truncate_and_append(&to_append);
            assert_eq!(u.offset, woffset);
            assert_eq!(u.entries, wentries);
            let entries_size = wentries.iter().map(entry_approximate_size).sum::<usize>();
            assert_eq!(u.entries_size, entries_size);
        }
    }
}
