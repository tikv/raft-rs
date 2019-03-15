//! Represents the storage trait and example implementation.
//!
//! The storage trait is used to house and eventually serialize the state of the system.
//! Custom implementations of this are normal and this is likely to be a key integration
//! point for your distributed storage.

// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

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

use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::eraftpb::{ConfChange, ConfState, Entry, HardState, Snapshot};

use crate::errors::{Error, Result, StorageError};
use crate::util;

/// Holds both the hard state (commit index, vote leader, term) and the configuration state
/// (Current node IDs)
#[derive(Debug, Clone, Getters, Setters)]
pub struct RaftState {
    /// Contains the last meta information including commit index, the vote leader, and the vote term.
    pub hard_state: HardState,
    /// Records the current node IDs like `[1, 2, 3]` in the cluster. Every Raft node must have a unique ID in the cluster;
    pub conf_state: ConfState,
    /// If this peer is in the middle of a membership change (The period between
    /// `BeginMembershipChange` and `FinalizeMembershipChange`) this will hold the final desired
    /// state.
    #[get = "pub"]
    #[set]
    pending_conf_state: Option<ConfState>,
    /// If `pending_conf_state` exists this will contain the index of the `BeginMembershipChange`
    /// entry.
    #[get = "pub"]
    #[set]
    pending_conf_state_start_index: Option<u64>,
}

/// Storage saves all the information about the current Raft implementation, including Raft Log, commit index, the leader to vote for, etc.
/// Pay attention to what is returned when there is no Log but it needs to get the `term` at index `first_index() - 1`. To solve this, you can use a dummy Log entry to keep the last truncated Log entry. See [`entries: vec![Entry::new()]`](src/storage.rs#L85) as a reference.
///
/// If any Storage method returns an error, the raft instance will
/// become inoperable and refuse to participate in elections; the
/// application is responsible for cleanup and recovery in this case.
pub trait Storage {
    /// `initial_state` is called when Raft is initialized. This interface will return a `RaftState` which contains `HardState` and `ConfState`;
    fn initial_state(&self) -> Result<RaftState>;
    /// Returns a slice of log entries in the range `[low, high)`.
    /// max_size limits the total size of the log entries returned if not `None`, however
    /// the slice of entries returned will always have length at least 1 if entries are
    /// found in the range.
    fn entries(&self, low: u64, high: u64, max_size: impl Into<Option<u64>>) -> Result<Vec<Entry>>;
    /// Returns the term of entry idx, which must be in the range
    /// [first_index()-1, last_index()]. The term of the entry before
    /// first_index is retained for matching purpose even though the
    /// rest of that entry may not be available.
    fn term(&self, idx: u64) -> Result<u64>;
    /// Returns the index of the first log entry that is
    /// possible available via entries (older entries have been incorporated
    /// into the latest snapshot; if storage only contains the dummy entry the
    /// first log entry is not available).
    fn first_index(&self) -> Result<u64>;
    /// The index of the last entry in the log.
    fn last_index(&self) -> Result<u64>;
    /// Returns the most recent snapshot.
    ///
    /// If snapshot is temporarily unavailable, it should return SnapshotTemporarilyUnavailable,
    /// so raft state machine could know that Storage needs some time to prepare
    /// snapshot and call snapshot later.
    fn snapshot(&self) -> Result<Snapshot>;
}

/// The Memory Storage Core instance holds the actual state of the storage struct. To access this
/// value, use the `rl` and `wl` functions on the main MemStorage implementation.
pub struct MemStorageCore {
    hard_state: HardState,
    snapshot: Snapshot,
    // TODO: maybe vec_deque
    // entries[i] has raft log position i+snapshot.get_metadata().get_index()
    entries: Vec<Entry>,
}

impl Default for MemStorageCore {
    fn default() -> MemStorageCore {
        MemStorageCore {
            // When starting from scratch populate the list with a dummy entry at term zero.
            entries: vec![Entry::new()],
            hard_state: HardState::new(),
            snapshot: Snapshot::new(),
        }
    }
}

impl MemStorageCore {
    /// Saves the current HardState.
    pub fn set_hardstate(&mut self, hs: HardState) {
        self.hard_state = hs;
    }

    /// Saves the current conf state.
    pub fn set_conf_state(
        &mut self,
        cs: ConfState,
        pending_membership_change: Option<(ConfState, u64)>,
    ) {
        self.snapshot.mut_metadata().set_conf_state(cs);
        if let Some((cs, idx)) = pending_membership_change {
            self.snapshot
                .mut_metadata()
                .set_pending_membership_change(cs);
            self.snapshot
                .mut_metadata()
                .set_pending_membership_change_index(idx);
        }
    }

    fn inner_last_index(&self) -> u64 {
        self.entries[0].get_index() + self.entries.len() as u64 - 1
    }

    /// Overwrites the contents of this Storage object with those of the given snapshot.
    pub fn apply_snapshot(&mut self, snapshot: Snapshot) -> Result<()> {
        // handle check for old snapshot being applied
        let index = self.snapshot.get_metadata().get_index();
        let snapshot_index = snapshot.get_metadata().get_index();
        if index >= snapshot_index {
            return Err(Error::Store(StorageError::SnapshotOutOfDate));
        }

        let mut e = Entry::new();
        e.set_term(snapshot.get_metadata().get_term());
        e.set_index(snapshot.get_metadata().get_index());
        self.entries = vec![e];
        self.snapshot = snapshot;
        Ok(())
    }

    /// Makes a snapshot which can be retrieved with snapshot() and
    /// can be used to reconstruct the state at that point.
    /// If any configuration changes have been made since the last compaction,
    /// the result of the last apply_conf_change must be passed in.
    pub fn create_snapshot(
        &mut self,
        idx: u64,
        cs: Option<ConfState>,
        pending_membership_change: Option<ConfChange>,
        data: Vec<u8>,
    ) -> Result<&Snapshot> {
        if idx <= self.snapshot.get_metadata().get_index() {
            return Err(Error::Store(StorageError::SnapshotOutOfDate));
        }

        let offset = self.entries[0].get_index();
        if idx > self.inner_last_index() {
            panic!(
                "snapshot {} is out of bound lastindex({})",
                idx,
                self.inner_last_index()
            )
        }
        self.snapshot.mut_metadata().set_index(idx);
        self.snapshot
            .mut_metadata()
            .set_term(self.entries[(idx - offset) as usize].get_term());
        if let Some(cs) = cs {
            self.snapshot.mut_metadata().set_conf_state(cs)
        }
        if let Some(pending_change) = pending_membership_change {
            let meta = self.snapshot.mut_metadata();
            meta.set_pending_membership_change(pending_change.get_configuration().clone());
            meta.set_pending_membership_change_index(pending_change.get_start_index());
        }
        self.snapshot.set_data(data);
        Ok(&self.snapshot)
    }

    /// Discards all log entries prior to compact_index.
    /// It is the application's responsibility to not attempt to compact an index
    /// greater than RaftLog.applied.
    pub fn compact(&mut self, compact_index: u64) -> Result<()> {
        let offset = self.entries[0].get_index();
        if compact_index <= offset {
            return Err(Error::Store(StorageError::Compacted));
        }
        if compact_index > self.inner_last_index() {
            panic!(
                "compact {} is out of bound lastindex({})",
                compact_index,
                self.inner_last_index()
            )
        }

        let i = (compact_index - offset) as usize;
        let entries = self.entries.drain(i..).collect();
        self.entries = entries;
        Ok(())
    }

    /// Append the new entries to storage.
    /// TODO: ensure the entries are continuous and
    /// entries[0].get_index() > self.entries[0].get_index()
    pub fn append(&mut self, ents: &[Entry]) -> Result<()> {
        if ents.is_empty() {
            return Ok(());
        }
        let first = self.entries[0].get_index() + 1;
        let last = ents[0].get_index() + ents.len() as u64 - 1;

        if last < first {
            return Ok(());
        }
        // truncate compacted entries
        let te: &[Entry] = if first > ents[0].get_index() {
            let start_ent = (first - ents[0].get_index()) as usize;
            &ents[start_ent..]
        } else {
            ents
        };

        let offset = te[0].get_index() - self.entries[0].get_index();
        if self.entries.len() as u64 > offset {
            let mut new_entries: Vec<Entry> = vec![];
            new_entries.extend_from_slice(&self.entries[..offset as usize]);
            new_entries.extend_from_slice(te);
            self.entries = new_entries;
        } else if self.entries.len() as u64 == offset {
            self.entries.extend_from_slice(te);
        } else {
            panic!(
                "missing log entry [last: {}, append at: {}]",
                self.inner_last_index(),
                te[0].get_index()
            )
        }

        Ok(())
    }
}

/// `MemStorage` is a thread-safe implementation of Storage trait.
/// It is mainly used for test purpose.
#[derive(Clone, Default)]
pub struct MemStorage {
    core: Arc<RwLock<MemStorageCore>>,
}

impl MemStorage {
    /// Returns a new memory storage value.
    pub fn new() -> MemStorage {
        MemStorage {
            ..Default::default()
        }
    }

    /// Opens up a read lock on the storage and returns a guard handle. Use this
    /// with functions that don't require mutation.
    pub fn rl(&self) -> RwLockReadGuard<'_, MemStorageCore> {
        self.core.read().unwrap()
    }

    /// Opens up a write lock on the storage and returns guard handle. Use this
    /// with functions that take a mutable reference to self.
    pub fn wl(&self) -> RwLockWriteGuard<'_, MemStorageCore> {
        self.core.write().unwrap()
    }
}

impl Storage for MemStorage {
    /// Implements the Storage trait.
    fn initial_state(&self) -> Result<RaftState> {
        let core = self.rl();
        let mut state = RaftState {
            hard_state: core.hard_state.clone(),
            conf_state: core.snapshot.get_metadata().get_conf_state().clone(),
            pending_conf_state: None,
            pending_conf_state_start_index: None,
        };
        if core.snapshot.get_metadata().has_pending_membership_change() {
            state.pending_conf_state = core
                .snapshot
                .get_metadata()
                .get_pending_membership_change()
                .clone()
                .into();
            state.pending_conf_state_start_index = core
                .snapshot
                .get_metadata()
                .get_pending_membership_change_index()
                .into();
        }
        Ok(state)
    }

    /// Implements the Storage trait.
    fn entries(&self, low: u64, high: u64, max_size: impl Into<Option<u64>>) -> Result<Vec<Entry>> {
        let max_size = max_size.into();
        let core = self.rl();
        let offset = core.entries[0].get_index();
        if low <= offset {
            return Err(Error::Store(StorageError::Compacted));
        }

        if high > core.inner_last_index() + 1 {
            panic!(
                "index out of bound (last: {}, high: {}",
                core.inner_last_index() + 1,
                high
            );
        }
        // only contains dummy entries.
        if core.entries.len() == 1 {
            return Err(Error::Store(StorageError::Unavailable));
        }

        let lo = (low - offset) as usize;
        let hi = (high - offset) as usize;
        let mut ents = core.entries[lo..hi].to_vec();
        util::limit_size(&mut ents, max_size);
        Ok(ents)
    }

    /// Implements the Storage trait.
    fn term(&self, idx: u64) -> Result<u64> {
        let core = self.rl();
        let offset = core.entries[0].get_index();
        if idx < offset {
            return Err(Error::Store(StorageError::Compacted));
        }
        if idx - offset >= core.entries.len() as u64 {
            return Err(Error::Store(StorageError::Unavailable));
        }
        Ok(core.entries[(idx - offset) as usize].get_term())
    }

    /// Implements the Storage trait.
    fn first_index(&self) -> Result<u64> {
        let core = self.rl();
        Ok(core.entries[0].get_index() + 1)
    }

    /// Implements the Storage trait.
    fn last_index(&self) -> Result<u64> {
        let core = self.rl();
        Ok(core.inner_last_index())
    }

    /// Implements the Storage trait.
    fn snapshot(&self) -> Result<Snapshot> {
        let core = self.rl();
        Ok(core.snapshot.clone())
    }
}

#[cfg(test)]
mod test {
    use crate::eraftpb::{ConfState, Entry, Snapshot};
    use crate::errors::{Error as RaftError, StorageError};
    use crate::storage::{MemStorage, Storage};
    use harness::setup_for_test;
    use protobuf;

    // TODO extract these duplicated utility functions for tests

    fn new_entry(index: u64, term: u64) -> Entry {
        let mut e = Entry::new();
        e.set_term(term);
        e.set_index(index);
        e
    }

    fn size_of<T: protobuf::Message>(m: &T) -> u32 {
        m.compute_size()
    }

    fn new_snapshot(index: u64, term: u64, nodes: Vec<u64>, data: Vec<u8>) -> Snapshot {
        let mut s = Snapshot::new();
        s.mut_metadata().set_index(index);
        s.mut_metadata().set_term(term);
        s.mut_metadata().mut_conf_state().set_nodes(nodes);
        s.set_data(data);
        s
    }

    #[test]
    fn test_storage_term() {
        setup_for_test();
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let mut tests = vec![
            (2, Err(RaftError::Store(StorageError::Compacted))),
            (3, Ok(3)),
            (4, Ok(4)),
            (5, Ok(5)),
            (6, Err(RaftError::Store(StorageError::Unavailable))),
        ];

        for (i, (idx, wterm)) in tests.drain(..).enumerate() {
            let storage = MemStorage::new();
            storage.wl().entries = ents.clone();

            let t = storage.term(idx);
            if t != wterm {
                panic!("#{}: expect res {:?}, got {:?}", i, wterm, t);
            }
        }
    }

    #[test]
    fn test_storage_entries() {
        setup_for_test();
        let ents = vec![
            new_entry(3, 3),
            new_entry(4, 4),
            new_entry(5, 5),
            new_entry(6, 6),
        ];
        let max_u64 = u64::max_value();
        let mut tests = vec![
            (
                2,
                6,
                max_u64,
                Err(RaftError::Store(StorageError::Compacted)),
            ),
            (
                3,
                4,
                max_u64,
                Err(RaftError::Store(StorageError::Compacted)),
            ),
            (4, 5, max_u64, Ok(vec![new_entry(4, 4)])),
            (4, 6, max_u64, Ok(vec![new_entry(4, 4), new_entry(5, 5)])),
            (
                4,
                7,
                max_u64,
                Ok(vec![new_entry(4, 4), new_entry(5, 5), new_entry(6, 6)]),
            ),
            // even if maxsize is zero, the first entry should be returned
            (4, 7, 0, Ok(vec![new_entry(4, 4)])),
            // limit to 2
            (
                4,
                7,
                u64::from(size_of(&ents[1]) + size_of(&ents[2])),
                Ok(vec![new_entry(4, 4), new_entry(5, 5)]),
            ),
            (
                4,
                7,
                u64::from(size_of(&ents[1]) + size_of(&ents[2]) + size_of(&ents[3]) / 2),
                Ok(vec![new_entry(4, 4), new_entry(5, 5)]),
            ),
            (
                4,
                7,
                u64::from(size_of(&ents[1]) + size_of(&ents[2]) + size_of(&ents[3]) - 1),
                Ok(vec![new_entry(4, 4), new_entry(5, 5)]),
            ),
            // all
            (
                4,
                7,
                u64::from(size_of(&ents[1]) + size_of(&ents[2]) + size_of(&ents[3])),
                Ok(vec![new_entry(4, 4), new_entry(5, 5), new_entry(6, 6)]),
            ),
        ];
        for (i, (lo, hi, maxsize, wentries)) in tests.drain(..).enumerate() {
            let storage = MemStorage::new();
            storage.wl().entries = ents.clone();
            let e = storage.entries(lo, hi, maxsize);
            if e != wentries {
                panic!("#{}: expect entries {:?}, got {:?}", i, wentries, e);
            }
        }
    }

    #[test]
    fn test_storage_last_index() {
        setup_for_test();
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let storage = MemStorage::new();
        storage.wl().entries = ents;

        let wresult = Ok(5);
        let result = storage.last_index();
        if result != wresult {
            panic!("want {:?}, got {:?}", wresult, result);
        }

        storage
            .wl()
            .append(&[new_entry(6, 5)])
            .expect("append failed");
        let wresult = Ok(6);
        let result = storage.last_index();
        if result != wresult {
            panic!("want {:?}, got {:?}", wresult, result);
        }
    }

    #[test]
    fn test_storage_first_index() {
        setup_for_test();
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let storage = MemStorage::new();
        storage.wl().entries = ents;

        let wresult = Ok(4);
        let result = storage.first_index();
        if result != wresult {
            panic!("want {:?}, got {:?}", wresult, result);
        }

        storage.wl().compact(4).expect("compact failed");
        let wresult = Ok(5);
        let result = storage.first_index();
        if result != wresult {
            panic!("want {:?}, got {:?}", wresult, result);
        }
    }

    #[test]
    fn test_storage_compact() {
        setup_for_test();
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let mut tests = vec![
            (2, Err(RaftError::Store(StorageError::Compacted)), 3, 3, 3),
            (3, Err(RaftError::Store(StorageError::Compacted)), 3, 3, 3),
            (4, Ok(()), 4, 4, 2),
            (5, Ok(()), 5, 5, 1),
        ];
        for (i, (idx, wresult, windex, wterm, wlen)) in tests.drain(..).enumerate() {
            let storage = MemStorage::new();
            storage.wl().entries = ents.clone();

            let result = storage.wl().compact(idx);
            if result != wresult {
                panic!("#{}: want {:?}, got {:?}", i, wresult, result);
            }
            let index = storage.wl().entries[0].get_index();
            if index != windex {
                panic!("#{}: want {}, index {}", i, windex, index);
            }
            let term = storage.wl().entries[0].get_term();
            if term != wterm {
                panic!("#{}: want {}, term {}", i, wterm, term);
            }
            let len = storage.wl().entries.len();
            if len != wlen {
                panic!("#{}: want {}, term {}", i, wlen, len);
            }
        }
    }

    #[test]
    fn test_storage_create_snapshot() {
        setup_for_test();
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let nodes = vec![1, 2, 3];
        let mut cs = ConfState::new();
        cs.set_nodes(nodes.clone());
        let data = b"data".to_vec();

        let mut tests = vec![
            (4, Ok(new_snapshot(4, 4, nodes.clone(), data.clone()))),
            (5, Ok(new_snapshot(5, 5, nodes.clone(), data.clone()))),
        ];
        for (i, (idx, wresult)) in tests.drain(..).enumerate() {
            let storage = MemStorage::new();
            storage.wl().entries = ents.clone();

            storage
                .wl()
                .create_snapshot(idx, Some(cs.clone()), None, data.clone())
                .expect("create snapshot failed");
            let result = storage.snapshot();
            if result != wresult {
                panic!("#{}: want {:?}, got {:?}", i, wresult, result);
            }
        }
    }

    #[test]
    fn test_storage_append() {
        setup_for_test();
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let mut tests = vec![
            (
                vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)],
                Ok(()),
                vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)],
            ),
            (
                vec![new_entry(3, 3), new_entry(4, 6), new_entry(5, 6)],
                Ok(()),
                vec![new_entry(3, 3), new_entry(4, 6), new_entry(5, 6)],
            ),
            (
                vec![
                    new_entry(3, 3),
                    new_entry(4, 4),
                    new_entry(5, 5),
                    new_entry(6, 5),
                ],
                Ok(()),
                vec![
                    new_entry(3, 3),
                    new_entry(4, 4),
                    new_entry(5, 5),
                    new_entry(6, 5),
                ],
            ),
            // truncate incoming entries, truncate the existing entries and append
            (
                vec![new_entry(2, 3), new_entry(3, 3), new_entry(4, 5)],
                Ok(()),
                vec![new_entry(3, 3), new_entry(4, 5)],
            ),
            // truncate the existing entries and append
            (
                vec![new_entry(4, 5)],
                Ok(()),
                vec![new_entry(3, 3), new_entry(4, 5)],
            ),
            // direct append
            (
                vec![new_entry(6, 6)],
                Ok(()),
                vec![
                    new_entry(3, 3),
                    new_entry(4, 4),
                    new_entry(5, 5),
                    new_entry(6, 6),
                ],
            ),
        ];
        for (i, (entries, wresult, wentries)) in tests.drain(..).enumerate() {
            let storage = MemStorage::new();
            storage.wl().entries = ents.clone();

            let result = storage.wl().append(&entries);
            if result != wresult {
                panic!("#{}: want {:?}, got {:?}", i, wresult, result);
            }
            let e = &storage.wl().entries;
            if *e != wentries {
                panic!("#{}: want {:?}, entries {:?}", i, wentries, e);
            }
        }
    }

    #[test]
    fn test_storage_apply_snapshot() {
        setup_for_test();
        let nodes = vec![1, 2, 3];
        let data = b"data".to_vec();

        let snapshots = vec![
            new_snapshot(4, 4, nodes.clone(), data.clone()),
            new_snapshot(3, 3, nodes.clone(), data.clone()),
        ];

        let storage = MemStorage::new();

        // Apply snapshot successfully
        let i = 0;
        let wresult = Ok(());
        let r = storage.wl().apply_snapshot(snapshots[i].clone());
        if r != wresult {
            panic!("#{}: want {:?}, got {:?}", i, wresult, r);
        }

        // Apply snapshot fails due to StorageError::SnapshotOutOfDate
        let i = 1;
        let wresult = Err(RaftError::Store(StorageError::SnapshotOutOfDate));
        let r = storage.wl().apply_snapshot(snapshots[i].clone());
        if r != wresult {
            panic!("#{}: want {:?}, got {:?}", i, wresult, r);
        }
    }
}
