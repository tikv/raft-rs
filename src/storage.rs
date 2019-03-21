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

use crate::eraftpb::*;
use crate::errors::{Error, Result, StorageError};
use crate::util::limit_size;

/// Holds both the hard state (commit index, vote leader, term) and the configuration state
/// (Current node IDs)
#[derive(Debug, Clone, Default, Getters, Setters)]
pub struct RaftState {
    /// Contains the last meta information including commit index, the vote leader, and the vote term.
    pub hard_state: HardState,

    /// Records the current node IDs like `[1, 2, 3]` in the cluster. Every Raft node must have a
    /// unique ID in the cluster;
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

/// Storage saves all the information about the current Raft implementation, including Raft Log,
/// commit index, the leader to vote for, etc.
///
/// If any Storage method returns an error, the raft instance will
/// become inoperable and refuse to participate in elections; the
/// application is responsible for cleanup and recovery in this case.
pub trait Storage {
    /// `initial_state` is called when Raft is initialized. This interface will return a `RaftState`
    /// which contains `HardState` and `ConfState`.
    fn initial_state(&self) -> Result<RaftState>;

    /// Returns a slice of log entries in the range `[low, high)`.
    /// max_size limits the total size of the log entries returned if not `None`, however
    /// the slice of entries returned will always have length at least 1 if entries are
    /// found in the range.
    ///
    /// # Panics
    ///
    /// Panics if `high` is higher than `Storage::last_index(&self) + 1`.
    fn entries(&self, low: u64, high: u64, max_size: impl Into<Option<u64>>) -> Result<Vec<Entry>>;

    /// Returns the term of entry idx, which must be in the range
    /// [first_index()-1, last_index()]. The term of the entry before
    /// first_index is retained for matching purpose even though the
    /// rest of that entry may not be available.
    fn term(&self, idx: u64) -> Result<u64>;

    /// Returns the index of the first log entry that is possible available via entries.
    /// If the `Storage` is just initialized with a snapshot, `snap_index + 1` will be returned.
    fn first_index(&self) -> Result<u64>;

    /// The index of the last entry in the log. If the `Storage` is just initialized with a
    /// snapshot, `last_index` should return the `snapshot index`.
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
    raft_state: RaftState,
    // entries[i] has raft log position i+snapshot.get_metadata().get_index()
    entries: Vec<Entry>,
    // Metadata of the last snapshot received.
    snapshot_metadata: SnapshotMetadata,
}

impl Default for MemStorageCore {
    fn default() -> MemStorageCore {
        MemStorageCore {
            raft_state: Default::default(),
            entries: vec![],
            // Every time a snapshot is applied to the storage, the metadata will be stored here.
            snapshot_metadata: Default::default(),
        }
    }
}

impl MemStorageCore {
    /// Saves the current HardState.
    pub fn set_hardstate(&mut self, hs: HardState) {
        self.raft_state.hard_state = hs;
    }

    /// Get the hard state.
    pub fn hard_state(&self) -> &HardState {
        &self.raft_state.hard_state
    }

    /// Commit to an index.
    ///
    /// # Panics
    ///
    /// Panics if there is no such entry in raft logs.
    pub fn commit_to(&mut self, index: u64) -> Result<()> {
        if index < self.inner_first_index()
            || index > self.inner_last_index()
            || self.entries.is_empty()
        {
            panic!("commit_to {} but the entry not exists", index);
        }

        let diff = (index - self.entries[0].get_index()) as usize;
        self.raft_state.hard_state.set_commit(index);
        self.raft_state
            .hard_state
            .set_term(self.entries[diff].get_term());
        Ok(())
    }

    /// Saves the current conf state.
    pub fn set_conf_state(
        &mut self,
        cs: ConfState,
        pending_membership_change: Option<(ConfState, u64)>,
    ) {
        self.raft_state.conf_state = cs;
        if let Some((cs, idx)) = pending_membership_change {
            self.raft_state.pending_conf_state = Some(cs);
            self.raft_state.pending_conf_state_start_index = Some(idx);
        }
    }

    fn inner_first_index(&self) -> u64 {
        match self.entries.first() {
            Some(e) => e.get_index(),
            None => self.snapshot_metadata.get_index() + 1,
        }
    }

    fn inner_last_index(&self) -> u64 {
        match self.entries.last() {
            Some(e) => e.get_index(),
            None => self.snapshot_metadata.get_index(),
        }
    }

    /// Overwrites the contents of this Storage object with those of the given snapshot.
    ///
    /// # Panics
    ///
    /// Panics if the snapshot index is less than the storage's first index.
    pub fn apply_snapshot(&mut self, mut snapshot: Snapshot) -> Result<()> {
        let mut meta = snapshot.take_metadata();
        let term = meta.get_term();
        let index = meta.get_index();

        if self.inner_first_index() > index {
            return Err(Error::Store(StorageError::SnapshotOutOfDate));
        }

        self.snapshot_metadata = meta.clone();

        self.raft_state.hard_state.set_term(term);
        self.raft_state.hard_state.set_commit(index);
        self.entries.clear();

        // Update conf states.
        self.raft_state.conf_state = meta.take_conf_state();
        if meta.get_pending_membership_change_index() > 0 {
            let cs = meta.take_pending_membership_change();
            let i = meta.get_pending_membership_change_index();
            self.raft_state.pending_conf_state = Some(cs);
            self.raft_state.pending_conf_state_start_index = Some(i);
        }
        Ok(())
    }

    fn inner_snapshot(&self) -> Snapshot {
        let mut snapshot = Snapshot::new();

        // Use the latest applied_idx to construct the snapshot.
        let applied_idx = self.raft_state.hard_state.get_commit();
        let term = self.raft_state.hard_state.get_term();
        snapshot.mut_metadata().set_index(applied_idx);
        snapshot.mut_metadata().set_term(term);

        snapshot
            .mut_metadata()
            .set_conf_state(self.raft_state.conf_state.clone());
        if let Some(ref cs) = self.raft_state.pending_conf_state {
            snapshot
                .mut_metadata()
                .set_pending_membership_change(cs.clone());
            let i = self.raft_state.pending_conf_state_start_index.unwrap();
            snapshot
                .mut_metadata()
                .set_pending_membership_change_index(i);
        }
        snapshot
    }

    /// Discards all log entries prior to compact_index.
    /// It is the application's responsibility to not attempt to compact an index
    /// greater than RaftLog.applied.
    ///
    /// # Panics
    ///
    /// Panics if `compact_index` is higher than `Storage::last_index(&self) + 1`.
    pub fn compact(&mut self, compact_index: u64) -> Result<()> {
        if compact_index <= self.inner_first_index() {
            // Don't need to treat this case as an error.
            return Ok(());
        }

        if compact_index > self.inner_last_index() + 1 {
            panic!(
                "compact not received raft logs: {}, last index: {}",
                compact_index,
                self.inner_last_index()
            );
        }

        if let Some(entry) = self.entries.first() {
            let offset = compact_index - entry.get_index();
            self.entries.drain(..offset as usize);
        }
        Ok(())
    }

    /// Append the new entries to storage.
    ///
    /// # Panics
    ///
    /// Panics if `ents` contains compacted entries, or there's a gap between `ents` and the last
    /// received entry in the storage.
    pub fn append(&mut self, ents: &[Entry]) -> Result<()> {
        if ents.is_empty() {
            return Ok(());
        }
        if self.inner_first_index() > ents[0].get_index() {
            panic!(
                "overwrite compacted raft logs, compacted: {}, append: {}",
                self.inner_first_index() - 1,
                ents[0].get_index(),
            );
        }
        if self.inner_last_index() + 1 < ents[0].get_index() {
            panic!(
                "raft logs should be continuous, last index: {}, new appended: {}",
                self.inner_last_index(),
                ents[0].get_index(),
            );
        }

        // Remove all entries overwritten by `ents`, and truncate `ents` if need.
        let diff = ents[0].get_index() - self.inner_first_index();
        self.entries.drain(diff as usize..);
        ents.iter().for_each(|e| self.entries.push(e.clone()));
        Ok(())
    }

    /// Commit to `idx` and set configuration to the given states. Only used for tests.
    pub fn commit_to_and_set_conf_states(
        &mut self,
        idx: u64,
        cs: Option<ConfState>,
        pending_membership_change: Option<ConfChange>,
    ) -> Result<()> {
        self.commit_to(idx)?;
        if let Some(cs) = cs {
            self.raft_state.conf_state = cs;
        }
        if let Some(mut pending_change) = pending_membership_change {
            let conf_state = pending_change.take_configuration();
            self.raft_state.pending_conf_state = Some(conf_state);
            let index = pending_change.get_start_index();
            self.raft_state.pending_conf_state_start_index = Some(index);
        }
        Ok(())
    }
}

/// `MemStorage` is a thread-safe but incomplete implementation of `Storage`, mainly for tests.
///
/// A real `Storage` should save both raft logs and applied data. However `MemStorage` only
/// contains raft logs. So you can call `MemStorage::append` to persist new received unstable raft
/// logs and then access them with `Storage` APIs. The only exception is `Storage::snapshot`. There
/// is no data in `Snapshot` returned by `MemStorage::snapshot` because applied data is not stored
/// in `MemStorage`.
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
        Ok(self.rl().raft_state.clone())
    }

    /// Implements the Storage trait.
    fn entries(&self, low: u64, high: u64, max_size: impl Into<Option<u64>>) -> Result<Vec<Entry>> {
        let max_size = max_size.into();
        let core = self.rl();
        if low < core.inner_first_index() {
            return Err(Error::Store(StorageError::Compacted));
        }

        if high > core.inner_last_index() + 1 {
            panic!(
                "index out of bound (last: {}, high: {})",
                core.inner_last_index() + 1,
                high
            );
        }

        let offset = core.entries[0].get_index();
        let lo = (low - offset) as usize;
        let hi = (high - offset) as usize;
        let mut ents = core.entries[lo..hi].to_vec();
        limit_size(&mut ents, max_size);
        Ok(ents)
    }

    /// Implements the Storage trait.
    fn term(&self, idx: u64) -> Result<u64> {
        let core = self.rl();
        if idx == core.snapshot_metadata.get_index() {
            return Ok(core.snapshot_metadata.get_term());
        }

        if idx < core.inner_first_index() {
            return Err(Error::Store(StorageError::Compacted));
        }

        let offset = core.entries[0].get_index();
        assert!(idx >= offset);
        if idx - offset >= core.entries.len() as u64 {
            return Err(Error::Store(StorageError::Unavailable));
        }
        Ok(core.entries[(idx - offset) as usize].get_term())
    }

    /// Implements the Storage trait.
    fn first_index(&self) -> Result<u64> {
        Ok(self.rl().inner_first_index())
    }

    /// Implements the Storage trait.
    fn last_index(&self) -> Result<u64> {
        Ok(self.rl().inner_last_index())
    }

    /// Implements the Storage trait.
    fn snapshot(&self) -> Result<Snapshot> {
        let core = self.rl();
        Ok(core.inner_snapshot())
    }
}

#[cfg(test)]
mod test {
    use std::panic::{self, AssertUnwindSafe};

    use harness::setup_for_test;
    use protobuf;

    use crate::eraftpb::{ConfState, Entry, Snapshot};
    use crate::errors::{Error as RaftError, StorageError};
    use crate::storage::{MemStorage, Storage};

    fn new_entry(index: u64, term: u64) -> Entry {
        let mut e = Entry::new();
        e.set_term(term);
        e.set_index(index);
        e
    }

    fn size_of<T: protobuf::Message>(m: &T) -> u32 {
        m.compute_size()
    }

    fn new_snapshot(index: u64, term: u64, nodes: Vec<u64>) -> Snapshot {
        let mut s = Snapshot::new();
        s.mut_metadata().set_index(index);
        s.mut_metadata().set_term(term);
        s.mut_metadata().mut_conf_state().set_nodes(nodes);
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
            (3, 4, max_u64, Ok(vec![new_entry(3, 3)])),
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

        storage.wl().append(&[new_entry(6, 5)]).unwrap();
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

        assert_eq!(storage.first_index(), Ok(3));
        storage.wl().compact(4).unwrap();
        assert_eq!(storage.first_index(), Ok(4));
    }

    #[test]
    fn test_storage_compact() {
        setup_for_test();
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let mut tests = vec![(2, 3, 3, 3), (3, 3, 3, 3), (4, 4, 4, 2), (5, 5, 5, 1)];
        for (i, (idx, windex, wterm, wlen)) in tests.drain(..).enumerate() {
            let storage = MemStorage::new();
            storage.wl().entries = ents.clone();

            storage.wl().compact(idx).unwrap();
            let index = storage.first_index().unwrap();
            if index != windex {
                panic!("#{}: want {}, index {}", i, windex, index);
            }
            let term = if let Ok(v) = storage.entries(index, index + 1, 1) {
                v.first().map_or(0, |e| e.get_term())
            } else {
                0
            };
            if term != wterm {
                panic!("#{}: want {}, term {}", i, wterm, term);
            }
            let last = storage.last_index().unwrap();
            let len = storage.entries(index, last + 1, 100).unwrap().len();
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
        let mut conf_state = ConfState::new();
        conf_state.set_nodes(nodes.clone());

        let mut tests = vec![
            (4, Ok(new_snapshot(4, 4, nodes.clone()))),
            (5, Ok(new_snapshot(5, 5, nodes.clone()))),
        ];
        for (i, (idx, wresult)) in tests.drain(..).enumerate() {
            let storage = MemStorage::new();
            storage.wl().entries = ents.clone();
            storage.wl().raft_state.hard_state.set_commit(idx);
            storage.wl().raft_state.hard_state.set_term(idx);
            storage.wl().raft_state.conf_state = conf_state.clone();

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
                Some(vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)]),
            ),
            (
                vec![new_entry(3, 3), new_entry(4, 6), new_entry(5, 6)],
                Some(vec![new_entry(3, 3), new_entry(4, 6), new_entry(5, 6)]),
            ),
            (
                vec![
                    new_entry(3, 3),
                    new_entry(4, 4),
                    new_entry(5, 5),
                    new_entry(6, 5),
                ],
                Some(vec![
                    new_entry(3, 3),
                    new_entry(4, 4),
                    new_entry(5, 5),
                    new_entry(6, 5),
                ]),
            ),
            // overwrite compacted raft logs is not allowed
            (
                vec![new_entry(2, 3), new_entry(3, 3), new_entry(4, 5)],
                None,
            ),
            // truncate the existing entries and append
            (
                vec![new_entry(4, 5)],
                Some(vec![new_entry(3, 3), new_entry(4, 5)]),
            ),
            // direct append
            (
                vec![new_entry(6, 6)],
                Some(vec![
                    new_entry(3, 3),
                    new_entry(4, 4),
                    new_entry(5, 5),
                    new_entry(6, 6),
                ]),
            ),
        ];
        for (i, (entries, wentries)) in tests.drain(..).enumerate() {
            let storage = MemStorage::new();
            storage.wl().entries = ents.clone();
            let res = panic::catch_unwind(AssertUnwindSafe(|| storage.wl().append(&entries)));
            if let Some(wentries) = wentries {
                assert!(res.is_ok());
                let e = &storage.wl().entries;
                if *e != wentries {
                    panic!("#{}: want {:?}, entries {:?}", i, wentries, e);
                }
            } else {
                assert!(res.is_err());
            }
        }
    }

    #[test]
    fn test_storage_apply_snapshot() {
        setup_for_test();
        let nodes = vec![1, 2, 3];
        let storage = MemStorage::new();

        // Apply snapshot successfully
        let snap = new_snapshot(4, 4, nodes.clone());
        assert!(storage.wl().apply_snapshot(snap).is_ok());

        // Apply snapshot fails due to StorageError::SnapshotOutOfDate
        let snap = new_snapshot(3, 3, nodes.clone());
        assert!(storage.wl().apply_snapshot(snap).is_err());
    }
}
