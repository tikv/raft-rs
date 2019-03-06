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

use std::cmp;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use protobuf::Message as PbMessage;

use crate::eraftpb::*;
use crate::errors::{Error, Result, StorageError};
use crate::{util, Config};

/// Used to track the history about configuration changes.
#[derive(Clone, Debug, Default)]
pub struct ConfStateWithIndex {
    /// Target configuration state.
    pub conf_state: ConfState,
    /// Index of the entry.
    pub index: u64,
    /// Indicates it's a membership change or not.
    pub in_membership_change: bool,
}

impl ConfStateWithIndex {
    pub(crate) fn apply_conf_change(base: ConfState, index: u64, mut cc: ConfChange) -> Self {
        let mut cs = ConfStateWithIndex::default();
        cs.conf_state = base;
        cs.index = index;
        match cc.get_change_type() {
            ConfChangeType::AddNode => {
                cs.conf_state.mut_nodes().push(cc.get_node_id());
            }
            ConfChangeType::AddLearnerNode => {
                cs.conf_state.mut_learners().push(cc.get_node_id());
            }
            ConfChangeType::RemoveNode => {
                let node_id = cc.get_node_id();
                let nodes = cs.conf_state.mut_nodes();
                if let Some(i) = nodes.iter().position(|id| *id == node_id) {
                    nodes.swap_remove(i);
                }
                let learners = cs.conf_state.mut_learners();
                if let Some(i) = learners.iter().position(|id| *id == node_id) {
                    learners.swap_remove(i);
                }
            }
            ConfChangeType::BeginMembershipChange => {
                assert_eq!(cc.get_start_index(), index);
                cs.conf_state = cc.take_configuration();
                cs.in_membership_change = true;
            }
            ConfChangeType::FinalizeMembershipChange => {
                cs.in_membership_change = false;
            }
        }
        cs
    }
}

/// Holds both the hard state (commit index, vote leader, term) and the configuration state
/// (Current node IDs)
#[derive(Debug, Clone, Default, Getters, Setters)]
pub struct RaftState {
    /// Contains the last meta information including commit index, the vote leader, and the vote term.
    pub hard_state: HardState,

    /// History configuration states and their indices. Not all history need to be reserved, the
    /// `Storage` instance need only to ensure the first entry's index is less than applied index
    /// so that when the peer sends snapshot to followers, it can choose a correct conf state.
    ///
    /// Entries in the field should be sorted by their indices.
    pub conf_states: Vec<ConfStateWithIndex>,
}

impl RaftState {
    /// Indicates the `RaftState` is initialized or not.
    pub fn initialized(&self) -> bool {
        !self.conf_states.is_empty()
    }
}

/// Storage saves all the information about the current Raft implementation, including Raft Log, commit index, the leader to vote for, etc.
/// Pay attention to what is returned when there is no Log but it needs to get the `term` at index `first_index() - 1`. To solve this, you can use a dummy Log entry to keep the last truncated Log entry. See [`entries: vec![Entry::new()]`](src/storage.rs#L85) as a reference.
///
/// If any Storage method returns an error, the raft instance will
/// become inoperable and refuse to paticipate in elections; the
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

    /// Returns the index of the first log entry that is possible available via entries.
    /// If the `Storage` is just initialized with a snapshot, `first_index` should return
    /// the `snapshot index + 1`.
    fn first_index(&self) -> Result<u64>;

    /// The index of the last entry in the log. If tne `Storage` is just initialized with a
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
            // When starting from scratch populate the list with a dummy entry at term zero.
            entries: vec![Entry::new()],
            snapshot_metadata: Default::default(),
        }
    }
}

impl MemStorageCore {
    /// Initialize a configuraftion state with index 1.
    pub(crate) fn initialize_conf_state(&mut self, cs: ConfStateWithIndex) {
        self.raft_state.conf_states.push(cs);
        self.entries[0].set_index(1);
        self.raft_state.hard_state.set_commit(1);
        self.snapshot_metadata.set_index(1);
    }

    /// Saves the current HardState.
    pub fn set_hardstate(&mut self, hs: HardState) {
        self.raft_state.hard_state = hs;
    }

    /// Apply to an index so that we can crate a latest snapshot from the storage.
    pub fn apply_to(&mut self, index: u64) {
        assert!(index >= self.entries[0].get_index());
        assert!(index <= self.entries.last().unwrap().get_index());
        for e in &self.entries {
            if e.get_index() == index {
                self.raft_state.hard_state.set_term(e.get_term());
                self.raft_state.hard_state.set_commit(index);
                break;
            }
        }
    }

    /// Save raft logs and apply configuration chagnes in them.
    pub fn append(&mut self, ents: &[Entry]) {
        if ents.is_empty() {
            return;
        }

        // Gap is now allowed in raft logs.
        assert!(self.entries.last().unwrap().get_index() + 1 >= ents[0].get_index());
        if self.entries.last().unwrap().get_index() + 1 < ents[0].get_index() {
            panic!(
                "missing log entry [last: {}, append at: {}]",
                self.inner_last_index(),
                ents[0].get_index()
            )
        }

        // Remove all entries overwritten by `ents`, and truncate `ents` if need.
        let next_idx = self.entries.last().unwrap().get_index() + 1;
        let first_idx = cmp::max(ents[0].get_index(), self.entries[0].get_index());
        if next_idx > first_idx {
            let diff = next_idx - first_idx;
            // Uncommitted raft logs can't be compacted.
            assert!(self.entries.len() >= diff as usize);

            let s = self.entries.len() - diff as usize;
            while self.entries.len() > s {
                self.entries.pop();
            }
        }

        let i = ents
            .iter()
            .position(|e| e.get_index() == first_idx)
            .unwrap();
        ents[i..].iter().for_each(|e| self.entries.push(e.clone()));
        self.handle_conf_changes_after_append(&ents[i..]);
    }

    fn handle_conf_changes_after_append(&mut self, ents: &[Entry]) {
        if ents.is_empty() {
            return;
        }
        // Revert some configuration changes.
        let recover_prior = ents[0].get_index();
        for i in (0..self.raft_state.conf_states.len()).rev() {
            if self.raft_state.conf_states[i].index < recover_prior {
                break;
            }
            self.raft_state.conf_states.pop();
        }

        for e in ents
            .iter()
            .filter(|e| e.get_entry_type() == EntryType::EntryConfChange)
        {
            let mut cc = ConfChange::new();
            cc.merge_from_bytes(e.get_data()).unwrap();
            let last = self.raft_state.conf_states.last().unwrap();
            let base = last.conf_state.clone();
            let index = e.get_index();
            let cs = ConfStateWithIndex::apply_conf_change(base, index, cc);
            self.raft_state.conf_states.push(cs);
        }
    }

    fn inner_first_index(&self) -> u64 {
        let first_index = self.entries[0].get_index();
        if self.snapshot_metadata.get_index() == first_index {
            return first_index + 1;
        }
        first_index
    }

    fn inner_last_index(&self) -> u64 {
        self.entries.last().unwrap().get_index()
    }

    /// Overwrites the contents of this Storage object with those of the given snapshot.
    ///
    /// # Panics
    ///
    /// Panics if the snapshot index is less than the storage's first index.
    pub fn apply_snapshot(&mut self, mut snapshot: Snapshot) {
        let mut meta = snapshot.take_metadata();
        let term = meta.get_term();
        let index = meta.get_index();

        if self.inner_first_index() > index {
            panic!(
                "apply snapshot with a less index than {}: {}",
                index,
                self.inner_first_index(),
            );
        }

        // Update metadata and dummy entries.
        self.entries.clear();
        let mut e = Entry::new();
        e.set_term(term);
        e.set_index(index);
        self.entries.push(e);
        self.snapshot_metadata = meta.clone();

        // Update conf states.
        self.raft_state.hard_state.set_term(term);
        self.raft_state.hard_state.set_commit(index);
        self.raft_state.conf_states.clear();

        let mut cs = ConfStateWithIndex::default();
        cs.conf_state = meta.take_conf_state();
        cs.index = meta.get_conf_state_index();
        self.raft_state.conf_states.push(cs);

        if meta.get_next_conf_state_index() > 0 {
            let mut cs = ConfStateWithIndex::default();
            cs.conf_state = meta.take_next_conf_state();
            cs.index = meta.get_next_conf_state_index();
            cs.in_membership_change = true;
            self.raft_state.conf_states.push(cs);
        }
    }

    fn create_snapshot(&self) -> Snapshot {
        let mut snapshot = Snapshot::new();

        // Use the latest applied_idx to construct the snapshot.
        let applied_idx = self.raft_state.hard_state.get_commit();
        let term = self.raft_state.hard_state.get_term();
        snapshot.mut_metadata().set_index(applied_idx);
        snapshot.mut_metadata().set_term(term);

        // Find the latest configuration state before applied index.
        let i = match self
            .raft_state
            .conf_states
            .iter()
            .position(|cs| cs.index > applied_idx)
        {
            Some(i) => i,
            None => self.raft_state.conf_states.len() - 1,
        };

        if self.raft_state.conf_states[i].in_membership_change {
            let cs = self.raft_state.conf_states[i - 1].clone();
            snapshot.mut_metadata().set_conf_state(cs.conf_state);
            snapshot.mut_metadata().set_conf_state_index(cs.index);
            let cs = self.raft_state.conf_states[i].clone();
            snapshot.mut_metadata().set_next_conf_state(cs.conf_state);
            snapshot.mut_metadata().set_next_conf_state_index(cs.index);
        } else {
            let cs = self.raft_state.conf_states[i].clone();
            snapshot.mut_metadata().set_conf_state(cs.conf_state);
            snapshot.mut_metadata().set_conf_state_index(cs.index);
        }
        snapshot
    }

    /// Discards all log entries prior to compact_index.
    /// It is the application's responsibility to not attempt to compact an index
    /// greater than RaftLog.applied.
    pub fn compact(&mut self, compact_index: u64) {
        if compact_index <= self.inner_first_index() {
            return;
        }

        if compact_index > self.inner_last_index() {
            // compact index must be less than last index, or entries will be empty.
            panic!(
                "compact {} is out of bound lastindex({})",
                compact_index,
                self.inner_last_index()
            )
        }

        let mut offset = self.inner_first_index();
        if self.snapshot_metadata.get_index() == self.entries[0].get_index() {
            // The first entry in entries is the dummpy entry.
            offset -= 1;
        }

        let i = (compact_index - offset) as usize;
        let entries = self.entries.drain(i..).collect();
        self.entries = entries;
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

    /// Create a `MemStorage` with a given `Config`.
    pub fn initialize_with_config(&self, cfg: &Config) {
        assert!(!self.initial_state().unwrap().initialized());
        trace!("crate storage with given config");
        let mut cs = ConfStateWithIndex::default();
        cs.conf_state.mut_nodes().extend(&cfg.peers);
        cs.conf_state.mut_learners().extend(&cfg.learners);
        cs.index = 1;
        self.wl().initialize_conf_state(cs.clone());
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
                "index out of bound (last: {}, high: {}",
                core.inner_last_index() + 1,
                high
            );
        }

        let offset = core.entries[0].get_index();
        let lo = (low - offset) as usize;
        let hi = (high - offset) as usize;
        let mut ents = core.entries[lo..hi].to_vec();
        util::limit_size(&mut ents, max_size);
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
        Ok(core.create_snapshot())
    }
}

#[cfg(test)]
mod test {
    use std::panic::{self, AssertUnwindSafe};

    use harness::setup_for_test;
    use protobuf;

    use crate::eraftpb::{ConfState, Entry, Snapshot};
    use crate::errors::{Error as RaftError, StorageError};
    use crate::storage::{ConfStateWithIndex, MemStorage, Storage};

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

        storage.wl().append(&[new_entry(6, 5)]);
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
        storage.wl().compact(4);
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

            storage.wl().compact(idx);
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
            let cs = ConfStateWithIndex {
                conf_state: conf_state.clone(),
                index: 0,
                in_membership_change: false,
            };
            storage.wl().raft_state.conf_states.push(cs);

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
                vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)],
            ),
            (
                vec![new_entry(3, 3), new_entry(4, 6), new_entry(5, 6)],
                vec![new_entry(3, 3), new_entry(4, 6), new_entry(5, 6)],
            ),
            (
                vec![
                    new_entry(3, 3),
                    new_entry(4, 4),
                    new_entry(5, 5),
                    new_entry(6, 5),
                ],
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
                vec![new_entry(3, 3), new_entry(4, 5)],
            ),
            // truncate the existing entries and append
            (
                vec![new_entry(4, 5)],
                vec![new_entry(3, 3), new_entry(4, 5)],
            ),
            // direct append
            (
                vec![new_entry(6, 6)],
                vec![
                    new_entry(3, 3),
                    new_entry(4, 4),
                    new_entry(5, 5),
                    new_entry(6, 6),
                ],
            ),
        ];
        for (i, (entries, wentries)) in tests.drain(..).enumerate() {
            let storage = MemStorage::new();
            storage.wl().entries = ents.clone();

            storage.wl().append(&entries);
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

        let snapshots = vec![
            new_snapshot(4, 4, nodes.clone()),
            new_snapshot(3, 3, nodes.clone()),
        ];

        let storage = MemStorage::new();

        // Apply snapshot successfully
        storage.wl().apply_snapshot(snapshots[0].clone());

        // Apply snapshot fails due to StorageError::SnapshotOutOfDate
        let res = panic::catch_unwind(AssertUnwindSafe(|| {
            storage.wl().apply_snapshot(snapshots[1].clone())
        }));
        assert!(res.is_err());
    }
}
