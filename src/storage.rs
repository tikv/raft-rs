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

use crate::eraftpb::{ConfState, Entry, HardState, Snapshot};
use crate::errors::Result;

/// Holds both the hard state (commit index, vote leader, term) and the configuration state
/// (Current node IDs)
#[derive(Debug, Clone, Getters, Setters)]
pub struct RaftState {
    /// Contains the last meta information including commit index, the vote leader, and the vote term.
    pub hard_state: HardState,

    /// History configuration states and their indices. Not all history need to be reserved, the
    /// `Storage` instance need only to ensure the first entry's index is less than applied index
    /// so that when the peer sends snapshot to followers, it can choose a correct conf state.
    ///
    /// Entries in the field should be sorted by their indices.
    pub conf_states: Vec<(ConfState, u64)>,

    /// Indicates whether it's in membership change context or not.
    pub in_membership_change: bool,
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
    /// max_size limits the total size of the log entries returned, but
    /// entries returns at least one entry if any.
    fn entries(&self, low: u64, high: u64, max_size: u64) -> Result<Vec<Entry>>;

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
