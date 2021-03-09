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

use std::cmp;

use crate::eraftpb::{Entry, Snapshot};
use crate::errors::{Error, Result, StorageError};
use crate::log_unstable::Unstable;
use crate::storage::Storage;
use crate::util;

use slog::Logger;

pub use crate::util::NO_LIMIT;

/// Raft log implementation
pub struct RaftLog<T: Storage> {
    /// Contains all stable entries since the last snapshot.
    pub store: T,

    /// Contains all unstable entries and snapshot.
    /// they will be saved into storage.
    pub unstable: Unstable,

    /// The highest log position that is known to be in stable storage
    /// on a quorum of nodes.
    ///
    /// Invariant: applied <= committed
    pub committed: u64,

    /// The highest log position that is known to be persisted in stable
    /// storage. It's used for limiting the upper bound of committed and
    /// persisted entries.
    ///
    /// Invariant: persisted < unstable.offset && applied <= persisted
    pub persisted: u64,

    /// The highest log position that the application has been instructed
    /// to apply to its state machine.
    ///
    /// Invariant: applied <= min(committed, persisted)
    pub applied: u64,
}

impl<T> ToString for RaftLog<T>
where
    T: Storage,
{
    fn to_string(&self) -> String {
        format!(
            "committed={}, persisted={}, applied={}, unstable.offset={}, unstable.entries.len()={}",
            self.committed,
            self.persisted,
            self.applied,
            self.unstable.offset,
            self.unstable.entries.len()
        )
    }
}

impl<T: Storage> RaftLog<T> {
    /// Creates a new raft log with a given storage and tag.
    pub fn new(store: T, logger: Logger) -> RaftLog<T> {
        let first_index = store.first_index().unwrap();
        let last_index = store.last_index().unwrap();

        // Initialize committed and applied pointers to the time of the last compaction.
        RaftLog {
            store,
            committed: first_index - 1,
            persisted: last_index,
            applied: first_index - 1,
            unstable: Unstable::new(last_index + 1, logger),
        }
    }

    /// Grabs the term from the last entry.
    ///
    /// # Panics
    ///
    /// Panics if there are entries but the last term has been discarded.
    pub fn last_term(&self) -> u64 {
        match self.term(self.last_index()) {
            Ok(t) => t,
            Err(e) => fatal!(
                self.unstable.logger,
                "unexpected error when getting the last term: {:?}",
                e
            ),
        }
    }

    /// Grab a read-only reference to the underlying storage.
    #[inline]
    pub fn store(&self) -> &T {
        &self.store
    }

    /// Grab a mutable reference to the underlying storage.
    #[inline]
    pub fn mut_store(&mut self) -> &mut T {
        &mut self.store
    }

    /// For a given index, finds the term associated with it.
    pub fn term(&self, idx: u64) -> Result<u64> {
        // the valid term range is [index of dummy entry, last index]
        let dummy_idx = self.first_index() - 1;
        if idx < dummy_idx || idx > self.last_index() {
            return Ok(0u64);
        }

        match self.unstable.maybe_term(idx) {
            Some(term) => Ok(term),
            _ => self.store.term(idx).map_err(|e| {
                match e {
                    Error::Store(StorageError::Compacted)
                    | Error::Store(StorageError::Unavailable) => {}
                    _ => fatal!(self.unstable.logger, "unexpected error: {:?}", e),
                }
                e
            }),
        }
    }

    /// Returns th first index in the store that is available via entries
    ///
    /// # Panics
    ///
    /// Panics if the store doesn't have a first index.
    pub fn first_index(&self) -> u64 {
        match self.unstable.maybe_first_index() {
            Some(idx) => idx,
            None => self.store.first_index().unwrap(),
        }
    }

    /// Returns the last index in the store that is available via entries.
    ///
    /// # Panics
    ///
    /// Panics if the store doesn't have a last index.
    pub fn last_index(&self) -> u64 {
        match self.unstable.maybe_last_index() {
            Some(idx) => idx,
            None => self.store.last_index().unwrap(),
        }
    }

    /// Finds the index of the conflict.
    ///
    /// It returns the first index of conflicting entries between the existing
    /// entries and the given entries, if there are any.
    ///
    /// If there are no conflicting entries, and the existing entries contain
    /// all the given entries, zero will be returned.
    ///
    /// If there are no conflicting entries, but the given entries contains new
    /// entries, the index of the first new entry will be returned.
    ///
    /// An entry is considered to be conflicting if it has the same index but
    /// a different term.
    ///
    /// The first entry MUST have an index equal to the argument 'from'.
    /// The index of the given entries MUST be continuously increasing.
    pub fn find_conflict(&self, ents: &[Entry]) -> u64 {
        for e in ents {
            if !self.match_term(e.index, e.term) {
                if e.index <= self.last_index() {
                    info!(
                        self.unstable.logger,
                        "found conflict at index {index}",
                        index = e.index;
                        "existing term" => self.term(e.index).unwrap_or(0),
                        "conflicting term" => e.term,
                    );
                }
                return e.index;
            }
        }
        0
    }

    /// Answers the question: Does this index belong to this term?
    pub fn match_term(&self, idx: u64, term: u64) -> bool {
        self.term(idx).map(|t| t == term).unwrap_or(false)
    }

    // TODO: revoke pub when there is a better way to append without proposals.
    /// Returns None if the entries cannot be appended. Otherwise,
    /// it returns Some((conflict_index, last_index)).
    ///
    /// # Panics
    ///
    /// Panics if it finds a conflicting index less than committed index.
    pub fn maybe_append(
        &mut self,
        idx: u64,
        term: u64,
        committed: u64,
        ents: &[Entry],
    ) -> Option<(u64, u64)> {
        if self.match_term(idx, term) {
            let conflict_idx = self.find_conflict(ents);
            if conflict_idx == 0 {
            } else if conflict_idx <= self.committed {
                fatal!(
                    self.unstable.logger,
                    "entry {} conflict with committed entry {}",
                    conflict_idx,
                    self.committed
                )
            } else {
                let start = (conflict_idx - (idx + 1)) as usize;
                self.append(&ents[start..]);
                // persisted should be decreased because entries are changed
                if self.persisted > conflict_idx - 1 {
                    self.persisted = conflict_idx - 1;
                }
            }
            let last_new_index = idx + ents.len() as u64;
            self.commit_to(cmp::min(committed, last_new_index));
            return Some((conflict_idx, last_new_index));
        }
        None
    }

    /// Sets the last committed value to the passed in value.
    ///
    /// # Panics
    ///
    /// Panics if the index goes past the last index.
    pub fn commit_to(&mut self, to_commit: u64) {
        // never decrease commit
        if self.committed >= to_commit {
            return;
        }
        if self.last_index() < to_commit {
            fatal!(
                self.unstable.logger,
                "to_commit {} is out of range [last_index {}]",
                to_commit,
                self.last_index()
            )
        }
        self.committed = to_commit;
    }

    /// Advance the applied index to the passed in value.
    ///
    /// # Panics
    ///
    /// Panics if the value passed in is not new or known.
    #[deprecated = "Call raft::commit_apply(idx) instead. Joint Consensus requires an on-apply hook to
    finalize a configuration change. This will become internal API in future versions."]
    pub fn applied_to(&mut self, idx: u64) {
        if idx == 0 {
            return;
        }
        if idx > cmp::min(self.committed, self.persisted) || idx < self.applied {
            fatal!(
                self.unstable.logger,
                "applied({}) is out of range [prev_applied({}), min(committed({}), persisted({}))]",
                idx,
                self.applied,
                self.committed,
                self.persisted,
            )
        }
        self.applied = idx;
    }

    /// Returns the last applied index.
    pub fn applied(&self) -> u64 {
        self.applied
    }

    /// Clears the unstable entries and moves the stable offset up to the
    /// last index, if there is any.
    pub fn stable_entries(&mut self, index: u64, term: u64) {
        self.unstable.stable_entries(index, term);
    }

    /// Clears the unstable snapshot.
    pub fn stable_snap(&mut self, index: u64) {
        self.unstable.stable_snap(index);
    }

    /// Returns a reference to the unstable log.
    pub fn unstable(&self) -> &Unstable {
        &self.unstable
    }

    /// Returns slice of entries that are not persisted.
    pub fn unstable_entries(&self) -> &[Entry] {
        &self.unstable.entries
    }

    /// Returns the snapshot that are not persisted.
    pub fn unstable_snapshot(&self) -> &Option<Snapshot> {
        &self.unstable.snapshot
    }

    /// Appends a set of entries to the unstable list.
    pub fn append(&mut self, ents: &[Entry]) -> u64 {
        trace!(
            self.unstable.logger,
            "Entries being appended to unstable list";
            "ents" => ?ents,
        );
        if ents.is_empty() {
            return self.last_index();
        }

        let after = ents[0].index - 1;
        if after < self.committed {
            fatal!(
                self.unstable.logger,
                "after {} is out of range [committed {}]",
                after,
                self.committed
            )
        }
        self.unstable.truncate_and_append(ents);
        self.last_index()
    }

    /// Returns entries starting from a particular index and not exceeding a bytesize.
    pub fn entries(&self, idx: u64, max_size: impl Into<Option<u64>>) -> Result<Vec<Entry>> {
        let max_size = max_size.into();
        let last = self.last_index();
        if idx > last {
            return Ok(Vec::new());
        }
        self.slice(idx, last + 1, max_size)
    }

    /// Returns all the entries.
    pub fn all_entries(&self) -> Vec<Entry> {
        let first_index = self.first_index();
        match self.entries(first_index, None) {
            Err(e) => {
                // try again if there was a racing compaction
                if e == Error::Store(StorageError::Compacted) {
                    return self.all_entries();
                }
                fatal!(self.unstable.logger, "unexpected error: {:?}", e);
            }
            Ok(ents) => ents,
        }
    }

    /// Determines if the given (lastIndex,term) log is more up-to-date
    /// by comparing the index and term of the last entry in the existing logs.
    /// If the logs have last entry with different terms, then the log with the
    /// later term is more up-to-date. If the logs end with the same term, then
    /// whichever log has the larger last_index is more up-to-date. If the logs are
    /// the same, the given log is up-to-date.
    pub fn is_up_to_date(&self, last_index: u64, term: u64) -> bool {
        term > self.last_term() || (term == self.last_term() && last_index >= self.last_index())
    }

    /// Returns committed and persisted entries since max(`since_idx` + 1, first_index).
    pub fn next_entries_since(&self, since_idx: u64) -> Option<Vec<Entry>> {
        let offset = cmp::max(since_idx + 1, self.first_index());
        let high = cmp::min(self.committed, self.persisted) + 1;
        if high > offset {
            match self.slice(offset, high, None) {
                Ok(vec) => return Some(vec),
                Err(e) => fatal!(self.unstable.logger, "{}", e),
            }
        }
        None
    }

    /// Returns all the available entries for execution.
    /// If applied is smaller than the index of snapshot, it returns all committed
    /// entries after the index of snapshot.
    pub fn next_entries(&self) -> Option<Vec<Entry>> {
        self.next_entries_since(self.applied)
    }

    /// Returns whether there are committed and persisted entries since
    /// max(`since_idx` + 1, first_index).
    pub fn has_next_entries_since(&self, since_idx: u64) -> bool {
        let offset = cmp::max(since_idx + 1, self.first_index());
        let high = cmp::min(self.committed, self.persisted) + 1;
        high > offset
    }

    /// Returns whether there are new entries.
    pub fn has_next_entries(&self) -> bool {
        self.has_next_entries_since(self.applied)
    }

    /// Returns the current snapshot
    pub fn snapshot(&self, request_index: u64) -> Result<Snapshot> {
        if let Some(snap) = self.unstable.snapshot.as_ref() {
            if snap.get_metadata().index >= request_index {
                return Ok(snap.clone());
            }
        }
        self.store.snapshot(request_index)
    }

    pub(crate) fn pending_snapshot(&self) -> Option<&Snapshot> {
        self.unstable.snapshot.as_ref()
    }

    fn must_check_outofbounds(&self, low: u64, high: u64) -> Option<Error> {
        if low > high {
            fatal!(self.unstable.logger, "invalid slice {} > {}", low, high)
        }
        let first_index = self.first_index();
        if low < first_index {
            return Some(Error::Store(StorageError::Compacted));
        }

        let length = self.last_index() + 1 - first_index;
        if low < first_index || high > first_index + length {
            fatal!(
                self.unstable.logger,
                "slice[{},{}] out of bound[{},{}]",
                low,
                high,
                first_index,
                self.last_index()
            )
        }
        None
    }

    /// Attempts to commit the index and term and returns whether it did.
    pub fn maybe_commit(&mut self, max_index: u64, term: u64) -> bool {
        if max_index > self.committed && self.term(max_index).map_or(false, |t| t == term) {
            debug!(
                self.unstable.logger,
                "committing index {index}",
                index = max_index
            );
            self.commit_to(max_index);
            true
        } else {
            false
        }
    }

    /// Attempts to persist the index and term and returns whether it did.
    pub fn maybe_persist(&mut self, index: u64, term: u64) -> bool {
        // It's possible that the term check can be passed but index is greater
        // than or equal to the first_update_index in some corner cases.
        // For example, there are 5 nodes, A B C D E.
        // 1. A is leader and it proposes some raft logs but only B receives these logs.
        // 2. B gets the Ready and the logs are persisted asynchronously.
        // 2. A crashes and C becomes leader after getting the vote from D and E.
        // 3. C proposes some raft logs and B receives these logs.
        // 4. C crashes and A restarts and becomes leader again after getting the vote from D and E.
        // 5. B receives the logs from A which are the same to the ones from step 1.
        // 6. The logs from Ready has been persisted on B so it calls on_persist_ready and comes to here.
        //
        // We solve this problem by not forwarding the persisted index. It's pretty intuitive
        // because the first_update_index means there are snapshot or some entries whose indexes
        // are greater than or equal to the first_update_index have not been persisted yet.
        let first_update_index = match &self.unstable.snapshot {
            Some(s) => s.get_metadata().index,
            None => self.unstable.offset,
        };
        if index > self.persisted
            && index < first_update_index
            && self.store.term(index).map_or(false, |t| t == term)
        {
            debug!(self.unstable.logger, "persisted index {}", index);
            self.persisted = index;
            true
        } else {
            false
        }
    }

    /// Attempts to persist the snapshot and returns whether it did.
    pub fn maybe_persist_snap(&mut self, index: u64) -> bool {
        if index > self.persisted {
            // commit index should not be less than snapshot's index
            if index > self.committed {
                fatal!(
                    self.unstable.logger,
                    "snapshot's index {} > committed {}",
                    index,
                    self.committed,
                )
            }
            // All of the indexes of later entries must be greater than snapshot's index
            if index >= self.unstable.offset {
                fatal!(
                    self.unstable.logger,
                    "snapshot's index {} >= offset {}",
                    index,
                    self.unstable.offset,
                );
            }

            debug!(self.unstable.logger, "snapshot's persisted index {}", index);
            self.persisted = index;
            true
        } else {
            false
        }
    }

    /// Grabs a slice of entries from the raft. Unlike a rust slice pointer, these are
    /// returned by value. The result is truncated to the max_size in bytes.
    pub fn slice(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
    ) -> Result<Vec<Entry>> {
        let max_size = max_size.into();
        if let Some(err) = self.must_check_outofbounds(low, high) {
            return Err(err);
        }

        let mut ents = vec![];
        if low == high {
            return Ok(ents);
        }

        if low < self.unstable.offset {
            let unstable_high = cmp::min(high, self.unstable.offset);
            match self.store.entries(low, unstable_high, max_size) {
                Err(e) => match e {
                    Error::Store(StorageError::Compacted) => return Err(e),
                    Error::Store(StorageError::Unavailable) => fatal!(
                        self.unstable.logger,
                        "entries[{}:{}] is unavailable from storage",
                        low,
                        unstable_high,
                    ),
                    _ => fatal!(self.unstable.logger, "unexpected error: {:?}", e),
                },
                Ok(entries) => {
                    ents = entries;
                    if (ents.len() as u64) < unstable_high - low {
                        return Ok(ents);
                    }
                }
            }
        }

        if high > self.unstable.offset {
            let offset = self.unstable.offset;
            let unstable = self.unstable.slice(cmp::max(low, offset), high);
            ents.extend_from_slice(unstable);
        }
        util::limit_size(&mut ents, max_size);
        Ok(ents)
    }

    /// Restores the current log from a snapshot.
    pub fn restore(&mut self, snapshot: Snapshot) {
        info!(
            self.unstable.logger,
            "log [{log}] starts to restore snapshot [index: {snapshot_index}, term: {snapshot_term}]",
            log = self.to_string(),
            snapshot_index = snapshot.get_metadata().index,
            snapshot_term = snapshot.get_metadata().term,
        );
        let index = snapshot.get_metadata().index;
        assert!(index >= self.committed, "{} < {}", index, self.committed);
        // If `persisted` is greater than `committed`, reset it to `committed`.
        // It's because only the persisted entries whose index are less than `commited` can be
        // considered the same as the data from snapshot.
        // Although there may be some persisted entries with greater index are also committed,
        // we can not judge them nor do we care about them because these entries can not be applied
        // thus the invariant which is `applied` <= min(`persisted`, `committed`) is satisfied.
        if self.persisted > self.committed {
            self.persisted = self.committed;
        }
        self.committed = index;
        self.unstable.restore(snapshot);
    }

    /// Returns the committed index and its term.
    pub fn commit_info(&self) -> (u64, u64) {
        match self.term(self.committed) {
            Ok(t) => (self.committed, t),
            Err(e) => fatal!(
                self.unstable.logger,
                "last committed entry at {} is missing: {:?}",
                self.committed,
                e
            ),
        }
    }
}

#[cfg(test)]
mod test {
    use std::{
        cmp,
        panic::{self, AssertUnwindSafe},
    };

    use crate::default_logger;
    use crate::eraftpb;
    use crate::errors::{Error, StorageError};
    use crate::raft_log::{self, RaftLog};
    use crate::storage::MemStorage;
    use protobuf::Message as PbMessage;

    fn new_entry(index: u64, term: u64) -> eraftpb::Entry {
        let mut e = eraftpb::Entry::default();
        e.term = term;
        e.index = index;
        e
    }

    fn new_snapshot(meta_index: u64, meta_term: u64) -> eraftpb::Snapshot {
        let mut meta = eraftpb::SnapshotMetadata::default();
        meta.index = meta_index;
        meta.term = meta_term;
        let mut snapshot = eraftpb::Snapshot::default();
        snapshot.set_metadata(meta);
        snapshot
    }

    #[test]
    fn test_find_conflict() {
        let l = default_logger();
        let previous_ents = vec![new_entry(1, 1), new_entry(2, 2), new_entry(3, 3)];
        let tests = vec![
            // no conflict, empty ent
            (vec![], 0),
            (vec![], 0),
            // no conflict
            (vec![new_entry(1, 1), new_entry(2, 2), new_entry(3, 3)], 0),
            (vec![new_entry(2, 2), new_entry(3, 3)], 0),
            (vec![new_entry(3, 3)], 0),
            // no conflict, but has new entries
            (
                vec![
                    new_entry(1, 1),
                    new_entry(2, 2),
                    new_entry(3, 3),
                    new_entry(4, 4),
                    new_entry(5, 4),
                ],
                4,
            ),
            (
                vec![
                    new_entry(2, 2),
                    new_entry(3, 3),
                    new_entry(4, 4),
                    new_entry(5, 4),
                ],
                4,
            ),
            (vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 4)], 4),
            (vec![new_entry(4, 4), new_entry(5, 4)], 4),
            // conflicts with existing entries
            (vec![new_entry(1, 4), new_entry(2, 4)], 1),
            (vec![new_entry(2, 1), new_entry(3, 4), new_entry(4, 4)], 2),
            (
                vec![
                    new_entry(3, 1),
                    new_entry(4, 2),
                    new_entry(5, 4),
                    new_entry(6, 4),
                ],
                3,
            ),
        ];
        for (i, &(ref ents, wconflict)) in tests.iter().enumerate() {
            let store = MemStorage::new();
            let mut raft_log = RaftLog::new(store, l.clone());
            raft_log.append(&previous_ents);
            let gconflict = raft_log.find_conflict(ents);
            if gconflict != wconflict {
                panic!("#{}: conflict = {}, want {}", i, gconflict, wconflict)
            }
        }
    }

    #[test]
    fn test_is_up_to_date() {
        let previous_ents = vec![new_entry(1, 1), new_entry(2, 2), new_entry(3, 3)];
        let store = MemStorage::new();
        let mut raft_log = RaftLog::new(store, default_logger());
        raft_log.append(&previous_ents);
        let tests = vec![
            // greater term, ignore lastIndex
            (raft_log.last_index() - 1, 4, true),
            (raft_log.last_index(), 4, true),
            (raft_log.last_index() + 1, 4, true),
            // smaller term, ignore lastIndex
            (raft_log.last_index() - 1, 2, false),
            (raft_log.last_index(), 2, false),
            (raft_log.last_index() + 1, 2, false),
            // equal term, lager lastIndex wins
            (raft_log.last_index() - 1, 3, false),
            (raft_log.last_index(), 3, true),
            (raft_log.last_index() + 1, 3, true),
        ];
        for (i, &(last_index, term, up_to_date)) in tests.iter().enumerate() {
            let g_up_to_date = raft_log.is_up_to_date(last_index, term);
            if g_up_to_date != up_to_date {
                panic!("#{}: uptodate = {}, want {}", i, g_up_to_date, up_to_date);
            }
        }
    }

    #[test]
    fn test_append() {
        let l = default_logger();
        let previous_ents = vec![new_entry(1, 1), new_entry(2, 2)];
        let tests = vec![
            (vec![], 2, vec![new_entry(1, 1), new_entry(2, 2)], 3),
            (
                vec![new_entry(3, 2)],
                3,
                vec![new_entry(1, 1), new_entry(2, 2), new_entry(3, 2)],
                3,
            ),
            // conflicts with index 1
            (vec![new_entry(1, 2)], 1, vec![new_entry(1, 2)], 1),
            // conflicts with index 2
            (
                vec![new_entry(2, 3), new_entry(3, 3)],
                3,
                vec![new_entry(1, 1), new_entry(2, 3), new_entry(3, 3)],
                2,
            ),
        ];
        for (i, &(ref ents, windex, ref wents, wunstable)) in tests.iter().enumerate() {
            let store = MemStorage::new();
            store.wl().append(&previous_ents).expect("append failed");
            let mut raft_log = RaftLog::new(store, l.clone());
            let index = raft_log.append(ents);
            if index != windex {
                panic!("#{}: last_index = {}, want {}", i, index, windex);
            }
            match raft_log.entries(1, None) {
                Err(e) => panic!("#{}: unexpected error {}", i, e),
                Ok(ref g) if g != wents => panic!("#{}: logEnts = {:?}, want {:?}", i, &g, &wents),
                _ => {
                    let goff = raft_log.unstable.offset;
                    if goff != wunstable {
                        panic!("#{}: unstable = {}, want {}", i, goff, wunstable);
                    }
                }
            }
        }
    }

    #[test]
    fn test_compaction_side_effects() {
        let last_index = 1000u64;
        let unstable_index = 750u64;
        let last_term = last_index;
        let storage = MemStorage::new();
        for i in 1..=unstable_index {
            storage
                .wl()
                .append(&[new_entry(i as u64, i as u64)])
                .expect("append failed");
        }
        let mut raft_log = RaftLog::new(storage, default_logger());
        for i in unstable_index..last_index {
            raft_log.append(&[new_entry(i as u64 + 1, i as u64 + 1)]);
        }
        assert!(
            raft_log.maybe_commit(last_index, last_term),
            "maybe_commit return false"
        );

        let offset = 500u64;
        raft_log.store.wl().compact(offset).expect("compact failed");

        assert_eq!(last_index, raft_log.last_index());

        for j in offset..=raft_log.last_index() {
            assert_eq!(j, raft_log.term(j).expect(""));
            if !raft_log.match_term(j, j) {
                panic!("match_term({}) = false, want true", j);
            }
        }

        {
            let unstable_ents = raft_log.unstable_entries();
            assert_eq!(last_index - unstable_index, unstable_ents.len() as u64);
            assert_eq!(unstable_index + 1, unstable_ents[0].index);
        }

        let mut prev = raft_log.last_index();
        raft_log.append(&[new_entry(prev + 1, prev + 1)]);
        assert_eq!(prev + 1, raft_log.last_index());

        prev = raft_log.last_index();
        let ents = raft_log.entries(prev, None).expect("unexpected error");
        assert_eq!(1, ents.len());
    }

    #[test]
    fn test_term_with_unstable_snapshot() {
        let storagesnapi = 10064;
        let unstablesnapi = storagesnapi + 5;
        let store = MemStorage::new();
        store
            .wl()
            .apply_snapshot(new_snapshot(storagesnapi, 1))
            .expect("apply failed.");
        let mut raft_log = RaftLog::new(store, default_logger());
        raft_log.restore(new_snapshot(unstablesnapi, 1));
        assert_eq!(raft_log.committed, unstablesnapi);
        assert_eq!(raft_log.persisted, storagesnapi);

        let tests = vec![
            // cannot get term from storage
            (storagesnapi, 0),
            // cannot get term from the gap between storage ents and unstable snapshot
            (storagesnapi + 1, 0),
            (unstablesnapi - 1, 0),
            // get term from unstable snapshot index
            (unstablesnapi, 1),
        ];

        for (i, &(index, w)) in tests.iter().enumerate() {
            let term = raft_log.term(index).expect("");
            if term != w {
                panic!("#{}: at = {}, want {}", i, term, w);
            }
        }
    }

    #[test]
    fn test_term() {
        let offset = 100u64;
        let num = 100u64;

        let store = MemStorage::new();
        store
            .wl()
            .apply_snapshot(new_snapshot(offset, 1))
            .expect("apply failed.");
        let mut raft_log = RaftLog::new(store, default_logger());
        for i in 1..num {
            raft_log.append(&[new_entry(offset + i, i)]);
        }

        let tests = vec![
            (offset - 1, 0),
            (offset, 1),
            (offset + num / 2, num / 2),
            (offset + num - 1, num - 1),
            (offset + num, 0),
        ];

        for (i, &(index, w)) in tests.iter().enumerate() {
            let term = raft_log.term(index).expect("");
            if term != w {
                panic!("#{}: at = {}, want {}", i, term, w);
            }
        }
    }

    #[test]
    fn test_log_restore() {
        let (index, term) = (1000u64, 1000u64);
        let store = MemStorage::new();
        store
            .wl()
            .apply_snapshot(new_snapshot(index, term))
            .expect("apply failed.");
        let entries = vec![new_entry(index + 1, term), new_entry(index + 2, term + 1)];
        store.wl().append(&entries).expect("");
        let raft_log = RaftLog::new(store, default_logger());

        assert_eq!(raft_log.all_entries(), entries);
        assert_eq!(index + 1, raft_log.first_index());
        assert_eq!(index, raft_log.committed);
        assert_eq!(index + 2, raft_log.persisted);
        assert_eq!(index + 3, raft_log.unstable.offset);

        assert_eq!(term, raft_log.term(index).unwrap());
        assert_eq!(term, raft_log.term(index + 1).unwrap());
        assert_eq!(term + 1, raft_log.term(index + 2).unwrap());
    }

    #[test]
    fn test_maybe_persist_with_snap() {
        let l = default_logger();
        let (snap_index, snap_term) = (5u64, 2u64);
        // persisted_index, persisted_term, new_entries, wpersisted
        let tests = vec![
            (snap_index + 1, snap_term, vec![], snap_index),
            (snap_index, snap_term, vec![], snap_index),
            (snap_index - 1, snap_term, vec![], snap_index),
            (snap_index + 1, snap_term + 1, vec![], snap_index),
            (snap_index, snap_term + 1, vec![], snap_index),
            (snap_index - 1, snap_term + 1, vec![], snap_index),
            (
                snap_index + 1,
                snap_term,
                vec![new_entry(snap_index + 1, snap_term)],
                snap_index + 1,
            ),
            (
                snap_index,
                snap_term,
                vec![new_entry(snap_index + 1, snap_term)],
                snap_index,
            ),
            (
                snap_index - 1,
                snap_term,
                vec![new_entry(snap_index + 1, snap_term)],
                snap_index,
            ),
            (
                snap_index + 1,
                snap_term + 1,
                vec![new_entry(snap_index + 1, snap_term)],
                snap_index,
            ),
            (
                snap_index,
                snap_term + 1,
                vec![new_entry(snap_index + 1, snap_term)],
                snap_index,
            ),
            (
                snap_index - 1,
                snap_term + 1,
                vec![new_entry(snap_index + 1, snap_term)],
                snap_index,
            ),
        ];

        for (i, &(stablei, stablet, ref new_ents, wpersist)) in tests.iter().enumerate() {
            let store = MemStorage::new();
            store
                .wl()
                .apply_snapshot(new_snapshot(snap_index, snap_term))
                .expect("");
            let mut raft_log = RaftLog::new(store, l.clone());
            assert_eq!(raft_log.persisted, snap_index);
            raft_log.append(new_ents);
            let unstable = raft_log.unstable_entries().to_vec();
            if let Some(e) = unstable.last() {
                raft_log.stable_entries(e.get_index(), e.get_term());
                raft_log.mut_store().wl().append(&unstable).expect("");
            }
            let is_changed = raft_log.persisted != wpersist;
            assert_eq!(raft_log.maybe_persist(stablei, stablet), is_changed);
            if raft_log.persisted != wpersist {
                panic!(
                    "#{}: persisted = {}, want {}",
                    i, raft_log.persisted, wpersist
                );
            }
        }

        let mut raft_log = RaftLog::new(MemStorage::new(), default_logger());
        raft_log.restore(new_snapshot(100, 1));
        assert_eq!(raft_log.unstable.offset, 101);
        raft_log.append(&[new_entry(101, 1)]);
        assert_eq!(raft_log.term(101), Ok(1));
        // 101 == offset, should not forward persisted
        assert!(!raft_log.maybe_persist(101, 1));
        raft_log.append(&[new_entry(102, 1)]);
        assert_eq!(raft_log.term(102), Ok(1));
        // 102 > offset, should not forward persisted
        assert!(!raft_log.maybe_persist(102, 1));
    }

    // TestUnstableEnts ensures unstableEntries returns the unstable part of the
    // entries correctly.
    #[test]
    fn test_unstable_ents() {
        let l = default_logger();
        let previous_ents = vec![new_entry(1, 1), new_entry(2, 2)];
        let tests = vec![(3, vec![]), (1, previous_ents.clone())];

        for (i, &(unstable, ref wents)) in tests.iter().enumerate() {
            // append stable entries to storage
            let store = MemStorage::new();
            store
                .wl()
                .append(&previous_ents[..(unstable - 1)])
                .expect("");

            // append unstable entries to raftlog
            let mut raft_log = RaftLog::new(store, l.clone());
            raft_log.append(&previous_ents[(unstable - 1)..]);

            let ents = raft_log.unstable_entries().to_vec();
            if let Some(e) = ents.last() {
                raft_log.stable_entries(e.get_index(), e.get_term());
            }
            if &ents != wents {
                panic!("#{}: unstableEnts = {:?}, want {:?}", i, ents, wents);
            }
            let w = previous_ents[previous_ents.len() - 1].index + 1;
            let g = raft_log.unstable.offset;
            if g != w {
                panic!("#{}: unstable = {}, want {}", i, g, w);
            }
        }
    }

    #[test]
    fn test_has_next_ents_and_next_ents() {
        let l = default_logger();
        let ents = [
            new_entry(4, 1),
            new_entry(5, 1),
            new_entry(6, 1),
            new_entry(7, 1),
        ];
        // applied, persisted, committed, expect_entries
        let tests = vec![
            (0, 3, 3, None),
            (0, 3, 4, None),
            (0, 4, 6, Some(&ents[..1])),
            (0, 6, 4, Some(&ents[..1])),
            (0, 5, 5, Some(&ents[..2])),
            (0, 5, 7, Some(&ents[..2])),
            (0, 7, 5, Some(&ents[..2])),
            (3, 4, 3, None),
            (3, 5, 5, Some(&ents[..2])),
            (3, 6, 7, Some(&ents[..3])),
            (3, 7, 6, Some(&ents[..3])),
            (4, 5, 5, Some(&ents[1..2])),
            (4, 5, 7, Some(&ents[1..2])),
            (4, 7, 5, Some(&ents[1..2])),
            (4, 7, 7, Some(&ents[1..4])),
            (5, 5, 5, None),
            (5, 7, 7, Some(&ents[2..4])),
            (7, 7, 7, None),
        ];
        for (i, &(applied, persisted, committed, ref expect_entries)) in tests.iter().enumerate() {
            let store = MemStorage::new();
            store.wl().apply_snapshot(new_snapshot(3, 1)).expect("");
            let mut raft_log = RaftLog::new(store, l.clone());
            raft_log.append(&ents);
            let unstable = raft_log.unstable_entries().to_vec();
            if let Some(e) = unstable.last() {
                raft_log.stable_entries(e.get_index(), e.get_term());
                raft_log.mut_store().wl().append(&unstable).expect("");
            }
            raft_log.maybe_persist(persisted, 1);
            assert_eq!(
                persisted, raft_log.persisted,
                "#{}: persisted = {}, want {}",
                i, raft_log.persisted, persisted
            );
            raft_log.maybe_commit(committed, 1);
            assert_eq!(
                committed, raft_log.committed,
                "#{}: committed = {}, want {}",
                i, raft_log.committed, committed
            );
            #[allow(deprecated)]
            raft_log.applied_to(applied);

            let expect_has_next = expect_entries.is_some();
            let actual_has_next = raft_log.has_next_entries();
            if actual_has_next != expect_has_next {
                panic!(
                    "#{}: hasNext = {}, want {}",
                    i, actual_has_next, expect_has_next
                );
            }

            let next_entries = raft_log.next_entries();
            if next_entries != expect_entries.map(|n| n.to_vec()) {
                panic!(
                    "#{}: next_entries = {:?}, want {:?}",
                    i, next_entries, expect_entries
                );
            }
        }
    }

    #[test]
    fn test_slice() {
        let (offset, num) = (100u64, 100u64);
        let (last, half) = (offset + num, offset + num / 2);
        let halfe = new_entry(half, half);

        let halfe_size = u64::from(halfe.compute_size());

        let store = MemStorage::new();
        store
            .wl()
            .apply_snapshot(new_snapshot(offset, 0))
            .expect("");
        for i in 1..(num / 2) {
            store
                .wl()
                .append(&[new_entry(offset + i, offset + i)])
                .expect("");
        }
        let mut raft_log = RaftLog::new(store, default_logger());
        for i in (num / 2)..num {
            raft_log.append(&[new_entry(offset + i, offset + i)]);
        }

        let tests = vec![
            // test no limit
            (offset - 1, offset + 1, raft_log::NO_LIMIT, vec![], false),
            (offset, offset + 1, raft_log::NO_LIMIT, vec![], false),
            (
                half - 1,
                half + 1,
                raft_log::NO_LIMIT,
                vec![new_entry(half - 1, half - 1), new_entry(half, half)],
                false,
            ),
            (
                half,
                half + 1,
                raft_log::NO_LIMIT,
                vec![new_entry(half, half)],
                false,
            ),
            (
                last - 1,
                last,
                raft_log::NO_LIMIT,
                vec![new_entry(last - 1, last - 1)],
                false,
            ),
            (last, last + 1, raft_log::NO_LIMIT, vec![], true),
            // test limit
            (
                half - 1,
                half + 1,
                0,
                vec![new_entry(half - 1, half - 1)],
                false,
            ),
            (
                half - 1,
                half + 1,
                halfe_size + 1,
                vec![new_entry(half - 1, half - 1)],
                false,
            ),
            (
                half - 2,
                half + 1,
                halfe_size + 1,
                vec![new_entry(half - 2, half - 2)],
                false,
            ),
            (
                half - 1,
                half + 1,
                halfe_size * 2,
                vec![new_entry(half - 1, half - 1), new_entry(half, half)],
                false,
            ),
            (
                half - 1,
                half + 2,
                halfe_size * 3,
                vec![
                    new_entry(half - 1, half - 1),
                    new_entry(half, half),
                    new_entry(half + 1, half + 1),
                ],
                false,
            ),
            (
                half,
                half + 2,
                halfe_size,
                vec![new_entry(half, half)],
                false,
            ),
            (
                half,
                half + 2,
                halfe_size * 2,
                vec![new_entry(half, half), new_entry(half + 1, half + 1)],
                false,
            ),
        ];

        for (i, &(from, to, limit, ref w, wpanic)) in tests.iter().enumerate() {
            let res =
                panic::catch_unwind(AssertUnwindSafe(|| raft_log.slice(from, to, Some(limit))));
            if res.is_err() ^ wpanic {
                panic!("#{}: panic = {}, want {}: {:?}", i, true, false, res);
            }
            if res.is_err() {
                continue;
            }
            let slice_res = res.unwrap();
            if from <= offset && slice_res != Err(Error::Store(StorageError::Compacted)) {
                let err = slice_res.err();
                panic!("#{}: err = {:?}, want {}", i, err, StorageError::Compacted);
            }
            if from > offset && slice_res.is_err() {
                panic!("#{}: unexpected error {}", i, slice_res.unwrap_err());
            }
            if let Ok(ref g) = slice_res {
                if g != w {
                    panic!("#{}: from {} to {} = {:?}, want {:?}", i, from, to, g, w);
                }
            }
        }
    }

    /// `test_log_maybe_append` ensures:
    /// If the given (index, term) matches with the existing log:
    ///     1. If an existing entry conflicts with a new one (same index
    ///     but different terms), delete the existing entry and all that
    ///     follow it and decrease the persisted
    ///     2. Append any new entries not already in the log
    /// If the given (index, term) does not match with the existing log:
    ///     return false
    #[test]
    fn test_log_maybe_append() {
        let l = default_logger();
        let previous_ents = vec![new_entry(1, 1), new_entry(2, 2), new_entry(3, 3)];
        let (last_index, last_term, commit, persist) = (3u64, 3u64, 1u64, 3u64);

        let tests = vec![
            // not match: term is different
            (
                last_term - 1,
                last_index,
                last_index,
                vec![new_entry(last_index + 1, 4)],
                None,
                commit,
                persist,
                false,
            ),
            // not match: index out of bound
            (
                last_term,
                last_index + 1,
                last_index,
                vec![new_entry(last_index + 2, 4)],
                None,
                commit,
                persist,
                false,
            ),
            // match with the last existing entry
            (
                last_term,
                last_index,
                last_index,
                vec![],
                Some(last_index),
                last_index,
                persist,
                false,
            ),
            // do not increase commit higher than lastnewi
            (
                last_term,
                last_index,
                last_index + 1,
                vec![],
                Some(last_index),
                last_index,
                persist,
                false,
            ),
            // commit up to the commit in the message
            (
                last_term,
                last_index,
                last_index - 1,
                vec![],
                Some(last_index),
                last_index - 1,
                persist,
                false,
            ),
            // commit do not decrease
            (
                last_term,
                last_index,
                0,
                vec![],
                Some(last_index),
                commit,
                persist,
                false,
            ),
            // commit do not decrease
            (0, 0, last_index, vec![], Some(0), commit, persist, false),
            (
                last_term,
                last_index,
                last_index,
                vec![new_entry(last_index + 1, 4)],
                Some(last_index + 1),
                last_index,
                persist,
                false,
            ),
            (
                last_term,
                last_index,
                last_index + 1,
                vec![new_entry(last_index + 1, 4)],
                Some(last_index + 1),
                last_index + 1,
                persist,
                false,
            ),
            // do not increase commit higher than lastnewi
            (
                last_term,
                last_index,
                last_index + 2,
                vec![new_entry(last_index + 1, 4)],
                Some(last_index + 1),
                last_index + 1,
                persist,
                false,
            ),
            (
                last_term,
                last_index,
                last_index + 2,
                vec![new_entry(last_index + 1, 4), new_entry(last_index + 2, 4)],
                Some(last_index + 2),
                last_index + 2,
                persist,
                false,
            ),
            // match with the the entry in the middle
            (
                last_term - 1,
                last_index - 1,
                last_index,
                vec![new_entry(last_index, 4)],
                Some(last_index),
                last_index,
                cmp::min(last_index - 1, persist),
                false,
            ),
            (
                last_term - 2,
                last_index - 2,
                last_index,
                vec![new_entry(last_index - 1, 4)],
                Some(last_index - 1),
                last_index - 1,
                cmp::min(last_index - 2, persist),
                false,
            ),
            // conflict with existing committed entry
            (
                last_term - 3,
                last_index - 3,
                last_index,
                vec![new_entry(last_index - 2, 4)],
                Some(last_index - 2),
                last_index - 2,
                cmp::min(last_index - 3, persist),
                true,
            ),
            (
                last_term - 2,
                last_index - 2,
                last_index,
                vec![new_entry(last_index - 1, 4), new_entry(last_index, 4)],
                Some(last_index),
                last_index,
                cmp::min(last_index - 2, persist),
                false,
            ),
            (
                last_term - 2,
                last_index - 2,
                last_index + 2,
                vec![
                    new_entry(last_index - 1, last_term - 1),
                    new_entry(last_index, 4),
                    new_entry(last_index + 1, 4),
                ],
                Some(last_index + 1),
                last_index + 1,
                cmp::min(last_index - 1, persist),
                false,
            ),
        ];

        for (i, &(log_term, index, committed, ref ents, wlasti, wcommit, wpersist, wpanic)) in
            tests.iter().enumerate()
        {
            let store = MemStorage::new();
            let mut raft_log = RaftLog::new(store, l.clone());
            raft_log.append(&previous_ents);
            raft_log.committed = commit;
            raft_log.persisted = persist;
            let res = panic::catch_unwind(AssertUnwindSafe(|| {
                raft_log
                    .maybe_append(index, log_term, committed, ents)
                    .map(|(_, last_idx)| last_idx)
            }));
            if res.is_err() ^ wpanic {
                panic!("#{}: panic = {}, want {}", i, res.is_err(), wpanic);
            }
            if res.is_err() {
                continue;
            }
            let glasti = res.unwrap();
            let gcommitted = raft_log.committed;
            let gpersisted = raft_log.persisted;
            if glasti != wlasti {
                panic!("#{}: lastindex = {:?}, want {:?}", i, glasti, wlasti);
            }
            if gcommitted != wcommit {
                panic!("#{}: committed = {}, want {}", i, gcommitted, wcommit);
            }
            if gpersisted != wpersist {
                panic!("#{}: persisted = {}, want {}", i, gpersisted, wpersist);
            }
            let ents_len = ents.len() as u64;
            if glasti.is_some() && ents_len != 0 {
                let (from, to) = (
                    raft_log.last_index() - ents_len + 1,
                    raft_log.last_index() + 1,
                );
                let gents = raft_log.slice(from, to, None).expect("");
                if &gents != ents {
                    panic!("#{}: appended entries = {:?}, want {:?}", i, gents, ents);
                }
            }
        }
    }

    #[test]
    fn test_commit_to() {
        let l = default_logger();
        let previous_ents = vec![new_entry(1, 1), new_entry(2, 2), new_entry(3, 3)];
        let previous_commit = 2u64;
        let tests = vec![
            (3, 3, false),
            (1, 2, false), // never decrease
            (4, 0, true),  // commit out of range -> panic
        ];
        for (i, &(commit, wcommit, wpanic)) in tests.iter().enumerate() {
            let store = MemStorage::new();
            let mut raft_log = RaftLog::new(store, l.clone());
            raft_log.append(&previous_ents);
            raft_log.committed = previous_commit;
            let has_panic =
                panic::catch_unwind(AssertUnwindSafe(|| raft_log.commit_to(commit))).is_err();
            if has_panic ^ wpanic {
                panic!("#{}: panic = {}, want {}", i, has_panic, wpanic)
            }
            if !has_panic && raft_log.committed != wcommit {
                let actual_committed = raft_log.committed;
                panic!("#{}: committed = {}, want {}", i, actual_committed, wcommit);
            }
        }
    }

    // TestCompaction ensures that the number of log entries is correct after compactions.
    #[test]
    fn test_compaction() {
        let l = default_logger();
        let tests = vec![
            // out of upper bound
            (1000, vec![1001u64], vec![0usize], true),
            (
                1000,
                vec![300, 500, 800, 900],
                vec![700, 500, 200, 100],
                false,
            ),
            // out of lower bound
            (1000, vec![300, 299], vec![700, 700], false),
        ];

        for (i, &(index, ref compact, ref wleft, should_panic)) in tests.iter().enumerate() {
            let store = MemStorage::new();
            for i in 1u64..index {
                store.wl().append(&[new_entry(i, 0)]).expect("");
            }
            let mut raft_log = RaftLog::new(store, l.clone());
            raft_log.maybe_commit(index - 1, 0);
            let committed = raft_log.committed;
            #[allow(deprecated)]
            raft_log.applied_to(committed);

            for (j, idx) in compact.iter().enumerate() {
                let res =
                    panic::catch_unwind(AssertUnwindSafe(|| raft_log.store.wl().compact(*idx)));
                if !(should_panic ^ res.is_ok()) {
                    panic!("#{}: should_panic: {}, but got: {:?}", i, should_panic, res);
                }
                if !should_panic {
                    let l = raft_log.all_entries().len();
                    if l != wleft[j] {
                        panic!("#{}.{} len = {}, want {}", i, j, l, wleft[j]);
                    }
                }
            }
        }
    }

    #[test]
    fn test_is_outofbounds() {
        let (offset, num) = (100u64, 100u64);
        let store = MemStorage::new();
        store
            .wl()
            .apply_snapshot(new_snapshot(offset, 0))
            .expect("");
        let mut raft_log = RaftLog::new(store, default_logger());
        for i in 1u64..=num {
            raft_log.append(&[new_entry(i + offset, 0)]);
        }
        let first = offset + 1;
        let tests = vec![
            (first - 2, first + 1, false, true),
            (first - 1, first + 1, false, true),
            (first, first, false, false),
            (first + num / 2, first + num / 2, false, false),
            (first + num - 1, first + num - 1, false, false),
            (first + num, first + num, false, false),
            (first + num, first + num + 1, true, false),
            (first + num + 1, first + num + 1, true, false),
        ];

        for (i, &(lo, hi, wpanic, w_err_compacted)) in tests.iter().enumerate() {
            let res =
                panic::catch_unwind(AssertUnwindSafe(|| raft_log.must_check_outofbounds(lo, hi)));
            if res.is_err() ^ wpanic {
                panic!(
                    "#{}: panic = {}, want {}: {:?}",
                    i,
                    res.is_err(),
                    wpanic,
                    res
                );
            }
            if res.is_err() {
                continue;
            }
            let check_res = res.unwrap();
            if w_err_compacted && check_res != Some(Error::Store(StorageError::Compacted)) {
                panic!(
                    "#{}: err = {:?}, want {}",
                    i,
                    check_res,
                    StorageError::Compacted
                );
            }
            if !w_err_compacted && check_res.is_some() {
                panic!("#{}: unexpected err {:?}", i, check_res)
            }
        }
    }

    #[test]
    fn test_restore_snap() {
        let store = MemStorage::new();
        store.wl().apply_snapshot(new_snapshot(100, 1)).expect("");
        let mut raft_log = RaftLog::new(store, default_logger());
        assert_eq!(raft_log.committed, 100);
        assert_eq!(raft_log.persisted, 100);
        raft_log.restore(new_snapshot(200, 1));
        assert_eq!(raft_log.committed, 200);
        assert_eq!(raft_log.persisted, 100);

        for i in 201..210 {
            raft_log.append(&[new_entry(i, 1)]);
        }
        raft_log
            .mut_store()
            .wl()
            .apply_snapshot(new_snapshot(200, 1))
            .expect("");
        raft_log.stable_snap(200);
        let unstable = raft_log.unstable_entries().to_vec();
        raft_log.stable_entries(209, 1);
        raft_log.mut_store().wl().append(&unstable).expect("");
        raft_log.maybe_persist(209, 1);
        assert_eq!(raft_log.persisted, 209);

        raft_log.restore(new_snapshot(205, 1));
        assert_eq!(raft_log.committed, 205);
        // persisted should reset to previous commit index(200)
        assert_eq!(raft_log.persisted, 200);

        // use smaller commit index, should panic
        assert!(
            panic::catch_unwind(AssertUnwindSafe(|| raft_log.restore(new_snapshot(204, 1))))
                .is_err()
        );
    }
}
