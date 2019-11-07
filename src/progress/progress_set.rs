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

use std::cell::RefCell;
use std::{cmp, fmt, iter, slice, u64};

use slog::Logger;

use crate::eraftpb::{ConfState, SnapshotMetadata};
use crate::errors::{Error, Result};
use crate::progress::Progress;
use crate::{HashMap, HashSet};

/// Get the majority number of given nodes count.
#[inline]
pub fn majority(total: usize) -> usize {
    (total / 2) + 1
}

/// A Raft internal representation of a Configuration.
///
/// This is corollary to a ConfState, but optimized for `contains` calls.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct Configuration {
    auto_leave: bool,

    // Sorted voters. Only the first is valid if it's not in joint.
    // Otherwise the first is incoming and the second is outgoing.
    voters: [Vec<u64>; 2],

    // Sorted learners. Shouldn't intersect with `voters`.
    learners: Vec<u64>,

    // Demoted learners in joint consensus, because `learners` shouldn't intersect with `voters`.
    learners_next: Vec<u64>,
}

impl From<ConfState> for Configuration {
    fn from(mut c: ConfState) -> Self {
        let mut configuration = Self {
            auto_leave: c.get_auto_leave(),
            voters: [c.take_voters(), c.take_voters_outgoing()],
            learners: c.take_learners(),
            learners_next: c.take_learners_next(),
        };
        configuration.voters[0].sort();
        configuration.voters[1].sort();
        configuration.learners.sort();
        configuration.learners_next.sort();
        configuration
    }
}

impl From<Configuration> for ConfState {
    fn from(c: Configuration) -> Self {
        let mut state = ConfState::default();
        state.set_auto_leave(c.auto_leave);
        state.set_voters(c.voters[0].clone());
        state.set_voters_outgoing(c.voters[1].clone());
        state.set_learners(c.learners.clone());
        state.set_learners_next(c.learners_next.clone());
        state
    }
}

impl Configuration {
    /// Create a new `ConfState` from the configuration itself.
    pub fn to_conf_state(&self) -> ConfState {
        self.clone().into()
    }

    /// Create a new `Configuration` from a given `ConfState`.
    pub fn from_conf_state(conf_state: &ConfState) -> Self {
        Self::from(conf_state.clone())
    }

    // Test the configuration is valid or not. It's invalid when
    // 1. `learners` or `learners_next` intersects with `voters`;
    // 2. `learners_next` isn't a subset of `voters[1]`;
    fn valid(&self) -> bool {
        fn find_equal(s1: &[u64], s2: &[u64]) -> bool {
            let (mut i, mut j) = (0, 0);
            while i < s1.len() && j < s2.len() {
                match s1[i].cmp(&s2[j]) {
                    cmp::Ordering::Equal => return true,
                    cmp::Ordering::Less => i += 1,
                    cmp::Ordering::Greater => j += 1,
                }
            }
            false
        }

        if find_equal(&self.voters[0], &self.learners)
            || find_equal(&self.voters[1], &self.learners)
            || find_equal(&self.voters[0], &self.learners_next)
        {
            return false;
        }
        self.learners_next
            .iter()
            .all(|l| self.voters[1].binary_search(l).is_ok())
    }

    fn has_quorum(&self, potential: &[u64]) -> bool {
        for cfg in &self.voters {
            if cfg.is_empty() {
                continue;
            }
            let c = potential.iter().filter(|p| cfg.binary_search(p).is_ok());
            if c.count() < majority(cfg.len()) {
                return false;
            }
        }
        true
    }
}

/// The status of an election according to a Candidate node.
///
/// This is returned by `progress_set.election_status(vote_map)`
#[derive(Clone, Copy, Debug)]
pub enum CandidacyStatus {
    /// The election has been won by this Raft.
    Elected,
    /// It is still possible to win the election.
    Eligible,
    /// It is no longer possible to win the election.
    Ineligible,
}

/// `ProgressSet` contains several `Progress`es,
/// which could be `Leader`, `Follower` and `Learner`.
#[derive(Clone, Getters)]
pub struct ProgressSet {
    progress: HashMap<u64, Progress>,
    configuration: Configuration,

    // A preallocated buffer for sorting in the maximal_committed_index function.
    // You should not depend on these values unless you just set them.
    // We use a cell to avoid taking a `&mut self`.
    sort_buffer: RefCell<Vec<u64>>,
    logger: Logger,
}

impl ProgressSet {
    /// Create a new progress set.
    pub fn new(logger: Logger) -> Self {
        ProgressSet {
            progress: Default::default(),
            configuration: Default::default(),
            sort_buffer: Default::default(),
            logger,
        }
    }

    pub(crate) fn restore_snapmeta(
        &mut self,
        meta: &SnapshotMetadata,
        next_idx: u64,
        max_inflight: usize,
    ) {
        self.restore_conf_state(meta.get_conf_state(), next_idx, max_inflight);
    }

    /// Restore a progress set from `conf_state`.
    pub fn restore_conf_state(
        &mut self,
        conf_state: &ConfState,
        next_idx: u64,
        max_inflight: usize,
    ) {
        self.configuration = Configuration::from_conf_state(conf_state);
        let pr = Progress::new(next_idx, max_inflight);
        let mut prs = HashMap::default();
        for id in self.voters().chain(self.learners()) {
            prs.insert(id, pr.clone());
        }
        self.progress = prs;
        self.assert_progress_and_configuration_consistent();
    }

    /// Returns the ids of all known voters.
    ///
    /// **Note:** Do not use this for majority/quorum calculation. The Raft node may be
    /// transitioning to a new configuration and have two qourums. Use `has_quorum` instead.
    #[inline]
    pub fn voter_ids(&self) -> HashSet<u64> {
        self.voters().collect()
    }

    /// Returns the ids of all known learners.
    ///
    /// **Note:** Do not use this for majority/quorum calculation. The Raft node may be
    /// transitioning to a new configuration and have two qourums. Use `has_quorum` instead.
    #[inline]
    pub fn learner_ids(&self) -> HashSet<u64> {
        self.learners().collect()
    }

    /// Grabs a reference to the progress of a node.
    #[inline]
    pub fn get(&self, id: u64) -> Option<&Progress> {
        self.progress.get(&id)
    }

    /// Grabs a mutable reference to the progress of a node.
    #[inline]
    pub fn get_mut(&mut self, id: u64) -> Option<&mut Progress> {
        self.progress.get_mut(&id)
    }

    /// Returns an iterator across all the nodes and their progress.
    ///
    /// **Note:** Do not use this for majority/quorum calculation. The Raft node may be
    /// transitioning to a new configuration and have two qourums. Use `has_quorum` instead.
    #[inline]
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (&u64, &Progress)> {
        self.progress.iter()
    }

    /// Returns a mutable iterator across all the nodes and their progress.
    ///
    /// **Note:** Do not use this for majority/quorum calculation. The Raft node may be
    /// transitioning to a new configuration and have two qourums. Use `has_quorum` instead.
    #[inline]
    pub fn iter_mut(&mut self) -> impl ExactSizeIterator<Item = (&u64, &mut Progress)> {
        self.progress.iter_mut()
    }

    /// Adds a voter to the group.
    ///
    /// # Errors
    ///
    /// * `id` is in the voter set.
    /// * `id` is in the learner set.
    pub fn insert_voter(&mut self, id: u64, pr: Progress) -> Result<()> {
        debug!(self.logger, "Inserting voter with id {id}", id = id);

        if self.learner_ids().contains(&id) {
            return Err(Error::Exists(id, "learners"));
        } else if self.voter_ids().contains(&id) {
            return Err(Error::Exists(id, "voters"));
        }

        self.configuration.voters[0].push(id);
        self.configuration.voters[0].sort();
        self.progress.insert(id, pr);
        self.assert_progress_and_configuration_consistent();
        Ok(())
    }

    /// Adds a learner to the group.
    ///
    /// # Errors
    ///
    /// * `id` is in the voter set.
    /// * `id` is in the learner set.
    pub fn insert_learner(&mut self, id: u64, pr: Progress) -> Result<()> {
        debug!(self.logger, "Inserting learner with id {id}", id = id);

        if self.learner_ids().contains(&id) {
            return Err(Error::Exists(id, "learners"));
        } else if self.voter_ids().contains(&id) {
            return Err(Error::Exists(id, "voters"));
        }

        self.configuration.learners.push(id);
        self.configuration.learners.sort();
        self.progress.insert(id, pr);
        self.assert_progress_and_configuration_consistent();
        Ok(())
    }

    /// Removes the peer from the set of voters or learners.
    ///
    /// # Errors
    ///
    pub fn remove(&mut self, id: u64) -> Result<Option<Progress>> {
        debug!(self.logger, "Removing peer with id {id}", id = id);
        if let Ok(pos) = self.configuration.voters[0].binary_search(&id) {
            self.configuration.voters[0].swap_remove(pos);
            self.configuration.voters[0].sort();
        }
        if let Ok(pos) = self.configuration.learners.binary_search(&id) {
            self.configuration.learners.swap_remove(pos);
            self.configuration.learners.sort();
        }
        let removed = self.progress.remove(&id);
        self.assert_progress_and_configuration_consistent();
        Ok(removed)
    }

    /// Promote a learner to a peer.
    pub fn promote_learner(&mut self, id: u64) -> Result<()> {
        debug!(self.logger, "Promoting peer with id {id}", id = id);

        match self.configuration.learners.binary_search(&id) {
            Ok(pos) => {
                self.configuration.learners.swap_remove(pos);
                self.configuration.learners.sort();
            }
            Err(_) => {
                // Wasn't already a learner. We can't promote what doesn't exist.
                return Err(Error::NotExists(id, "learners"));
            }
        }

        self.configuration.voters[0].push(id);
        self.configuration.voters[0].sort();
        self.assert_progress_and_configuration_consistent();
        Ok(())
    }

    /// Returns the maximal committed index for the cluster.
    ///
    /// Eg. If the matched indexes are [2,2,2,4,5], it will return 2.
    pub fn maximal_committed_index(&self) -> u64 {
        let mut matched = self.sort_buffer.borrow_mut();
        let mut committed = u64::MAX;
        for cfg in &self.configuration.voters {
            if !cfg.is_empty() {
                matched.clear();
                for id in cfg {
                    matched.push(self.progress[id].matched);
                }
                matched.sort_by(|a, b| b.cmp(a));
                committed = cmp::min(committed, matched[matched.len() / 2]);
            }
        }
        committed
    }

    /// Returns the Candidate's eligibility in the current election.
    ///
    /// If it is still eligible, it should continue polling nodes and checking.
    /// Eventually, the election will result in this returning either `Elected`
    /// or `Ineligible`, meaning the election can be concluded.
    pub fn candidacy_status(&self, votes: &HashMap<u64, bool>) -> CandidacyStatus {
        let mut accepts = Vec::with_capacity(votes.len());
        let mut rejects = Vec::with_capacity(votes.len());
        for (id, vote) in votes {
            if *vote {
                accepts.push(*id);
            } else {
                rejects.push(*id);
            }
        }

        if self.configuration.has_quorum(&accepts) {
            return CandidacyStatus::Elected;
        } else if self.configuration.has_quorum(&rejects) {
            return CandidacyStatus::Ineligible;
        }
        CandidacyStatus::Eligible
    }

    /// Determines if the current quorum is active according to the this raft node.
    /// Doing this will set the `recent_active` of each peer to false.
    ///
    /// This should only be called by the leader.
    pub fn quorum_recently_active(&mut self, perspective_of: u64) -> bool {
        let mut active = Vec::with_capacity(self.progress.len());
        for (id, pr) in &mut self.progress {
            if *id == perspective_of {
                active.push(*id);
                continue;
            }
            if pr.recent_active {
                active.push(*id);
            }
        }
        for pr in self.progress.values_mut() {
            pr.recent_active = false;
        }
        self.configuration.has_quorum(&active)
    }

    /// Determine if a quorum is formed from the given set of nodes.
    ///
    /// This is the only correct way to verify you have reached a quorum for the whole group.
    pub fn has_quorum(&self, potential: &[u64]) -> bool {
        self.configuration.has_quorum(potential)
    }

    /// Transform self to `ConfState`.
    pub fn to_conf_state(&self) -> ConfState {
        self.configuration.to_conf_state()
    }

    #[inline(always)]
    fn assert_progress_and_configuration_consistent(&self) {
        debug_assert!(self.configuration.valid());
        debug_assert!(self.progress.len() == self.voters().count() + self.learners().count());
    }

    pub(crate) fn promotable(&self, id: u64) -> bool {
        !self.progress.is_empty() && self.voters().any(|p| p == id)
    }
}

impl fmt::Debug for ProgressSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.configuration.fmt(f)
    }
}

impl<'a> ProgressSet {
    /// Create an iterator over all nodes which can send vote messages.
    pub fn voters(&'a self) -> VotersIter<'a> {
        VotersIter {
            incoming: self.configuration.voters[0].iter().peekable(),
            outgoing: self.configuration.voters[1].iter().peekable(),
        }
    }

    /// Create an iterator over all nodes which can't send vote messages.
    pub fn learners(&'a self) -> impl Iterator<Item = u64> + 'a {
        self.configuration.learners.iter().cloned()
    }
}

pub struct VotersIter<'a> {
    incoming: iter::Peekable<slice::Iter<'a, u64>>,
    outgoing: iter::Peekable<slice::Iter<'a, u64>>,
}

impl<'a> Iterator for VotersIter<'a> {
    type Item = u64;
    fn next(&mut self) -> Option<Self::Item> {
        match (self.incoming.peek(), self.outgoing.peek()) {
            (Some(v1), Some(v2)) => match v1.cmp(v2) {
                cmp::Ordering::Equal => {
                    self.incoming.next();
                    self.outgoing.next().cloned()
                }
                cmp::Ordering::Less => self.incoming.next().cloned(),
                cmp::Ordering::Greater => self.outgoing.next().cloned(),
            },
            (Some(_), None) => self.incoming.next().cloned(),
            (None, Some(_)) => self.outgoing.next().cloned(),
            _ => None,
        }
    }
}

// TODO: Reorganize this whole file into separate files.
// See https://github.com/pingcap/raft-rs/issues/125
#[cfg(test)]
mod test_progress_set {
    use super::{ProgressSet, Result};
    use crate::default_logger;
    use crate::progress::Progress;

    const CANARY: u64 = 123;

    #[test]
    fn test_insert_redundant_voter() -> Result<()> {
        let mut set = ProgressSet::new(default_logger());
        let default_progress = Progress::new(0, 256);
        let mut canary_progress = Progress::new(0, 256);
        canary_progress.matched = CANARY;
        set.insert_voter(1, default_progress.clone())?;
        assert!(
            set.insert_voter(1, canary_progress).is_err(),
            "Should return an error on redundant insert."
        );
        assert_eq!(
            *set.get(1).expect("Should be inserted."),
            default_progress,
            "The ProgressSet was mutated in a `insert_voter` that returned error."
        );
        Ok(())
    }

    #[test]
    fn test_insert_redundant_learner() -> Result<()> {
        let mut set = ProgressSet::new(default_logger());
        let default_progress = Progress::new(0, 256);
        let mut canary_progress = Progress::new(0, 256);
        canary_progress.matched = CANARY;
        set.insert_learner(1, default_progress.clone())?;
        assert!(
            set.insert_learner(1, canary_progress).is_err(),
            "Should return an error on redundant insert."
        );
        assert_eq!(
            *set.get(1).expect("Should be inserted."),
            default_progress,
            "The ProgressSet was mutated in a `insert_learner` that returned error."
        );
        Ok(())
    }

    #[test]
    fn test_insert_learner_that_is_voter() -> Result<()> {
        let mut set = ProgressSet::new(default_logger());
        let default_progress = Progress::new(0, 256);
        let mut canary_progress = Progress::new(0, 256);
        canary_progress.matched = CANARY;
        set.insert_voter(1, default_progress.clone())?;
        assert!(
            set.insert_learner(1, canary_progress).is_err(),
            "Should return an error on invalid learner insert."
        );
        assert_eq!(
            *set.get(1).expect("Should be inserted."),
            default_progress,
            "The ProgressSet was mutated in a `insert_learner` that returned error."
        );
        Ok(())
    }

    #[test]
    fn test_insert_voter_that_is_learner() -> Result<()> {
        let mut set = ProgressSet::new(default_logger());
        let default_progress = Progress::new(0, 256);
        let mut canary_progress = Progress::new(0, 256);
        canary_progress.matched = CANARY;
        set.insert_learner(1, default_progress.clone())?;
        assert!(
            set.insert_voter(1, canary_progress).is_err(),
            "Should return an error on invalid voter insert."
        );
        assert_eq!(
            *set.get(1).expect("Should be inserted."),
            default_progress,
            "The ProgressSet was mutated in a `insert_voter` that returned error."
        );
        Ok(())
    }

    #[test]
    fn test_promote_learner() -> Result<()> {
        let mut set = ProgressSet::new(default_logger());
        let default_progress = Progress::new(0, 256);
        set.insert_voter(1, default_progress)?;
        let pre = set.get(1).expect("Should have been inserted").clone();
        assert!(
            set.promote_learner(1).is_err(),
            "Should return an error on invalid promote_learner."
        );
        assert!(
            set.promote_learner(2).is_err(),
            "Should return an error on invalid promote_learner."
        );
        assert_eq!(pre, *set.get(1).expect("Peer should not have been deleted"));
        Ok(())
    }
}
