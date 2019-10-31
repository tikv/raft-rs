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
use std::{cmp, fmt, iter, mem, slice, u64};

use slog::Logger;

use crate::eraftpb::{ConfState, SnapshotMetadata};
use crate::errors::{Error, Result};
use crate::progress::Progress;
use crate::HashMap;

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

    fn len(&self) -> usize {
        self.voters[0].len() + self.voters[1].len() + self.learners.len()
    }

    pub(crate) fn enter_joint(&mut self) -> Result<()> {
        if !self.voters[1].is_empty() {
            return Err(Error::AlreadyInJoint);
        }
        self.voters[1] = self.voters[0].clone();
        Ok(())
    }

    /// Add `id` as a voter into the configuration, or promote it from learner.
    ///
    /// If any error occurs, `self` won't be touched.
    pub(crate) fn make_voter(&mut self, id: u64) -> Result<()> {
        if self.voters[0].binary_search(&id).is_ok() {
            return Err(Error::Exists(id, "voters"));
        }
        if let Ok(pos) = self.learners.binary_search(&id) {
            self.learners.swap_remove(pos);
            self.learners.sort();
        } else if let Ok(pos) = self.learners_next.binary_search(&id) {
            self.learners_next.swap_remove(pos);
            self.learners_next.sort();
        }
        self.voters[0].push(id);
        self.voters[0].sort();
        Ok(())
    }

    /// Add `id` as a learner into the configuration.
    ///
    /// If any error occurs, `self` won't be touched.
    pub(crate) fn make_learner(&mut self, id: u64) -> Result<()> {
        if self.voters[0].binary_search(&id).is_ok() {
            return Err(Error::Exists(id, "voters"));
        }
        if self.learners.binary_search(&id).is_ok() {
            return Err(Error::Exists(id, "learners"));
        }
        if self.learners_next.binary_search(&id).is_ok() {
            return Err(Error::Exists(id, "learners_next"));
        }
        if self.voters[1].binary_search(&id).is_ok() {
            // It's demoted in the current joint consensus.
            self.learners_next.push(id);
            self.learners_next.sort();
        } else {
            self.learners.push(id);
            self.learners.sort();
        }
        Ok(())
    }

    /// Remove `id` from the configuration.
    ///
    /// If any error occurs, `self` won't be touched.
    pub(crate) fn remove_peer(&mut self, id: u64) -> Result<()> {
        if let Ok(pos) = self.voters[0].binary_search(&id) {
            self.voters[0].swap_remove(pos);
            self.voters[0].sort();
            return Ok(());
        }
        if let Ok(pos) = self.learners.binary_search(&id) {
            self.learners.swap_remove(pos);
            self.learners.sort();
            return Ok(());
        }
        if let Ok(pos) = self.learners_next.binary_search(&id) {
            self.learners_next.swap_remove(pos);
            self.learners_next.sort();
            return Ok(());
        }
        Err(Error::NotExists(id, "voters/learners/learners_next"))
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
    pub(crate) fn new(logger: Logger) -> Self {
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

    pub(crate) fn restore_conf_state(
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
        if self.learners().any(|p| p == id) {
            return Err(Error::Exists(id, "learners"));
        } else if self.voters().any(|p| p == id) {
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
        if self.learners().any(|p| p == id) {
            return Err(Error::Exists(id, "learners"));
        } else if self.voters().any(|p| p == id) {
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
    /// * There is a pending membership change.
    pub fn remove(&mut self, id: u64) -> Result<Option<Progress>> {
        debug!(self.logger, "Removing peer with id {id}", id = id);
        for i in 0..=1 {
            if let Ok(pos) = self.configuration.voters[i].binary_search(&id) {
                self.configuration.voters[i].swap_remove(pos);
                self.configuration.voters[i].sort();
                // TODO: handle learners_next. we don't have demotion!
            }
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
            Err(_) => return Err(Error::NotExists(id, "learners")),
            Ok(pos) => {
                self.configuration.learners.swap_remove(pos);
                self.configuration.learners.sort();
            }
        }
        self.configuration.voters[0].push(id);
        self.configuration.voters[0].sort();
        self.assert_progress_and_configuration_consistent();
        Ok(())
    }

    #[inline(always)]
    fn assert_progress_and_configuration_consistent(&self) {
        debug_assert!(self.configuration.valid());
        debug_assert!(self.progress.len() == self.configuration.len());
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
                pr.recent_active = false;
            }
        }
        self.configuration.has_quorum(&active)
    }

    /// Determines if the current quorum is active according to the this raft node.
    pub fn has_quorum(&self, potential: &[u64]) -> bool {
        self.configuration.has_quorum(potential)
    }

    /// Transform self to `ConfState`.
    pub fn to_conf_state(&self) -> ConfState {
        self.configuration.to_conf_state()
    }

    /// Clone the current `Configuration`, and make it enter joint.
    pub(crate) fn joint_configuration(&self, auto_leave: bool) -> Result<Configuration> {
        let mut c = self.configuration.clone();
        c.enter_joint()?;
        c.auto_leave = auto_leave;
        Ok(c)
    }

    /// Clone the current `Configuration`.
    pub(crate) fn clone_configuration(&self) -> Configuration {
        self.configuration.clone()
    }

    pub(crate) fn leave_joint(&mut self) -> Result<()> {
        if self.configuration.voters[1].is_empty() {
            return Err(Error::NotInJoint);
        }
        self.configuration.voters[1] = vec![];
        for id in mem::replace(&mut self.configuration.learners_next, Default::default()) {
            self.configuration.learners.push(id);
        }
        self.configuration.learners.sort();
        Ok(())
    }

    pub(crate) fn switch_to(&mut self, c: Configuration, next_idx: u64, ins_size: usize) {
        self.configuration = c;
        let mut prs = mem::replace(&mut self.progress, Default::default());
        for v in self.voters().chain(self.learners()) {
            prs.entry(v).or_insert_with(|| {
                let mut pr = Progress::new(next_idx, ins_size);
                // When a node is first added/promoted, we should mark it as recently active.
                // Otherwise, check_quorum may cause us to step down if it is invoked
                // before the added node has a chance to communicate with us.
                pr.recent_active = true;
                pr
            });
        }
        self.progress = prs;
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
