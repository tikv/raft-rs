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

use slog::Logger;

use crate::eraftpb::{ConfState, SnapshotMetadata};
use crate::errors::{Error, Result};
use crate::progress::Progress;
use crate::{DefaultHashBuilder, HashMap, HashSet};

/// Get the majority number of given nodes count.
#[inline]
pub fn majority(total: usize) -> usize {
    (total / 2) + 1
}

/// A Raft internal representation of a Configuration.
///
/// This is corollary to a ConfState, but optimized for `contains` calls.
#[derive(Clone, Debug, Default, PartialEq, Getters)]
pub struct Configuration {
    /// The voter set.
    #[get = "pub"]
    voters: HashSet<u64>,
    /// The learner set.
    #[get = "pub"]
    learners: HashSet<u64>,
}

impl Configuration {
    /// Create a new configuration with the given configuration.
    pub fn new(
        voters: impl IntoIterator<Item = u64>,
        learners: impl IntoIterator<Item = u64>,
    ) -> Self {
        Self {
            voters: voters.into_iter().collect(),
            learners: learners.into_iter().collect(),
        }
    }

    /// Create a new `ConfState` from the configuration itself.
    pub fn to_conf_state(&self) -> ConfState {
        let mut state = ConfState::default();
        state.set_voters(self.voters.iter().cloned().collect());
        state.set_learners(self.learners.iter().cloned().collect());
        state
    }

    /// Create a new `Configuration` from a given `ConfState`.
    pub fn from_conf_state(conf_state: &ConfState) -> Self {
        Self {
            voters: conf_state.voters.iter().cloned().collect(),
            learners: conf_state.learners.iter().cloned().collect(),
        }
    }
}

impl<Iter1, Iter2> From<(Iter1, Iter2)> for Configuration
where
    Iter1: IntoIterator<Item = u64>,
    Iter2: IntoIterator<Item = u64>,
{
    fn from((voters, learners): (Iter1, Iter2)) -> Self {
        Self {
            voters: voters.into_iter().collect(),
            learners: learners.into_iter().collect(),
        }
    }
}

impl Configuration {
    fn with_capacity(voters: usize, learners: usize) -> Self {
        Self {
            voters: HashSet::with_capacity_and_hasher(voters, DefaultHashBuilder::default()),
            learners: HashSet::with_capacity_and_hasher(learners, DefaultHashBuilder::default()),
        }
    }

    /// Validates that the configuration is not problematic.
    ///
    /// Namely:
    /// * There can be no overlap of voters and learners.
    /// * There must be at least one voter.
    pub fn valid(&self) -> Result<()> {
        if let Some(id) = self.voters.intersection(&self.learners).next() {
            Err(Error::Exists(*id, "learners"))
        } else if self.voters.is_empty() {
            Err(Error::ConfigInvalid(
                "There must be at least one voter.".into(),
            ))
        } else {
            Ok(())
        }
    }

    fn has_quorum(&self, potential_quorum: &HashSet<u64>) -> bool {
        self.voters.intersection(potential_quorum).count() >= majority(self.voters.len())
    }

    /// Returns whether or not the given `id` is a member of this configuration.
    #[inline]
    pub fn contains(&self, id: u64) -> bool {
        self.voters.contains(&id) || self.learners.contains(&id)
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

    /// The current configuration state of the cluster.
    #[get = "pub"]
    configuration: Configuration,

    // A preallocated buffer for sorting in the maximal_committed_index function.
    // You should not depend on these values unless you just set them.
    // We use a cell to avoid taking a `&mut self`.
    sort_buffer: RefCell<Vec<u64>>,
    pub(crate) logger: Logger,
}

impl ProgressSet {
    /// Creates a new ProgressSet.
    pub fn new(logger: Logger) -> Self {
        Self::with_capacity(0, 0, logger)
    }

    /// Create a progress set with the specified sizes already reserved.
    pub fn with_capacity(voters: usize, learners: usize, logger: Logger) -> Self {
        ProgressSet {
            progress: HashMap::with_capacity_and_hasher(
                voters + learners,
                DefaultHashBuilder::default(),
            ),
            sort_buffer: RefCell::from(Vec::with_capacity(voters)),
            configuration: Configuration::with_capacity(voters, learners),
            logger,
        }
    }

    fn clear(&mut self) {
        self.progress.clear();
        self.configuration.voters.clear();
        self.configuration.learners.clear();
    }

    pub(crate) fn restore_snapmeta(
        &mut self,
        meta: &SnapshotMetadata,
        next_idx: u64,
        max_inflight: usize,
    ) {
        self.clear();
        let pr = Progress::new(next_idx, max_inflight);
        for id in &meta.conf_state.as_ref().unwrap().voters {
            self.progress.insert(*id, pr.clone());
            self.configuration.voters.insert(*id);
        }
        for id in &meta.conf_state.as_ref().unwrap().learners {
            self.progress.insert(*id, pr.clone());
            self.configuration.learners.insert(*id);
        }

        self.assert_progress_and_configuration_consistent();
    }

    /// Returns the status of voters.
    ///
    /// **Note:** Do not use this for majority/quorum calculation. The Raft node may be
    /// transitioning to a new configuration and have two qourums. Use `has_quorum` instead.
    #[inline]
    pub fn voters(&self) -> impl Iterator<Item = (&u64, &Progress)> {
        let set = self.voter_ids();
        self.progress.iter().filter(move |(&k, _)| set.contains(&k))
    }

    /// Returns the status of learners.
    ///
    /// **Note:** Do not use this for majority/quorum calculation. The Raft node may be
    /// transitioning to a new configuration and have two qourums. Use `has_quorum` instead.
    #[inline]
    pub fn learners(&self) -> impl Iterator<Item = (&u64, &Progress)> {
        let set = self.learner_ids();
        self.progress.iter().filter(move |(&k, _)| set.contains(&k))
    }

    /// Returns the mutable status of voters.
    ///
    /// **Note:** Do not use this for majority/quorum calculation. The Raft node may be
    /// transitioning to a new configuration and have two qourums. Use `has_quorum` instead.
    #[inline]
    pub fn voters_mut(&mut self) -> impl Iterator<Item = (&u64, &mut Progress)> {
        let ids = self.voter_ids();
        self.progress
            .iter_mut()
            .filter(move |(k, _)| ids.contains(k))
    }

    /// Returns the mutable status of learners.
    ///
    /// **Note:** Do not use this for majority/quorum calculation. The Raft node may be
    /// transitioning to a new configuration and have two qourums. Use `has_quorum` instead.
    #[inline]
    pub fn learners_mut(&mut self) -> impl Iterator<Item = (&u64, &mut Progress)> {
        let ids = self.learner_ids();
        self.progress
            .iter_mut()
            .filter(move |(k, _)| ids.contains(k))
    }

    /// Returns the ids of all known voters.
    ///
    /// **Note:** Do not use this for majority/quorum calculation. The Raft node may be
    /// transitioning to a new configuration and have two qourums. Use `has_quorum` instead.
    #[inline]
    pub fn voter_ids(&self) -> HashSet<u64> {
        self.configuration().voters().clone()
    }

    /// Returns the ids of all known learners.
    ///
    /// **Note:** Do not use this for majority/quorum calculation. The Raft node may be
    /// transitioning to a new configuration and have two qourums. Use `has_quorum` instead.
    #[inline]
    pub fn learner_ids(&self) -> HashSet<u64> {
        self.configuration().learners().clone()
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

        self.configuration.voters.insert(id);
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

        self.configuration.learners.insert(id);
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
        self.configuration.learners.remove(&id);
        self.configuration.voters.remove(&id);
        let removed = self.progress.remove(&id);

        self.assert_progress_and_configuration_consistent();
        Ok(removed)
    }

    /// Promote a learner to a peer.
    pub fn promote_learner(&mut self, id: u64) -> Result<()> {
        debug!(self.logger, "Promoting peer with id {id}", id = id);

        if !self.configuration.learners.remove(&id) {
            // Wasn't already a learner. We can't promote what doesn't exist.
            return Err(Error::NotExists(id, "learners"));
        }
        if !self.configuration.voters.insert(id) {
            // Already existed, the caller should know this was a noop.
            return Err(Error::Exists(id, "voters"));
        }

        self.assert_progress_and_configuration_consistent();
        Ok(())
    }

    #[inline(always)]
    fn assert_progress_and_configuration_consistent(&self) {
        debug_assert!(self
            .configuration
            .voters
            .union(&self.configuration.learners)
            .all(|v| self.progress.contains_key(v)));
        assert_eq!(
            self.voter_ids().len() + self.learner_ids().len(),
            self.progress.len()
        );
    }

    /// Returns the maximal committed index for the cluster.
    ///
    /// Eg. If the matched indexes are [2,2,2,4,5], it will return 2.
    pub fn maximal_committed_index(&self) -> u64 {
        let mut matched = self.sort_buffer.borrow_mut();
        matched.clear();
        self.configuration.voters().iter().for_each(|id| {
            let peer = &self.progress[id];
            matched.push(peer.matched);
        });
        // Reverse sort.
        matched.sort_by(|a, b| b.cmp(a));
        matched[matched.len() / 2]
    }

    /// Returns the Candidate's eligibility in the current election.
    ///
    /// If it is still eligible, it should continue polling nodes and checking.
    /// Eventually, the election will result in this returning either `Elected`
    /// or `Ineligible`, meaning the election can be concluded.
    pub fn candidacy_status<'a>(
        &self,
        votes: impl IntoIterator<Item = (&'a u64, &'a bool)>,
    ) -> CandidacyStatus {
        let (accepts, rejects) = votes.into_iter().fold(
            (HashSet::default(), HashSet::default()),
            |(mut accepts, mut rejects), (&id, &accepted)| {
                if accepted {
                    accepts.insert(id);
                } else {
                    rejects.insert(id);
                }
                (accepts, rejects)
            },
        );

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
        let mut active = HashSet::default();
        for (&id, pr) in self.voters_mut() {
            if id == perspective_of {
                active.insert(id);
                continue;
            }
            if pr.recent_active {
                active.insert(id);
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
    #[inline]
    pub fn has_quorum(&self, potential_quorum: &HashSet<u64>) -> bool {
        self.configuration.has_quorum(potential_quorum)
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
