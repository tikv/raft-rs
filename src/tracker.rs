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

mod inflights;
mod progress;
mod state;

pub use self::inflights::Inflights;
pub use self::progress::Progress;
pub use self::state::ProgressState;

use slog::Logger;

use crate::eraftpb::{ConfState, SnapshotMetadata};
use crate::errors::{Error, Result};
use crate::quorum::{AckedIndexer, Index, VoteResult};
use crate::util::Union;
use crate::{DefaultHashBuilder, HashMap, HashSet, JointConfig};

/// Config reflects the configuration tracked in a ProgressTracker.
#[derive(Clone, Debug, Default, PartialEq, Getters)]
pub struct Configuration {
    #[get = "pub"]
    voters: JointConfig,
    /// Learners is a set of IDs corresponding to the learners active in the
    /// current configuration.
    ///
    /// Invariant: Learners and Voters does not intersect, i.e. if a peer is in
    /// either half of the joint config, it can't be a learner; if it is a
    /// learner it can't be in either half of the joint config. This invariant
    /// simplifies the implementation since it allows peers to have clarity about
    /// its current role without taking into account joint consensus.
    #[get = "pub"]
    learners: HashSet<u64>,
    /// When we turn a voter into a learner during a joint consensus transition,
    /// we cannot add the learner directly when entering the joint state. This is
    /// because this would violate the invariant that the intersection of
    /// voters and learners is empty. For example, assume a Voter is removed and
    /// immediately re-added as a learner (or in other words, it is demoted):
    ///
    /// Initially, the configuration will be
    ///
    ///   voters:   {1 2 3}
    ///   learners: {}
    ///
    /// and we want to demote 3. Entering the joint configuration, we naively get
    ///
    ///   voters:   {1 2} & {1 2 3}
    ///   learners: {3}
    ///
    /// but this violates the invariant (3 is both voter and learner). Instead,
    /// we get
    ///
    ///   voters:   {1 2} & {1 2 3}
    ///   learners: {}
    ///   next_learners: {3}
    ///
    /// Where 3 is now still purely a voter, but we are remembering the intention
    /// to make it a learner upon transitioning into the final configuration:
    ///
    ///   voters:   {1 2}
    ///   learners: {3}
    ///   next_learners: {}
    ///
    /// Note that next_learners is not used while adding a learner that is not
    /// also a voter in the joint config. In this case, the learner is added
    /// right away when entering the joint configuration, so that it is caught up
    /// as soon as possible.
    #[get = "pub"]
    learners_next: HashSet<u64>,
    /// True if the configuration is joint and a transition to the incoming
    /// configuration should be carried out automatically by Raft when this is
    /// possible. If false, the configuration will be joint until the application
    /// initiates the transition manually.
    auto_leave: bool,
}

impl Configuration {
    /// Create a new configuration with the given configuration.
    pub fn new(
        voters: impl IntoIterator<Item = u64>,
        learners: impl IntoIterator<Item = u64>,
    ) -> Self {
        Self {
            voters: JointConfig::new(voters.into_iter().collect()),
            auto_leave: false,
            learners: learners.into_iter().collect(),
            learners_next: HashSet::default(),
        }
    }

    fn with_capacity(voters: usize, learners: usize) -> Self {
        Self {
            voters: JointConfig::with_capacity(voters),
            learners: HashSet::with_capacity_and_hasher(learners, DefaultHashBuilder::default()),
            learners_next: HashSet::default(),
            auto_leave: false,
        }
    }

    /// Create a new `ConfState` from the configuration itself.
    pub fn to_conf_state(&self) -> ConfState {
        // Note: Different from etcd, we don't sort.
        let mut state = ConfState::default();
        state.set_voters(self.voters.incoming.raw_slice());
        state.set_voters_outgoing(self.voters.outgoing.raw_slice());
        state.set_learners(self.learners.iter().cloned().collect());
        state.set_learners_next(self.learners_next.iter().cloned().collect());
        state.auto_leave = self.auto_leave;
        state
    }
}

impl AckedIndexer for HashMap<u64, Progress> {
    fn acked_index(&self, voter_id: u64) -> Option<Index> {
        self.get(&voter_id).map(|p| Index {
            index: p.matched,
            group_id: p.commit_group_id,
        })
    }
}

/// `ProgressTracker` contains several `Progress`es,
/// which could be `Leader`, `Follower` and `Learner`.
#[derive(Clone, Getters)]
pub struct ProgressTracker {
    progress: HashMap<u64, Progress>,

    /// The current configuration state of the cluster.
    #[get = "pub"]
    conf: Configuration,
    #[doc(hidden)]
    #[get = "pub"]
    votes: HashMap<u64, bool>,
    #[get = "pub(crate)"]
    max_inflight: usize,

    group_commit: bool,
    pub(crate) logger: Logger,
}

impl ProgressTracker {
    /// Creates a new ProgressTracker.
    pub fn new(max_inflight: usize, logger: Logger) -> Self {
        Self::with_capacity(0, 0, max_inflight, logger)
    }

    /// Create a progress set with the specified sizes already reserved.
    pub fn with_capacity(
        voters: usize,
        learners: usize,
        max_inflight: usize,
        logger: Logger,
    ) -> Self {
        ProgressTracker {
            progress: HashMap::with_capacity_and_hasher(
                voters + learners,
                DefaultHashBuilder::default(),
            ),
            conf: Configuration::with_capacity(voters, learners),
            votes: HashMap::with_capacity_and_hasher(voters, DefaultHashBuilder::default()),
            max_inflight,
            group_commit: false,
            logger,
        }
    }

    /// Configures group commit.
    pub fn enable_group_commit(&mut self, enable: bool) {
        self.group_commit = enable;
    }

    /// Whether enable group commit.
    pub fn group_commit(&self) -> bool {
        self.group_commit
    }

    fn clear(&mut self) {
        self.progress.clear();
        self.conf.voters.clear();
        self.conf.learners.clear();
    }

    /// Returns true if (and only if) there is only one voting member
    /// (i.e. the leader) in the current configuration.
    pub fn is_singleton(&self) -> bool {
        self.conf.voters.is_singleton()
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
            // TODO: use conf_change package to update ProgressTracker.
            self.conf.voters.incoming.voters.insert(*id);
        }
        for id in &meta.conf_state.as_ref().unwrap().learners {
            self.progress.insert(*id, pr.clone());
            self.conf.learners.insert(*id);
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
        self.progress.iter().filter(move |(k, _)| set.contains(**k))
    }

    /// Returns the status of learners.
    ///
    /// **Note:** Do not use this for majority/quorum calculation. The Raft node may be
    /// transitioning to a new configuration and have two qourums. Use `has_quorum` instead.
    #[inline]
    pub fn learners(&self) -> impl Iterator<Item = (&u64, &Progress)> {
        let set = self.learner_ids();
        self.progress.iter().filter(move |(k, _)| set.contains(**k))
    }

    /// Returns the mutable status of voters.
    ///
    /// **Note:** Do not use this for majority/quorum calculation. The Raft node may be
    /// transitioning to a new configuration and have two qourums. Use `has_quorum` instead.
    #[inline]
    pub fn voters_mut(&mut self) -> impl Iterator<Item = (&u64, &mut Progress)> {
        let ids = &self.conf.voters;
        self.progress
            .iter_mut()
            .filter(move |(k, _)| ids.contains(**k))
    }

    /// Returns the mutable status of learners.
    ///
    /// **Note:** Do not use this for majority/quorum calculation. The Raft node may be
    /// transitioning to a new configuration and have two qourums. Use `has_quorum` instead.
    #[inline]
    pub fn learners_mut(&mut self) -> impl Iterator<Item = (&u64, &mut Progress)> {
        let ids = Union::new(&self.conf.learners, &self.conf.learners_next);
        self.progress
            .iter_mut()
            .filter(move |(k, _)| ids.contains(**k))
    }

    /// Returns the ids of all known voters.
    ///
    /// **Note:** Do not use this for majority/quorum calculation. The Raft node may be
    /// transitioning to a new configuration and have two qourums. Use `has_quorum` instead.
    #[inline]
    pub fn voter_ids(&self) -> Union<'_> {
        self.conf.voters.ids()
    }

    /// Returns the ids of all known learners.
    ///
    /// **Note:** Do not use this for majority/quorum calculation. The Raft node may be
    /// transitioning to a new configuration and have two qourums. Use `has_quorum` instead.
    #[inline]
    pub fn learner_ids(&self) -> Union<'_> {
        Union::new(&self.conf.learners, &self.conf.learners_next)
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

        if self.learner_ids().contains(id) {
            return Err(Error::Exists(id, "learners"));
        } else if self.voter_ids().contains(id) {
            return Err(Error::Exists(id, "voters"));
        }

        // TODO: use conf change to update configuration.
        self.conf.voters.incoming.voters.insert(id);
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

        if self.learner_ids().contains(id) {
            return Err(Error::Exists(id, "learners"));
        } else if self.voter_ids().contains(id) {
            return Err(Error::Exists(id, "voters"));
        }

        self.conf.learners.insert(id);
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
        self.conf.learners.remove(&id);
        self.conf.voters.incoming.voters.remove(&id);
        let removed = self.progress.remove(&id);

        self.assert_progress_and_configuration_consistent();
        Ok(removed)
    }

    /// Promote a learner to a peer.
    pub fn promote_learner(&mut self, id: u64) -> Result<()> {
        debug!(self.logger, "Promoting peer with id {id}", id = id);

        if !self.conf.learners.remove(&id) {
            // Wasn't already a learner. We can't promote what doesn't exist.
            return Err(Error::NotExists(id, "learners"));
        }
        if !self.conf.voters.incoming.voters.insert(id) {
            // Already existed, the caller should know this was a noop.
            return Err(Error::Exists(id, "voters"));
        }

        self.assert_progress_and_configuration_consistent();
        Ok(())
    }

    #[inline(always)]
    fn assert_progress_and_configuration_consistent(&self) {
        debug_assert!(self
            .conf
            .voters
            .incoming
            .voters
            .union(&self.conf.learners)
            .all(|v| self.progress.contains_key(v)));
    }

    /// Returns the maximal committed index for the cluster. The bool flag indicates whether
    /// the index is computed by group commit algorithm successfully.
    ///
    /// Eg. If the matched indexes are [2,2,2,4,5], it will return 2.
    /// If the matched indexes and groups are `[(1, 1), (2, 2), (3, 2)]`, it will return 1.
    pub fn maximal_committed_index(&mut self) -> (u64, bool) {
        self.conf
            .voters
            .committed_index(self.group_commit, &self.progress)
    }

    /// Prepares for a new round of vote counting via recordVote.
    pub fn reset_votes(&mut self) {
        self.votes.clear();
    }

    /// Records that the node with the given id voted for this Raft
    /// instance if v == true (and declined it otherwise).
    pub fn record_vote(&mut self, id: u64, vote: bool) {
        self.votes.entry(id).or_insert(vote);
    }

    /// TallyVotes returns the number of granted and rejected Votes, and whether the
    /// election outcome is known.
    pub fn tally_votes(&self) -> (usize, usize, VoteResult) {
        // Make sure to populate granted/rejected correctly even if the Votes slice
        // contains members no longer part of the configuration. This doesn't really
        // matter in the way the numbers are used (they're informational), but might
        // as well get it right.
        let (mut granted, mut rejected) = (0, 0);
        for (id, vote) in &self.votes {
            if !self.conf.voters.contains(*id) {
                continue;
            }
            if *vote {
                granted += 1;
            } else {
                rejected += 1;
            }
        }
        let result = self.vote_result(&self.votes);
        (granted, rejected, result)
    }

    /// Returns the Candidate's eligibility in the current election.
    ///
    /// If it is still eligible, it should continue polling nodes and checking.
    /// Eventually, the election will result in this returning either `Elected`
    /// or `Ineligible`, meaning the election can be concluded.
    pub fn vote_result(&self, votes: &HashMap<u64, bool>) -> VoteResult {
        self.conf.voters.vote_result(|id| votes.get(&id).cloned())
    }

    /// Determines if the current quorum is active according to the this raft node.
    /// Doing this will set the `recent_active` of each peer to false.
    ///
    /// This should only be called by the leader.
    pub fn quorum_recently_active(&mut self, perspective_of: u64) -> bool {
        let mut active = HashSet::default();
        for (&id, pr) in self.voters_mut() {
            if id == perspective_of {
                pr.recent_active = true;
                active.insert(id);
            } else if pr.recent_active {
                active.insert(id);
                pr.recent_active = false;
            }
        }
        self.has_quorum(&active)
    }

    /// Determine if a quorum is formed from the given set of nodes.
    ///
    /// This is the only correct way to verify you have reached a quorum for the whole group.
    #[inline]
    pub fn has_quorum(&self, potential_quorum: &HashSet<u64>) -> bool {
        self.conf
            .voters
            .vote_result(|id| potential_quorum.get(&id).map(|_| true))
            == VoteResult::Won
    }
}

// TODO: Reorganize this whole file into separate files.
// See https://github.com/pingcap/raft-rs/issues/125
#[cfg(test)]
mod test_progress_set {
    use super::{ProgressTracker, Result};
    use crate::default_logger;
    use crate::Progress;

    const CANARY: u64 = 123;

    #[test]
    fn test_insert_redundant_voter() -> Result<()> {
        let mut set = ProgressTracker::new(256, default_logger());
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
            "The ProgressTracker was mutated in a `insert_voter` that returned error."
        );
        Ok(())
    }

    #[test]
    fn test_insert_redundant_learner() -> Result<()> {
        let mut set = ProgressTracker::new(256, default_logger());
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
            "The ProgressTracker was mutated in a `insert_learner` that returned error."
        );
        Ok(())
    }

    #[test]
    fn test_insert_learner_that_is_voter() -> Result<()> {
        let mut set = ProgressTracker::new(256, default_logger());
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
            "The ProgressTracker was mutated in a `insert_learner` that returned error."
        );
        Ok(())
    }

    #[test]
    fn test_insert_voter_that_is_learner() -> Result<()> {
        let mut set = ProgressTracker::new(256, default_logger());
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
            "The ProgressTracker was mutated in a `insert_voter` that returned error."
        );
        Ok(())
    }

    #[test]
    fn test_promote_learner() -> Result<()> {
        let mut set = ProgressTracker::new(256, default_logger());
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
