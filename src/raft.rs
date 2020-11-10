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
use std::ops::{Deref, DerefMut};

use crate::eraftpb::{
    ConfChange, ConfChangeV2, ConfState, Entry, EntryType, HardState, Message, MessageType,
    Snapshot,
};
use protobuf::Message as _;
use raft_proto::ConfChangeI;
use rand::{self, Rng};
use slog::{self, Logger};

use super::errors::{Error, Result, StorageError};
use super::raft_log::RaftLog;
use super::read_only::{ReadOnly, ReadOnlyOption, ReadState};
use super::storage::Storage;
use super::Config;
use crate::confchange::Changer;
use crate::quorum::VoteResult;
use crate::util;
use crate::util::NO_LIMIT;
use crate::{confchange, Progress, ProgressState, ProgressTracker};

// CAMPAIGN_PRE_ELECTION represents the first phase of a normal election when
// Config.pre_vote is true.
const CAMPAIGN_PRE_ELECTION: &[u8] = b"CampaignPreElection";
// CAMPAIGN_ELECTION represents a normal (time-based) election (the second phase
// of the election when Config.pre_vote is true).
const CAMPAIGN_ELECTION: &[u8] = b"CampaignElection";
// CAMPAIGN_TRANSFER represents the type of leader transfer.
const CAMPAIGN_TRANSFER: &[u8] = b"CampaignTransfer";

/// The role of the node.
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum StateRole {
    /// The node is a follower of the leader.
    Follower,
    /// The node could become a leader.
    Candidate,
    /// The node is a leader.
    Leader,
    /// The node could become a candidate, if `prevote` is enabled.
    PreCandidate,
}

impl Default for StateRole {
    fn default() -> StateRole {
        StateRole::Follower
    }
}

/// A constant represents invalid id of raft.
pub const INVALID_ID: u64 = 0;
/// A constant represents invalid index of raft log.
pub const INVALID_INDEX: u64 = 0;

/// SoftState provides state that is useful for logging and debugging.
/// The state is volatile and does not need to be persisted to the WAL.
#[derive(Default, PartialEq, Debug)]
pub struct SoftState {
    /// The potential leader of the cluster.
    pub leader_id: u64,
    /// The soft role this node may take.
    pub raft_state: StateRole,
}

/// UncommittedState is used to keep track of imformation of uncommitted
/// log entries on 'leader' node
struct UncommittedState {
    /// Specify maximum of uncommited entry size.
    /// When this limit is reached, all proposals to append new log will be dropped
    max_uncommitted_size: usize,

    /// Record current uncommitted entries size.
    uncommitted_size: usize,

    /// Record index of last log entry when node becomes leader from candidate.
    /// See https://github.com/tikv/raft-rs/pull/398#discussion_r502417531 for more detail
    last_log_tail_index: u64,
}

impl UncommittedState {
    #[inline]
    pub fn is_no_limit(&self) -> bool {
        self.max_uncommitted_size == NO_LIMIT as usize
    }

    pub fn maybe_increase_uncommitted_size(&mut self, ents: &[Entry]) -> bool {
        // fast path
        if self.is_no_limit() {
            return true;
        }

        let size: usize = ents.iter().map(|ent| ent.get_data().len()).sum();

        // 1. we should never drop an entry without any data(eg. leader election)
        // 2. we should allow at least one uncommitted entry
        // 3. add these entries will not cause size overlimit
        if size == 0
            || self.uncommitted_size == 0
            || size + self.uncommitted_size <= self.max_uncommitted_size
        {
            self.uncommitted_size += size;
            true
        } else {
            false
        }
    }

    pub fn maybe_reduce_uncommitted_size(&mut self, ents: &[Entry]) -> bool {
        // fast path
        if self.is_no_limit() || ents.is_empty() {
            return true;
        }

        // user may advance a 'Ready' which is generated before this node becomes leader
        let size: usize = ents
            .iter()
            .skip_while(|ent| ent.index <= self.last_log_tail_index)
            .map(|ent| ent.get_data().len())
            .sum();

        if size > self.uncommitted_size {
            self.uncommitted_size = 0;
            false
        } else {
            self.uncommitted_size -= size;
            true
        }
    }
}

/// The core struct of raft consensus.
///
/// It's a helper struct to get around rust borrow checks.
#[derive(Getters)]
pub struct RaftCore<T: Storage> {
    /// The current election term.
    pub term: u64,

    /// Which peer this raft is voting for.
    pub vote: u64,

    /// The ID of this node.
    pub id: u64,

    /// The current read states.
    pub read_states: Vec<ReadState>,

    /// The persistent log.
    pub raft_log: RaftLog<T>,

    /// The maximum number of messages that can be inflight.
    pub max_inflight: usize,

    /// The maximum length (in bytes) of all the entries.
    pub max_msg_size: u64,

    /// The peer is requesting snapshot, it is the index that the follower
    /// needs it to be included in a snapshot.
    pub pending_request_snapshot: u64,

    /// The current role of this node.
    pub state: StateRole,

    /// Indicates whether state machine can be promoted to leader,
    /// which is true when it's a voter and its own id is in progress list.
    promotable: bool,

    /// The leader id
    pub leader_id: u64,

    /// ID of the leader transfer target when its value is not None.
    ///
    /// If this is Some(id), we follow the procedure defined in raft thesis 3.10.
    pub lead_transferee: Option<u64>,

    /// Only one conf change may be pending (in the log, but not yet
    /// applied) at a time. This is enforced via `pending_conf_index`, which
    /// is set to a value >= the log index of the latest pending
    /// configuration change (if any). Config changes are only allowed to
    /// be proposed if the leader's applied index is greater than this
    /// value.
    ///
    /// This value is conservatively set in cases where there may be a configuration change pending,
    /// but scanning the log is possibly expensive. This implies that the index stated here may not
    /// necessarily be a config change entry, and it may not be a `BeginMembershipChange` entry, even if
    /// we set this to one.
    pub pending_conf_index: u64,

    /// The queue of read-only requests.
    pub read_only: ReadOnly,

    /// Ticks since it reached last electionTimeout when it is leader or candidate.
    /// Number of ticks since it reached last electionTimeout or received a
    /// valid message from current leader when it is a follower.
    pub election_elapsed: usize,

    /// Number of ticks since it reached last heartbeatTimeout.
    /// only leader keeps heartbeatElapsed.
    heartbeat_elapsed: usize,

    /// Whether to check the quorum
    pub check_quorum: bool,

    /// Enable the prevote algorithm.
    ///
    /// This enables a pre-election vote round on Candidates prior to disrupting the cluster.
    ///
    /// Enable this if greater cluster stability is preferred over faster elections.
    pub pre_vote: bool,

    skip_bcast_commit: bool,
    batch_append: bool,

    heartbeat_timeout: usize,
    election_timeout: usize,

    // randomized_election_timeout is a random number between
    // [min_election_timeout, max_election_timeout - 1]. It gets reset
    // when raft changes its state to follower or candidate.
    randomized_election_timeout: usize,
    min_election_timeout: usize,
    max_election_timeout: usize,

    /// The logger for the raft structure.
    pub(crate) logger: slog::Logger,

    /// The election priority of this node.
    pub priority: u64,

    /// Track uncommitted log entry on this node
    uncommitted_state: UncommittedState,
}

/// A struct that represents the raft consensus itself. Stores details concerning the current
/// and possible state the system can take.
pub struct Raft<T: Storage> {
    prs: ProgressTracker,

    /// The list of messages.
    pub msgs: Vec<Message>,
    /// Internal raftCore.
    pub r: RaftCore<T>,
}

impl<T: Storage> Deref for Raft<T> {
    type Target = RaftCore<T>;

    #[inline]
    fn deref(&self) -> &RaftCore<T> {
        &self.r
    }
}

impl<T: Storage> DerefMut for Raft<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.r
    }
}

trait AssertSend: Send {}

impl<T: Storage + Send> AssertSend for Raft<T> {}

fn new_message(to: u64, field_type: MessageType, from: Option<u64>) -> Message {
    let mut m = Message::default();
    m.to = to;
    if let Some(id) = from {
        m.from = id;
    }
    m.set_msg_type(field_type);
    m
}

/// Maps vote and pre_vote message types to their correspond responses.
pub fn vote_resp_msg_type(t: MessageType) -> MessageType {
    match t {
        MessageType::MsgRequestVote => MessageType::MsgRequestVoteResponse,
        MessageType::MsgRequestPreVote => MessageType::MsgRequestPreVoteResponse,
        _ => panic!("Not a vote message: {:?}", t),
    }
}

impl<T: Storage> Raft<T> {
    /// Creates a new raft for use on the node.
    #[allow(clippy::new_ret_no_self)]
    pub fn new(c: &Config, store: T, logger: &Logger) -> Result<Self> {
        c.validate()?;
        let logger = logger.new(o!("raft_id" => c.id));
        let raft_state = store.initial_state()?;
        let conf_state = &raft_state.conf_state;
        let voters = &conf_state.voters;
        let learners = &conf_state.learners;

        let mut r = Raft {
            prs: ProgressTracker::with_capacity(
                voters.len(),
                learners.len(),
                c.max_inflight_msgs,
                logger.clone(),
            ),
            msgs: Default::default(),
            r: RaftCore {
                id: c.id,
                read_states: Default::default(),
                raft_log: RaftLog::new(store, logger.clone()),
                max_inflight: c.max_inflight_msgs,
                max_msg_size: c.max_size_per_msg,
                pending_request_snapshot: INVALID_INDEX,
                state: StateRole::Follower,
                promotable: false,
                check_quorum: c.check_quorum,
                pre_vote: c.pre_vote,
                read_only: ReadOnly::new(c.read_only_option),
                heartbeat_timeout: c.heartbeat_tick,
                election_timeout: c.election_tick,
                leader_id: Default::default(),
                lead_transferee: None,
                term: Default::default(),
                election_elapsed: Default::default(),
                pending_conf_index: Default::default(),
                vote: Default::default(),
                heartbeat_elapsed: Default::default(),
                randomized_election_timeout: Default::default(),
                min_election_timeout: c.min_election_tick(),
                max_election_timeout: c.max_election_tick(),
                skip_bcast_commit: c.skip_bcast_commit,
                batch_append: c.batch_append,
                logger,
                priority: c.priority,
                uncommitted_state: UncommittedState {
                    max_uncommitted_size: c.max_uncommitted_size as usize,
                    uncommitted_size: 0,
                    last_log_tail_index: 0,
                },
            },
        };
        confchange::restore(&mut r.prs, r.r.raft_log.last_index(), conf_state)?;
        let new_cs = r.post_conf_change();
        if !raft_proto::conf_state_eq(&new_cs, conf_state) {
            fatal!(
                r.logger,
                "invalid restore: {:?} != {:?}",
                conf_state,
                new_cs
            );
        }

        if raft_state.hard_state != HardState::default() {
            r.load_state(&raft_state.hard_state);
        }
        if c.applied > 0 {
            r.commit_apply(c.applied);
        }
        r.become_follower(r.term, INVALID_ID);

        info!(
            r.logger,
            "newRaft";
            "term" => r.term,
            "commit" => r.raft_log.committed,
            "applied" => r.raft_log.applied,
            "last index" => r.raft_log.last_index(),
            "last term" => r.raft_log.last_term(),
            "peers" => ?r.prs.conf().voters,
        );
        Ok(r)
    }

    /// Sets priority of node.
    pub fn set_priority(&mut self, priority: u64) {
        self.priority = priority;
    }

    /// Creates a new raft for use on the node with the default logger.
    ///
    /// The default logger is an `slog` to `log` adapter.
    #[allow(clippy::new_ret_no_self)]
    #[cfg(feature = "default-logger")]
    pub fn with_default_logger(c: &Config, store: T) -> Result<Self> {
        Self::new(c, store, &crate::default_logger())
    }

    /// Grabs an immutable reference to the store.
    #[inline]
    pub fn store(&self) -> &T {
        &self.raft_log.store
    }

    /// Grabs a mutable reference to the store.
    #[inline]
    pub fn mut_store(&mut self) -> &mut T {
        &mut self.raft_log.store
    }

    /// Grabs a reference to the snapshot
    #[inline]
    pub fn snap(&self) -> Option<&Snapshot> {
        self.raft_log.unstable.snapshot.as_ref()
    }

    /// Returns the number of pending read-only messages.
    #[inline]
    pub fn pending_read_count(&self) -> usize {
        self.read_only.pending_read_count()
    }

    /// Returns how many read states exist.
    #[inline]
    pub fn ready_read_count(&self) -> usize {
        self.read_states.len()
    }

    /// Returns a value representing the softstate at the time of calling.
    pub fn soft_state(&self) -> SoftState {
        SoftState {
            leader_id: self.leader_id,
            raft_state: self.state,
        }
    }

    /// Returns a value representing the hardstate at the time of calling.
    pub fn hard_state(&self) -> HardState {
        let mut hs = HardState::default();
        hs.term = self.term;
        hs.vote = self.vote;
        hs.commit = self.raft_log.committed;
        hs
    }

    /// Returns whether the current raft is in lease.
    pub fn in_lease(&self) -> bool {
        self.state == StateRole::Leader && self.check_quorum
    }

    /// For testing leader lease
    #[doc(hidden)]
    pub fn set_randomized_election_timeout(&mut self, t: usize) {
        assert!(self.min_election_timeout <= t && t < self.max_election_timeout);
        self.randomized_election_timeout = t;
    }

    /// Fetch the length of the election timeout.
    pub fn election_timeout(&self) -> usize {
        self.election_timeout
    }

    /// Fetch the length of the heartbeat timeout
    pub fn heartbeat_timeout(&self) -> usize {
        self.heartbeat_timeout
    }

    /// Fetch the number of ticks elapsed since last heartbeat.
    pub fn heartbeat_elapsed(&self) -> usize {
        self.heartbeat_elapsed
    }

    /// Return the length of the current randomized election timeout.
    pub fn randomized_election_timeout(&self) -> usize {
        self.randomized_election_timeout
    }

    /// Set whether skip broadcast empty commit messages at runtime.
    #[inline]
    pub fn skip_bcast_commit(&mut self, skip: bool) {
        self.skip_bcast_commit = skip;
    }

    /// Set whether batch append msg at runtime.
    #[inline]
    pub fn set_batch_append(&mut self, batch_append: bool) {
        self.batch_append = batch_append;
    }

    /// Configures group commit.
    ///
    /// If group commit is enabled, only logs replicated to at least two
    /// different groups are committed.
    ///
    /// You should use `assign_commit_groups` to configure peer groups.
    pub fn enable_group_commit(&mut self, enable: bool) {
        self.mut_prs().enable_group_commit(enable);
        if StateRole::Leader == self.state && !enable && self.maybe_commit() {
            self.bcast_append();
        }
    }

    /// Whether enable group commit.
    pub fn group_commit(&self) -> bool {
        self.prs().group_commit()
    }

    /// Assigns groups to peers.
    ///
    /// The tuple is (`peer_id`, `group_id`). `group_id` should be larger than 0.
    ///
    /// The group information is only stored in memory. So you need to configure
    /// it every time a raft state machine is initialized or a snapshot is applied.
    pub fn assign_commit_groups(&mut self, ids: &[(u64, u64)]) {
        let prs = self.mut_prs();
        for (peer_id, group_id) in ids {
            assert!(*group_id > 0);
            if let Some(pr) = prs.get_mut(*peer_id) {
                pr.commit_group_id = *group_id;
            } else {
                continue;
            }
        }
        if StateRole::Leader == self.state && self.group_commit() && self.maybe_commit() {
            self.bcast_append();
        }
    }

    /// Removes all commit group configurations.
    pub fn clear_commit_group(&mut self) {
        for (_, pr) in self.mut_prs().iter_mut() {
            pr.commit_group_id = 0;
        }
    }

    /// Checks whether the raft group is using group commit and consistent
    /// over group.
    ///
    /// If it can't get a correct answer, `None` is returned.
    pub fn check_group_commit_consistent(&mut self) -> Option<bool> {
        if self.state != StateRole::Leader {
            return None;
        }
        // Previous leader may have reach consistency already.
        //
        // check applied_index instead of committed_index to avoid pending conf change.
        if !self.apply_to_current_term() {
            return None;
        }
        let (index, use_group_commit) = self.mut_prs().maximal_committed_index();
        debug!(
            self.logger,
            "check group commit consistent";
            "index" => index,
            "use_group_commit" => use_group_commit,
            "committed" => self.raft_log.committed
        );
        Some(use_group_commit && index == self.raft_log.committed)
    }

    /// Checks if logs are committed to its term.
    ///
    /// The check is useful usually when raft is leader.
    pub fn commit_to_current_term(&self) -> bool {
        self.raft_log
            .term(self.raft_log.committed)
            .map_or(false, |t| t == self.term)
    }

    /// Checks if logs are applied to current term.
    pub fn apply_to_current_term(&self) -> bool {
        self.raft_log
            .term(self.raft_log.applied)
            .map_or(false, |t| t == self.term)
    }
}

impl<T: Storage> RaftCore<T> {
    // send persists state to stable storage and then sends to its mailbox.
    fn send(&mut self, mut m: Message, msgs: &mut Vec<Message>) {
        debug!(
            self.logger,
            "Sending from {from} to {to}",
            from = self.id,
            to = m.to;
            "msg" => ?m,
        );
        if m.from == INVALID_ID {
            m.from = self.id;
        }
        if m.get_msg_type() == MessageType::MsgRequestVote
            || m.get_msg_type() == MessageType::MsgRequestPreVote
            || m.get_msg_type() == MessageType::MsgRequestVoteResponse
            || m.get_msg_type() == MessageType::MsgRequestPreVoteResponse
        {
            if m.term == 0 {
                // All {pre-,}campaign messages need to have the term set when
                // sending.
                // - MsgVote: m.Term is the term the node is campaigning for,
                //   non-zero as we increment the term when campaigning.
                // - MsgVoteResp: m.Term is the new r.Term if the MsgVote was
                //   granted, non-zero for the same reason MsgVote is
                // - MsgPreVote: m.Term is the term the node will campaign,
                //   non-zero as we use m.Term to indicate the next term we'll be
                //   campaigning for
                // - MsgPreVoteResp: m.Term is the term received in the original
                //   MsgPreVote if the pre-vote was granted, non-zero for the
                //   same reasons MsgPreVote is
                fatal!(
                    self.logger,
                    "term should be set when sending {:?}",
                    m.get_msg_type()
                );
            }
        } else {
            if m.term != 0 {
                fatal!(
                    self.logger,
                    "term should not be set when sending {:?} (was {})",
                    m.get_msg_type(),
                    m.term
                );
            }
            // do not attach term to MsgPropose, MsgReadIndex
            // proposals are a way to forward to the leader and
            // should be treated as local message.
            // MsgReadIndex is also forwarded to leader.
            if m.get_msg_type() != MessageType::MsgPropose
                && m.get_msg_type() != MessageType::MsgReadIndex
            {
                m.term = self.term;
            }
        }
        if m.get_msg_type() == MessageType::MsgRequestVote
            || m.get_msg_type() == MessageType::MsgRequestPreVote
        {
            m.priority = self.priority;
        }
        msgs.push(m);
    }

    fn prepare_send_snapshot(&mut self, m: &mut Message, pr: &mut Progress, to: u64) -> bool {
        if !pr.recent_active {
            debug!(
                self.logger,
                "ignore sending snapshot to {} since it is not recently active",
                to;
            );
            return false;
        }

        m.set_msg_type(MessageType::MsgSnapshot);
        let snapshot_r = self.raft_log.snapshot(pr.pending_request_snapshot);
        if let Err(e) = snapshot_r {
            if e == Error::Store(StorageError::SnapshotTemporarilyUnavailable) {
                debug!(
                    self.logger,
                    "failed to send snapshot to {} because snapshot is temporarily \
                     unavailable",
                    to;
                );
                return false;
            }
            fatal!(self.logger, "unexpected error: {:?}", e);
        }
        let snapshot = snapshot_r.unwrap();
        if snapshot.get_metadata().index == 0 {
            fatal!(self.logger, "need non-empty snapshot");
        }
        let (sindex, sterm) = (snapshot.get_metadata().index, snapshot.get_metadata().term);
        m.set_snapshot(snapshot);
        debug!(
            self.logger,
            "[firstindex: {first_index}, commit: {committed}] sent snapshot[index: {snapshot_index}, term: {snapshot_term}] to {to}",
            first_index = self.raft_log.first_index(),
            committed = self.raft_log.committed,
            snapshot_index = sindex,
            snapshot_term = sterm,
            to = to;
            "progress" => ?pr,
        );
        pr.become_snapshot(sindex);
        debug!(
            self.logger,
            "paused sending replication messages to {}",
            to;
            "progress" => ?pr,
        );
        true
    }

    fn prepare_send_entries(
        &mut self,
        m: &mut Message,
        pr: &mut Progress,
        term: u64,
        ents: Vec<Entry>,
    ) {
        m.set_msg_type(MessageType::MsgAppend);
        m.index = pr.next_idx - 1;
        m.log_term = term;
        m.set_entries(ents.into());
        m.commit = self.raft_log.committed;
        if !m.entries.is_empty() {
            let last = m.entries.last().unwrap().index;
            pr.update_state(last);
        }
    }

    fn try_batching(
        &mut self,
        to: u64,
        msgs: &mut [Message],
        pr: &mut Progress,
        ents: &mut Vec<Entry>,
    ) -> bool {
        // if MsgAppend for the receiver already exists, try_batching
        // will append the entries to the existing MsgAppend
        let mut is_batched = false;
        for msg in msgs {
            if msg.get_msg_type() == MessageType::MsgAppend && msg.to == to {
                if !ents.is_empty() {
                    if !util::is_continuous_ents(msg, ents) {
                        return is_batched;
                    }
                    let mut batched_entries: Vec<_> = msg.take_entries().into();
                    batched_entries.append(ents);
                    msg.set_entries(batched_entries.into());
                    let last_idx = msg.entries.last().unwrap().index;
                    pr.update_state(last_idx);
                }
                msg.commit = self.raft_log.committed;
                is_batched = true;
                break;
            }
        }
        is_batched
    }

    /// Sends an append RPC with new entries (if any) and the current commit index to the given
    /// peer.
    fn send_append(&mut self, to: u64, pr: &mut Progress, msgs: &mut Vec<Message>) {
        self.maybe_send_append(to, pr, true, msgs);
    }

    /// Sends an append RPC with new entries to the given peer,
    /// if necessary. Returns true if a message was sent. The allow_empty
    /// argument controls whether messages with no entries will be sent
    /// ("empty" messages are useful to convey updated Commit indexes, but
    /// are undesirable when we're sending multiple messages in a batch).
    fn maybe_send_append(
        &mut self,
        to: u64,
        pr: &mut Progress,
        allow_empty: bool,
        msgs: &mut Vec<Message>,
    ) -> bool {
        if pr.is_paused() {
            trace!(
                self.logger,
                "Skipping sending to {to}, it's paused",
                to = to;
                "progress" => ?pr,
            );
            return false;
        }
        let mut m = Message::default();
        m.to = to;
        if pr.pending_request_snapshot != INVALID_INDEX {
            // Check pending request snapshot first to avoid unnecessary loading entries.
            if !self.prepare_send_snapshot(&mut m, pr, to) {
                return false;
            }
        } else {
            let ents = self.raft_log.entries(pr.next_idx, self.max_msg_size);
            if !allow_empty && ents.as_ref().ok().map_or(true, |e| e.is_empty()) {
                return false;
            }
            let term = self.raft_log.term(pr.next_idx - 1);
            match (term, ents) {
                (Ok(term), Ok(mut ents)) => {
                    if self.batch_append && self.try_batching(to, msgs, pr, &mut ents) {
                        return true;
                    }
                    self.prepare_send_entries(&mut m, pr, term, ents)
                }
                _ => {
                    // send snapshot if we failed to get term or entries.
                    if !self.prepare_send_snapshot(&mut m, pr, to) {
                        return false;
                    }
                }
            }
        }
        self.send(m, msgs);
        true
    }

    // send_heartbeat sends an empty MsgAppend
    fn send_heartbeat(
        &mut self,
        to: u64,
        pr: &Progress,
        ctx: Option<Vec<u8>>,
        msgs: &mut Vec<Message>,
    ) {
        // Attach the commit as min(to.matched, self.raft_log.committed).
        // When the leader sends out heartbeat message,
        // the receiver(follower) might not be matched with the leader
        // or it might not have all the committed entries.
        // The leader MUST NOT forward the follower's commit to
        // an unmatched index.
        let mut m = Message::default();
        m.to = to;
        m.set_msg_type(MessageType::MsgHeartbeat);
        let commit = cmp::min(pr.matched, self.raft_log.committed);
        m.commit = commit;
        if let Some(context) = ctx {
            m.context = context;
        }
        self.send(m, msgs);
    }
}

impl<T: Storage> Raft<T> {
    /// Sends an append RPC with new entries (if any) and the current commit index to the given
    /// peer.
    pub fn send_append(&mut self, to: u64) {
        let pr = self.prs.get_mut(to).unwrap();
        self.r.send_append(to, pr, &mut self.msgs)
    }

    /// Sends RPC, with entries to all peers that are not up-to-date
    /// according to the progress recorded in r.prs().
    pub fn bcast_append(&mut self) {
        let self_id = self.id;
        let core = &mut self.r;
        let msgs = &mut self.msgs;
        self.prs
            .iter_mut()
            .filter(|&(id, _)| *id != self_id)
            .for_each(|(id, pr)| core.send_append(*id, pr, msgs));
    }

    /// Broadcasts heartbeats to all the followers if it's leader.
    pub fn ping(&mut self) {
        if self.state == StateRole::Leader {
            self.bcast_heartbeat();
        }
    }

    /// Sends RPC, without entries to all the peers.
    pub fn bcast_heartbeat(&mut self) {
        let ctx = self.read_only.last_pending_request_ctx();
        self.bcast_heartbeat_with_ctx(ctx)
    }

    #[cfg_attr(feature = "cargo-clippy", allow(clippy::needless_pass_by_value))]
    fn bcast_heartbeat_with_ctx(&mut self, ctx: Option<Vec<u8>>) {
        let self_id = self.id;
        let core = &mut self.r;
        let msgs = &mut self.msgs;
        self.prs
            .iter_mut()
            .filter(|&(id, _)| *id != self_id)
            .for_each(|(id, pr)| core.send_heartbeat(*id, pr, ctx.clone(), msgs));
    }

    /// Attempts to advance the commit index. Returns true if the commit index
    /// changed (in which case the caller should call `r.bcast_append`).
    pub fn maybe_commit(&mut self) -> bool {
        let mci = self.mut_prs().maximal_committed_index().0;
        if self.r.raft_log.maybe_commit(mci, self.r.term) {
            let (self_id, committed) = (self.id, self.raft_log.committed);
            self.mut_prs()
                .get_mut(self_id)
                .unwrap()
                .update_committed(committed);
            return true;
        }
        false
    }

    /// Commit that the Raft peer has applied up to the given index.
    ///
    /// Registers the new applied index to the Raft log.
    ///
    /// # Hooks
    ///
    /// * Post: Checks to see if it's time to finalize a Joint Consensus state.
    pub fn commit_apply(&mut self, applied: u64) {
        let old_applied = self.raft_log.applied;
        #[allow(deprecated)]
        self.raft_log.applied_to(applied);

        // TODO: it may never auto_leave if leader steps down before enter joint is applied.
        if self.prs.conf().auto_leave
            && old_applied <= self.pending_conf_index
            && applied >= self.pending_conf_index
            && self.state == StateRole::Leader
        {
            // If the current (and most recent, at least for this leader's term)
            // configuration should be auto-left, initiate that now. We use a
            // nil Data which unmarshals into an empty ConfChangeV2 and has the
            // benefit that appendEntry can never refuse it based on its size
            // (which registers as zero).
            let mut entry = Entry::default();
            entry.set_entry_type(EntryType::EntryConfChangeV2);

            // append_entry will never refuse an empty
            if !self.append_entry(&mut [entry]) {
                panic!("appending an empty EntryConfChangeV2 should never be dropped")
            }
            self.pending_conf_index = self.raft_log.last_index();
            info!(self.logger, "initiating automatic transition out of joint configuration"; "config" => ?self.prs.conf());
        }
    }

    /// Resets the current node to a given term.
    pub fn reset(&mut self, term: u64) {
        if self.term != term {
            self.term = term;
            self.vote = INVALID_ID;
        }
        self.leader_id = INVALID_ID;
        self.reset_randomized_election_timeout();
        self.election_elapsed = 0;
        self.heartbeat_elapsed = 0;

        self.abort_leader_transfer();

        self.prs.reset_votes();

        self.pending_conf_index = 0;
        self.read_only = ReadOnly::new(self.read_only.option);
        self.pending_request_snapshot = INVALID_INDEX;

        let last_index = self.raft_log.last_index();
        let committed = self.raft_log.committed;
        let self_id = self.id;
        for (&id, mut pr) in self.mut_prs().iter_mut() {
            pr.reset(last_index + 1);
            if id == self_id {
                pr.matched = last_index;
                pr.committed_index = committed;
            }
        }
    }

    /// Appends a slice of entries to the log.
    /// The entries are updated to match the current index and term.
    /// Only called by leader currently
    #[must_use]
    pub fn append_entry(&mut self, es: &mut [Entry]) -> bool {
        if !self.maybe_increase_uncommitted_size(es) {
            return false;
        }

        let mut li = self.raft_log.last_index();
        for (i, e) in es.iter_mut().enumerate() {
            e.term = self.term;
            e.index = li + 1 + i as u64;
        }
        // use latest "last" index after truncate/append
        li = self.raft_log.append(es);

        let self_id = self.id;
        self.mut_prs().get_mut(self_id).unwrap().maybe_update(li);

        // Regardless of maybe_commit's return, our caller will call bcastAppend.
        self.maybe_commit();

        true
    }

    /// Returns true to indicate that there will probably be some readiness need to be handled.
    pub fn tick(&mut self) -> bool {
        match self.state {
            StateRole::Follower | StateRole::PreCandidate | StateRole::Candidate => {
                self.tick_election()
            }
            StateRole::Leader => self.tick_heartbeat(),
        }
    }

    // TODO: revoke pub when there is a better way to test.
    /// Run by followers and candidates after self.election_timeout.
    ///
    /// Returns true to indicate that there will probably be some readiness need to be handled.
    pub fn tick_election(&mut self) -> bool {
        self.election_elapsed += 1;
        if !self.pass_election_timeout() || !self.promotable {
            return false;
        }

        self.election_elapsed = 0;
        let m = new_message(INVALID_ID, MessageType::MsgHup, Some(self.id));
        let _ = self.step(m);
        true
    }

    // tick_heartbeat is run by leaders to send a MsgBeat after self.heartbeat_timeout.
    // Returns true to indicate that there will probably be some readiness need to be handled.
    fn tick_heartbeat(&mut self) -> bool {
        self.heartbeat_elapsed += 1;
        self.election_elapsed += 1;

        let mut has_ready = false;
        if self.election_elapsed >= self.election_timeout {
            self.election_elapsed = 0;
            if self.check_quorum {
                let m = new_message(INVALID_ID, MessageType::MsgCheckQuorum, Some(self.id));
                has_ready = true;
                let _ = self.step(m);
            }
            if self.state == StateRole::Leader && self.lead_transferee.is_some() {
                self.abort_leader_transfer()
            }
        }

        if self.state != StateRole::Leader {
            return has_ready;
        }

        if self.heartbeat_elapsed >= self.heartbeat_timeout {
            self.heartbeat_elapsed = 0;
            has_ready = true;
            let m = new_message(INVALID_ID, MessageType::MsgBeat, Some(self.id));
            let _ = self.step(m);
        }
        has_ready
    }

    /// Converts this node to a follower.
    pub fn become_follower(&mut self, term: u64, leader_id: u64) {
        let pending_request_snapshot = self.pending_request_snapshot;
        self.reset(term);
        self.leader_id = leader_id;
        self.state = StateRole::Follower;
        self.pending_request_snapshot = pending_request_snapshot;
        info!(
            self.logger,
            "became follower at term {term}",
            term = self.term;
        );
    }

    // TODO: revoke pub when there is a better way to test.
    /// Converts this node to a candidate
    ///
    /// # Panics
    ///
    /// Panics if a leader already exists.
    pub fn become_candidate(&mut self) {
        assert_ne!(
            self.state,
            StateRole::Leader,
            "invalid transition [leader -> candidate]"
        );
        let term = self.term + 1;
        self.reset(term);
        let id = self.id;
        self.vote = id;
        self.state = StateRole::Candidate;
        info!(
            self.logger,
            "became candidate at term {term}",
            term = self.term;
        );
    }

    /// Converts this node to a pre-candidate
    ///
    /// # Panics
    ///
    /// Panics if a leader already exists.
    pub fn become_pre_candidate(&mut self) {
        assert_ne!(
            self.state,
            StateRole::Leader,
            "invalid transition [leader -> pre-candidate]"
        );
        // Becoming a pre-candidate changes our state.
        // but doesn't change anything else. In particular it does not increase
        // self.term or change self.vote.
        self.state = StateRole::PreCandidate;
        self.prs.reset_votes();
        // If a network partition happens, and leader is in minority partition,
        // it will step down, and become follower without notifying others.
        self.leader_id = INVALID_ID;
        info!(
            self.logger,
            "became pre-candidate at term {term}",
            term = self.term;
        );
    }

    // TODO: revoke pub when there is a better way to test.
    /// Makes this raft the leader.
    ///
    /// # Panics
    ///
    /// Panics if this is a follower node.
    pub fn become_leader(&mut self) {
        trace!(self.logger, "ENTER become_leader");
        assert_ne!(
            self.state,
            StateRole::Follower,
            "invalid transition [follower -> leader]"
        );
        let term = self.term;
        self.reset(term);
        self.leader_id = self.id;
        self.state = StateRole::Leader;

        // update uncommitted state
        self.uncommitted_state.uncommitted_size = 0;
        self.uncommitted_state.last_log_tail_index = self.raft_log.last_index();

        // Followers enter replicate mode when they've been successfully probed
        // (perhaps after having received a snapshot as a result). The leader is
        // trivially in this state. Note that r.reset() has initialized this
        // progress with the last index already.
        let id = self.id;
        self.mut_prs().get_mut(id).unwrap().become_replicate();

        // Conservatively set the pending_conf_index to the last index in the
        // log. There may or may not be a pending config change, but it's
        // safe to delay any future proposals until we commit all our
        // pending log entries, and scanning the entire tail of the log
        // could be expensive.
        self.pending_conf_index = self.raft_log.last_index();

        // no need to check result becase append_entry never refuse entries
        // which size is zero
        if !self.append_entry(&mut [Entry::default()]) {
            panic!("appending an empty entry should never be dropped")
        }

        info!(
            self.logger,
            "became leader at term {term}",
            term = self.term;
        );
        trace!(self.logger, "EXIT become_leader");
    }

    fn num_pending_conf(&self, ents: &[Entry]) -> usize {
        ents.iter()
            .filter(|e| {
                e.get_entry_type() == EntryType::EntryConfChange
                    || e.get_entry_type() == EntryType::EntryConfChangeV2
            })
            .count()
    }

    /// Campaign to attempt to become a leader.
    ///
    /// If prevote is enabled, this is handled as well.
    pub fn campaign(&mut self, campaign_type: &[u8]) {
        let (vote_msg, term) = if campaign_type == CAMPAIGN_PRE_ELECTION {
            self.become_pre_candidate();
            // Pre-vote RPCs are sent for next term before we've incremented self.term.
            (MessageType::MsgRequestPreVote, self.term + 1)
        } else {
            self.become_candidate();
            (MessageType::MsgRequestVote, self.term)
        };
        let self_id = self.id;
        if VoteResult::Won == self.poll(self_id, vote_msg, true) {
            // We won the election after voting for ourselves (which must mean that
            // this is a single-node cluster).
            return;
        }

        let mut voters = [0; 7];
        let mut voter_cnt = 0;

        // Only send vote request to voters.
        for id in self.prs.conf().voters().ids().iter() {
            if id == self_id {
                continue;
            }

            if voter_cnt == voters.len() {
                self.log_broadcast_vote(vote_msg, &voters);
                voter_cnt = 0;
            }
            voters[voter_cnt] = id;
            voter_cnt += 1;
            let mut m = new_message(id, vote_msg, None);
            m.term = term;
            m.index = self.raft_log.last_index();
            m.log_term = self.raft_log.last_term();
            if campaign_type == CAMPAIGN_TRANSFER {
                m.context = campaign_type.to_vec();
            }
            self.r.send(m, &mut self.msgs);
        }
        if voter_cnt > 0 {
            self.log_broadcast_vote(vote_msg, &voters[..voter_cnt]);
        }
    }

    #[inline]
    fn log_broadcast_vote(&self, t: MessageType, ids: &[u64]) {
        info!(
            self.logger,
            "broadcasting vote request";
            "type" => ?t,
            "term" => self.term,
            "log_term" => self.raft_log.last_term(),
            "log_index" => self.raft_log.last_index(),
            "to" => ?ids,
        );
    }

    /// Steps the raft along via a message. This should be called everytime your raft receives a
    /// message from a peer.
    pub fn step(&mut self, m: Message) -> Result<()> {
        // Handle the message term, which may result in our stepping down to a follower.
        if m.term == 0 {
            // local message
        } else if m.term > self.term {
            if m.get_msg_type() == MessageType::MsgRequestVote
                || m.get_msg_type() == MessageType::MsgRequestPreVote
            {
                let force = m.context == CAMPAIGN_TRANSFER;
                let in_lease = self.check_quorum
                    && self.leader_id != INVALID_ID
                    && self.election_elapsed < self.election_timeout;
                if !force && in_lease {
                    // if a server receives RequestVote request within the minimum election
                    // timeout of hearing from a current leader, it does not update its term
                    // or grant its vote
                    //
                    // This is included in the 3rd concern for Joint Consensus, where if another
                    // peer is removed from the cluster it may try to hold elections and disrupt
                    // stability.
                    info!(
                        self.logger,
                        "[logterm: {log_term}, index: {log_index}, vote: {vote}] ignored vote from \
                         {from} [logterm: {msg_term}, index: {msg_index}]: lease is not expired",
                        log_term = self.raft_log.last_term(),
                        log_index = self.raft_log.last_index(),
                        vote = self.vote,
                        from = m.from,
                        msg_term = m.log_term,
                        msg_index = m.index;
                        "term" => self.term,
                        "remaining ticks" => self.election_timeout - self.election_elapsed,
                        "msg type" => ?m.get_msg_type(),
                    );

                    return Ok(());
                }
            }

            if m.get_msg_type() == MessageType::MsgRequestPreVote
                || (m.get_msg_type() == MessageType::MsgRequestPreVoteResponse && !m.reject)
            {
                // For a pre-vote request:
                // Never change our term in response to a pre-vote request.
                //
                // For a pre-vote response with pre-vote granted:
                // We send pre-vote requests with a term in our future. If the
                // pre-vote is granted, we will increment our term when we get a
                // quorum. If it is not, the term comes from the node that
                // rejected our vote so we should become a follower at the new
                // term.
            } else {
                info!(
                    self.logger,
                    "received a message with higher term from {from}",
                    from = m.from;
                    "term" => self.term,
                    "message_term" => m.term,
                    "msg type" => ?m.get_msg_type(),
                );
                if m.get_msg_type() == MessageType::MsgAppend
                    || m.get_msg_type() == MessageType::MsgHeartbeat
                    || m.get_msg_type() == MessageType::MsgSnapshot
                {
                    self.become_follower(m.term, m.from);
                } else {
                    self.become_follower(m.term, INVALID_ID);
                }
            }
        } else if m.term < self.term {
            if (self.check_quorum || self.pre_vote)
                && (m.get_msg_type() == MessageType::MsgHeartbeat
                    || m.get_msg_type() == MessageType::MsgAppend)
            {
                // We have received messages from a leader at a lower term. It is possible
                // that these messages were simply delayed in the network, but this could
                // also mean that this node has advanced its term number during a network
                // partition, and it is now unable to either win an election or to rejoin
                // the majority on the old term. If checkQuorum is false, this will be
                // handled by incrementing term numbers in response to MsgVote with a higher
                // term, but if checkQuorum is true we may not advance the term on MsgVote and
                // must generate other messages to advance the term. The net result of these
                // two features is to minimize the disruption caused by nodes that have been
                // removed from the cluster's configuration: a removed node will send MsgVotes
                // which will be ignored, but it will not receive MsgApp or MsgHeartbeat, so it
                // will not create disruptive term increases, by notifying leader of this node's
                // activeness.
                // The above comments also true for Pre-Vote
                //
                // When follower gets isolated, it soon starts an election ending
                // up with a higher term than leader, although it won't receive enough
                // votes to win the election. When it regains connectivity, this response
                // with "pb.MsgAppResp" of higher term would force leader to step down.
                // However, this disruption is inevitable to free this stuck node with
                // fresh election. This can be prevented with Pre-Vote phase.
                let to_send = new_message(m.from, MessageType::MsgAppendResponse, None);
                self.r.send(to_send, &mut self.msgs);
            } else if m.get_msg_type() == MessageType::MsgRequestPreVote {
                // Before pre_vote enable, there may be a recieving candidate with higher term,
                // but less log. After update to pre_vote, the cluster may deadlock if
                // we drop messages with a lower term.
                info!(
                    self.logger,
                    "{} [log_term: {}, index: {}, vote: {}] rejected {:?} from {} [log_term: {}, index: {}] at term {}",
                    self.id,
                    self.raft_log.last_term(),
                    self.raft_log.last_index(),
                    self.vote,
                    m.get_msg_type(),
                    m.from,
                    m.log_term,
                    m.index,
                    self.term,
                );

                let mut to_send = new_message(m.from, MessageType::MsgRequestPreVoteResponse, None);
                to_send.term = self.term;
                to_send.reject = true;
                self.r.send(to_send, &mut self.msgs);
            } else {
                // ignore other cases
                info!(
                    self.logger,
                    "ignored a message with lower term from {from}",
                    from = m.from;
                    "term" => self.term,
                    "msg type" => ?m.get_msg_type(),
                    "msg term" => m.term
                );
            }
            return Ok(());
        }

        #[cfg(feature = "failpoints")]
        fail_point!("before_step");

        match m.get_msg_type() {
            MessageType::MsgHup => self.hup(false),
            MessageType::MsgRequestVote | MessageType::MsgRequestPreVote => {
                // We can vote if this is a repeat of a vote we've already cast...
                let can_vote = (self.vote == m.from) ||
                    // ...we haven't voted and we don't think there's a leader yet in this term...
                    (self.vote == INVALID_ID && self.leader_id == INVALID_ID) ||
                    // ...or this is a PreVote for a future term...
                    (m.get_msg_type() == MessageType::MsgRequestPreVote && m.term > self.term);
                // ...and we believe the candidate is up to date.
                if can_vote
                    && self.raft_log.is_up_to_date(m.index, m.log_term)
                    && (m.index > self.raft_log.last_index() || self.priority <= m.priority)
                {
                    // When responding to Msg{Pre,}Vote messages we include the term
                    // from the message, not the local term. To see why consider the
                    // case where a single node was previously partitioned away and
                    // it's local term is now of date. If we include the local term
                    // (recall that for pre-votes we don't update the local term), the
                    // (pre-)campaigning node on the other end will proceed to ignore
                    // the message (it ignores all out of date messages).
                    // The term in the original message and current local term are the
                    // same in the case of regular votes, but different for pre-votes.
                    self.log_vote_approve(&m);
                    let mut to_send =
                        new_message(m.from, vote_resp_msg_type(m.get_msg_type()), None);
                    to_send.reject = false;
                    to_send.term = m.term;
                    self.r.send(to_send, &mut self.msgs);
                    if m.get_msg_type() == MessageType::MsgRequestVote {
                        // Only record real votes.
                        self.election_elapsed = 0;
                        self.vote = m.from;
                    }
                } else {
                    self.log_vote_reject(&m);
                    let mut to_send =
                        new_message(m.from, vote_resp_msg_type(m.get_msg_type()), None);
                    to_send.reject = true;
                    to_send.term = self.term;
                    self.r.send(to_send, &mut self.msgs);
                }
            }
            _ => match self.state {
                StateRole::PreCandidate | StateRole::Candidate => self.step_candidate(m)?,
                StateRole::Follower => self.step_follower(m)?,
                StateRole::Leader => self.step_leader(m)?,
            },
        }
        Ok(())
    }

    fn hup(&mut self, transfer_leader: bool) {
        if self.state == StateRole::Leader {
            debug!(
                self.logger,
                "ignoring MsgHup because already leader";
            );
            return;
        }

        // If there is a pending snapshot, its index will be returned by
        // `maybe_first_index`. Note that snapshot updates configuration
        // already, so as long as pending entries don't contain conf change
        // it's safe to start campaign.
        let first_index = match self.raft_log.unstable.maybe_first_index() {
            Some(idx) => idx,
            None => self.raft_log.applied + 1,
        };

        let ents = self
            .raft_log
            .slice(first_index, self.raft_log.committed + 1, None)
            .unwrap_or_else(|e| {
                fatal!(
                    self.logger,
                    "unexpected error getting unapplied entries [{}, {}): {:?}",
                    first_index,
                    self.raft_log.committed + 1,
                    e
                );
            });
        let n = self.num_pending_conf(&ents);
        if n != 0 {
            warn!(
                self.logger,
                "cannot campaign at term {term} since there are still {pending_changes} pending \
                 configuration changes to apply",
                term = self.term,
                pending_changes = n;
            );
            return;
        }
        info!(
            self.logger,
            "starting a new election";
            "term" => self.term,
        );
        if transfer_leader {
            self.campaign(CAMPAIGN_TRANSFER);
        } else if self.pre_vote {
            self.campaign(CAMPAIGN_PRE_ELECTION);
        } else {
            self.campaign(CAMPAIGN_ELECTION);
        }
    }

    fn log_vote_approve(&self, m: &Message) {
        info!(
            self.logger,
            "[logterm: {log_term}, index: {log_index}, vote: {vote}] cast vote for {from} [logterm: {msg_term}, index: {msg_index}] \
             at term {term}",
            log_term = self.raft_log.last_term(),
            log_index = self.raft_log.last_index(),
            vote = self.vote,
            from = m.from,
            msg_term = m.log_term,
            msg_index = m.index,
            term = self.term;
            "msg type" => ?m.get_msg_type(),
        );
    }

    fn log_vote_reject(&self, m: &Message) {
        info!(
            self.logger,
            "[logterm: {log_term}, index: {log_index}, vote: {vote}] rejected vote from {from} [logterm: {msg_term}, index: \
             {msg_index}] at term {term}",
            log_term = self.raft_log.last_term(),
            log_index = self.raft_log.last_index(),
            vote = self.vote,
            from = m.from,
            msg_term = m.log_term,
            msg_index = m.index,
            term = self.term;
            "msg type" => ?m.get_msg_type(),
        );
    }

    fn handle_append_response(&mut self, m: &Message) {
        let pr = match self.prs.get_mut(m.from) {
            Some(pr) => pr,
            None => {
                debug!(
                    self.logger,
                    "no progress available for {}",
                    m.from;
                );
                return;
            }
        };
        pr.recent_active = true;

        // update followers committed index via append response
        pr.update_committed(m.commit);

        if m.reject {
            debug!(
                self.r.logger,
                "received msgAppend rejection";
                "last index" => m.reject_hint,
                "from" => m.from,
                "index" => m.index,
            );

            if pr.maybe_decr_to(m.index, m.reject_hint, m.request_snapshot) {
                debug!(
                    self.r.logger,
                    "decreased progress of {}",
                    m.from;
                    "progress" => ?pr,
                );
                if pr.state == ProgressState::Replicate {
                    pr.become_probe();
                }
                self.send_append(m.from);
            }
            return;
        }

        let old_paused = pr.is_paused();
        if !pr.maybe_update(m.index) {
            return;
        }

        match pr.state {
            ProgressState::Probe => pr.become_replicate(),
            ProgressState::Snapshot => {
                if pr.maybe_snapshot_abort() {
                    debug!(
                        self.r.logger,
                        "snapshot aborted, resumed sending replication messages to {from}",
                        from = m.from;
                        "progress" => ?pr,
                    );
                    pr.become_probe();
                }
            }
            ProgressState::Replicate => pr.ins.free_to(m.get_index()),
        }

        if self.maybe_commit() {
            if self.should_bcast_commit() {
                self.bcast_append()
            }
        } else if old_paused {
            self.send_append(m.from)
        }
        // Hack to get around borrow check. It may be possible to move L1448 above L1433 to
        // get around the problem. But here choose to keep consistent with Etcd.
        let pr = self.prs.get_mut(m.from).unwrap();
        // We've updated flow control information above, which may
        // allow us to send multiple (size-limited) in-flight messages
        // at once (such as when transitioning from probe to
        // replicate, or when freeTo() covers multiple messages). If
        // we have more entries to send, send as many messages as we
        // can (without sending empty messages for the commit index)
        while self.r.maybe_send_append(m.from, pr, false, &mut self.msgs) {}

        // Transfer leadership is in progress.
        if Some(m.from) == self.r.lead_transferee {
            let last_index = self.r.raft_log.last_index();
            if pr.matched == last_index {
                info!(
                    self.logger,
                    "sent MsgTimeoutNow to {from} after received MsgAppResp",
                    from = m.from;
                );
                self.send_timeout_now(m.from);
            }
        }
    }

    fn handle_heartbeat_response(&mut self, m: &Message) {
        // Update the node. Drop the value explicitly since we'll check the qourum after.
        let pr = match self.prs.get_mut(m.from) {
            Some(pr) => pr,
            None => {
                debug!(
                    self.logger,
                    "no progress available for {}",
                    m.from;
                );
                return;
            }
        };
        // update followers committed index via heartbeat response
        pr.update_committed(m.commit);
        pr.recent_active = true;
        pr.resume();

        // free one slot for the full inflights window to allow progress.
        if pr.state == ProgressState::Replicate && pr.ins.full() {
            pr.ins.free_first_one();
        }
        // Does it request snapshot?
        if pr.matched < self.r.raft_log.last_index() || pr.pending_request_snapshot != INVALID_INDEX
        {
            self.r.send_append(m.from, pr, &mut self.msgs);
        }

        if self.read_only.option != ReadOnlyOption::Safe || m.context.is_empty() {
            return;
        }

        match self.r.read_only.recv_ack(m.from, &m.context) {
            Some(acks) if self.prs.has_quorum(acks) => {}
            _ => return,
        }

        for rs in self.r.read_only.advance(&m.context, &self.r.logger) {
            if let Some(m) = self.handle_ready_read_index(rs.req, rs.index) {
                self.r.send(m, &mut self.msgs);
            }
        }
    }

    fn handle_transfer_leader(&mut self, m: &Message) {
        if self.prs().get(m.from).is_none() {
            debug!(
                self.logger,
                "no progress available for {}",
                m.from;
            );
            return;
        }

        let from = m.from;
        if self.prs.conf().learners.contains(&from) {
            debug!(
                self.logger,
                "ignored transferring leadership";
                "to" => from,
            );
            return;
        }
        let lead_transferee = from;
        if let Some(last_lead_transferee) = self.lead_transferee {
            if last_lead_transferee == lead_transferee {
                info!(
                    self.logger,
                    "[term {term}] transfer leadership to {lead_transferee} is in progress, ignores request \
                     to same node {lead_transferee}",
                    term = self.term,
                    lead_transferee = lead_transferee;
                );
                return;
            }
            self.abort_leader_transfer();
            info!(
                self.logger,
                "[term {term}] abort previous transferring leadership to {last_lead_transferee}",
                term = self.term,
                last_lead_transferee = last_lead_transferee;
            );
        }
        if lead_transferee == self.id {
            debug!(
                self.logger,
                "already leader; ignored transferring leadership to self";
            );
            return;
        }
        // Transfer leadership to third party.
        info!(
            self.logger,
            "[term {term}] starts to transfer leadership to {lead_transferee}",
            term = self.term,
            lead_transferee = lead_transferee;
        );
        // Transfer leadership should be finished in one electionTimeout
        // so reset r.electionElapsed.
        self.election_elapsed = 0;
        self.lead_transferee = Some(lead_transferee);
        let pr = self.prs.get_mut(from).unwrap();
        if pr.matched == self.r.raft_log.last_index() {
            self.send_timeout_now(lead_transferee);
            info!(
                self.logger,
                "sends MsgTimeoutNow to {lead_transferee} immediately as {lead_transferee} already has up-to-date log",
                lead_transferee = lead_transferee;
            );
        } else {
            self.r.send_append(lead_transferee, pr, &mut self.msgs);
        }
    }

    fn handle_snapshot_status(&mut self, m: &Message) {
        let pr = match self.prs.get_mut(m.from) {
            Some(pr) => pr,
            None => {
                debug!(
                    self.logger,
                    "no progress available for {}",
                    m.from;
                );
                return;
            }
        };
        if pr.state != ProgressState::Snapshot {
            return;
        }
        if m.reject {
            pr.snapshot_failure();
            pr.become_probe();
            debug!(
                self.r.logger,
                "snapshot failed, resumed sending replication messages to {from}",
                from = m.from;
                "progress" => ?pr,
            );
        } else {
            pr.become_probe();
            debug!(
                self.r.logger,
                "snapshot succeeded, resumed sending replication messages to {from}",
                from = m.from;
                "progress" => ?pr,
            );
        }
        // If snapshot finish, wait for the msgAppResp from the remote node before sending
        // out the next msgAppend.
        // If snapshot failure, wait for a heartbeat interval before next try
        pr.pause();
        pr.pending_request_snapshot = INVALID_INDEX;
    }

    fn handle_unreachable(&mut self, m: &Message) {
        let pr = match self.prs.get_mut(m.from) {
            Some(pr) => pr,
            None => {
                debug!(
                    self.logger,
                    "no progress available for {}",
                    m.from;
                );
                return;
            }
        };
        // During optimistic replication, if the remote becomes unreachable,
        // there is huge probability that a MsgAppend is lost.
        if pr.state == ProgressState::Replicate {
            pr.become_probe();
        }
        debug!(
            self.r.logger,
            "failed to send message to {from} because it is unreachable",
            from = m.from;
            "progress" => ?pr,
        );
    }

    fn step_leader(&mut self, mut m: Message) -> Result<()> {
        // These message types do not require any progress for m.From.
        match m.get_msg_type() {
            MessageType::MsgBeat => {
                self.bcast_heartbeat();
                return Ok(());
            }
            MessageType::MsgCheckQuorum => {
                if !self.check_quorum_active() {
                    warn!(
                        self.logger,
                        "stepped down to follower since quorum is not active";
                    );
                    let term = self.term;
                    self.become_follower(term, INVALID_ID);
                }
                return Ok(());
            }
            MessageType::MsgPropose => {
                if m.entries.is_empty() {
                    fatal!(self.logger, "stepped empty MsgProp");
                }
                if !self.prs.progress().contains_key(&self.id) {
                    // If we are not currently a member of the range (i.e. this node
                    // was removed from the configuration while serving as leader),
                    // drop any new proposals.
                    return Err(Error::ProposalDropped);
                }
                if self.lead_transferee.is_some() {
                    debug!(
                        self.logger,
                        "[term {term}] transfer leadership to {lead_transferee} is in progress; dropping \
                         proposal",
                        term = self.term,
                        lead_transferee = self.lead_transferee.unwrap();
                    );
                    return Err(Error::ProposalDropped);
                }

                for (i, e) in m.mut_entries().iter_mut().enumerate() {
                    let mut cc;
                    if e.get_entry_type() == EntryType::EntryConfChange {
                        let mut cc_v1 = ConfChange::default();
                        if let Err(e) = cc_v1.merge_from_bytes(e.get_data()) {
                            error!(self.logger, "invalid confchange"; "error" => ?e);
                            return Err(Error::ProposalDropped);
                        }
                        cc = cc_v1.into_v2();
                    } else if e.get_entry_type() == EntryType::EntryConfChangeV2 {
                        cc = ConfChangeV2::default();
                        if let Err(e) = cc.merge_from_bytes(e.get_data()) {
                            error!(self.logger, "invalid confchangev2"; "error" => ?e);
                            return Err(Error::ProposalDropped);
                        }
                    } else {
                        continue;
                    }

                    let reason = if self.has_pending_conf() {
                        "possible unapplied conf change"
                    } else {
                        let already_joint = confchange::joint(self.prs.conf());
                        let want_leave = cc.changes.is_empty();
                        if already_joint && !want_leave {
                            "must transition out of joint config first"
                        } else if !already_joint && want_leave {
                            "not in joint state; refusing empty conf change"
                        } else {
                            ""
                        }
                    };

                    if reason.is_empty() {
                        self.pending_conf_index = self.raft_log.last_index() + i as u64 + 1;
                    } else {
                        info!(
                            self.logger,
                            "ignoring conf change";
                            "conf change" => ?cc,
                            "reason" => reason,
                            "config" => ?self.prs.conf(),
                            "index" => self.pending_conf_index,
                            "applied" => self.raft_log.applied,
                        );
                        *e = Entry::default();
                        e.set_entry_type(EntryType::EntryNormal);
                    }
                }
                if !self.append_entry(&mut m.mut_entries()) {
                    // return ProposalDropped when uncommitted size limit is reached
                    debug!(
                        self.logger,
                        "entries are dropped due to overlimit of max uncommitted size, uncommitted_size: {}",
                        self.uncommitted_size()
                    );
                    return Err(Error::ProposalDropped);
                }
                self.bcast_append();
                return Ok(());
            }
            MessageType::MsgReadIndex => {
                if !self.commit_to_current_term() {
                    // Reject read only request when this leader has not committed any log entry
                    // in its term.
                    return Ok(());
                }

                if self.prs().is_singleton() {
                    let read_index = self.raft_log.committed;
                    if let Some(m) = self.handle_ready_read_index(m, read_index) {
                        self.r.send(m, &mut self.msgs);
                    }
                    return Ok(());
                }

                // thinking: use an interally defined context instead of the user given context.
                // We can express this in terms of the term and index instead of
                // a user-supplied value.
                // This would allow multiple reads to piggyback on the same message.
                match self.read_only.option {
                    ReadOnlyOption::Safe => {
                        let ctx = m.entries[0].data.to_vec();
                        self.r
                            .read_only
                            .add_request(self.r.raft_log.committed, m, self.r.id);
                        self.bcast_heartbeat_with_ctx(Some(ctx));
                    }
                    ReadOnlyOption::LeaseBased => {
                        let read_index = self.raft_log.committed;
                        if let Some(m) = self.handle_ready_read_index(m, read_index) {
                            self.r.send(m, &mut self.msgs);
                        }
                    }
                }
                return Ok(());
            }
            _ => {}
        }

        match m.get_msg_type() {
            MessageType::MsgAppendResponse => {
                self.handle_append_response(&m);
            }
            MessageType::MsgHeartbeatResponse => {
                self.handle_heartbeat_response(&m);
            }
            MessageType::MsgSnapStatus => {
                self.handle_snapshot_status(&m);
            }
            MessageType::MsgUnreachable => {
                self.handle_unreachable(&m);
            }
            MessageType::MsgTransferLeader => {
                self.handle_transfer_leader(&m);
            }
            _ => {
                if self.prs().get(m.from).is_none() {
                    debug!(
                        self.logger,
                        "no progress available for {}",
                        m.from;
                    );
                }
            }
        }

        Ok(())
    }

    fn poll(&mut self, from: u64, t: MessageType, vote: bool) -> VoteResult {
        self.prs.record_vote(from, vote);
        let (gr, rj, res) = self.prs.tally_votes();
        // Unlike etcd, we log when necessary.
        if from != self.id {
            info!(
                self.logger,
                "received votes response";
                "vote" => vote,
                "from" => from,
                "rejections" => rj,
                "approvals" => gr,
                "type" => ?t,
                "term" => self.term,
            );
        }

        match res {
            VoteResult::Won => {
                if self.state == StateRole::PreCandidate {
                    self.campaign(CAMPAIGN_ELECTION);
                } else {
                    self.become_leader();
                    self.bcast_append();
                }
            }
            VoteResult::Lost => {
                // pb.MsgPreVoteResp contains future term of pre-candidate
                // m.term > self.term; reuse self.term
                let term = self.term;
                self.become_follower(term, INVALID_ID);
            }
            VoteResult::Pending => (),
        }
        res
    }

    // step_candidate is shared by state Candidate and PreCandidate; the difference is
    // whether they respond to MsgRequestVote or MsgRequestPreVote.
    fn step_candidate(&mut self, m: Message) -> Result<()> {
        match m.get_msg_type() {
            MessageType::MsgPropose => {
                info!(
                    self.logger,
                    "no leader at term {term}; dropping proposal",
                    term = self.term;
                );
                return Err(Error::ProposalDropped);
            }
            MessageType::MsgAppend => {
                debug_assert_eq!(self.term, m.term);
                self.become_follower(m.term, m.from);
                self.handle_append_entries(&m);
            }
            MessageType::MsgHeartbeat => {
                debug_assert_eq!(self.term, m.term);
                self.become_follower(m.term, m.from);
                self.handle_heartbeat(m);
            }
            MessageType::MsgSnapshot => {
                debug_assert_eq!(self.term, m.term);
                self.become_follower(m.term, m.from);
                self.handle_snapshot(m);
            }
            MessageType::MsgRequestPreVoteResponse | MessageType::MsgRequestVoteResponse => {
                // Only handle vote responses corresponding to our candidacy (while in
                // state Candidate, we may get stale MsgPreVoteResp messages in this term from
                // our pre-candidate state).
                if (self.state == StateRole::PreCandidate
                    && m.get_msg_type() != MessageType::MsgRequestPreVoteResponse)
                    || (self.state == StateRole::Candidate
                        && m.get_msg_type() != MessageType::MsgRequestVoteResponse)
                {
                    return Ok(());
                }

                self.poll(m.from, m.get_msg_type(), !m.reject);
            }
            MessageType::MsgTimeoutNow => debug!(
                self.logger,
                "{term} ignored MsgTimeoutNow from {from}",
                term = self.term,
                from = m.from;
                "state" => ?self.state,
            ),
            _ => {}
        }
        Ok(())
    }

    fn step_follower(&mut self, mut m: Message) -> Result<()> {
        match m.get_msg_type() {
            MessageType::MsgPropose => {
                if self.leader_id == INVALID_ID {
                    info!(
                        self.logger,
                        "no leader at term {term}; dropping proposal",
                        term = self.term;
                    );
                    return Err(Error::ProposalDropped);
                }
                m.to = self.leader_id;
                self.r.send(m, &mut self.msgs);
            }
            MessageType::MsgAppend => {
                self.election_elapsed = 0;
                self.leader_id = m.from;
                self.handle_append_entries(&m);
            }
            MessageType::MsgHeartbeat => {
                self.election_elapsed = 0;
                self.leader_id = m.from;
                self.handle_heartbeat(m);
            }
            MessageType::MsgSnapshot => {
                self.election_elapsed = 0;
                self.leader_id = m.from;
                self.handle_snapshot(m);
            }
            MessageType::MsgTransferLeader => {
                if self.leader_id == INVALID_ID {
                    info!(
                        self.logger,
                        "no leader at term {term}; dropping leader transfer msg",
                        term = self.term;
                    );
                    return Ok(());
                }
                m.to = self.leader_id;
                self.r.send(m, &mut self.msgs);
            }
            MessageType::MsgTimeoutNow => {
                if self.promotable {
                    info!(
                        self.logger,
                        "[term {term}] received MsgTimeoutNow from {from} and starts an election to \
                         get leadership.",
                        term = self.term,
                        from = m.from;
                    );
                    // Leadership transfers never use pre-vote even if self.pre_vote is true; we
                    // know we are not recovering from a partition so there is no need for the
                    // extra round trip.
                    self.hup(true);
                } else {
                    info!(
                        self.logger,
                        "received MsgTimeoutNow from {} but is not promotable",
                        m.from;
                    );
                }
            }
            MessageType::MsgReadIndex => {
                if self.leader_id == INVALID_ID {
                    info!(
                        self.logger,
                        "no leader at term {term}; dropping index reading msg",
                        term = self.term;
                    );
                    return Ok(());
                }
                m.to = self.leader_id;
                self.r.send(m, &mut self.msgs);
            }
            MessageType::MsgReadIndexResp => {
                if m.entries.len() != 1 {
                    error!(
                        self.logger,
                        "invalid format of MsgReadIndexResp from {}",
                        m.from;
                        "entries count" => m.entries.len(),
                    );
                    return Ok(());
                }
                let rs = ReadState {
                    index: m.index,
                    request_ctx: m.take_entries()[0].take_data(),
                };
                self.read_states.push(rs);
                // `index` and `term` in MsgReadIndexResp is the leader's commit index and its current term,
                // the log entry in the leader's commit index will always have the leader's current term,
                // because the leader only handle MsgReadIndex after it has committed log entry in its term.
                self.raft_log.maybe_commit(m.index, m.term);
            }
            _ => {}
        }
        Ok(())
    }

    /// Request a snapshot from a leader.
    pub fn request_snapshot(&mut self, request_index: u64) -> Result<()> {
        if self.state == StateRole::Leader {
            info!(
                self.logger,
                "can not request snapshot on leader; dropping request snapshot";
            );
        } else if self.leader_id == INVALID_ID {
            info!(
                self.logger,
                "drop request snapshot because of no leader";
                "term" => self.term,
            );
        } else if self.snap().is_some() {
            info!(
                self.logger,
                "there is a pending snapshot; dropping request snapshot";
            );
        } else if self.pending_request_snapshot != INVALID_INDEX {
            info!(
                self.logger,
                "there is a pending snapshot; dropping request snapshot";
            );
        } else {
            self.pending_request_snapshot = request_index;
            self.send_request_snapshot();
            return Ok(());
        }
        Err(Error::RequestSnapshotDropped)
    }

    // TODO: revoke pub when there is a better way to test.
    /// For a given message, append the entries to the log.
    pub fn handle_append_entries(&mut self, m: &Message) {
        if self.pending_request_snapshot != INVALID_INDEX {
            self.send_request_snapshot();
            return;
        }
        if m.index < self.raft_log.committed {
            debug!(
                self.logger,
                "got message with lower index than committed.";
            );
            let mut to_send = Message::default();
            to_send.set_msg_type(MessageType::MsgAppendResponse);
            to_send.to = m.from;
            to_send.index = self.raft_log.committed;
            to_send.commit = self.raft_log.committed;
            self.r.send(to_send, &mut self.msgs);
            return;
        }

        let mut to_send = Message::default();
        to_send.to = m.from;
        to_send.set_msg_type(MessageType::MsgAppendResponse);

        if let Some((_, last_idx)) = self
            .raft_log
            .maybe_append(m.index, m.log_term, m.commit, &m.entries)
        {
            to_send.set_index(last_idx);
        } else {
            debug!(
                self.logger,
                "rejected msgApp [logterm: {msg_log_term}, index: {msg_index}] \
                from {from}",
                msg_log_term = m.log_term,
                msg_index = m.index,
                from = m.from;
                "index" => m.index,
                "logterm" => ?self.raft_log.term(m.index),
            );
            to_send.index = m.index;
            to_send.reject = true;
            to_send.reject_hint = self.raft_log.last_index();
        }

        to_send.set_commit(self.raft_log.committed);
        self.r.send(to_send, &mut self.msgs);
    }

    // TODO: revoke pub when there is a better way to test.
    /// For a message, commit and send out heartbeat.
    pub fn handle_heartbeat(&mut self, mut m: Message) {
        self.raft_log.commit_to(m.commit);
        if self.pending_request_snapshot != INVALID_INDEX {
            self.send_request_snapshot();
            return;
        }
        let mut to_send = Message::default();
        to_send.set_msg_type(MessageType::MsgHeartbeatResponse);
        to_send.to = m.from;
        to_send.context = m.take_context();
        to_send.commit = self.raft_log.committed;
        self.r.send(to_send, &mut self.msgs);
    }

    fn handle_snapshot(&mut self, mut m: Message) {
        let metadata = m.get_snapshot().get_metadata();
        let (sindex, sterm) = (metadata.index, metadata.term);
        if self.restore(m.take_snapshot()) {
            info!(
                self.logger,
                "[commit: {commit}, term: {term}] restored snapshot [index: {snapshot_index}, term: {snapshot_term}]",
                term = self.term,
                commit = self.raft_log.committed,
                snapshot_index = sindex,
                snapshot_term = sterm;
            );
            let mut to_send = Message::default();
            to_send.set_msg_type(MessageType::MsgAppendResponse);
            to_send.to = m.from;
            to_send.index = self.raft_log.last_index();
            self.r.send(to_send, &mut self.msgs);
        } else {
            info!(
                self.logger,
                "[commit: {commit}] ignored snapshot [index: {snapshot_index}, term: {snapshot_term}]",
                commit = self.raft_log.committed,
                snapshot_index = sindex,
                snapshot_term = sterm;
            );
            let mut to_send = Message::default();
            to_send.set_msg_type(MessageType::MsgAppendResponse);
            to_send.to = m.from;
            to_send.index = self.raft_log.committed;
            self.r.send(to_send, &mut self.msgs);
        }
    }

    /// Recovers the state machine from a snapshot. It restores the log and the
    /// configuration of state machine.
    pub fn restore(&mut self, snap: Snapshot) -> bool {
        if snap.get_metadata().index < self.raft_log.committed {
            return false;
        }
        if self.state != StateRole::Follower {
            // This is defense-in-depth: if the leader somehow ended up applying a
            // snapshot, it could move into a new term without moving into a
            // follower state. This should never fire, but if it did, we'd have
            // prevented damage by returning early, so log only a loud warning.
            //
            // At the time of writing, the instance is guaranteed to be in follower
            // state when this method is called.
            warn!(self.logger, "non-follower attempted to restore snapshot"; "state" => ?self.state);
            self.become_follower(self.term + 1, INVALID_INDEX);
            return false;
        }

        // More defense-in-depth: throw away snapshot if recipient is not in the
        // config. This shouldn't ever happen (at the time of writing) but lots of
        // code here and there assumes that r.id is in the progress tracker.
        let meta = snap.get_metadata();
        let (snap_index, snap_term) = (meta.index, meta.term);
        let cs = meta.get_conf_state();
        if cs
            .get_voters()
            .iter()
            .chain(cs.get_learners())
            .all(|id| *id != self.id)
        {
            warn!(self.logger, "attempted to restore snapshot but it is not in the ConfState"; "conf_state" => ?cs);
            return false;
        }

        // Now go ahead and actually restore.

        if self.pending_request_snapshot == INVALID_INDEX
            && self.raft_log.match_term(meta.index, meta.term)
        {
            info!(
                self.logger,
                "fast-forwarded commit to snapshot";
                "commit" => self.raft_log.committed,
                "last_index" => self.raft_log.last_index(),
                "last_term" => self.raft_log.last_term(),
                "snapshot_index" => snap_index,
                "snapshot_term" => snap_term
            );
            self.raft_log.commit_to(meta.index);
            return false;
        }

        self.raft_log.restore(snap);
        let cs = self
            .r
            .raft_log
            .pending_snapshot()
            .unwrap()
            .get_metadata()
            .get_conf_state();

        self.prs.clear();
        let last_index = self.raft_log.last_index();
        if let Err(e) = confchange::restore(&mut self.prs, last_index, cs) {
            // This should never happen. Either there's a bug in our config change
            // handling or the client corrupted the conf change.
            fatal!(self.logger, "unable to restore config {:?}: {}", cs, e);
        }
        let new_cs = self.post_conf_change();
        let cs = self
            .r
            .raft_log
            .pending_snapshot()
            .unwrap()
            .get_metadata()
            .get_conf_state();
        if !raft_proto::conf_state_eq(cs, &new_cs) {
            fatal!(self.logger, "invalid restore: {:?} != {:?}", cs, new_cs);
        }

        // TODO: this is untested and likely unneeded
        let pr = self.prs.get_mut(self.id).unwrap();
        pr.maybe_update(pr.next_idx - 1);

        self.pending_request_snapshot = INVALID_INDEX;

        info!(
            self.logger,
            "restored snapshot";
            "commit" => self.raft_log.committed,
            "last_index" => self.raft_log.last_index(),
            "last_term" => self.raft_log.last_term(),
            "snapshot_index" => snap_index,
            "snapshot_term" => snap_term,
        );

        true
    }

    /// Updates the in-memory state and, when necessary, carries out additional actions
    /// such as reacting to the removal of nodes or changed quorum requirements.
    pub fn post_conf_change(&mut self) -> ConfState {
        info!(self.logger, "switched to configuration"; "config" => ?self.prs.conf());
        // TODO: instead of creating a conf state, validating conf state inside
        // progress tracker is better.
        let cs = self.prs.conf().to_conf_state();
        let is_voter = self.prs.conf().voters.contains(self.id);
        self.promotable = is_voter;
        if !is_voter && self.state == StateRole::Leader {
            // This node is leader and was removed or demoted. We prevent demotions
            // at the time writing but hypothetically we handle them the same way as
            // removing the leader: stepping down into the next Term.
            //
            // TODO(tbg): step down (for sanity) and ask follower with largest Match
            // to TimeoutNow (to avoid interruption). This might still drop some
            // proposals but it's better than nothing.
            //
            // TODO(tbg): test this branch. It is untested at the time of writing.
            return cs;
        }

        // The remaining steps only make sense if this node is the leader and there
        // are other nodes.
        if self.state != StateRole::Leader || cs.voters.is_empty() {
            return cs;
        }

        if self.maybe_commit() {
            // If the configuration change means that more entries are committed now,
            // broadcast/append to everyone in the updated config.
            self.bcast_append();
        } else {
            // Otherwise, still probe the newly added replicas; there's no reason to
            // let them wait out a heartbeat interval (or the next incoming proposal).
            let self_id = self.id;
            let core = &mut self.r;
            let msgs = &mut self.msgs;
            self.prs
                .iter_mut()
                .filter(|&(id, _)| *id != self_id)
                .for_each(|(id, pr)| {
                    core.maybe_send_append(*id, pr, false, msgs);
                });
        }

        // The quorum size is now smaller, consider to response some read requests.
        // If there is only one peer, all pending read requests must be responded.
        if let Some(ctx) = self.read_only.last_pending_request_ctx() {
            let prs = &self.prs;
            if self
                .r
                .read_only
                .recv_ack(self.id, &ctx)
                .map_or(false, |acks| prs.has_quorum(acks))
            {
                for rs in self.r.read_only.advance(&ctx, &self.r.logger) {
                    if let Some(m) = self.handle_ready_read_index(rs.req, rs.index) {
                        self.r.send(m, &mut self.msgs);
                    }
                }
            }
        }

        if self
            .lead_transferee
            .map_or(false, |e| !self.prs.conf().voters.contains(e))
        {
            self.abort_leader_transfer();
        }
        cs
    }

    /// Check if there is any pending confchange.
    ///
    /// This method can be false positive.
    #[inline]
    pub fn has_pending_conf(&self) -> bool {
        self.pending_conf_index > self.raft_log.applied
    }

    /// Specifies if the commit should be broadcast.
    pub fn should_bcast_commit(&self) -> bool {
        !self.skip_bcast_commit || self.has_pending_conf()
    }

    /// Indicates whether state machine can be promoted to leader,
    /// which is true when it's a voter and its own id is in progress list.
    pub fn promotable(&self) -> bool {
        self.promotable
    }

    #[doc(hidden)]
    pub fn apply_conf_change(&mut self, cc: &ConfChangeV2) -> Result<ConfState> {
        let mut changer = Changer::new(&self.prs);
        let (cfg, changes) = if cc.leave_joint() {
            changer.leave_joint()?
        } else if let Some(auto_leave) = cc.enter_joint() {
            changer.enter_joint(auto_leave, &cc.changes)?
        } else {
            changer.simple(&cc.changes)?
        };
        self.prs
            .apply_conf(cfg, changes, self.raft_log.last_index());
        Ok(self.post_conf_change())
    }

    /// Returns a read-only reference to the progress set.
    pub fn prs(&self) -> &ProgressTracker {
        &self.prs
    }

    /// Returns a mutable reference to the progress set.
    pub fn mut_prs(&mut self) -> &mut ProgressTracker {
        &mut self.prs
    }

    // TODO: revoke pub when there is a better way to test.
    /// For a given hardstate, load the state into self.
    pub fn load_state(&mut self, hs: &HardState) {
        if hs.commit < self.raft_log.committed || hs.commit > self.raft_log.last_index() {
            fatal!(
                self.logger,
                "hs.commit {} is out of range [{}, {}]",
                hs.commit,
                self.raft_log.committed,
                self.raft_log.last_index()
            )
        }
        self.raft_log.committed = hs.commit;
        self.term = hs.term;
        self.vote = hs.vote;
    }

    /// `pass_election_timeout` returns true iff `election_elapsed` is greater
    /// than or equal to the randomized election timeout in
    /// [`election_timeout`, 2 * `election_timeout` - 1].
    pub fn pass_election_timeout(&self) -> bool {
        self.election_elapsed >= self.randomized_election_timeout
    }

    /// Regenerates and stores the election timeout.
    pub fn reset_randomized_election_timeout(&mut self) {
        let prev_timeout = self.randomized_election_timeout;
        let timeout =
            rand::thread_rng().gen_range(self.min_election_timeout, self.max_election_timeout);
        debug!(
            self.logger,
            "reset election timeout {prev_timeout} -> {timeout} at {election_elapsed}",
            prev_timeout = prev_timeout,
            timeout = timeout,
            election_elapsed = self.election_elapsed;
        );
        self.randomized_election_timeout = timeout;
    }

    // check_quorum_active returns true if the quorum is active from
    // the view of the local raft state machine. Otherwise, it returns
    // false.
    // check_quorum_active also resets all recent_active to false.
    // check_quorum_active can only called by leader.
    fn check_quorum_active(&mut self) -> bool {
        let self_id = self.id;
        self.mut_prs().quorum_recently_active(self_id)
    }

    /// Issues a message to timeout immediately.
    pub fn send_timeout_now(&mut self, to: u64) {
        let msg = new_message(to, MessageType::MsgTimeoutNow, None);
        self.r.send(msg, &mut self.msgs);
    }

    /// Stops the transfer of a leader.
    pub fn abort_leader_transfer(&mut self) {
        self.lead_transferee = None;
    }

    fn send_request_snapshot(&mut self) {
        let mut m = Message::default();
        m.set_msg_type(MessageType::MsgAppendResponse);
        m.index = self.raft_log.committed;
        m.reject = true;
        m.reject_hint = self.raft_log.last_index();
        m.to = self.leader_id;
        m.request_snapshot = self.pending_request_snapshot;
        self.r.send(m, &mut self.msgs);
    }

    fn handle_ready_read_index(&mut self, mut req: Message, index: u64) -> Option<Message> {
        if req.from == INVALID_ID || req.from == self.id {
            let rs = ReadState {
                index,
                request_ctx: req.take_entries()[0].take_data(),
            };
            self.read_states.push(rs);
            return None;
        }
        let mut to_send = Message::default();
        to_send.set_msg_type(MessageType::MsgReadIndexResp);
        to_send.to = req.from;
        to_send.index = index;
        to_send.set_entries(req.take_entries());
        Some(to_send)
    }

    /// Reduce size of 'ents' from uncommitted size.
    pub fn reduce_uncommitted_size(&mut self, ents: &[Entry]) {
        // fast path for non-leader endpoint
        if self.state != StateRole::Leader {
            return;
        }

        if !self.uncommitted_state.maybe_reduce_uncommitted_size(ents) {
            // this will make self.uncommitted size not accurate.
            // but in most situation, this behaviour will not cause big problem
            warn!(
                self.r.logger,
                "try to reduce uncommitted size less than 0, first index of pending ents is {}",
                ents[0].get_index()
            );
        }
    }

    /// Increase size of 'ents' to uncommitted size. Return true when size limit
    /// is satisfied. Otherwise return false and uncommitted size remains unchanged.
    /// For raft with no limit(or non-leader raft), it always return true.
    #[inline]
    pub fn maybe_increase_uncommitted_size(&mut self, ents: &[Entry]) -> bool {
        self.uncommitted_state.maybe_increase_uncommitted_size(ents)
    }

    /// Return current uncommitted size recorded by uncommitted_state
    #[inline]
    pub fn uncommitted_size(&self) -> usize {
        self.uncommitted_state.uncommitted_size
    }
}
