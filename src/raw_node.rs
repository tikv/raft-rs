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

//! The raw node of the raft module.
//!
//! This module contains the value types for the node and it's connection to other
//! nodes but not the raft consensus itself. Generally, you'll interact with the
//! RawNode first and use it to access the inner workings of the consensus protocol.

use std::{mem, collections::VecDeque};

use protobuf::Message as PbMessage;
use raft_proto::ConfChangeI;

use crate::{config::Config, StateRole};
use crate::eraftpb::{ConfState, Entry, EntryType, HardState, Message, MessageType, Snapshot};
use crate::errors::{Error, Result};
use crate::read_only::ReadState;
use crate::{Raft, SoftState, Status, Storage};
use slog::Logger;

/// Represents a Peer node in the cluster.
#[derive(Debug, Default)]
pub struct Peer {
    /// The ID of the peer.
    pub id: u64,
    /// If there is context associated with the peer (like connection information), it can be
    /// serialized and stored here.
    pub context: Option<Vec<u8>>,
}

/// The status of the snapshot.
#[derive(Debug, PartialEq, Copy, Clone)]
pub enum SnapshotStatus {
    /// Represents that the snapshot is finished being created.
    Finish,
    /// Indicates that the snapshot failed to build or is not ready.
    Failure,
}

/// Checks if certain message type should be used internally.
pub fn is_local_msg(t: MessageType) -> bool {
    match t {
        MessageType::MsgHup
        | MessageType::MsgBeat
        | MessageType::MsgUnreachable
        | MessageType::MsgSnapStatus
        | MessageType::MsgCheckQuorum => true,
        _ => false,
    }
}

fn is_response_msg(t: MessageType) -> bool {
    match t {
        MessageType::MsgAppendResponse
        | MessageType::MsgRequestVoteResponse
        | MessageType::MsgHeartbeatResponse
        | MessageType::MsgUnreachable
        | MessageType::MsgRequestPreVoteResponse => true,
        _ => false,
    }
}

/// For a given snapshot, determine if it's empty or not.
#[deprecated(since = "0.6.0", note = "Please use `Snapshot::is_empty` instead")]
pub fn is_empty_snap(s: &Snapshot) -> bool {
    s.is_empty()
}

/// Ready encapsulates the entries and messages that are ready to read,
/// be saved to stable storage, committed or sent to other peers.
/// All fields in Ready are read-only.
#[derive(Default, Debug, PartialEq)]
pub struct Ready {
    number: u64,

    ss: Option<SoftState>,

    hs: Option<HardState>,

    /// ReadStates specifies the state for read only query.
    pub read_states: Vec<ReadState>,

    /// Entries specifies entries to be saved to stable storage.
    pub entries: Vec<Entry>,

    /// Snapshot specifies the snapshot to be applied to application.
    pub snapshot: Snapshot,

    /// CommittedEntries specifies entries to be committed to a
    /// store/state-machine. These have previously been committed to stable
    /// store.
    pub committed_entries: Option<Vec<Entry>>,

    /// Messages specifies outbound messages to be sent AFTER Entries are
    /// committed to stable storage.
    /// If it contains a MsgSnap message, the application MUST report back to raft
    /// when the snapshot has been received or has failed by calling ReportSnapshot.
    pub messages: Vec<Vec<Message>>,

    must_sync: bool,
}

impl Ready {
    /// The number of current AsyncReady.
    /// It is used for identifying the different AsyncReady and AsyncReadyRecord.
    #[inline]
    pub fn number(&self) -> u64 {
        self.number
    }

    /// The current volatile state of a Node.
    /// SoftState will be nil if there is no update.
    /// It is not required to consume or store SoftState.
    #[inline]
    pub fn ss(&self) -> Option<&SoftState> {
        self.ss.as_ref()
    }

    /// The current state of a Node to be saved to stable storage BEFORE
    /// Messages are sent.
    /// HardState will be equal to empty state if there is no update.
    #[inline]
    pub fn hs(&self) -> Option<&HardState> {
        self.hs.as_ref()
    }

    /// Snapshot specifies the snapshot to be saved to stable storage.
    #[inline]
    pub fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }

    /// MustSync indicates whether the HardState and Entries must be synchronously
    /// written to disk or if an asynchronous write is permissible.
    #[inline]
    pub fn must_sync(&self) -> bool {
        self.must_sync
    }
}

/// ReadyRecord encapsulates the needed data for sync reply
#[derive(Default, Debug, PartialEq)]
struct ReadyRecord {
    number: u64,
    // (index, term) of the last entry from the entries in Ready
    last_log: Option<(u64, u64)>,
    snapshot: Snapshot,
    messages: Vec<Message>,
}

/// PersistLastReadyResult encapsulates the committed entries and messages that are ready to
/// be applied or be sent to other peers.
#[derive(Default, Debug, PartialEq)]
pub struct PersistLastReadyResult {
    /// CommittedEntries specifies entries to be committed to a
    /// store/state-machine. These have previously been committed to stable
    /// store.
    pub committed_entries: Option<Vec<Entry>>,

    /// Messages specifies outbound messages to be sent
    pub messages: Vec<Vec<Message>>,
}

/// RawNode is a thread-unsafe Node.
/// The methods of this struct correspond to the methods of Node and are described
/// more fully there.
pub struct RawNode<T: Storage> {
    /// The internal raft state.
    pub raft: Raft<T>,
    prev_ss: SoftState,
    prev_hs: HardState,
    // Current max number of RecordAsync and AsyncReadyRecord.
    max_number: u64,
    records: VecDeque<ReadyRecord>,
    // If there is a pending snapshot.
    pending_snapshot: bool,
    // Index of last persisted log
    last_persisted_index: u64,
    // Messages that need to be sent to other peers
    messages: Vec<Vec<Message>>,
}

impl<T: Storage> RawNode<T> {
    #[allow(clippy::new_ret_no_self)]
    /// Create a new RawNode given some [`Config`](../struct.Config.html).
    pub fn new(config: &Config, store: T, logger: &Logger) -> Result<Self> {
        assert_ne!(config.id, 0, "config.id must not be zero");
        let r = Raft::new(config, store, logger)?;
        let mut rn = RawNode {
            raft: r,
            prev_hs: Default::default(),
            prev_ss: Default::default(),
            max_number: 0,
            records: VecDeque::new(),
            pending_snapshot: false,
            last_persisted_index: 0,
            messages: Vec::new(),
        };
        rn.prev_hs = rn.raft.hard_state();
        rn.prev_ss = rn.raft.soft_state();
        info!(
            rn.raft.logger,
            "RawNode created with id {id}.",
            id = rn.raft.id
        );
        Ok(rn)
    }

    /// Create a new RawNode given some [`Config`](../struct.Config.html) and the default logger.
    ///
    /// The default logger is an `slog` to `log` adapter.
    #[cfg(feature = "default-logger")]
    #[allow(clippy::new_ret_no_self)]
    pub fn with_default_logger(c: &Config, store: T) -> Result<Self> {
        Self::new(c, store, &crate::default_logger())
    }

    /// Sets priority of node.
    #[inline]
    pub fn set_priority(&mut self, priority: u64) {
        self.raft.set_priority(priority);
    }

    /// Tick advances the internal logical clock by a single tick.
    ///
    /// Returns true to indicate that there will probably be some readiness which
    /// needs to be handled.
    pub fn tick(&mut self) -> bool {
        self.raft.tick()
    }

    /// Campaign causes this RawNode to transition to candidate state.
    pub fn campaign(&mut self) -> Result<()> {
        let mut m = Message::default();
        m.set_msg_type(MessageType::MsgHup);
        self.raft.step(m)
    }

    /// Propose proposes data be appended to the raft log.
    pub fn propose(&mut self, context: Vec<u8>, data: Vec<u8>) -> Result<()> {
        let mut m = Message::default();
        m.set_msg_type(MessageType::MsgPropose);
        m.from = self.raft.id;
        let mut e = Entry::default();
        e.data = data;
        e.context = context;
        m.set_entries(vec![e].into());
        self.raft.step(m)
    }

    /// Broadcast heartbeats to all the followers.
    ///
    /// If it's not leader, nothing will happen.
    pub fn ping(&mut self) {
        self.raft.ping()
    }

    /// ProposeConfChange proposes a config change.
    ///
    /// If the node enters joint state with `auto_leave` set to true, it's
    /// caller's responsibility to propose an empty conf change again to force
    /// leaving joint state.
    #[cfg_attr(feature = "cargo-clippy", allow(clippy::needless_pass_by_value))]
    pub fn propose_conf_change(&mut self, context: Vec<u8>, cc: impl ConfChangeI) -> Result<()> {
        let (data, ty) = if let Some(cc) = cc.as_v1() {
            (cc.write_to_bytes()?, EntryType::EntryConfChange)
        } else {
            (cc.as_v2().write_to_bytes()?, EntryType::EntryConfChangeV2)
        };
        let mut m = Message::default();
        m.set_msg_type(MessageType::MsgPropose);
        let mut e = Entry::default();
        e.set_entry_type(ty);
        e.data = data;
        e.context = context;
        m.set_entries(vec![e].into());
        self.raft.step(m)
    }

    /// Applies a config change to the local node. The app must call this when it
    /// applies a configuration change, except when it decides to reject the
    /// configuration change, in which case no call must take place.
    pub fn apply_conf_change(&mut self, cc: &impl ConfChangeI) -> Result<ConfState> {
        self.raft.apply_conf_change(&cc.as_v2())
    }

    /// Step advances the state machine using the given message.
    pub fn step(&mut self, m: Message) -> Result<()> {
        // ignore unexpected local messages receiving over network
        if is_local_msg(m.get_msg_type()) {
            return Err(Error::StepLocalMsg);
        }
        if self.raft.prs().get(m.from).is_some() || !is_response_msg(m.get_msg_type()) {
            return self.raft.step(m);
        }
        Err(Error::StepPeerNotFound)
    }

    /// Given an index, creates a new Ready value from that index.
    pub fn ready_since(&mut self, since_idx: Option<u64>) -> Ready {
        let raft = &mut self.raft;

        self.max_number += 1;
        let mut rd = Ready {
            number: self.max_number,
            ..Default::default()
        };
        let mut rd_record = ReadyRecord {
            number: self.max_number,
            ..Default::default()
        };

        // If there is a pending snapshot, do not get the whole Ready
        if self.pending_snapshot {
            self.records.push_back(rd_record);
            return rd;
        }

        if self.prev_ss.raft_state != StateRole::Leader && raft.state == StateRole::Leader {
            // TODO: Add more annotations
            assert_eq!(self.prev_ss.raft_state, StateRole::Candidate);
            for record in self.records.drain(..) {
                assert_eq!(record.last_log, None);
                assert!(record.snapshot.is_empty());
                if !record.messages.is_empty() {
                    self.messages.push(record.messages);
                }
            }
        }

        let ss = raft.soft_state();
        if ss != self.prev_ss {
            rd.ss = Some(ss);
        }
        let hs = raft.hard_state();
        if hs != self.prev_hs {
            if hs.vote != self.prev_hs.vote || hs.term != self.prev_hs.term || !rd.entries.is_empty() {
                rd.must_sync = true;
            }
            rd.hs = Some(hs);
        }

        if !raft.read_states.is_empty() {
            mem::swap(&mut raft.read_states, &mut rd.read_states);
        }

        rd.entries = raft.raft_log.unstable_entries().unwrap_or(&[]).to_vec();
        if let Some(e) = rd.entries.last() {
            rd_record.last_log = Some((e.get_index(), e.get_term()));
        }

        if raft.raft_log.unstable.snapshot.is_some() {
            rd.snapshot = raft.raft_log.unstable.snapshot.clone().unwrap();
            rd_record.snapshot = rd.snapshot.clone();
            self.pending_snapshot = true;
        }

        rd.committed_entries = raft
            .raft_log
            .next_entries_between(since_idx, Some(self.last_persisted_index));

        if !self.messages.is_empty() {
            mem::swap(&mut self.messages, &mut rd.messages);
        }
        if !raft.msgs.is_empty() {
            if raft.state == StateRole::Leader {
                let mut msgs = Vec::new();
                mem::swap(&mut raft.msgs, &mut msgs);
                rd.messages.push(msgs);
            } else {
                mem::swap(&mut raft.msgs, &mut rd_record.messages);
            }
        }
        self.records.push_back(rd_record);
        rd
    }

    /// Ready returns the current point-in-time state of this RawNode.
    pub fn ready(&mut self) -> Ready {
        self.ready_since(None)
    }

    /// Given an index, can determine if there is a ready state from that time.
    pub fn has_ready_since(&self, since_idx: Option<u64>) -> bool {
        if self.pending_snapshot {
            // If there is a pending snapshot, there is no ready.
            return false;
        }
        let raft = &self.raft;
        if raft.soft_state() != self.prev_ss {
            return true;
        }
        let hs = raft.hard_state();
        if hs != self.prev_hs {
            return true;
        }

        if !raft.read_states.is_empty() {
            return true;
        }

        if raft.raft_log.unstable_entries().is_some() {
            return true;
        }

        if self.snap().map_or(false, |s| !s.is_empty()) {
            return true;
        }

        if raft.raft_log.has_next_entries_between(since_idx, None) {
            return true;
        }

        if !raft.msgs.is_empty() || !self.messages.is_empty() {
            return true;
        }
        false
    }

    /// HasReady called when RawNode user need to check if any Ready pending.
    /// Checking logic in this method should be consistent with Ready.containsUpdates().
    #[inline]
    pub fn has_ready(&self) -> bool {
        self.has_ready_since(None)
    }

    fn commit_ready(&mut self, rd: Ready) {
        if rd.ss.is_some() {
            self.prev_ss = rd.ss.unwrap();
        }
        if let Some(hs) = rd.hs {
            if hs != HardState::default() {
                self.prev_hs = hs;
            }
        }
        let rd_record = self.records.back().unwrap();
        assert!(rd_record.number == rd.number);
        if let Some(e) = rd_record.last_log {
            self.raft.raft_log.stable_to(e.0, e.1);
        }
    }

    fn commit_apply(&mut self, applied: u64) {
        self.raft.commit_apply(applied);
    }

    /// Notifies that the ready of this number has been well persisted.
    pub fn on_persist_ready(&mut self, number: u64) {
        loop {
            let record = if let Some(record) = self.records.pop_front() {
                if record.number > number {
                    break;
                }
                record
            } else {
                break;
            };
            if let Some(last_log) = record.last_log {
                self.raft.on_persist_entries(last_log.0, last_log.1);
                self.last_persisted_index = last_log.0;
            }
            if !record.snapshot.is_empty() {
                self.raft
                    .raft_log
                    .stable_snap_to(record.snapshot.get_metadata().index);
                self.pending_snapshot = false;
            }
            if !record.messages.is_empty() {
                self.messages.push(record.messages);
            }
        }
    }

    /// Notifies that the last ready has been well persisted.
    /// Returns the PersistLastReadyResult that contains committed entries and messages.
    /// Note that it must be called after handling the last AsyncReady.
    pub fn on_persist_last_ready(&mut self, since_idx: Option<u64>) -> PersistLastReadyResult {
        self.on_persist_ready(self.max_number);

        let raft = &mut self.raft;
        let mut res = PersistLastReadyResult {
            committed_entries: raft
                .raft_log
                .next_entries_between(since_idx, Some(self.last_persisted_index)),
            messages: vec![],
        };

        mem::swap(&mut res.messages, &mut self.messages);
        if !raft.msgs.is_empty() && raft.state == StateRole::Leader {
            let mut msgs = Vec::new();
            mem::swap(&mut raft.msgs, &mut msgs);
            res.messages.push(msgs);
        }
        res
    }

    /// Advance notifies the RawNode that the application has applied and saved progress in the
    /// last Ready results.
    pub fn advance(&mut self, rd: Ready) -> PersistLastReadyResult {
        self.commit_ready(rd);
        // FIXME: it's wrong. now the last index of committed entries may be less than hardstate.commit
        // Maybe split it to two steps or remove it and just use advance_append and advance_apply?
        let commit_idx = self.prev_hs.commit;
        if commit_idx != 0 {
            // In most cases, prevHardSt and rd.HardState will be the same
            // because when there are new entries to apply we just sent a
            // HardState with an updated Commit value. However, on initial
            // startup the two are different because we don't send a HardState
            // until something changes, but we do send any un-applied but
            // committed entries (and previously-committed entries may be
            // incorporated into the snapshot, even if rd.CommittedEntries is
            // empty). Therefore we mark all committed entries as applied
            // whether they were included in rd.HardState or not.
            self.advance_apply(commit_idx);
        }
        self.on_persist_last_ready(None)
    }

    /// Appends and commits the ready value.
    #[inline]
    pub fn advance_append(&mut self, rd: Ready, since_idx: Option<u64>) -> PersistLastReadyResult {
        self.commit_ready(rd);
        self.on_persist_last_ready(since_idx)
    }

    /// Advance apply to the passed index.
    #[inline]
    pub fn advance_apply(&mut self, applied: u64) {
        self.commit_apply(applied);
    }

    /// Grabs the snapshot from the raft if available.
    #[inline]
    pub fn snap(&self) -> Option<&Snapshot> {
        self.raft.snap()
    }

    /// Status returns the current status of the given group.
    #[inline]
    pub fn status(&self) -> Status {
        Status::new(&self.raft)
    }

    /// ReportUnreachable reports the given node is not reachable for the last send.
    pub fn report_unreachable(&mut self, id: u64) {
        let mut m = Message::default();
        m.set_msg_type(MessageType::MsgUnreachable);
        m.from = id;
        // we don't care if it is ok actually
        let _ = self.raft.step(m);
    }

    /// ReportSnapshot reports the status of the sent snapshot.
    pub fn report_snapshot(&mut self, id: u64, status: SnapshotStatus) {
        let rej = status == SnapshotStatus::Failure;
        let mut m = Message::default();
        m.set_msg_type(MessageType::MsgSnapStatus);
        m.from = id;
        m.reject = rej;
        // we don't care if it is ok actually
        let _ = self.raft.step(m);
    }

    /// Request a snapshot from a leader.
    /// The snapshot's index must be greater or equal to the request_index.
    pub fn request_snapshot(&mut self, request_index: u64) -> Result<()> {
        self.raft.request_snapshot(request_index)
    }

    /// TransferLeader tries to transfer leadership to the given transferee.
    pub fn transfer_leader(&mut self, transferee: u64) {
        let mut m = Message::default();
        m.set_msg_type(MessageType::MsgTransferLeader);
        m.from = transferee;
        let _ = self.raft.step(m);
    }

    /// ReadIndex requests a read state. The read state will be set in ready.
    /// Read State has a read index. Once the application advances further than the read
    /// index, any linearizable read requests issued before the read request can be
    /// processed safely. The read state will have the same rctx attached.
    pub fn read_index(&mut self, rctx: Vec<u8>) {
        let mut m = Message::default();
        m.set_msg_type(MessageType::MsgReadIndex);
        let mut e = Entry::default();
        e.data = rctx;
        m.set_entries(vec![e].into());
        let _ = self.raft.step(m);
    }

    /// Returns the store as an immutable reference.
    #[inline]
    pub fn store(&self) -> &T {
        self.raft.store()
    }

    /// Returns the store as a mutable reference.
    #[inline]
    pub fn mut_store(&mut self) -> &mut T {
        self.raft.mut_store()
    }

    /// Set whether skip broadcast empty commit messages at runtime.
    #[inline]
    pub fn skip_bcast_commit(&mut self, skip: bool) {
        self.raft.skip_bcast_commit(skip)
    }

    /// Set whether to batch append msg at runtime.
    #[inline]
    pub fn set_batch_append(&mut self, batch_append: bool) {
        self.raft.set_batch_append(batch_append)
    }
}

#[cfg(test)]
mod test {
    use crate::eraftpb::MessageType;

    use super::is_local_msg;

    #[test]
    fn test_is_local_msg() {
        let tests = vec![
            (MessageType::MsgHup, true),
            (MessageType::MsgBeat, true),
            (MessageType::MsgUnreachable, true),
            (MessageType::MsgSnapStatus, true),
            (MessageType::MsgCheckQuorum, true),
            (MessageType::MsgPropose, false),
            (MessageType::MsgAppend, false),
            (MessageType::MsgAppendResponse, false),
            (MessageType::MsgRequestVote, false),
            (MessageType::MsgRequestVoteResponse, false),
            (MessageType::MsgSnapshot, false),
            (MessageType::MsgHeartbeat, false),
            (MessageType::MsgHeartbeatResponse, false),
            (MessageType::MsgTransferLeader, false),
            (MessageType::MsgTimeoutNow, false),
            (MessageType::MsgReadIndex, false),
            (MessageType::MsgReadIndexResp, false),
            (MessageType::MsgRequestPreVote, false),
            (MessageType::MsgRequestPreVoteResponse, false),
        ];
        for (msg_type, result) in tests {
            assert_eq!(is_local_msg(msg_type), result);
        }
    }
}
