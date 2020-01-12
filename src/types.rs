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

use serde::{Serialize, Deserialize};

#[derive(Clone, Debug, Default, PartialEq,  Serialize, Deserialize)]
pub struct ConfState {
    pub voters: Vec<u64>,
    pub learners: Vec<u64>,
    /// The voters in the outgoing config. If not empty the node is in joint consensus.
    pub voters_outgoing: Vec<u64>,
    /// The nodes that will become learners when the outgoing config is removed.
    /// These nodes are necessarily currently in nodes_joint (or they would have
    /// been added to the incoming config right away).
    pub learners_next: Vec<u64>,
    /// If set, the config is joint and Raft will automatically transition into
    /// the final config (i.e. remove the outgoing config) when this is safe.
    pub auto_leave: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    /// The current `ConfState`.
    pub conf_state: ConfState,
    /// The applied index.
    pub index: u64,
    /// The term of the applied index.
    pub term: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Snapshot {
    pub data: Vec<u8>,
    pub metadata: SnapshotMetadata,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum MessageType {
    MsgHup,
    MsgBeat,
    MsgPropose,
    MsgAppend,
    MsgAppendResponse,
    MsgRequestVote,
    MsgRequestVoteResponse,
    MsgSnapshot,
    MsgHeartbeat,
    MsgHeartbeatResponse,
    MsgUnreachable,
    MsgSnapStatus,
    MsgCheckQuorum,
    MsgTransferLeader,
    MsgTimeoutNow,
    MsgReadIndex,
    MsgReadIndexResp,
    MsgRequestPreVote,
    MsgRequestPreVoteResponse,
}

impl Default for MessageType {
    fn default() -> Self {
        Self::MsgHeartbeat
    }
}


#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]/// Entry type
pub enum EntryType {
    /// Entry normal
    EntryNormal,
    /// Entry conf change
    EntryConfChange(ConfChange),
    /// Entry conf change v2
    EntryConfChangeV2(ConfChangeV2),
}

impl Default for EntryType {
    fn default() -> Self {
        Self::EntryNormal
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Entry {
    pub entry_type: EntryType,
    pub term: u64,
    pub index: u64,
    pub data: Vec<u8>,
    pub context: Vec<u8>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Message {
    pub message_type: MessageType,
    pub to: u64,
    pub from: u64,
    pub term: u64,
    pub log_term: u64,
    pub index: u64,
    pub entries: Vec<Entry>,
    pub commit: u64,
    pub snapshot: Snapshot,
    pub request_snapshot: u64,
    pub reject: bool,
    pub reject_hint: u64,
    pub context: Vec<u8>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct HardState {
    pub term: u64,
    pub vote: u64,
    pub commit: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ConfChangeTransition {
    /// Automatically use the simple protocol if possible, otherwise fall back
    /// to ConfChangeType::Implicit. Most applications will want to use this.
    Auto,
    /// Use joint consensus unconditionally, and transition out of them
    /// automatically (by proposing a zero configuration change).
    ///
    /// This option is suitable for applications that want to minimize the time
    /// spent in the joint configuration and do not store the joint configuration
    /// in the state machine (outside of InitialState).
    Implicit,
    /// Use joint consensus and remain in the joint configuration until the
    /// application proposes a no-op configuration change. This is suitable for
    /// applications that want to explicitly control the transitions, for example
    /// to use a custom payload (via the Context field).
    Explicit,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ConfChangeType {
    AddNode,
    RemoveNode,
    AddLearnerNode,
}

impl Default for ConfChangeType {
    fn default() -> Self {
        Self::AddNode
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ConfChange {
    pub change_type: ConfChangeType,
    pub node_id: u64,
    pub context: Vec<u8>,
    pub id: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ConfChangeSingle {
    pub change_type: ConfChangeType,
    pub node_id: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ConfChangeV2 {
    pub transition: ConfChangeTransition,
    pub changes: Vec<ConfChangeSingle>,
    pub context: Vec<u8>,
}
