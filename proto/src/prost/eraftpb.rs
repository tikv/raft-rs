/// The entry is a type of change that needs to be applied. It contains two data fields.
/// While the fields are built into the model; their usage is determined by the entry_type.
///
/// For normal entries, the data field should contain the data change that should be applied.
/// The context field can be used for any contextual data that might be relevant to the
/// application of the data.
///
/// For configuration changes, the data will contain the ConfChange message and the
/// context will provide anything needed to assist the configuration change. The context
/// if for the user to set and use in this case.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Entry {
    #[prost(enumeration = "EntryType", tag = "1")]
    pub entry_type: i32,
    #[prost(uint64, tag = "2")]
    pub term: u64,
    #[prost(uint64, tag = "3")]
    pub index: u64,
    #[prost(bytes, tag = "4")]
    pub data: std::vec::Vec<u8>,
    #[prost(bytes, tag = "6")]
    pub context: std::vec::Vec<u8>,
    /// Deprecated! It is kept for backward compatibility.
    /// TODO: remove it in the next major release.
    #[prost(bool, tag = "5")]
    pub sync_log: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SnapshotMetadata {
    #[prost(message, optional, tag = "1")]
    pub conf_state: ::std::option::Option<ConfState>,
    #[prost(message, optional, tag = "4")]
    pub pending_membership_change: ::std::option::Option<ConfState>,
    #[prost(uint64, tag = "5")]
    pub pending_membership_change_index: u64,
    #[prost(uint64, tag = "2")]
    pub index: u64,
    #[prost(uint64, tag = "3")]
    pub term: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Snapshot {
    #[prost(bytes, tag = "1")]
    pub data: std::vec::Vec<u8>,
    #[prost(message, optional, tag = "2")]
    pub metadata: ::std::option::Option<SnapshotMetadata>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Message {
    #[prost(enumeration = "MessageType", tag = "1")]
    pub msg_type: i32,
    #[prost(uint64, tag = "2")]
    pub to: u64,
    #[prost(uint64, tag = "3")]
    pub from: u64,
    #[prost(uint64, tag = "4")]
    pub term: u64,
    #[prost(uint64, tag = "5")]
    pub log_term: u64,
    #[prost(uint64, tag = "6")]
    pub index: u64,
    #[prost(message, repeated, tag = "7")]
    pub entries: ::std::vec::Vec<Entry>,
    #[prost(uint64, tag = "8")]
    pub commit: u64,
    #[prost(message, optional, tag = "9")]
    pub snapshot: ::std::option::Option<Snapshot>,
    #[prost(bool, tag = "10")]
    pub reject: bool,
    #[prost(uint64, tag = "11")]
    pub reject_hint: u64,
    #[prost(bytes, tag = "12")]
    pub context: std::vec::Vec<u8>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HardState {
    #[prost(uint64, tag = "1")]
    pub term: u64,
    #[prost(uint64, tag = "2")]
    pub vote: u64,
    #[prost(uint64, tag = "3")]
    pub commit: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ConfState {
    #[prost(uint64, repeated, tag = "1")]
    pub nodes: ::std::vec::Vec<u64>,
    #[prost(uint64, repeated, tag = "2")]
    pub learners: ::std::vec::Vec<u64>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ConfChange {
    #[prost(uint64, tag = "1")]
    pub id: u64,
    #[prost(enumeration = "ConfChangeType", tag = "2")]
    pub change_type: i32,
    /// Used in `AddNode`, `RemoveNode`, and `AddLearnerNode`.
    #[prost(uint64, tag = "3")]
    pub node_id: u64,
    #[prost(bytes, tag = "4")]
    pub context: std::vec::Vec<u8>,
    /// Used in `BeginMembershipChange` and `FinalizeMembershipChange`.
    #[prost(message, optional, tag = "5")]
    pub configuration: ::std::option::Option<ConfState>,
    /// Used in `BeginMembershipChange` and `FinalizeMembershipChange`.
    /// Because `RawNode::apply_conf_change` takes a `ConfChange` instead of an `Entry` we must
    /// include this index so it can be known.
    #[prost(uint64, tag = "6")]
    pub start_index: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum EntryType {
    EntryNormal = 0,
    EntryConfChange = 1,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum MessageType {
    MsgHup = 0,
    MsgBeat = 1,
    MsgPropose = 2,
    MsgAppend = 3,
    MsgAppendResponse = 4,
    MsgRequestVote = 5,
    MsgRequestVoteResponse = 6,
    MsgSnapshot = 7,
    MsgHeartbeat = 8,
    MsgHeartbeatResponse = 9,
    MsgUnreachable = 10,
    MsgSnapStatus = 11,
    MsgCheckQuorum = 12,
    MsgTransferLeader = 13,
    MsgTimeoutNow = 14,
    MsgReadIndex = 15,
    MsgReadIndexResp = 16,
    MsgRequestPreVote = 17,
    MsgRequestPreVoteResponse = 18,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ConfChangeType {
    AddNode = 0,
    RemoveNode = 1,
    AddLearnerNode = 2,
    BeginMembershipChange = 3,
    FinalizeMembershipChange = 4,
}
