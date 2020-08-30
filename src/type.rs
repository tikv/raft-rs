
enum EntryType {
    EntryNormal = 0,
    EntryConfChange = 1,
    EntryConfChangeV2 = 2,
}

// The entry is a type of change that needs to be applied. It contains two data fields.
// While the fields are built into the model; their usage is determined by the entry_type.
//
// For normal entries, the data field should contain the data change that should be applied.
// The context field can be used for any contextual data that might be relevant to the
// application of the data.
//
// For configuration changes, the data will contain the ConfChange message and the
// context will provide anything needed to assist the configuration change. The context
// if for the user to set and use in this case.


pub struct Entry<T: Default> {
    pub entry_type: EntryType,
    pub term: u64,
    pub index: u64,
    pub context: Vec<u8>,
    pub sync_log: bool,
    pub data: T,
    // Deprecated! It is kept for backward compatibility.
    // TODO: remove it in the next major release.
}


pub struct SnapshotMetadata {
    // The current `ConfState`.
    pub conf_state: ConfState,
    // The applied index.
    pub index: u64,
    // The term of the applied index.
    pub term: u64,
}

pub struct Snapshot {
    pub data: Vec<u8>,
    pub metadata: SnapshotMetadata,
}

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

pub struct Message <T: Default> {
    pub msg_type: MessageType,
    pub to: u64,
    pub from: u64,
    pub term: u64,
    pub log_term: u64,
    pub index: u64,
    pub entries: Vec<Entry<T>>,
    pub commit: u64,
    pub snapshot: Snapshot,
    pub request_snapshot: u64,
    pub reject: bool,
    pub reject_hint: u64,
    pub context: Vec<u8>,
}

pub struct HardState {
    pub term: u64,
    pub vote: u64,
    pub commit: u64,
}

pub enum ConfChangeTransition {
    // Automatically use the simple protocol if possible, otherwise fall back
    // to ConfChangeType::Implicit. Most applications will want to use this.
    Auto = 0,
    // Use joint consensus unconditionally, and transition out of them
    // automatically (by proposing a zero configuration change).
    //
    // This option is suitable for applications that want to minimize the time
    // spent in the joint configuration and do not store the joint configuration
    // in the state machine (outside of InitialState).
    Implicit = 1,
    // Use joint consensus and remain in the joint configuration until the
    // application proposes a no-op configuration change. This is suitable for
    // applications that want to explicitly control the transitions, for example
    // to use a custom payload (via the Context field).
    Explicit = 2,
}

pub struct ConfState {
    pub voters: Vec<u64>,
    pub learners: Vec<u64>,

    // The voters in the outgoing config. If not empty the node is in joint consensus.
    pub voters_outgoing: Vec<u64>,
    // The nodes that will become learners when the outgoing config is removed.
    // These nodes are necessarily currently in nodes_joint (or they would have
    // been added to the incoming config right away).
    pub learners_next: Vec<u64>,
    // If set, the config is joint and Raft will automatically transition into
    // the final config (i.e. remove the outgoing config) when this is safe.
    pub auto_leave: bool,
}

pub enum ConfChangeType {
    AddNode    = 0,
    RemoveNode = 1,
    AddLearnerNode = 2,
}

pub struct ConfChange {
    pub id: u64,
    pub node_id: u64,
    pub change_type: ConfChangeType,
    pub context: Vec<u64>,
}

// ConfChangeSingle is an individual configuration change operation. Multiple
// such operations can be carried out atomically via a ConfChangeV2.
pub struct ConfChangeSingle {
    pub conf_type: ConfChangeType,
    pub node_id: u64,
}

// ConfChangeV2 messages initiate configuration changes. They support both the
// simple "one at a time" membership change protocol and full Joint Consensus
// allowing for arbitrary changes in membership.
//
// The supplied context is treated as an opaque payload and can be used to
// attach an action on the state machine to the application of the config change
// proposal. Note that contrary to Joint Consensus as outlined in the Raft
// paper[1], configuration changes become active when they are *applied* to the
// state machine (not when they are appended to the log).
//
// The simple protocol can be used whenever only a single change is made.
//
// Non-simple changes require the use of Joint Consensus, for which two
// configuration changes are run. The first configuration change specifies the
// desired changes and transitions the Raft group into the joint configuration,
// in which quorum requires a majority of both the pre-changes and post-changes
// configuration. Joint Consensus avoids entering fragile intermediate
// configurations that could compromise survivability. For example, without the
// use of Joint Consensus and running across three availability zones with a
// replication factor of three, it is not possible to replace a voter without
// entering an intermediate configuration that does not survive the outage of
// one availability zone.
//
// The provided ConfChangeTransition specifies how (and whether) Joint Consensus
// is used, and assigns the task of leaving the joint configuration either to
// Raft or the application. Leaving the joint configuration is accomplished by
// proposing a ConfChangeV2 with only and optionally the Context field
// populated.
//
// For details on Raft membership changes, see:
//
// [1]: https://github.com/ongardie/dissertation/blob/master/online-trim.pdf
pub struct ConfChangeV2 {
    pub transition: ConfChangeTransition,
    pub changes: Vec<ConfChangeSingle>,
    pub context: Vec<u8>,
}