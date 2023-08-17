use lazy_static::lazy_static;
use raft_proto::prelude::{ConfChange, ConfChangeV2, Message, Snapshot};
use std::sync::{Arc, RwLock};

lazy_static! {
    static ref DESERIALIZER: RwLock<Arc<dyn CustomDeserializer + Send + Sync>> =
        RwLock::new(Arc::new(DefaultDeserializer));
}

use crate::prelude::Entry;

#[derive(Debug)]
pub enum Bytes {
    Prost(Vec<u8>),
    Protobuf(bytes::Bytes),
}

impl From<Vec<u8>> for Bytes {
    fn from(vec: Vec<u8>) -> Self {
        Bytes::Prost(vec)
    }
}

impl From<bytes::Bytes> for Bytes {
    fn from(bytes: bytes::Bytes) -> Self {
        Bytes::Protobuf(bytes)
    }
}

/// Sample Docs
pub trait CustomDeserializer {
    /// Sample Docs
    fn entry_context_deserialize(&self, v: &Bytes) -> String {
        format!("{:?}", v)
    }

    /// Sample Docs
    fn entry_data_deserialize(&self, v: &Bytes) -> String {
        format!("{:?}", v)
    }

    /// Sample Docs
    fn confchangev2_context_deserialize(&self, v: &Bytes) -> String {
        format!("{:?}", v)
    }

    /// Sample Docs
    fn confchange_context_deserialize(&self, v: &Bytes) -> String {
        format!("{:?}", v)
    }

    /// Sample Docs
    fn message_context_deserializer(&self, v: &Bytes) -> String {
        format!("{:?}", v)
    }

    /// Sample Docs
    fn snapshot_data_deserializer(&self, v: &Bytes) -> String {
        format!("{:?}", v)
    }
}

struct DefaultDeserializer;
impl CustomDeserializer for DefaultDeserializer {}

/// Sample Docs
pub fn format_entry(entry: &Entry) -> String {
    let derializer = DESERIALIZER.read().unwrap();

    format!(
        "Entry {{ context: {context:}, data: {data:}, entry_type: {entry_type:?}, index: {index:}, sync_log: {sync_log:}, term: {term:} }}",
        data=derializer.entry_data_deserialize(&entry.data.clone().into()),
        context=derializer.entry_context_deserialize(&entry.context.clone().into()),
        entry_type=entry.get_entry_type(),
        index=entry.get_index(),
        sync_log=entry.get_sync_log(),
        term=entry.get_term(),
    )
}

/// Sample Docs
pub fn format_confchange(cc: &ConfChange) -> String {
    let derializer = DESERIALIZER.read().unwrap();

    format!(
        "ConfChange {{ change_type: {change_type:?}, node_id: {node_id:}, context: {context:}, id: {id:} }}",
        change_type = cc.get_change_type(),
        node_id = cc.get_node_id(),
        id = cc.get_id(),
        context = derializer.confchange_context_deserialize(&cc.context.clone().into())
    )
}

/// Sample Docs
pub fn format_confchangev2(cc: &ConfChangeV2) -> String {
    let derializer = DESERIALIZER.read().unwrap();

    format!(
        "ConfChangeV2 {{ transition: {transition:?}, changes: {changes:?}, context: {context:} }}",
        transition = cc.transition,
        changes = cc.changes,
        context = derializer.confchangev2_context_deserialize(&cc.context.clone().into())
    )
}

/// Sample Docs
pub fn format_snapshot(snapshot: &Snapshot) -> String {
    let derializer = DESERIALIZER.read().unwrap();

    format!(
        "Snapshot {{ data: {data:}, metadata: {metadata:?} }}",
        data = derializer.snapshot_data_deserializer(&snapshot.data.clone().into()),
        metadata = snapshot.metadata,
    )
}

/// Sample Docs
pub fn format_message(msg: &Message) -> String {
    let derializer = DESERIALIZER.read().unwrap();

    format!(
        "Message {{ msg_type: {msg_type:?}, to: {to:}, from: {from:}, term: {term:}, log_term: {log_term:}, index: {index:}, entries: [{entries:}], commit: {commit:}, commit_term: {commit_term:}, snapshot: {snapshot:}, request_snapshot: {request_snapshot:}, reject: {reject:}, reject_hint: {reject_hint:}, context: {context:}, deprecated_priority: {deprecated_priority:}, priority: {priority:} }}",
        msg_type=msg.get_msg_type(),
        to=msg.get_to(),
        from=msg.get_from(),
        term=msg.get_term(),
        log_term=msg.get_log_term(),
        index=msg.get_index(),
        entries=msg.get_entries().iter().map(|e| format_entry(e)).collect::<Vec<String>>().join(", "),
        commit=msg.get_commit(),
        commit_term=msg.get_commit_term(),
        snapshot=format_snapshot(msg.get_snapshot()),
        request_snapshot=msg.get_request_snapshot(),
        reject=msg.get_reject(),
        reject_hint=msg.get_reject_hint(),
        context=derializer.message_context_deserializer(&msg.context.clone().into()),
        deprecated_priority=msg.get_deprecated_priority(),
        priority=msg.get_priority(),
    )
}

/// Sample Docs
pub fn set_custom_deserializer<D: CustomDeserializer + 'static + Send + Sync>(deserializer: D) {
    let deserializer = Arc::new(deserializer);
    let mut global_deserializer = DESERIALIZER.write().unwrap();
    *global_deserializer = deserializer;
}
