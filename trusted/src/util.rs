use crate::StdError;
use alloc::vec::Vec;
use core::fmt;
use core::result::Result;
use raft::eraftpb::{
    ConfChange as RaftConfigChange, ConfChangeType as RaftConfigChangeType,
    ConfState as RaftConfState, Message as RaftMessage, Snapshot as RaftSnapshot,
};
use slog::{o, Logger};

#[derive(Debug)]
pub enum UtilError {
    Decoding,
    Encoding,
}

impl StdError for UtilError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        None
    }
}

impl fmt::Display for UtilError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            UtilError::Decoding => write!(f, "Failed to decode"),
            UtilError::Encoding => write!(f, "Failed to encode"),
        }
    }
}

#[cfg(feature = "std")]
pub mod log {
    extern crate slog_term;
    use super::*;
    use slog::Drain;

    pub fn create_logger(node_id: u64) -> Logger {
        let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
        Logger::root(
            slog_term::FullFormat::new(plain).build().fuse(),
            o!("raft_id" => format!("{}", node_id)),
        )
    }
}

#[cfg(not(feature = "std"))]
pub mod log {
    use super::*;
    use slog::Discard;

    pub fn create_logger(_node_id: u64) -> Logger {
        Logger::root(Discard, o!())
    }
}

#[cfg(feature = "protobuf-codec")]
pub mod raft {
    extern crate protobuf;

    use super::*;
    use prost::bytes::Bytes;
    use util::raft::protobuf::Message;

    pub fn deserialize_raft_message(message_contents: &Vec<u8>) -> Result<RaftMessage, UtilError> {
        let mut message = RaftMessage::new();
        message
            .merge_from_bytes(message_contents.as_ref())
            .map_err(|_e| UtilError::Decoding)?;
        Ok(message)
    }

    pub fn serialize_raft_message(message: &RaftMessage) -> Result<Vec<u8>, UtilError> {
        message.write_to_bytes().map_err(|_e| UtilError::Encoding)
    }

    pub fn deserialize_config_change(
        change_contents: &Bytes,
    ) -> Result<RaftConfigChange, UtilError> {
        let mut config_change = RaftConfigChange::default();
        config_change
            .merge_from_bytes(change_contents.as_ref())
            .map_err(|_e| UtilError::Decoding)?;
        Ok(config_change)
    }

    pub fn create_raft_config_change(
        node_id: u64,
        change_type: RaftConfigChangeType,
    ) -> RaftConfigChange {
        RaftConfigChange {
            change_type,
            node_id,
            ..Default::default()
        }
    }

    pub fn get_conf_state(snapshot: &RaftSnapshot) -> &RaftConfState {
        snapshot.get_metadata().get_conf_state()
    }
}

#[cfg(feature = "prost-codec")]
pub mod raft {
    use super::*;
    use prost::Message;

    pub fn deserialize_raft_message(message_contents: &Vec<u8>) -> Result<RaftMessage, UtilError> {
        RaftMessage::decode(message_contents.as_ref()).map_err(|_e| UtilError::Decoding)
    }

    pub fn serialize_raft_message(message: &RaftMessage) -> Result<Vec<u8>, UtilError> {
        Ok(message.encode_to_vec())
    }

    pub fn deserialize_config_change(
        change_contents: &Vec<u8>,
    ) -> Result<RaftConfigChange, UtilError> {
        RaftConfigChange::decode(change_contents.as_ref()).map_err(|_e| UtilError::Decoding)
    }

    pub fn create_raft_config_change(
        node_id: u64,
        change_type: RaftConfigChangeType,
    ) -> RaftConfigChange {
        RaftConfigChange {
            change_type: change_type.into(),
            node_id,
            ..Default::default()
        }
    }

    pub fn get_conf_state(snapshot: &RaftSnapshot) -> &RaftConfState {
        snapshot
            .metadata
            .as_ref()
            .unwrap()
            .conf_state
            .as_ref()
            .unwrap()
    }
}
