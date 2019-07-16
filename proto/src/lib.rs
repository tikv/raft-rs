// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#[cfg(feature = "prost-codec")]
pub use crate::prost::eraftpb;
#[cfg(feature = "protobuf-codec")]
pub use crate::protobuf::eraftpb;

#[cfg(feature = "prost-codec")]
mod prost;
#[cfg(feature = "protobuf-codec")]
mod protobuf;

pub mod prelude {
    pub use crate::eraftpb::{
        ConfChange, ConfChangeType, ConfState, Entry, EntryType, HardState, Message, MessageType,
        Snapshot, SnapshotMetadata,
    };
}
