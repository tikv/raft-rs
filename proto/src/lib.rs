// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

pub use crate::prost::eraftpb;

mod prost;

pub mod prelude {
    pub use crate::eraftpb::{
        ConfChange, ConfChangeType, ConfState, Entry, EntryType, HardState, Message, MessageType,
        Snapshot, SnapshotMetadata,
    };
}
