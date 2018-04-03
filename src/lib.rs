// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

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

#![cfg_attr(feature = "dev", feature(plugin))]
#![cfg_attr(feature = "dev", plugin(clippy))]
#![cfg_attr(not(feature = "dev"), allow(unknown_lints))]

extern crate flat_map;
extern crate fnv;
#[macro_use]
extern crate log;
extern crate protobuf;
#[macro_use]
extern crate quick_error;
extern crate rand;

pub mod eraftpb;
mod raft_log;
pub mod storage;
mod raft;
mod progress;
mod errors;
mod log_unstable;
mod status;
pub mod raw_node;
mod read_only;
pub mod util;

pub use self::storage::{RaftState, Storage};
pub use self::errors::{Error, Result, StorageError};
pub use self::raft::{quorum, vote_resp_msg_type, Config, Raft, SoftState, StateRole, INVALID_ID,
                     INVALID_INDEX};
pub use self::raft_log::{RaftLog, NO_LIMIT};
pub use self::raw_node::{is_empty_snap, Peer, RawNode, Ready, SnapshotStatus};
pub use self::status::Status;
pub use self::log_unstable::Unstable;
pub use self::progress::{Inflights, Progress, ProgressSet, ProgressState};
pub use self::read_only::{ReadOnlyOption, ReadState};

pub mod prelude {
    //! A "prelude" for crates using the `raft` crate.
    //!
    //! This prelude is similar to the standard library's prelude in that you'll
    //! almost always want to import its entire contents, but unlike the standard
    //! library's prelude you'll have to do so manually:
    //!
    //! ```
    //! use raft::prelude::*;
    //! ```
    //!
    //! The prelude may grow over time as additional items see ubiquitous use.

    pub use eraftpb::{ConfChange, ConfChangeType, ConfState, Entry, EntryType, HardState, Message,
                      MessageType, Snapshot, SnapshotMetadata};

    pub use raft::{Config, Raft};

    pub use storage::{RaftState, Storage};

    pub use raw_node::{Peer, RawNode, Ready, SnapshotStatus};

    pub use progress::Progress;

    pub use status::Status;

    pub use read_only::{ReadOnlyOption, ReadState};
}
