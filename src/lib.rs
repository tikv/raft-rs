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

/*!

## Creating a Raft node

You can use [`RawNode::new`](raw_node/struct.RawNode.html#method.new) to create the Raft node. To create the Raft node, you need to provide a [`Storage`](storage/trait.Storage.html) component, and a [`Config`](struct.Config.html) to the [`RawNode::new`](raw_node/struct.RawNode.html#method.new) function.

```rust
use raft::{
    Config,
    storage::MemStorage,
    raw_node::RawNode,
};

// Select some defaults, then change what we need.
let id = 1;
let peers = vec![];
let storage = MemStorage::default();
let config = Config::new(id);
// ... Make any configuration changes.
// After, make sure it's valid!
config.validate().unwrap();
// We'll use the built-in `MemStorage`, but you will likely want your own.
// Finally, create our Raft node!
let mut node = RawNode::new(&config, storage, peers).unwrap();
```

## Ticking the Raft node

Use a timer to tick the Raft node at regular intervals. See the following example using Rust channel `recv_timeout` to drive the Raft node at least every 100ms, calling [`tick()`](raw_node/struct.RawNode.html#method.tick) each time.

```rust
# use raft::{Config, storage::MemStorage, raw_node::RawNode};
# let mut node = RawNode::new(&Config::new(1), MemStorage::default(), vec![]).unwrap();
use std::{sync::mpsc::{channel, RecvTimeoutError}, time::{Instant, Duration}};

// We're using a channel, but this could be any stream of events.
let (tx, rx) = channel();
let timeout = Duration::from_millis(100);

// Send the `tx` somewhere else...

let ticks = 5; // Only tick 5 times.
let mut remaining_timeout = timeout;
for _ in 0..ticks {
    let now = Instant::now();

    match rx.recv_timeout(remaining_timeout) {
        Ok(()) => {
            // Let's save this for later.
            unimplemented!()
        },
        Err(RecvTimeoutError::Timeout) => (),
        Err(RecvTimeoutError::Disconnected) => unimplemented!(),
    }
    let elapsed = now.elapsed();
    if elapsed >= remaining_timeout {
        remaining_timeout = timeout;
        // We drive Raft every 100ms.
        node.tick();
    } else {
        remaining_timeout -= elapsed;
    }
}
```

## Proposing to, and stepping the Raft node

Using the `propose` function you can drive the Raft node when the client sends a request to the Raft server. You can call `propose` to add the request to the Raft log explicitly.

In most cases, the client needs to wait for a response for the request. For example, if the client writes a value to a key and wants to know whether the write succeeds or not, but the write flow is asynchronous in Raft, so the write log entry must be replicated to other followers, then committed and at last applied to the state machine, so here we need a way to notify the client after the write is finished.

One simple way is to use a unique ID for the client request, and save the associated callback function in a hash map. When the log entry is applied, we can get the ID from the decoded entry, call the corresponding callback, and notify the client.

You can call the `step` function when you receive the Raft messages from other nodes.

Here is a simple example to use `propose` and `step`:

```rust,ignore
let mut cbs = HashMap::new();
loop {
    match receiver.recv_timeout(d) {
        Ok(Msg::Propose { id, callback }) => {
            cbs.insert(id, callback);
            node.propose(vec![id], false).unwrap();
        }
        Ok(Msg::Raft(m)) => node.step(m).unwrap(),
        // ...
    }
    //...
}
```

In the above example, we use a channel to receive the `propose` and `step` messages. We only propose the request ID to the Raft log. In your own practice, you can embed the ID in your request and propose the encoded binary request data.

## Processing the `Ready` State

When your Raft node is ticked and running, Raft should enter a `Ready` state. You need to first use `has_ready` to check whether Raft is ready. If yes, use the `ready` function to get a `Ready` state:

```rust,ignore
if !node.has_ready() {
    return;
}

// The Raft is ready, we can do something now.
let mut ready = node.ready();
```

The `Ready` state contains quite a bit of information, and you need to check and process them one by one:

1. Check whether `snapshot` is empty or not. If not empty, it means that the Raft node has received a Raft snapshot from the leader and we must apply the snapshot:

    ```rust,ignore
    if !raft::is_empty_snap(&ready.snapshot) {
        // This is a snapshot, we need to apply the snapshot at first.
        node.mut_store()
            .wl()
            .apply_snapshot(ready.snapshot.clone())
            .unwrap();
    }

    ```

2. Check whether `entries` is empty or not. If not empty, it means that there are newly added entries but has not been committed yet, we must append the entries to the Raft log:

    ```rust,ignore
    if !ready.entries.is_empty() {
        // Append entries to the Raft log
        node.mut_store().wl().append(&ready.entries).unwrap();
    }

    ```

3. Check whether `hs` is empty or not. If not empty, it means that the `HardState` of the node has changed. For example, the node may vote for a new leader, or the commit index has been increased. We must persist the changed `HardState`:

    ```rust,ignore
    if let Some(ref hs) = ready.hs {
        // Raft HardState changed, and we need to persist it.
        node.mut_store().wl().set_hardstate(hs.clone());
    }
    ```

4. Check whether `messages` is empty or not. If not, it means that the node will send messages to other nodes. There has been an optimization for sending messages: if the node is a leader, this can be done together with step 1 in parallel; if the node is not a leader, it needs to reply the messages to the leader after appending the Raft entries:

    ```rust,ignore
    if !is_leader {
        // If not leader, the follower needs to reply the messages to
        // the leader after appending Raft entries.
        let msgs = ready.messages.drain(..);
        for _msg in msgs {
            // Send messages to other peers.
        }
    }
    ```

5. Check whether `committed_entires` is empty or not. If not, it means that there are some newly committed log entries which you must apply to the state machine. Of course, after applying, you need to update the applied index and resume `apply` later:

    ```rust,ignore
    if let Some(committed_entries) = ready.committed_entries.take() {
        let mut _last_apply_index = 0;
        for entry in committed_entries {
            // Mostly, you need to save the last apply index to resume applying
            // after restart. Here we just ignore this because we use a Memory storage.
            _last_apply_index = entry.get_index();

            if entry.get_data().is_empty() {
                // Emtpy entry, when the peer becomes Leader it will send an empty entry.
                continue;
            }

            match entry.get_entry_type() {
                EntryType::EntryNormal => handle_normal(entry),
                EntryType::EntryConfChange => handle_conf_change(entry),
            }
        }
    }
    ```

6. Call `advance` to prepare for the next `Ready` state.

    ```rust,ignore
    node.advance(ready);
    ```

For more information, check out an [example](examples/single_mem_node/main.rs#L113-L179).

*/

#![cfg_attr(feature = "dev", feature(plugin))]
#![cfg_attr(feature = "dev", plugin(clippy))]
#![cfg_attr(not(feature = "dev"), allow(unknown_lints))]
#![deny(missing_docs)]

extern crate fxhash;
#[macro_use]
extern crate log;
extern crate protobuf;
#[macro_use]
extern crate quick_error;
#[cfg(test)]
extern crate env_logger;
extern crate rand;

mod config;
/// This module supplies the needed message types. However, it is autogenerated and thus cannot be
/// documented by field.
pub mod eraftpb;
mod errors;
mod log_unstable;
mod progress;
mod raft;
mod raft_log;
pub mod raw_node;
mod read_only;
mod status;
pub mod storage;
pub mod util;

pub use self::config::Config;
pub use self::errors::{Error, Result, StorageError};
pub use self::log_unstable::Unstable;
pub use self::progress::{Inflights, Progress, ProgressSet, ProgressState};
pub use self::raft::{
    quorum, vote_resp_msg_type, Raft, SoftState, StateRole, INVALID_ID, INVALID_INDEX,
};
pub use self::raft_log::{RaftLog, NO_LIMIT};
pub use self::raw_node::{is_empty_snap, Peer, RawNode, Ready, SnapshotStatus};
pub use self::read_only::{ReadOnlyOption, ReadState};
pub use self::status::Status;
pub use self::storage::{RaftState, Storage};

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

    pub use eraftpb::{
        ConfChange, ConfChangeType, ConfState, Entry, EntryType, HardState, Message, MessageType,
        Snapshot, SnapshotMetadata,
    };

    pub use config::Config;
    pub use raft::Raft;

    pub use storage::{RaftState, Storage};

    pub use raw_node::{Peer, RawNode, Ready, SnapshotStatus};

    pub use progress::Progress;

    pub use status::Status;

    pub use read_only::{ReadOnlyOption, ReadState};
}

/// Do any common test initialization. Eg set up logging, setup fail-rs.
#[cfg(test)]
pub fn setup_for_test() {
    let _ = env_logger::try_init();
}
