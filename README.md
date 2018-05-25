# Raft-rs

[![Build Status](https://travis-ci.org/pingcap/raft-rs.svg?branch=master)](https://travis-ci.org/pingcap/raft-rs)

## Problem and Importance

When building a distributed system one principal goal is often to build in *fault-tolerance*. That is, if one particular node in a network goes down, or if there is a network partition, the entire cluster does not fall over. The cluster of nodes taking part in a distributed consensus protocol must come to agreement regarding values, and once that decision is reached, that choice is final.

Distributed Consensus Algorithms often take the form of a replicated state machine and log. Each state machine accepts inputs from its log, and represents the value(s) to be replicated, for example, a hash table. They allow a collection of machines to work as a coherent group that can survive the failures of some of its members.

Two well known Distributed Consensus Algorithms are Paxos and Raft. Paxos is used in systems like [Chubby](http://research.google.com/archive/chubby.html) by Google, and Raft is used in things like [`tikv`](https://github.com/pingcap/tikv) or [`etcd`](https://github.com/coreos/etcd/tree/master/raft). Raft is generally seen as a more understandable and simpler to implement than Paxos.

## Design

Raft replicates the state machine through logs. If you can ensure that all the machines have the same sequence of logs, after applying all logs in order, the state machine will reach a consistent state. 

A complete Raft model contains 4 essential parts: 

1. Consensus Module, the core consensus algorithm module;

2. Log, the place to keep the Raft logs;

3. State Machine, the place to save the user data;

4. Transport, the network layer for communication.

![The design of raft-rs](media/the-design-of-raft-rs.png) 

> Note: This raft-rs implementation includes the core Consensus Module only, not the other parts. The core Consensus Module in raft-rs is customizable, flexible, and resilient. You can directly use raft-rs, but you will need to build your own Log, State Machine and Transport components. 

## How to use raft-rs

Before using Raft-rs, include this project as a dependency:

```bash
[dependencies]
raft = "0.2"
```

### Step 1: Create the Raft node

You can use `RawNode::new()` to create the Raft node. To create the Raft node, you need to build a Log component, which is called Raft Storage in raft-rs, and then configure the Raft node.

1. Build Raft Storage

	Raft Storage saves all the information about the current Raft implementation, including Raft Log, commit index, the leader to vote for, etc.
	
	Storage is a trait defined in [storage.rs](src/storage.rs). See the following trait interfaces in detail:
	
	* `initial_state` is called when Raft is initialized. This interface will return a `RaftState` which contains `HardState` and `ConfState`;
	
	    * `HardState` contains the last meta information including commit index, the vote leader, and the vote term;
	
	    * `ConfState` records the current node IDs like `[1, 2, 3]` in the cluster. Every Raft node must have a unique ID in the cluster; 
	
	* `entries` returns the Log entries in an interval `[low, high)`;
	
	* `term` returns the term of the entry at Log index;
	
	* `first_index` and `last_index` return the first and last index of the Log;
	
	Pay attention to what is returned when there is no Log but it needs to get the `term` at index `first_index() - 1`. To solve this, you can use a dummy Log entry to keep the last truncated Log entry. See [`entries: vec![Entry::new()]`](src/storage.rs#L85) as a reference.
	
	* The last interface is `snapshot`, which returns a Snapshot of the current state machine. This Snapshot data will be sent to another node.

2. Configure the Raft node

	After creating the Raft Storage, use `RawNode::new()` to create the Raft node. The `new` function needs a Raft Storage and a configuration. The following fields are important for this configuration:
	
	* `id`: the unique ID of the node in the cluster, which must be unique; 
	
	* `election_tick`: how many ticks the follower re-campaigns if it doesn’t receive any message from the leader;
	
	* `heartbeat_tick`: how many ticks the leader sends the heartbeat to the followers to keep alive;
	
	* `applied`: the last applied index for the state machine. Raft will resume applying Log entries to the state machine from this index; 
	
	* `max_size_per_msg`: Raft can send many Log entries at the same time, so we need to limit the maximum size of the sending message. This is an optimization for Transport in batch;
	
	* `max_inflight_msgs`: how many messages the leader can send to the followers without acknowledgement. This is an optimization for the Transport in pipeline.
	
	* `election_tick` must be larger than `heartbeat_tick`. If our tick interval is 100 ms, we can use 10 for `election_tick` and 3 for `heartbeat_tick`, which means the leader will send heartbeat to the followers every 300 ms and the follower will re-campaign without receiving any messages after 1 second. 
	
	* The `read_only_option` enables you to choose the linearizability mode or the lease mode to read data. If you don’t care about the read consistency and want a higher read performance, you can use the lease mode.
	
	Other important fields like `check_quorum` and `pre_vote` are used to avoid the disturbance and make the cluster more stable. 
	
### Step 2: Drive and Run the Raft node

1. Use a timer to run the Raft node regularly. See the following example for using Rust channel `recv_timeout`:

    ```rust
    let mut t = Instant::now();
    let mut timeout = Duration::from_millis(100);

    loop {
        match receiver.recv_timeout(timeout) {
            Ok(...) => (),
            Err(RecvTimeoutError::Timeout) => (),
            Err(RecvTimeoutError::Disconnected) => return,
        }
        let d = t.elaspsed();
        if d >= timeout {
            t = Instant::now();
            timeout = Duration::from_millis(100);
            // We drive Raft every 100ms.
            r.tick();
        } else {
            timeout -= d;
        }
    }
    ```
    As is shown in the above example, the Raft node is driven to run every 100 ms set by the `tick` function.

2. Use the `propose` function to drive the Raft node when the client sends a request to the Raft server. You can call `propose` to add the request to the Raft log explicitly.

    In most cases, the client needs to wait for a response for the request. For example, if the client writes a value to a key and wants to know whether the write succeeds or not, but the write flow is asynchronous in Raft, so the write log entry must be replicated to other followers, then committed and at last applied to the state machine, so here we need a way to notify the client after the write is finished. 
  
    One simple way is to use a unique ID for the client request, and save the associated callback function in a hash map. When the log entry is applied, we can get the ID from the decoded entry, call the corresponding callback, and notify the client. 

3. You can call the `step` function when you receive the Raft messages from other nodes. 

    Here is a simple example to use `propose` and `step`:

    ```rust
    let mut cbs = HashMap::new();
    loop {
        match receiver.recv_timeout(d) {
            Ok(Msg::Propose { id, callback }) => {
                cbs.insert(id, callback);
                r.propose(vec![id], false).unwrap();
            }
            Ok(Msg::Raft(m)) => r.step(m).unwrap(),
            ...
        }
    ...
    }
    ```

In the above example, we use a channel to receive the `propose` and `step` messages. We only propose the request ID to the Raft log. In your own practice, you can embed the ID in your request and propose the encoded binary request data. 

### Step 3: Process the `Ready` State

When your Raft node is driven and run, Raft may enter a `Ready` state. You need to first use `has_ready` to check whether Raft is ready. If yes, use the `ready` function to get a `Ready` state:
```rust
if !r.has_ready() {
    return;
}

// The Raft is ready, we can do something now.
let mut ready = r.ready();
```

The `Ready` state contains many information, and you need to check and process them one by one:

1. Check whether `snapshot` is empty or not. If not empty, it means that the Raft node has received a Raft snapshot from the leader and we must apply the snapshot:
    ```rust
    if !raft::is_empty_snap(&ready.snapshot) {
        // This is a snapshot, we need to apply the snapshot at first.
        r.mut_store()
            .wl()
            .apply_snapshot(ready.snapshot.clone())
            .unwrap();
    }

    ```

2. Check whether `entries` is empty or not. If not empty, it means that there are newly added entries but has not been committed yet, we must append the entries to the Raft log:
    ```rust
    if !ready.entries.is_empty() {
        // Append entries to the Raft log
        r.mut_store().wl().append(&ready.entries).unwrap();
    }

    ```

3. Check whether `hs` is empty or not. If not empty, it means that the `HardState` of the node has changed. For example, the node may vote for a new leader, or the commit index has been increased. We must persist the changed `HardState`:
    ```rust
    if let Some(ref hs) = ready.hs {
        // Raft HardState changed, and we need to persist it.
        r.mut_store().wl().set_hardstate(hs.clone());
    }
    ```

4. Check whether `messages` is empty or not. If not, it means that the node will send messages to other nodes. There has been an optimization for sending messages: if the node is a leader, this can be done together with step 1 in parallel; if the node is not a leader, it needs to reply the messages to the leader after appending the Raft entries:
    ```rust
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
    ```rust
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

            if entry.get_entry_type() == EntryType::EntryNormal {
                if let Some(cb) = cbs.remove(entry.get_data().get(0).unwrap()) {
                    cb();
                }
            }

            // TODO: handle EntryConfChange
        }
    }
    ```

6. Call `advance` to prepare for the next `Ready` state.
    ```rust
    r.advance(ready);
    ```

For more information, check out an [example](examples/single_mem_node/main.rs#L113-L179). 


## Developing raft-rs

`raft` is intended to track the latest `stable`, though you'll need to use `nightly` to simulate a full CI build with `clippy`.

Using `rustup` you can get started this way:

```bash
rustup override set stable
rustup toolchain install nightly
```

In order to have your PR merged running the following must finish without error:

```bash
cargo +nightly test --features dev
```

You may optionally want to install `cargo-check` to allow for automated rebuilding while editing:

```bash
cargo watch -s "cargo check --features dev"
```

If proto file `eraftpb.proto` changed, run the command to regenerate `eraftpb.rs`:

```bash
protoc proto/eraftpb.proto --rust_out=src
```

You can check `Cargo.toml` to find which version of `protobuf-codegen` is required.

## Acknowledgments

Thanks [etcd](https://github.com/coreos/etcd) for providing the amazing Go implementation!

## Projects using raft-rs

- [TiKV](https://github.com/pingcap/tikv), a distributed transactional key value database powered by Rust and Raft.

## Links for Further Research

* [The Raft site](https://raftconsensus.github.io/)
* [The Secret Lives of Data - Raft](http://thesecretlivesofdata.com/raft/)
* [Raft Paper](http://ramcloud.stanford.edu/raft.pdf)
* [Raft Dissertation](https://github.com/ongardie/dissertation#readme)
* [Raft Refloated](https://www.cl.cam.ac.uk/~ms705/pub/papers/2015-osr-raft.pdf)
* [Implement Raft in Rust](https://www.pingcap.com/blog/implement-raft-in-rust/)
