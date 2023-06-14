// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

// Copyright 2015 CoreOS, Inc.
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

use harness::*;
use raft::eraftpb::*;
use raft::storage::MemStorage;
use raft::*;
use raft_proto::ConfChangeI;
use slog::Logger;

#[allow(clippy::declare_interior_mutable_const)]
pub const NOP_STEPPER: Option<Interface> = Some(Interface { raft: None });

pub fn ltoa(raft_log: &RaftLog<MemStorage>) -> String {
    let mut s = format!("committed: {}\n", raft_log.committed);
    s = s + &format!("applied: {}\n", raft_log.applied);
    for (i, e) in raft_log.all_entries().iter().enumerate() {
        s = s + &format!("#{}: {:?}\n", i, e);
    }
    s
}

pub fn new_storage() -> MemStorage {
    MemStorage::new()
}

pub fn new_test_config(id: u64, election_tick: usize, heartbeat_tick: usize) -> Config {
    Config {
        id,
        election_tick,
        heartbeat_tick,
        max_size_per_msg: NO_LIMIT,
        max_inflight_msgs: 256,
        ..Default::default()
    }
}

pub fn new_test_raft(
    id: u64,
    peers: Vec<u64>,
    election: usize,
    heartbeat: usize,
    storage: MemStorage,
    l: &Logger,
) -> Interface {
    let config = new_test_config(id, election, heartbeat);
    if storage.initial_state().unwrap().initialized() && peers.is_empty() {
        panic!("new_test_raft with empty peers on initialized store");
    }
    if !peers.is_empty() && !storage.initial_state().unwrap().initialized() {
        storage.initialize_with_conf_state((peers, vec![]));
    }
    new_test_raft_with_config(&config, storage, l)
}

pub fn new_test_raft_with_prevote(
    id: u64,
    peers: Vec<u64>,
    election: usize,
    heartbeat: usize,
    storage: MemStorage,
    pre_vote: bool,
    l: &Logger,
) -> Interface {
    let mut config = new_test_config(id, election, heartbeat);
    config.pre_vote = pre_vote;
    if storage.initial_state().unwrap().initialized() && peers.is_empty() {
        panic!("new_test_raft with empty peers on initialized store");
    }
    if !peers.is_empty() && !storage.initial_state().unwrap().initialized() {
        storage.initialize_with_conf_state((peers, vec![]));
    }
    new_test_raft_with_config(&config, storage, l)
}

pub fn new_test_raft_with_logs(
    id: u64,
    peers: Vec<u64>,
    election: usize,
    heartbeat: usize,
    storage: MemStorage,
    logs: &[Entry],
    l: &Logger,
) -> Interface {
    let config = new_test_config(id, election, heartbeat);
    if storage.initial_state().unwrap().initialized() && peers.is_empty() {
        panic!("new_test_raft with empty peers on initialized store");
    }
    if !peers.is_empty() && !storage.initial_state().unwrap().initialized() {
        storage.initialize_with_conf_state((peers, vec![]));
    }
    storage.wl().append(logs).unwrap();
    new_test_raft_with_config(&config, storage, l)
}

pub fn new_test_raft_with_config(config: &Config, storage: MemStorage, l: &Logger) -> Interface {
    Interface::new(Raft::new(config, storage, l).unwrap())
}

pub fn hard_state(term: u64, commit: u64, vote: u64) -> HardState {
    HardState {
        term,
        vote,
        commit,
        ..Default::default()
    }
}

pub fn soft_state(leader_id: u64, raft_state: StateRole) -> SoftState {
    SoftState {
        leader_id,
        raft_state,
    }
}

pub const SOME_DATA: Option<&'static str> = Some("somedata");

pub fn new_message_with_entries(from: u64, to: u64, ty: MessageType, ents: Vec<Entry>) -> Message {
    let mut m = Message {
        msg_type: ty,
        to,
        from,
        ..Default::default()
    };
    if !ents.is_empty() {
        m.entries = ents.into();
    }
    m
}

pub fn new_message(from: u64, to: u64, t: MessageType, n: usize) -> Message {
    let mut m = new_message_with_entries(from, to, t, vec![]);
    if n > 0 {
        let mut ents = Vec::with_capacity(n);
        for _ in 0..n {
            ents.push(new_entry(0, 0, SOME_DATA));
        }
        m.entries = ents.into();
    }
    m
}

pub fn new_entry(term: u64, index: u64, data: Option<&str>) -> Entry {
    let mut e = Entry {
        term,
        index,
        ..Default::default()
    };
    if let Some(d) = data {
        e.data = d.as_bytes().to_vec().into();
    }
    e
}

pub fn empty_entry(term: u64, index: u64) -> Entry {
    new_entry(term, index, None)
}

pub fn new_snapshot(index: u64, term: u64, voters: Vec<u64>) -> Snapshot {
    let mut s = Snapshot::default();
    s.mut_metadata().index = index;
    s.mut_metadata().term = term;
    s.mut_metadata().mut_conf_state().voters = voters;
    s
}

pub fn conf_change(ty: ConfChangeType, node_id: u64) -> ConfChange {
    ConfChange {
        change_type: ty,
        node_id,
        ..Default::default()
    }
}

pub fn remove_node(node_id: u64) -> ConfChangeV2 {
    conf_change(ConfChangeType::RemoveNode, node_id).into_v2()
}

pub fn add_node(node_id: u64) -> ConfChangeV2 {
    conf_change(ConfChangeType::AddNode, node_id).into_v2()
}

pub fn add_learner(node_id: u64) -> ConfChangeV2 {
    conf_change(ConfChangeType::AddLearnerNode, node_id).into_v2()
}

pub fn conf_state(voters: Vec<u64>, learners: Vec<u64>) -> ConfState {
    ConfState {
        voters,
        learners,
        ..Default::default()
    }
}

pub fn conf_state_v2(
    voters: Vec<u64>,
    learners: Vec<u64>,
    voters_outgoing: Vec<u64>,
    learners_next: Vec<u64>,
    auto_leave: bool,
) -> ConfState {
    let mut cs = conf_state(voters, learners);
    cs.set_voters_outgoing(voters_outgoing);
    cs.set_learners_next(learners_next);
    cs.auto_leave = auto_leave;
    cs
}

pub fn conf_change_v2(steps: Vec<ConfChangeSingle>) -> ConfChangeV2 {
    let mut cc = ConfChangeV2::default();
    cc.set_changes(steps.into());
    cc
}
