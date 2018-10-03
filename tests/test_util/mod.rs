// Copyright 2018 PingCAP, Inc.
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

use env_logger;
use protobuf::RepeatedField;
use raft::eraftpb::*;
use raft::storage::MemStorage;
use raft::*;
use rand;
use std::collections::HashMap;
use std::ops::*;

/// Do any common test initialization. Eg set up logging, setup fail-rs.
pub fn setup_for_test() {
    let _ = env_logger::try_init();
}

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

pub fn new_test_config(
    id: u64,
    peers: Vec<u64>,
    election_tick: usize,
    heartbeat_tick: usize,
) -> Config {
    Config {
        id,
        peers,
        election_tick,
        heartbeat_tick,
        max_size_per_msg: NO_LIMIT,
        max_inflight_msgs: 256,
        ..Default::default()
    }
}

/// Compare to upstream, we use struct instead of trait here.
/// Because to be able to cast Interface later, we have to make
/// Raft derive Any, which will require a lot of dependencies to derive Any.
/// That's not worthy for just testing purpose.
pub struct Interface {
    pub raft: Option<Raft<MemStorage>>,
}

impl Interface {
    pub fn new(r: Raft<MemStorage>) -> Interface {
        Interface { raft: Some(r) }
    }

    pub fn step(&mut self, m: Message) -> Result<()> {
        match self.raft {
            Some(_) => Raft::step(self, m),
            None => Ok(()),
        }
    }

    pub fn read_messages(&mut self) -> Vec<Message> {
        match self.raft {
            Some(_) => self.msgs.drain(..).collect(),
            None => vec![],
        }
    }

    fn initial(&mut self, id: u64, ids: &[u64]) {
        if self.raft.is_some() {
            self.id = id;
            let prs = self.take_prs();
            self.set_prs(ProgressSet::with_capacity(
                ids.len(),
                prs.learner_ids().len(),
            ));
            for id in ids {
                let progress = Progress::default();
                if prs.learner_ids().contains(id) {
                    if let Err(e) = self.mut_prs().insert_learner(*id, progress) {
                        panic!("{}", e);
                    }
                } else if let Err(e) = self.mut_prs().insert_voter(*id, progress) {
                    panic!("{}", e);
                }
            }
            let term = self.term;
            self.reset(term);
        }
    }
}

impl Deref for Interface {
    type Target = Raft<MemStorage>;
    fn deref(&self) -> &Raft<MemStorage> {
        self.raft.as_ref().unwrap()
    }
}

impl DerefMut for Interface {
    fn deref_mut(&mut self) -> &mut Raft<MemStorage> {
        self.raft.as_mut().unwrap()
    }
}

pub fn new_test_raft(
    id: u64,
    peers: Vec<u64>,
    election: usize,
    heartbeat: usize,
    storage: MemStorage,
) -> Interface {
    Interface::new(Raft::new(&new_test_config(id, peers, election, heartbeat), storage).unwrap())
}

pub fn new_test_raft_with_prevote(
    id: u64,
    peers: Vec<u64>,
    election: usize,
    heartbeat: usize,
    storage: MemStorage,
    pre_vote: bool,
) -> Interface {
    let mut config = new_test_config(id, peers, election, heartbeat);
    config.pre_vote = pre_vote;
    new_test_raft_with_config(&config, storage)
}

pub fn new_test_raft_with_config(config: &Config, storage: MemStorage) -> Interface {
    Interface::new(Raft::new(config, storage).unwrap())
}

pub fn hard_state(t: u64, c: u64, v: u64) -> HardState {
    let mut hs = HardState::new();
    hs.set_term(t);
    hs.set_commit(c);
    hs.set_vote(v);
    hs
}

pub const SOME_DATA: Option<&'static str> = Some("somedata");

pub fn new_message_with_entries(from: u64, to: u64, t: MessageType, ents: Vec<Entry>) -> Message {
    let mut m = Message::new();
    m.set_from(from);
    m.set_to(to);
    m.set_msg_type(t);
    if !ents.is_empty() {
        m.set_entries(RepeatedField::from_vec(ents));
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
        m.set_entries(RepeatedField::from_vec(ents));
    }
    m
}

pub fn new_entry(term: u64, index: u64, data: Option<&str>) -> Entry {
    let mut e = Entry::new();
    e.set_index(index);
    e.set_term(term);
    if let Some(d) = data {
        e.set_data(d.as_bytes().to_vec());
    }
    e
}

pub fn empty_entry(term: u64, index: u64) -> Entry {
    new_entry(term, index, None)
}

pub fn new_snapshot(index: u64, term: u64, nodes: Vec<u64>) -> Snapshot {
    let mut s = Snapshot::new();
    s.mut_metadata().set_index(index);
    s.mut_metadata().set_term(term);
    s.mut_metadata().mut_conf_state().set_nodes(nodes);
    s
}

#[derive(Default, Debug, PartialEq, Eq, Hash)]
struct Connem {
    from: u64,
    to: u64,
}

#[allow(declare_interior_mutable_const)]
pub const NOP_STEPPER: Option<Interface> = Some(Interface { raft: None });

#[derive(Default)]
pub struct Network {
    pub peers: HashMap<u64, Interface>,
    pub storage: HashMap<u64, MemStorage>,
    dropm: HashMap<Connem, f64>,
    ignorem: HashMap<MessageType, bool>,
}

impl Network {
    // initializes a network from peers.
    // A nil node will be replaced with a new *stateMachine.
    // A *stateMachine will get its k, id.
    // When using stateMachine, the address list is always [1, n].
    pub fn new(peers: Vec<Option<Interface>>) -> Network {
        Network::new_with_config(peers, false)
    }

    // new_with_config is like new but sets the configuration pre_vote explicitly
    // for any state machines it creates.
    pub fn new_with_config(mut peers: Vec<Option<Interface>>, pre_vote: bool) -> Network {
        let size = peers.len();
        let peer_addrs: Vec<u64> = (1..size as u64 + 1).collect();
        let mut nstorage = HashMap::new();
        let mut npeers = HashMap::new();
        for (p, id) in peers.drain(..).zip(peer_addrs.clone()) {
            match p {
                None => {
                    nstorage.insert(id, new_storage());
                    let r = new_test_raft_with_prevote(
                        id,
                        peer_addrs.clone(),
                        10,
                        1,
                        nstorage[&id].clone(),
                        pre_vote,
                    );
                    npeers.insert(id, r);
                }
                Some(mut p) => {
                    p.initial(id, &peer_addrs);
                    npeers.insert(id, p);
                }
            }
        }
        Network {
            peers: npeers,
            storage: nstorage,
            ..Default::default()
        }
    }

    pub fn ignore(&mut self, t: MessageType) {
        self.ignorem.insert(t, true);
    }

    pub fn filter(&self, mut msgs: Vec<Message>) -> Vec<Message> {
        msgs.drain(..)
            .filter(|m| {
                if self
                    .ignorem
                    .get(&m.get_msg_type())
                    .cloned()
                    .unwrap_or(false)
                {
                    return false;
                }
                // hups never go over the network, so don't drop them but panic
                assert_ne!(m.get_msg_type(), MessageType::MsgHup, "unexpected msgHup");
                let perc = self
                    .dropm
                    .get(&Connem {
                        from: m.get_from(),
                        to: m.get_to(),
                    }).cloned()
                    .unwrap_or(0f64);
                rand::random::<f64>() >= perc
            }).collect()
    }

    pub fn send(&mut self, msgs: Vec<Message>) {
        let mut msgs = msgs;
        while !msgs.is_empty() {
            let mut new_msgs = vec![];
            for m in msgs.drain(..) {
                let resp = {
                    let p = self.peers.get_mut(&m.get_to()).unwrap();
                    let _ = p.step(m);
                    p.read_messages()
                };
                new_msgs.append(&mut self.filter(resp));
            }
            msgs.append(&mut new_msgs);
        }
    }

    pub fn drop(&mut self, from: u64, to: u64, perc: f64) {
        self.dropm.insert(Connem { from, to }, perc);
    }

    pub fn cut(&mut self, one: u64, other: u64) {
        self.drop(one, other, 1f64);
        self.drop(other, one, 1f64);
    }

    pub fn isolate(&mut self, id: u64) {
        for i in 0..self.peers.len() as u64 {
            let nid = i + 1;
            if nid != id {
                self.drop(id, nid, 1.0);
                self.drop(nid, id, 1.0);
            }
        }
    }

    pub fn recover(&mut self) {
        self.dropm = HashMap::new();
        self.ignorem = HashMap::new();
    }
}
