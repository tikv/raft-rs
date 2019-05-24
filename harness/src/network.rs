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

use super::interface::Interface;
use raft::{
    eraftpb::{ConfState, Message, MessageType},
    storage::MemStorage,
    Config, Raft, Result, NO_LIMIT,
};
use rand;
use std::collections::HashMap;

/// A connection from one node to another.
///
/// Used in by `Network` for determnining drop rates on messages.
#[derive(Default, Debug, PartialEq, Eq, Hash)]
struct Connection {
    from: u64,
    to: u64,
}

/// A simulated network for testing.
///
/// You can use this to create a test network of Raft nodes.
///
/// *Please note:* no actual network calls are made.
#[derive(Default)]
pub struct Network {
    /// The set of raft peers.
    pub peers: HashMap<u64, Interface>,
    /// The storage of the raft peers.
    pub storage: HashMap<u64, MemStorage>,
    /// Drop messages from `from` to `to` at a rate of `f64`.
    dropm: HashMap<Connection, f64>,
    /// Drop messages of type `MessageType`.
    ignorem: HashMap<MessageType, bool>,
}

impl Network {
    /// Get a base config. Calling `Network::new` will initialize peers with this config.
    pub fn default_config() -> Config {
        Config {
            election_tick: 10,
            heartbeat_tick: 1,
            max_size_per_msg: NO_LIMIT,
            max_inflight_msgs: 256,
            ..Default::default()
        }
    }

    /// Initializes a network from `peers`.
    ///
    /// Nodes will recieve their ID based on their index in the vector, starting with 1.
    ///
    /// A `None` node will be replaced with a new Raft node, and its configuration will
    /// be `peers`.
    pub fn new(peers: Vec<Option<Interface>>) -> Network {
        let config = Network::default_config();
        Network::new_with_config(peers, &config)
    }

    /// Initialize a network from `peers` with explicitly specified `config`.
    pub fn new_with_config(mut peers: Vec<Option<Interface>>, config: &Config) -> Network {
        let mut nstorage = HashMap::new();
        let mut npeers = HashMap::new();

        let peer_addrs: Vec<u64> = (1..=peers.len() as u64).collect();
        for (p, id) in peers.drain(..).zip(&peer_addrs) {
            match p {
                None => {
                    let conf_state = ConfState::from((peer_addrs.clone(), vec![]));
                    let store = MemStorage::new_with_conf_state(conf_state);
                    nstorage.insert(*id, store.clone());
                    let mut config = config.clone();
                    config.id = *id;
                    config.tag = format!("{}", id);
                    let r = Raft::new(&config, store).unwrap().into();
                    npeers.insert(*id, r);
                }
                Some(r) => {
                    if r.raft.as_ref().map_or(false, |r| r.id != *id) {
                        panic!("peer {} in peers has a wrong position", r.id);
                    }
                    npeers.insert(*id, r);
                }
            }
        }
        Network {
            peers: npeers,
            storage: nstorage,
            ..Default::default()
        }
    }

    /// Ignore a given `MessageType`.
    pub fn ignore(&mut self, t: MessageType) {
        self.ignorem.insert(t, true);
    }

    /// Filter out messages that should be dropped according to rules set by `ignore` or `drop`.
    pub fn filter(&self, msgs: impl IntoIterator<Item = Message>) -> Vec<Message> {
        msgs.into_iter()
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
                    .get(&Connection {
                        from: m.from,
                        to: m.to,
                    })
                    .cloned()
                    .unwrap_or(0f64);
                rand::random::<f64>() >= perc
            })
            .collect()
    }

    pub fn read_messages(&mut self) -> Vec<Message> {
        self.peers
            .iter_mut()
            .flat_map(|(_peer, progress)| progress.read_messages())
            .collect()
    }

    /// Instruct the cluster to `step` through the given messages.
    pub fn send(&mut self, msgs: Vec<Message>) {
        let mut msgs = msgs;
        while !msgs.is_empty() {
            let mut new_msgs = vec![];
            for m in msgs.drain(..) {
                let resp = {
                    let p = self.peers.get_mut(&m.to).unwrap();
                    let _ = p.step(m);
                    p.read_messages()
                };
                new_msgs.append(&mut self.filter(resp));
            }
            msgs.append(&mut new_msgs);
        }
    }

    /// Dispatches the given messages to the appropriate peers.
    ///
    /// Unlike `send` this does not gather and send any responses. It also does not ignore errors.
    pub fn dispatch(&mut self, messages: impl IntoIterator<Item = Message>) -> Result<()> {
        for message in self.filter(messages.into_iter().map(Into::into)) {
            let to = message.to;
            let peer = self.peers.get_mut(&to).unwrap();
            peer.step(message)?;
        }
        Ok(())
    }

    /// Ignore messages from `from` to `to` at `perc` percent chance.
    ///
    /// `perc` set to `1f64` is a 100% chance, `0f64` is a 0% chance.
    pub fn drop(&mut self, from: u64, to: u64, perc: f64) {
        self.dropm.insert(Connection { from, to }, perc);
    }

    /// Cut the communication between the two given nodes.
    pub fn cut(&mut self, one: u64, other: u64) {
        self.drop(one, other, 1f64);
        self.drop(other, one, 1f64);
    }

    /// Isolate the given raft to and from all other raft in the cluster.
    pub fn isolate(&mut self, id: u64) {
        for i in 0..self.peers.len() as u64 {
            let nid = i + 1;
            if nid != id {
                self.drop(id, nid, 1.0);
                self.drop(nid, id, 1.0);
            }
        }
    }

    /// Recover the cluster conditions applied with `drop` and `ignore`.
    pub fn recover(&mut self) {
        self.dropm = HashMap::new();
        self.ignorem = HashMap::new();
    }
}
