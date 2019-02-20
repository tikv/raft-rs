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
    eraftpb::{Message, MessageType},
    storage::MemStorage,
    Config, Raft, Result, NO_LIMIT,
};
use rand;
use std::collections::HashMap;

#[derive(Default, Debug, PartialEq, Eq, Hash)]
struct Connem {
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
    dropm: HashMap<Connem, f64>,
    ignorem: HashMap<MessageType, bool>,
}

impl Network {
    /// Initializes a network from peers.
    ///
    /// Nodes will recieve their ID based on their index in the vector, starting with 1.
    ///
    /// A `None` node will be replaced with a new Raft node.
    pub fn new(peers: Vec<Option<Interface>>) -> Network {
        Network::new_with_config(peers, false)
    }

    /// Explicitly set the pre_vote option on newly created rafts.
    ///
    /// **TODO:** Make this accept any config.
    pub fn new_with_config(mut peers: Vec<Option<Interface>>, pre_vote: bool) -> Network {
        let size = peers.len();
        let peer_addrs: Vec<u64> = (1..=size as u64).collect();
        let mut nstorage = HashMap::new();
        let mut npeers = HashMap::new();
        for (p, id) in peers.drain(..).zip(peer_addrs.clone()) {
            match p {
                None => {
                    nstorage.insert(id, MemStorage::default());
                    let r = Interface::new(
                        Raft::new(
                            &Config {
                                id,
                                peers: peer_addrs.clone(),
                                election_tick: 10,
                                heartbeat_tick: 1,
                                max_size_per_msg: NO_LIMIT,
                                max_inflight_msgs: 256,
                                pre_vote,
                                ..Default::default()
                            },
                            nstorage[&id].clone(),
                        )
                        .unwrap(),
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
                    .get(&Connem {
                        from: m.get_from(),
                        to: m.get_to(),
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
                    let p = self.peers.get_mut(&m.get_to()).unwrap();
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
            let to = message.get_to();
            let peer = self.peers.get_mut(&to).unwrap();
            peer.step(message)?;
        }
        Ok(())
    }

    /// Ignore messages from `from` to `to` at `perc` percent chance.
    ///
    /// `perc` set to `1f64` is a 100% chance, `0f64` is a 0% chance.
    pub fn drop(&mut self, from: u64, to: u64, perc: f64) {
        self.dropm.insert(Connem { from, to }, perc);
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
