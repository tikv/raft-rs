// Copyright 2019 PingCAP, Inc.
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
//

use std::ops::{Deref, DerefMut};

use harness::Network;
use protobuf::Message as PbMessage;
use raft::{
    default_logger,
    eraftpb::{
        ConfChange, ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2,
        ConfState, Entry, EntryType, Message, MessageType,
    },
    storage::{MemStorage, Storage},
    Config, HashMap, HashSet, Raft, Result,
};

use crate::test_util::new_message;

// Test that the API itself works.
//
// * Errors are returned from isuse.
// * Happy path returns happy values.
mod api {
    use super::*;

    // Test that the cluster can transition from a single node to a whole cluster.
    #[test]
    fn can_transition() -> Result<()> {
        let base = ConfState::from((vec![1], vec![]));
        let target = ConfState::from((vec![1, 2, 3], vec![4]));
        let store = MemStorage::new_with_conf_state(base.clone());
        let mut raft = Raft::new(&Config::new(1), store, &default_logger())?;

        let cc = generate_conf_change_v2(&base, &target);
        raft.apply_conf_change(&cc)?;
        assert_eq!(
            raft.prs().to_conf_state(),
            ConfState::transition(&base, &target)
        );

        raft.apply_conf_change(&ConfChangeV2::default())?;
        assert_eq!(raft.prs().to_conf_state(), target);
        Ok(())
    }

    // Test that the cluster can transition with all actions.
    #[test]
    fn can_transition_complex() -> Result<()> {
        // Add voter 16, learner 17, remove voter 12, learner 15, promote 14 and demote 13.
        let base = ConfState::from((vec![11, 12, 13], vec![14, 15]));
        let target = ConfState::from((vec![11, 14, 16], vec![13, 17]));
        let store = MemStorage::new_with_conf_state(base.clone());
        let mut raft = Raft::new(&Config::new(11), store, &default_logger())?;

        let cc = generate_conf_change_v2(&base, &target);
        raft.apply_conf_change(&cc)?;
        assert_eq!(
            raft.prs().to_conf_state(),
            ConfState::transition(&base, &target)
        );

        raft.apply_conf_change(&ConfChangeV2::default())?;
        assert_eq!(raft.prs().to_conf_state(), target);
        Ok(())
    }

    // Test if the process rejects an overlapping voter and learner set.
    #[test]
    fn checks_for_overlapping_membership() -> Result<()> {
        let base = ConfState::from((vec![1], vec![]));
        let target = ConfState::from((vec![1, 2, 3], vec![1, 2, 3]));
        let store = MemStorage::new_with_conf_state(base.clone());
        let mut raft = Raft::new(&Config::new(1), store, &default_logger())?;

        let cc = generate_conf_change_v2(&base, &target);
        assert!(raft.apply_conf_change(&cc).is_err());
        assert_eq!(raft.prs().to_conf_state(), base);
        Ok(())
    }

    // Test leave joint many times is allowed.
    #[test]
    fn test_redundant_leave_joint() -> Result<()> {
        let base = ConfState::from((vec![1, 2, 3], vec![4]));
        let store = MemStorage::new_with_conf_state(base.clone());
        let mut raft = Raft::new(&Config::new(1), store, &default_logger())?;
        for _ in 0..3 {
            assert!(raft.apply_conf_change(&ConfChangeV2::default()).is_err());
            assert_eq!(raft.prs().to_conf_state(), base);
        }
        Ok(())
    }
}

#[test]
fn test_transition_complex_auto() -> Result<()> {
    test_transition_complex(ConfChangeTransition::Auto)
}

#[test]
fn test_transition_complex_implict() -> Result<()> {
    test_transition_complex(ConfChangeTransition::Implicit)
}

fn test_transition_complex(transition: ConfChangeTransition) -> Result<()> {
    let l = default_logger();
    let base = ConfState::from((vec![1, 2, 3], vec![4, 5]));
    let target = ConfState::from((vec![1, 4, 6], vec![3, 7]));
    let mut scenario = Scenario::transition(1, base, target, &l)?;

    info!(l, "Proposing configuration change");
    scenario.propose_change_v2(transition);
    info!(l, "Advancing peers {:?} to enter joint", &[1, 2, 3, 4, 5]);
    scenario.handle_raft_readies(&[1, 2, 3, 4, 5]);
    scenario.must_in_joint(&[1, 2, 3, 4, 5]);

    let msgs = scenario.read_messages();
    scenario.filter_and_send(msgs);

    info!(l, "Leaving joint...");
    scenario.handle_raft_readies(&[1, 2, 3, 4, 5, 6, 7]);

    // FIXME: Learner 5 can't know it's removed.
    scenario.must_leave_joint(&[1, 2, 3, 4, 6, 7]);
    Ok(())
}

#[test]
fn test_transition_simple_auto() -> Result<()> {
    test_transition_simple(ConfChangeTransition::Auto)
}

#[test]
fn test_transition_simple_implict() -> Result<()> {
    test_transition_simple(ConfChangeTransition::Implicit)
}

fn test_transition_simple(transition: ConfChangeTransition) -> Result<()> {
    let l = default_logger();
    let base = ConfState::from((vec![1, 2, 3], vec![4]));

    for (i, target) in vec![
        ConfState::from((vec![1, 2, 3], vec![4, 5])), // Add learner.
        ConfState::from((vec![1, 2, 3, 5], vec![4])), // Add voter.
        ConfState::from((vec![1, 2, 3, 4], vec![])),  // Promote learner.
        ConfState::from((vec![1, 2, 3], vec![])),     // Remove learner.
        ConfState::from((vec![1, 2], vec![4])),       // Remove voter.
    ]
    .into_iter()
    .enumerate()
    {
        let mut scenario = Scenario::transition(1, base.clone(), target, &l)?;
        info!(l, "Proposing configuration change");
        scenario.propose_change_v2(transition);
        info!(l, "Advancing peers {:?} to enter joint", &[1, 2, 3, 4]);
        scenario.handle_raft_readies(&[1, 2, 3, 4]);

        if let ConfChangeTransition::Auto = transition {
            // Peer 5 can't know it's added until it receives a snapshot.
            scenario.must_leave_joint(&[1, 2, 3, 4]);
        } else {
            scenario.must_in_joint(&[1, 2, 3, 4]);
            let msgs = scenario.read_messages();
            scenario.filter_and_send(msgs);

            info!(l, "Leaving joint...");
            if scenario.peers.contains_key(&5) {
                scenario.handle_raft_readies(&[1, 2, 3, 4, 5]);
            } else {
                scenario.handle_raft_readies(&[1, 2, 3, 4]);
            };

            if i == 3 {
                // FIXME: Learner 4 can't know it's removed.
                scenario.must_leave_joint(&[1, 2, 3]);
            } else if i == 0 || i == 1 {
                scenario.must_leave_joint(&[1, 2, 3, 4, 5]);
            } else {
                scenario.must_leave_joint(&[1, 2, 3, 4]);
            }
        }
    }

    Ok(())
}

#[test]
fn test_remove_leader_auto() -> Result<()> {
    test_remove_leader(ConfChangeTransition::Auto)
}

#[test]
fn test_remove_leader_implict() -> Result<()> {
    test_remove_leader(ConfChangeTransition::Implicit)
}

// Ported from https://github.com/etcd-io/etcd/blob/master/raft/testdata/confchange_v1_remove_leader.txt.
fn test_remove_leader(transition: ConfChangeTransition) -> Result<()> {
    let l = default_logger();
    let base = ConfState::from((vec![1, 2, 3], vec![]));
    let target = ConfState::from((vec![2, 3], vec![]));
    let mut scenario = Scenario::transition(1, base, target, &l)?;

    info!(l, "Proposing configuration change");
    scenario.propose_change_v2(transition);

    let normal_entry_index = if let ConfChangeTransition::Auto = transition {
        info!(l, "Proposing a normal entry, and then broadcast it");
        let msgs = scenario.generate_normal_proposal(b"hello, world".to_vec());
        scenario.dispatch(msgs).unwrap();
        scenario.read_and_dispatch_messages_from(&[1]);

        info!(l, "Advancing peers {:?}", &[1, 2, 3]);
        scenario.handle_raft_readies(&[1, 2, 3]);
        scenario.must_leave_joint(&[1, 2, 3]);
        4
    } else {
        scenario.handle_raft_readies(&[1, 2, 3]);
        scenario.must_in_joint(&[1, 2, 3]);
        let msgs = scenario.read_messages();
        scenario.filter_and_send(msgs);

        info!(l, "Proposing a normal entry, and then broadcast it");
        let msgs = scenario.generate_normal_proposal(b"hello, world".to_vec());
        scenario.dispatch(msgs).unwrap();
        scenario.read_and_dispatch_messages_from(&[1]);

        info!(l, "Leaving joint...");
        scenario.handle_raft_readies(&[1, 2, 3]);
        5
    };

    // After the old leader has receives append responses from 2 and 3,
    // it can commit the entry following the conf change which removes itself.
    scenario.read_and_dispatch_messages_from(&[2]);
    {
        let r = scenario.peers[&1].raft.as_ref().unwrap();
        assert!(r.raft_log.next_entries().is_none());
    }
    scenario.read_and_dispatch_messages_from(&[3]);
    {
        let r = scenario.peers[&1].raft.as_ref().unwrap();
        let committed = r.raft_log.next_entries().unwrap().to_vec();
        let index = committed.first().map(|e| e.get_index()).unwrap();
        assert_eq!(index, normal_entry_index);
    }

    // TODO: Can't elect a new leader because the old one can still send heartbeats.
    info!(l, "Prompting after leader is removed.");
    scenario.tick_until_election(&[1, 2, 3]);
    let leaders: Vec<_> = scenario.peer_leaders().values().cloned().collect();
    assert_eq!(leaders, vec![1, 1, 1]);

    Ok(())
}

// Test followers can leave joint correctly with snapshots.
#[test]
fn test_leave_joint_by_snapshot() -> Result<()> {
    let l = default_logger();
    let base = ConfState::from((vec![1, 2, 3], vec![]));
    let target = ConfState::from((vec![1, 2, 3], vec![4]));
    let mut scenario = Scenario::transition(1, base, target, &l)?;

    info!(l, "Proposing configuration change");
    scenario.propose_change_v2(ConfChangeTransition::Explicit);
    info!(l, "Advancing peers {:?} to enter joint", &[1, 2, 3]);
    scenario.handle_raft_readies(&[1, 2, 3]);
    scenario.must_in_joint(&[1, 2, 3]);

    scenario.propose_normal(b"hello, world".to_vec());
    scenario.must_in_joint(&[4]);

    // Isolate 4 and then leave joint on other peers.
    scenario.isolate(4);
    for _ in 0..3 {
        scenario.propose_normal(b"hello, world".to_vec());
    }
    info!(l, "Proposing leave joint");
    scenario.propose_leave_joint();
    scenario.handle_raft_readies(&[1, 2, 3]);
    scenario.must_leave_joint(&[1, 2, 3]);

    let s1 = scenario.peers[&1].raft.as_ref().unwrap().store().clone();
    let last_index = s1.last_index()?;
    s1.wl().compact(last_index)?;

    scenario.recover();
    scenario.propose_normal(b"hello, world".to_vec());
    scenario.handle_raft_readies(&[4]);
    scenario.must_leave_joint(&[4]);

    Ok(())
}

// Test peers recover before the leave entry has been persisted.
#[test]
fn test_recover_before_leave() -> Result<()> {
    let l = default_logger();
    let base = ConfState::from((vec![1, 2, 3], vec![]));
    let target = ConfState::from((vec![1, 2, 3, 4], vec![]));
    let mut scenario = Scenario::transition(1, base, target, &l)?;

    info!(l, "Proposing configuration change");
    scenario.propose_change_v2(ConfChangeTransition::Implicit);
    info!(l, "Advancing peers {:?} to enter joint", &[1, 2, 3]);
    scenario.handle_raft_readies(&[1, 2, 3]);
    scenario.must_in_joint(&[1, 2, 3]);

    // After power cycle, the new leader will broad cast the leave entry.
    scenario.power_cycle(&[1, 2, 3], 2)?;
    scenario.handle_raft_readies(&[1, 2, 3]);
    let msgs = scenario.read_messages();
    scenario.send(msgs);

    scenario.handle_raft_readies(&[1, 2, 3]);
    scenario.must_leave_joint(&[1, 2, 3]);

    Ok(())
}

/// A test harness providing some useful utility and shorthand functions appropriate for this test suite.
///
/// Since it derefs into `Network` it can be used the same way. So it acts as a transparent set of utilities over the standard `Network`.
/// The goal here is to boil down the test suite for Joint Consensus into the simplest terms possible, while allowing for control.
struct Scenario {
    leader: u64,
    base: ConfState,
    target: ConfState,
    auto_leave: bool,
    network: Network,
    logger: slog::Logger,
}

impl Deref for Scenario {
    type Target = Network;
    fn deref(&self) -> &Network {
        &self.network
    }
}

impl DerefMut for Scenario {
    fn deref_mut(&mut self) -> &mut Network {
        &mut self.network
    }
}

impl Scenario {
    /// Create a new scenario in transition with the given state.
    fn transition(
        leader: u64,
        base: ConfState,
        target: ConfState,
        logger: &slog::Logger,
    ) -> Result<Scenario> {
        info!(logger, "Beginning scenario"; "base" => ?base, "target" => ?target);

        let mut starting_peers = Vec::new();
        let mut pending_peers = HashSet::default();
        pending_peers.extend(target.get_voters().iter().cloned());
        pending_peers.extend(target.get_learners().iter().cloned());

        for id in base.get_voters().iter().chain(base.get_learners()) {
            let store = MemStorage::new_with_conf_state(base.clone());
            let cfg = Self::generate_config(*id, &store);
            let n = Some(Raft::new(&cfg, store, logger)?.into());
            starting_peers.push(n);
            pending_peers.remove(id);
        }

        let mut network = Network::new(starting_peers, logger);
        for id in pending_peers {
            let store = MemStorage::default();
            let cfg = Self::generate_config(id, &store);
            let r = Raft::new(&cfg, store, logger)?.into();
            network.insert(id, r);
        }

        let mut scenario = Scenario {
            leader,
            base: base,
            target: target,
            auto_leave: true,
            network,
            logger: logger.clone(),
        };

        let message = new_message(leader, leader, MessageType::MsgHup, 0);
        scenario.send(vec![message]);
        Ok(scenario)
    }

    /// Return the leader id according to each peer.
    fn peer_leaders(&self) -> HashMap<u64, u64> {
        self.peers
            .iter()
            .map(|(&id, peer)| (id, peer.leader_id))
            .collect()
    }

    fn generate_conf_change_v2(&self, transition: ConfChangeTransition) -> Vec<Message> {
        let mut cc = generate_conf_change_v2(&self.base, &self.target);
        cc.set_transition(transition);
        let message = {
            let data = cc.write_to_bytes().unwrap();
            let mut m = Message::default();
            m.set_msg_type(MessageType::MsgPropose);
            let mut e = Entry::default();
            e.set_entry_type(EntryType::EntryConfChangeV2);
            e.data = data;
            m.set_entries(vec![e].into());
            m.to = self.leader;
            m
        };
        vec![message]
    }

    fn generate_normal_proposal(&self, data: Vec<u8>) -> Vec<Message> {
        let message = {
            let mut m = Message::default();
            m.set_msg_type(MessageType::MsgPropose);
            let mut e = Entry::default();
            e.set_entry_type(EntryType::EntryNormal);
            e.data = data;
            m.set_entries(vec![e].into());
            m.to = self.leader;
            m
        };
        vec![message]
    }

    // Propose a configuration change on the leader, and then handle all responses.
    fn propose_change_v2(&mut self, transition: ConfChangeTransition) {
        if let ConfChangeTransition::Explicit = transition {
            self.auto_leave = false;
        }
        let msgs = self.generate_conf_change_v2(transition);
        self.send(msgs);
    }

    // Propose an empty conf change to leave joint.
    fn propose_leave_joint(&mut self) {
        assert!(!self.auto_leave);
        let mut msgs = Vec::with_capacity(1);
        msgs.push({
            let mut m = Message::default();
            m.set_msg_type(MessageType::MsgPropose);
            let mut e = Entry::default();
            e.set_entry_type(EntryType::EntryConfChangeV2);
            e.data = ConfChangeV2::default().write_to_bytes().unwrap();
            m.set_entries(vec![e].into());
            m.to = self.leader;
            m
        });
        self.send(msgs);
    }

    // Propose a normal entry on the leader, and then handle all responses.
    fn propose_normal(&mut self, data: Vec<u8>) {
        let msgs = self.generate_normal_proposal(data);
        self.send(msgs);
    }

    fn handle_raft_ready(&mut self, id: u64) {
        info!(self.logger, "handle raft ready for {}", id);
        let mut raft = self.peers.get_mut(&id).unwrap().raft.take().unwrap();
        let store = raft.store().clone();
        let mut stable_to = None;
        // Handle pending snapshot if need.
        if let Some(ref s) = raft.raft_log.unstable.snapshot {
            let snap = s.clone();
            store.wl().apply_snapshot(snap).unwrap();
        }
        // Handle unstable entries.
        let entries = raft.raft_log.unstable_entries().unwrap_or(&[]).to_vec();
        if let Some(e) = entries.last() {
            store.wl().append(&entries).unwrap();
            stable_to = Some((e.get_index(), e.get_term()));
        }
        // Handle committed entries.
        if let Some(entries) = raft.raft_log.next_entries() {
            for entry in entries {
                let mut cc = ConfChangeV2::default();
                let entry_type = entry.get_entry_type();
                if entry_type == EntryType::EntryConfChangeV2 {
                    cc.merge_from_bytes(entry.get_data()).unwrap();
                } else if entry_type == EntryType::EntryConfChange {
                    let mut c1 = ConfChange::default();
                    c1.merge_from_bytes(entry.get_data()).unwrap();
                    cc = c1.into();
                }
                if entry_type != EntryType::EntryNormal {
                    match raft.apply_conf_change(&cc) {
                        Ok(_) => store.wl().set_conf_state(raft.prs().to_conf_state()),
                        Err(e) => error!(self.logger, "Apply conf change fail: {:?}", e),
                    }
                }
                store.wl().commit_to(entry.get_index()).unwrap();
                raft.commit_apply(entry.get_index());
            }
        }
        if let Some((index, term)) = stable_to {
            raft.raft_log.stable_to(index, term);
        }
        store.wl().set_hardstate(raft.hard_state());
        self.peers.get_mut(&id).unwrap().raft = Some(raft);
    }

    fn handle_raft_readies(&mut self, peers: &[u64]) {
        for id in peers {
            self.handle_raft_ready(*id);
        }
    }

    fn must_in_joint(&self, peers: &[u64]) {
        let mut transition = ConfState::transition(&self.base, &self.target);
        transition.set_auto_leave(self.auto_leave);
        for id in peers {
            let raft = self.peers.get(id).and_then(|r| r.raft.as_ref()).unwrap();
            assert_eq!(raft.prs().to_conf_state(), transition);
        }
    }

    fn must_leave_joint(&self, peers: &[u64]) {
        for id in peers {
            let raft = self.peers.get(id).and_then(|r| r.raft.as_ref()).unwrap();
            assert_eq!(raft.prs().to_conf_state(), self.target);
        }
    }

    // Reads and dispatch messages from each peer in `peers`.
    fn read_and_dispatch_messages_from(&mut self, peers: &[u64]) {
        for id in peers {
            let msgs = self.peers.get_mut(id).unwrap().read_messages();
            self.dispatch(msgs).unwrap();
        }
    }

    // Tick the given peer until it starts a new election.
    fn tick_until_election(&mut self, peers: &[u64]) {
        let mut ticks = std::usize::MAX;
        for id in peers {
            let peer = self.peers.get(id).unwrap();
            let t = peer.randomized_election_timeout() + 1 - peer.election_elapsed;
            ticks = std::cmp::min(ticks, t);
        }
        for _ in 0..=ticks {
            for id in peers {
                let peer = self.peers.get_mut(id).unwrap();
                peer.tick();
            }
            let messages = self.read_messages();
            self.send(messages);
        }
    }

    // Power cycle given peers.
    fn power_cycle(&mut self, peers: &[u64], new_leader: u64) -> Result<()> {
        for id in peers {
            self.peers.get_mut(id).unwrap().raft.take();
            let store = self.storage.get(id).unwrap().clone();
            let cfg = Self::generate_config(*id, &store);
            let r = Raft::new(&cfg, store, &self.logger)?;
            self.peers.get_mut(id).unwrap().raft = Some(r);
        }
        self.leader = new_leader;
        let message = new_message(new_leader, new_leader, MessageType::MsgHup, 0);
        self.send(vec![message]);
        Ok(())
    }

    // Generate a `Config` to initialize a `Raft`.
    fn generate_config(id: u64, store: &MemStorage) -> Config {
        Config {
            id,
            applied: store.wl().hard_state().get_commit(),
            ..Default::default()
        }
    }
}

// Generate a conf change from `base` to `target`. all peer lists need to be sorted.
fn generate_conf_change_v2(base: &ConfState, target: &ConfState) -> ConfChangeV2 {
    let mut cc = ConfChangeV2::default();
    for v in base.get_voters() {
        if target.get_voters().binary_search(v).is_err() {
            // Remove voter.
            let mut change = ConfChangeSingle::default();
            change.set_change_type(ConfChangeType::RemoveNode);
            change.set_node_id(*v);
            cc.mut_changes().push(change);
        }
        if target.get_learners().binary_search(v).is_ok() {
            // Add back as learner.
            let mut change = ConfChangeSingle::default();
            change.set_change_type(ConfChangeType::AddLearnerNode);
            change.set_node_id(*v);
            cc.mut_changes().push(change);
        }
    }
    for v in base.get_learners() {
        if target.get_voters().binary_search(v).is_err()
            && target.get_learners().binary_search(v).is_err()
        {
            // Remove learner.
            let mut change = ConfChangeSingle::default();
            change.set_change_type(ConfChangeType::RemoveNode);
            change.set_node_id(*v);
            cc.mut_changes().push(change);
        }
    }
    for v in target.get_voters() {
        if base.get_voters().binary_search(v).is_err() {
            // Add or promote a voter.
            let mut change = ConfChangeSingle::default();
            change.set_change_type(ConfChangeType::AddNode);
            change.set_node_id(*v);
            cc.mut_changes().push(change);
        }
    }
    for v in target.get_learners() {
        if base.get_learners().binary_search(v).is_err()
            && base.get_voters().binary_search(v).is_err()
        {
            // Add as learner.
            let mut change = ConfChangeSingle::default();
            change.set_change_type(ConfChangeType::AddLearnerNode);
            change.set_node_id(*v);
            cc.mut_changes().push(change);
        }
    }
    cc
}
