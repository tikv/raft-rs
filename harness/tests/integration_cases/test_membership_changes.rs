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
    let msgs = scenario.read_messages();
    scenario.filter_and_send(msgs);

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
            let msgs = scenario.read_messages();
            scenario.filter_and_send(msgs);

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

/***********
// Test that small cluster is able to progress through removing a leader.

    /// If the leader fails after the `Begin`, then recovers after the `Finalize`, the group should ignore it.
    #[test]
    fn leader_fails_and_recovers() -> Result<()> {
        let l = default_logger();
        let leader = 1;
        let old_configuration = (vec![1, 2, 3], vec![]);
        let new_configuration = (vec![2, 3], vec![]);
        let mut scenario = Scenario::new(leader, old_configuration, new_configuration, &l)?;
        scenario.spawn_new_peers()?;
        scenario.propose_change_message()?;

        info!(l, "Allowing quorum to commit");
        scenario.expect_read_and_dispatch_messages_from(&[1, 2, 3])?;

        info!(l, "Advancing leader, now entered the joint");
        scenario.assert_can_apply_transition_entry_at_index(
            &[1],
            3,
            ConfChangeType::BeginMembershipChange,
        );
        scenario.assert_in_membership_change(&[1]);

        info!(l, "Leader replicates the commit and finalize entry.");
        scenario.expect_read_and_dispatch_messages_from(&[1])?;
        scenario.assert_can_apply_transition_entry_at_index(
            &[2, 3],
            3,
            ConfChangeType::BeginMembershipChange,
        );
        scenario.assert_in_membership_change(&[1, 2, 3]);

        info!(l, "Cluster leaving the joint.");
        scenario.expect_read_and_dispatch_messages_from(&[2, 3, 1])?;

        scenario.isolate(1); // Simulate the leader failing.

        scenario.assert_can_apply_transition_entry_at_index(
            &[2, 3],
            4,
            ConfChangeType::FinalizeMembershipChange,
        );
        scenario.assert_not_in_membership_change(&[2, 3]);

        // At this point, 1 thinks it is a leader, but actually it isn't anymore.

        info!(l, "Prompting a new election.");
        {
            let new_leader = scenario.peers.get_mut(&2).unwrap();
            for _ in new_leader.election_elapsed..=(new_leader.randomized_election_timeout() + 1) {
                new_leader.tick();
            }
        }
        let messages = scenario.read_messages();
        scenario.send(messages);

        scenario.recover();
        // Here we note that the old leader (1) has NOT applied the finalize operation and thus thinks it is still leader.
        //
        // The Raft paper notes that a removed leader should not disrupt the cluster.
        // It suggests doing this by ignoring any `RequestVote` when it has heard from the leader within the minimum election timeout.

        info!(l, "Verifying that old leader cannot disrupt the cluster.");
        {
            let old_leader = scenario.peers.get_mut(&1).unwrap();
            for _ in old_leader.heartbeat_elapsed()..=(old_leader.heartbeat_timeout() + 1) {
                old_leader.tick();
            }
        }
        let messages = scenario.read_messages();
        scenario.send(messages);

        let peer_leader = scenario.peer_leaders();
        assert_ne!(peer_leader[&2], 1);
        assert_ne!(peer_leader[&3], 1);
        Ok(())
    }
}

// Test that small cluster is able to progress through replacing a voter.
mod three_peers_replace_voter {
    use super::*;

    /// The leader power cycles before actually sending the messages.
    #[test]
    fn leader_power_cycles_no_compaction() -> Result<()> {
        let l = default_logger();
        let leader = 1;
        let old_configuration = (vec![1, 2, 3], vec![]);
        let new_configuration = (vec![1, 2, 4], vec![]);
        let mut scenario = Scenario::new(leader, old_configuration, new_configuration, &l)?;
        scenario.spawn_new_peers()?;
        scenario.propose_change_message()?;

        info!(l, "Allowing quorum to commit");
        scenario.expect_read_and_dispatch_messages_from(&[1, 2, 3])?;

        info!(l, "Advancing leader, now entered the joint");
        scenario.assert_can_apply_transition_entry_at_index(
            &[1],
            3,
            ConfChangeType::BeginMembershipChange,
        );
        scenario.assert_in_membership_change(&[1]);

        info!(l, "Leader replicates the commit and finalize entry.");
        scenario.expect_read_and_dispatch_messages_from(&[1])?;
        scenario.assert_can_apply_transition_entry_at_index(
            &[2, 3],
            3,
            ConfChangeType::BeginMembershipChange,
        );
        scenario.assert_in_membership_change(&[1, 2, 3]);

        info!(l, "Leader power cycles.");
        assert_eq!(scenario.peers[&1].began_membership_change_at(), Some(3));

        if let Some(idx) = scenario.peers[&1].began_membership_change_at() {
            let raft = scenario.peers.get_mut(&1).unwrap();
            let conf_state: ConfState = raft.prs().configuration().to_conf_state();
            let new_conf_state: ConfState = raft
                .prs()
                .next_configuration()
                .as_ref()
                .unwrap()
                .to_conf_state();
            raft.mut_store()
                .wl()
                .set_conf_state(conf_state, Some((new_conf_state, idx)));
        }

        scenario.power_cycle(&[1], None);
        assert_eq!(scenario.peers[&1].began_membership_change_at(), Some(3));
        scenario.assert_in_membership_change(&[1]);
        {
            let peer = scenario.peers.get_mut(&1).unwrap();
            peer.become_candidate();
            peer.become_leader();
            for _ in peer.heartbeat_elapsed()..=(peer.heartbeat_timeout() + 1) {
                peer.tick();
            }
        }

        info!(l, "Allowing new peers to catch up.");
        scenario.expect_read_and_dispatch_messages_from(&[4, 1, 4, 1, 4, 1])?;
        scenario.assert_can_apply_transition_entry_at_index(
            &[4],
            3,
            ConfChangeType::BeginMembershipChange,
        );
        scenario.assert_in_membership_change(&[1, 2, 3, 4]);

        info!(l, "Cluster leaving the joint.");
        scenario.expect_read_and_dispatch_messages_from(&[4, 3, 2, 1, 4, 3, 2, 1])?;
        assert_eq!(scenario.peers[&1].began_membership_change_at(), Some(3));
        scenario.assert_can_apply_transition_entry_at_index(
            &[1, 2, 3, 4],
            5,
            ConfChangeType::FinalizeMembershipChange,
        );
        scenario.assert_not_in_membership_change(&[1, 2, 4]);

        Ok(())
    }

    /// The leader power cycles before actually sending the messages.
    #[test]
    fn leader_power_cycles_compacted_log() -> Result<()> {
        let l = default_logger();
        let leader = 1;
        let old_configuration = (vec![1, 2, 3], vec![]);
        let new_configuration = (vec![1, 2, 4], vec![]);
        let mut scenario = Scenario::new(leader, old_configuration, new_configuration, &l)?;
        scenario.spawn_new_peers()?;
        scenario.propose_change_message()?;

        info!(l, "Allowing quorum to commit");
        scenario.expect_read_and_dispatch_messages_from(&[1, 2, 3])?;

        info!(l, "Advancing leader, now entered the joint");
        scenario.assert_can_apply_transition_entry_at_index(
            &[1],
            3,
            ConfChangeType::BeginMembershipChange,
        );
        scenario.assert_in_membership_change(&[1]);

        info!(l, "Leader replicates the commit and finalize entry.");
        scenario.expect_read_and_dispatch_messages_from(&[1])?;
        scenario.assert_can_apply_transition_entry_at_index(
            &[2, 3],
            3,
            ConfChangeType::BeginMembershipChange,
        );
        scenario.assert_in_membership_change(&[1, 2, 3]);

        info!(l, "Compacting leader's log");
        // This snapshot has a term 1.
        let snapshot = {
            let peer = scenario.peers.get_mut(&1).unwrap();
            peer.raft_log.store.wl().commit_to_and_set_conf_states(
                3,
                Some(peer.prs().configuration().to_conf_state()),
                peer.pending_membership_change().clone(),
            )?;
            let snapshot = peer.raft_log.snapshot(0)?;
            peer.raft_log.store.wl().compact(3)?;
            snapshot
        };

        // At this point, there is a sentinel at index 3, term 2.

        info!(l, "Leader power cycles.");
        assert_eq!(scenario.peers[&1].began_membership_change_at(), Some(3));
        scenario.power_cycle(&[1], snapshot.clone());
        {
            let peer = scenario.peers.get_mut(&1).unwrap();
            peer.become_candidate();
            peer.become_leader();
        }

        assert_eq!(scenario.peers[&1].began_membership_change_at(), Some(3));
        scenario.assert_in_membership_change(&[1]);

        info!(l, "Allowing new peers to catch up.");
        scenario.expect_read_and_dispatch_messages_from(&[1, 4, 1])?; // 1, 4, 1, 4, 1])?;
        scenario.assert_in_membership_change(&[1, 2, 3, 4]);

        {
            assert_eq!(
                4,
                scenario.peers.get_mut(&4).unwrap().raft_log.unstable.offset
            );
            let new_peer = scenario.peers.get_mut(&4).unwrap();
            let snap = new_peer.raft_log.snapshot(0).unwrap();
            new_peer.raft_log.store.wl().apply_snapshot(snap).unwrap();
            new_peer
                .raft_log
                .stable_snap_to(snapshot.get_metadata().index);
        }

        info!(l, "Cluster leaving the joint.");
        scenario.expect_read_and_dispatch_messages_from(&[4, 1, 4, 3, 2, 1, 3, 2, 1])?;
        assert_eq!(scenario.peers[&1].began_membership_change_at(), Some(3));
        scenario.assert_can_apply_transition_entry_at_index(
            &[1, 2, 3, 4],
            5,
            ConfChangeType::FinalizeMembershipChange,
        );
        scenario.assert_not_in_membership_change(&[1, 2, 3, 4]);

        Ok(())
    }

    // Ensure if a peer in the old quorum fails, but the quorum is still big enough, it's ok.
    #[test]
    fn pending_delete_fails_after_begin() -> Result<()> {
        let l = default_logger();
        let leader = 1;
        let old_configuration = (vec![1, 2, 3], vec![]);
        let new_configuration = (vec![1, 2, 4], vec![]);
        let mut scenario = Scenario::new(leader, old_configuration, new_configuration, &l)?;
        scenario.spawn_new_peers()?;
        scenario.propose_change_message()?;

        info!(l, "Allowing quorum to commit");
        scenario.expect_read_and_dispatch_messages_from(&[1, 2, 3])?;

        info!(l, "Advancing leader, now entered the joint");
        scenario.assert_can_apply_transition_entry_at_index(
            &[1],
            3,
            ConfChangeType::BeginMembershipChange,
        );
        scenario.assert_in_membership_change(&[1]);

        scenario.isolate(3); // Take 3 down.

        info!(l, "Leader replicates the commit and finalize entry.");
        scenario.expect_read_and_dispatch_messages_from(&[1])?;
        scenario.assert_can_apply_transition_entry_at_index(
            &[2],
            3,
            ConfChangeType::BeginMembershipChange,
        );
        scenario.assert_in_membership_change(&[1, 2]);

        info!(l, "Allowing new peers to catch up.");
        scenario.expect_read_and_dispatch_messages_from(&[4, 1, 4, 1])?;
        scenario.assert_can_apply_transition_entry_at_index(
            &[4],
            3,
            ConfChangeType::BeginMembershipChange,
        );
        scenario.assert_in_membership_change(&[1, 2, 4]);

        info!(l, "Cluster leaving the joint.");
        scenario.expect_read_and_dispatch_messages_from(&[2, 1, 4])?;
        scenario.assert_can_apply_transition_entry_at_index(
            &[1, 2, 4],
            4,
            ConfChangeType::FinalizeMembershipChange,
        );
        scenario.assert_not_in_membership_change(&[1, 2, 4]);

        Ok(())
    }

    // Ensure if a peer in the new quorum fails, but the quorum is still big enough, it's ok.
    #[test]
    fn pending_create_with_quorum_fails_after_begin() -> Result<()> {
        let l = default_logger();
        let leader = 1;
        let old_configuration = (vec![1, 2, 3], vec![]);
        let new_configuration = (vec![1, 2, 4], vec![]);
        let mut scenario = Scenario::new(leader, old_configuration, new_configuration, &l)?;
        scenario.spawn_new_peers()?;
        scenario.propose_change_message()?;

        info!(l, "Allowing quorum to commit");
        scenario.expect_read_and_dispatch_messages_from(&[1, 2, 3])?;

        info!(l, "Advancing leader, now entered the joint");
        scenario.assert_can_apply_transition_entry_at_index(
            &[1],
            3,
            ConfChangeType::BeginMembershipChange,
        );
        scenario.assert_in_membership_change(&[1]);

        scenario.isolate(4); // Take 4 down.

        info!(l, "Leader replicates the commit and finalize entry.");
        scenario.expect_read_and_dispatch_messages_from(&[1])?;
        scenario.assert_can_apply_transition_entry_at_index(
            &[2, 3],
            3,
            ConfChangeType::BeginMembershipChange,
        );
        scenario.assert_in_membership_change(&[1, 2, 3]);

        info!(l, "Cluster leaving the joint.");
        scenario.expect_read_and_dispatch_messages_from(&[2, 1])?;
        scenario.assert_can_apply_transition_entry_at_index(
            &[1, 2, 3],
            4,
            ConfChangeType::FinalizeMembershipChange,
        );
        scenario.assert_not_in_membership_change(&[1, 2, 3]);

        Ok(())
    }

    // Ensure if the peer pending a deletion and the peer pending a creation both fail it's still ok (so long as both quorums hold).
    #[test]
    fn pending_create_and_destroy_both_fail() -> Result<()> {
        let l = default_logger();
        let leader = 1;
        let old_configuration = (vec![1, 2, 3], vec![]);
        let new_configuration = (vec![1, 2, 4], vec![]);
        let mut scenario = Scenario::new(leader, old_configuration, new_configuration, &l)?;
        scenario.spawn_new_peers()?;
        scenario.propose_change_message()?;

        info!(l, "Allowing quorum to commit");
        scenario.expect_read_and_dispatch_messages_from(&[1, 2, 3])?;

        info!(l, "Advancing leader, now entered the joint");
        scenario.assert_can_apply_transition_entry_at_index(
            &[1],
            3,
            ConfChangeType::BeginMembershipChange,
        );
        scenario.assert_in_membership_change(&[1]);

        scenario.isolate(3); // Take 3 down.
        scenario.isolate(4); // Take 4 down.

        info!(l, "Leader replicates the commit and finalize entry.");
        scenario.expect_read_and_dispatch_messages_from(&[1])?;
        scenario.assert_can_apply_transition_entry_at_index(
            &[2],
            3,
            ConfChangeType::BeginMembershipChange,
        );
        scenario.assert_in_membership_change(&[1, 2]);

        info!(l, "Cluster leaving the joint.");
        scenario.expect_read_and_dispatch_messages_from(&[2, 1])?;
        scenario.assert_can_apply_transition_entry_at_index(
            &[1, 2],
            4,
            ConfChangeType::FinalizeMembershipChange,
        );
        scenario.assert_not_in_membership_change(&[1, 2]);

        Ok(())
    }

    // Ensure if the old quorum fails during the joint state progress will halt until the peer group is recovered.
    #[test]
    fn old_quorum_fails() -> Result<()> {
        let l = default_logger();
        let leader = 1;
        let old_configuration = (vec![1, 2, 3], vec![]);
        let new_configuration = (vec![1, 2, 4], vec![]);
        let mut scenario = Scenario::new(leader, old_configuration, new_configuration, &l)?;
        scenario.spawn_new_peers()?;
        scenario.propose_change_message()?;

        info!(l, "Allowing quorum to commit");
        scenario.expect_read_and_dispatch_messages_from(&[1, 2, 3])?;

        info!(l, "Advancing leader, now entered the joint");
        scenario.assert_can_apply_transition_entry_at_index(
            &[1],
            3,
            ConfChangeType::BeginMembershipChange,
        );
        scenario.assert_in_membership_change(&[1]);

        info!(l, "Old quorum fails.");
        scenario.isolate(3); // Take 3 down.
        scenario.isolate(2); // Take 2 down.

        info!(l, "Leader replicates the commit and finalize entry.");
        scenario.expect_read_and_dispatch_messages_from(&[1, 4, 1, 4, 1, 4])?;
        scenario.assert_can_apply_transition_entry_at_index(
            &[4],
            3,
            ConfChangeType::BeginMembershipChange,
        );
        scenario.assert_in_membership_change(&[1, 4]);
        scenario.assert_not_in_membership_change(&[2, 3]);

        info!(
            l,
            "Spinning for awhile to ensure nothing spectacular happens"
        );
        for _ in scenario.peers[&leader].heartbeat_elapsed()
            ..=scenario.peers[&leader].heartbeat_timeout()
        {
            scenario.peers.iter_mut().for_each(|(_, peer)| {
                peer.tick();
            });
            let messages = scenario.read_messages();
            scenario.dispatch(messages)?;
        }

        scenario.assert_in_membership_change(&[1, 4]);
        scenario.assert_not_in_membership_change(&[2, 3]);

        info!(l, "Recovering old qourum.");
        scenario.recover();

        for _ in scenario.peers[&leader].heartbeat_elapsed()
            ..=scenario.peers[&leader].heartbeat_timeout()
        {
            scenario.peers.iter_mut().for_each(|(_, peer)| {
                peer.tick();
            });
        }

        info!(l, "Giving the peer group time to recover.");
        scenario.expect_read_and_dispatch_messages_from(&[1, 2, 3, 4, 1, 2, 3, 1])?;
        scenario.assert_can_apply_transition_entry_at_index(
            &[2, 3],
            3,
            ConfChangeType::BeginMembershipChange,
        );
        scenario.assert_in_membership_change(&[1, 2, 3, 4]);

        info!(l, "Failed peers confirming they have commited the begin.");
        scenario.expect_read_and_dispatch_messages_from(&[2, 3])?;

        info!(l, "Cluster leaving the joint.");
        scenario.expect_read_and_dispatch_messages_from(&[1])?;
        scenario.assert_can_apply_transition_entry_at_index(
            &[1, 2, 3, 4],
            4,
            ConfChangeType::FinalizeMembershipChange,
        );
        scenario.assert_not_in_membership_change(&[1, 2, 3, 4]);

        Ok(())
    }

    // Ensure if the new quorum fails during the joint state progress will halt until the peer group is recovered.
    #[test]
    fn new_quorum_fails() -> Result<()> {
        let l = default_logger();
        let leader = 1;
        let old_configuration = (vec![1, 2, 3], vec![]);
        let new_configuration = (vec![1, 2, 4], vec![]);
        let mut scenario = Scenario::new(leader, old_configuration, new_configuration, &l)?;
        scenario.spawn_new_peers()?;
        scenario.propose_change_message()?;

        info!(l, "Allowing quorum to commit");
        scenario.expect_read_and_dispatch_messages_from(&[1, 2, 3])?;

        info!(l, "Advancing leader, now entered the joint");
        scenario.assert_can_apply_transition_entry_at_index(
            &[1],
            3,
            ConfChangeType::BeginMembershipChange,
        );
        scenario.assert_in_membership_change(&[1]);

        info!(l, "New quorum fails.");
        scenario.isolate(4); // Take 4 down.
        scenario.isolate(2); // Take 2 down.

        info!(l, "Leader replicates the commit and finalize entry.");
        scenario.expect_read_and_dispatch_messages_from(&[1, 3])?;

        info!(
            l,
            "Leader waits to let the new quorum apply this before progressing."
        );
        scenario.assert_in_membership_change(&[1]);
        scenario.assert_not_in_membership_change(&[2, 3, 4]);

        info!(
            l,
            "Spinning for awhile to ensure nothing spectacular happens"
        );
        for _ in scenario.peers[&leader].heartbeat_elapsed()
            ..=scenario.peers[&leader].heartbeat_timeout()
        {
            scenario.peers.iter_mut().for_each(|(_, peer)| {
                peer.tick();
            });
            let messages = scenario.read_messages();
            scenario.dispatch(messages)?;
        }

        scenario.assert_in_membership_change(&[1]);
        scenario.assert_not_in_membership_change(&[2, 3, 4]);

        info!(l, "Recovering new qourum.");
        scenario.recover();

        for _ in scenario.peers[&leader].heartbeat_elapsed()
            ..=scenario.peers[&leader].heartbeat_timeout()
        {
            scenario.peers.iter_mut().for_each(|(_, peer)| {
                peer.tick();
            });
        }

        info!(l, "Giving the peer group time to recover.");
        scenario.expect_read_and_dispatch_messages_from(&[1, 2, 3, 4, 1, 2, 4, 1, 4, 1])?;
        scenario.assert_can_apply_transition_entry_at_index(
            &[2, 3, 4],
            3,
            ConfChangeType::BeginMembershipChange,
        );
        scenario.assert_in_membership_change(&[1, 2, 3, 4]);

        info!(l, "Failed peers confirming they have commited the begin.");
        scenario.expect_read_and_dispatch_messages_from(&[2, 4])?;

        info!(l, "Cluster leaving the joint.");
        scenario.expect_read_and_dispatch_messages_from(&[1])?;
        scenario.assert_can_apply_transition_entry_at_index(
            &[1, 2, 3, 4],
            4,
            ConfChangeType::FinalizeMembershipChange,
        );
        scenario.assert_not_in_membership_change(&[1, 2, 3, 4]);

        Ok(())
    }
}

// Test that small cluster is able to progress through adding a more with a learner.
mod three_peers_to_five_with_learner {
    use super::*;

    /// In a steady state transition should proceed without issue.
    #[test]
    fn stable() -> Result<()> {
        let l = default_logger();
        let leader = 1;
        let old_configuration = (vec![1, 2, 3], vec![]);
        let new_configuration = (vec![1, 2, 3, 4, 5], vec![6]);
        let mut scenario = Scenario::new(leader, old_configuration, new_configuration, &l)?;
        scenario.spawn_new_peers()?;
        scenario.propose_change_message()?;

        info!(l, "Allowing quorum to commit");
        scenario.expect_read_and_dispatch_messages_from(&[1, 2, 3])?;

        info!(l, "Advancing leader, now entered the joint");
        scenario.assert_can_apply_transition_entry_at_index(
            &[1],
            3,
            ConfChangeType::BeginMembershipChange,
        );
        scenario.assert_in_membership_change(&[1]);

        info!(l, "Leader replicates the commit and finalize entry.");
        scenario.expect_read_and_dispatch_messages_from(&[1])?;
        scenario.assert_can_apply_transition_entry_at_index(
            &[2, 3],
            3,
            ConfChangeType::BeginMembershipChange,
        );
        scenario.assert_in_membership_change(&[1, 2, 3]);

        info!(l, "Allowing new peers to catch up.");
        scenario.expect_read_and_dispatch_messages_from(&[4, 5, 6, 1, 4, 5, 6, 1, 4])?;
        scenario.assert_can_apply_transition_entry_at_index(
            &[4, 5, 6],
            3,
            ConfChangeType::BeginMembershipChange,
        );
        scenario.assert_in_membership_change(&[1, 2, 3, 4, 5, 6]);

        info!(l, "Cluster leaving the joint.");
        scenario.expect_read_and_dispatch_messages_from(&[3, 2, 1])?;
        scenario.assert_can_apply_transition_entry_at_index(
            &[1, 2, 3, 4, 5, 6],
            4,
            ConfChangeType::FinalizeMembershipChange,
        );
        scenario.assert_not_in_membership_change(&[1, 2, 3, 4, 5, 6]);

        Ok(())
    }

    /// In this, a single node (of 3) halts during the transition.
    #[test]
    fn minority_old_followers_halt_at_start() -> Result<()> {
        let l = default_logger();
        let leader = 1;
        let old_configuration = (vec![1, 2, 3], vec![]);
        let new_configuration = (vec![1, 2, 3, 4, 5], vec![6]);
        let mut scenario = Scenario::new(leader, old_configuration, new_configuration, &l)?;
        scenario.spawn_new_peers()?;
        scenario.isolate(3);
        scenario.propose_change_message()?;

        info!(l, "Allowing quorum to commit");
        scenario.expect_read_and_dispatch_messages_from(&[1, 2])?;

        info!(l, "Advancing leader, now entered the joint");
        scenario.assert_can_apply_transition_entry_at_index(
            &[1],
            3,
            ConfChangeType::BeginMembershipChange,
        );
        scenario.assert_in_membership_change(&[1]);

        info!(l, "Leader replicates the commit and finalize entry.");
        scenario.expect_read_and_dispatch_messages_from(&[1])?;
        scenario.assert_can_apply_transition_entry_at_index(
            &[2],
            3,
            ConfChangeType::BeginMembershipChange,
        );
        scenario.assert_in_membership_change(&[1, 2]);
        scenario.assert_not_in_membership_change(&[3]);

        info!(l, "Allowing new peers to catch up.");
        scenario.expect_read_and_dispatch_messages_from(&[4, 5, 6, 1, 4, 5, 6, 1])?;
        scenario.assert_can_apply_transition_entry_at_index(
            &[4, 5, 6],
            3,
            ConfChangeType::BeginMembershipChange,
        );
        scenario.assert_in_membership_change(&[1, 2, 4, 5, 6]);
        scenario.assert_not_in_membership_change(&[3]);

        scenario.expect_read_and_dispatch_messages_from(&[4, 5, 6])?;

        info!(l, "Cluster leaving the joint.");
        {
            let leader = scenario.peers.get_mut(&1).unwrap();
            let ticks = leader.heartbeat_timeout();
            for _ in 0..=ticks {
                leader.tick();
            }
        }
        scenario.expect_read_and_dispatch_messages_from(&[2, 1, 4, 5, 6, 1, 4, 5, 6, 1])?;
        scenario.assert_can_apply_transition_entry_at_index(
            &[1, 2, 4, 5],
            4,
            ConfChangeType::FinalizeMembershipChange,
        );
        scenario.assert_not_in_membership_change(&[1, 2, 4, 5]);
        scenario.assert_not_in_membership_change(&[3]);

        Ok(())
    }
}

mod intermingled_config_changes {
    use super::*;

    // In this test, we make sure that if the peer group is sent a `BeginMembershipChange`, then immediately a `AddNode` entry, that the `AddNode` is rejected by the leader.
    #[test]
    fn begin_then_add_node() -> Result<()> {
        let l = default_logger();
        let leader = 1;
        let old_configuration = (vec![1, 2, 3], vec![]);
        let new_configuration = (vec![1, 2, 3, 4], vec![]);
        let mut scenario = Scenario::new(leader, old_configuration, new_configuration, &l)?;
        scenario.spawn_new_peers()?;
        scenario.propose_change_message()?;

        info!(l, "Allowing quorum to commit");
        scenario.expect_read_and_dispatch_messages_from(&[1, 2, 3])?;

        info!(l, "Advancing leader, now entered the joint");
        scenario.assert_can_apply_transition_entry_at_index(
            &[1],
            3,
            ConfChangeType::BeginMembershipChange,
        );
        scenario.assert_in_membership_change(&[1]);

        info!(l, "Leader recieves an add node proposal, which it rejects since it is already in transition.");
        let _ = scenario.propose_add_node_message(4);
        assert_eq!(
            scenario.peers[&scenario.old_leader]
                .raft_log
                .entries(5, 1)
                .unwrap()[0]
                .get_entry_type(),
            EntryType::EntryNormal
        );

        info!(l, "Leader replicates the commit and finalize entry.");
        scenario.expect_read_and_dispatch_messages_from(&[1])?;
        scenario.assert_can_apply_transition_entry_at_index(
            &[2, 3],
            3,
            ConfChangeType::BeginMembershipChange,
        );
        scenario.assert_in_membership_change(&[1, 2, 3]);

        info!(l, "Allowing new peers to catch up.");
        scenario.expect_read_and_dispatch_messages_from(&[4, 1, 4, 1, 4])?;
        scenario.assert_can_apply_transition_entry_at_index(
            &[4],
            3,
            ConfChangeType::BeginMembershipChange,
        );
        scenario.assert_in_membership_change(&[1, 2, 3, 4]);

        info!(l, "Cluster leaving the joint.");
        scenario.expect_read_and_dispatch_messages_from(&[3, 2, 1])?;
        scenario.assert_can_apply_transition_entry_at_index(
            &[1, 2, 3, 4],
            4,
            ConfChangeType::FinalizeMembershipChange,
        );
        scenario.assert_not_in_membership_change(&[1, 2, 3, 4]);

        Ok(())
    }
}

mod compaction {
    use super::*;

    // Ensure that if a Raft compacts its log before finalizing that there are no failures.
    #[test]
    fn begin_compact_then_finalize() -> Result<()> {
        let l = default_logger();
        let leader = 1;
        let old_configuration = (vec![1, 2, 3], vec![]);
        let new_configuration = (vec![1, 2, 3], vec![4]);
        let mut scenario = Scenario::new(leader, old_configuration, new_configuration, &l)?;
        scenario.spawn_new_peers()?;
        scenario.propose_change_message()?;

        info!(l, "Allowing quorum to commit");
        scenario.expect_read_and_dispatch_messages_from(&[1, 2, 3])?;

        info!(l, "Advancing leader, now entered the joint");
        scenario.assert_can_apply_transition_entry_at_index(
            &[1],
            3,
            ConfChangeType::BeginMembershipChange,
        );
        scenario.assert_in_membership_change(&[1]);

        info!(l, "Leader replicates the commit and finalize entry.");
        scenario.expect_read_and_dispatch_messages_from(&[1])?;
        scenario.assert_can_apply_transition_entry_at_index(
            &[2, 3],
            3,
            ConfChangeType::BeginMembershipChange,
        );
        scenario.assert_in_membership_change(&[1, 2, 3]);

        info!(l, "Allowing new peers to catch up.");
        scenario.expect_read_and_dispatch_messages_from(&[4, 1, 4, 1])?;
        scenario.assert_can_apply_transition_entry_at_index(
            &[4],
            3,
            ConfChangeType::BeginMembershipChange,
        );
        scenario.assert_in_membership_change(&[1, 2, 3, 4]);

        info!(l, "Compacting the leaders log");
        scenario
            .peers
            .get_mut(&1)
            .unwrap()
            .raft_log
            .store
            .wl()
            .compact(2)?;

        info!(l, "Cluster leaving the joint.");
        scenario.expect_read_and_dispatch_messages_from(&[3, 2, 1])?;
        scenario.assert_can_apply_transition_entry_at_index(
            &[1, 2, 3, 4],
            4,
            ConfChangeType::FinalizeMembershipChange,
        );
        scenario.assert_not_in_membership_change(&[1, 2, 3, 4]);

        Ok(())
    }
}
************************************/

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
            let n = Some(Raft::new(&Config::new(*id), store, logger)?.into());
            starting_peers.push(n);
            pending_peers.remove(id);
        }

        let mut network = Network::new(starting_peers, logger);
        for id in pending_peers {
            let store = MemStorage::default();
            let r = Raft::new(&Config::new(id), store, logger)?.into();
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
                if entry.get_entry_type() == EntryType::EntryConfChangeV2 {
                    cc.merge_from_bytes(entry.get_data()).unwrap();
                } else if entry.get_entry_type() == EntryType::EntryConfChange {
                    let mut c1 = ConfChange::default();
                    c1.merge_from_bytes(entry.get_data()).unwrap();
                    cc = c1.into();
                } else {
                    continue;
                }
                match raft.apply_conf_change(&cc) {
                    Ok(_) => store.wl().set_conf_state(raft.prs().to_conf_state()),
                    Err(e) => error!(self.logger, "Apply conf change fail: {:?}", e),
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

    /*********************
    /// Checks that the given peers are not in a transition state.
    fn assert_not_in_membership_change<'a>(&self, peers: impl IntoIterator<Item = &'a u64>) {
        for peer in peers.into_iter().map(|id| &self.peers[id]) {
            assert!(
                !peer.is_in_membership_change(),
                "Peer {} should not have been in a membership change.",
                peer.id
            );
        }
    }

    // Checks that the given peers are in a transition state.
    fn assert_in_membership_change<'a>(&self, peers: impl IntoIterator<Item = &'a u64>) {
        for peer in peers.into_iter().map(|id| &self.peers[id]) {
            assert!(
                peer.is_in_membership_change(),
                "Peer {} should have been in a membership change.",
                peer.id
            );
        }
    }

    /// Reads the pending entries to be applied to a raft peer, checks one is of the expected variant, and applies it. Then, it advances the node to that point in the configuration change.


    /// Simulate a power cycle in the given nodes.
    ///
    /// This means that the MemStorage is kept, but nothing else.
    fn power_cycle<'a>(
        &mut self,
        peers: impl IntoIterator<Item = &'a u64>,
        snapshot: impl Into<Option<Snapshot>>,
    ) {
        let peers = peers.into_iter().cloned();
        let snapshot = snapshot.into();
        for id in peers {
            debug!(self.logger, "Power cycling {id}.", id = id);
            let applied = self.peers[&id].raft_log.applied;
            let mut peer = self.peers.remove(&id).expect("Peer did not exist.");
            let store = peer.mut_store().clone();

            let mut peer = Raft::new(
                &Config {
                    id,
                    applied,
                    ..Default::default()
                },
                store,
                &self.logger,
            )
            .expect("Could not create new Raft");

            if let Some(ref snapshot) = snapshot {
                peer.restore(snapshot.clone());
            };
            self.peers.insert(id, peer.into());
        }
    }

    // Verify there is a transition entry at the given index of the given variant.
    fn assert_membership_change_entry_at<'a>(
        &self,
        peers: impl IntoIterator<Item = &'a u64>,
        index: u64,
        entry_type: ConfChangeType,
    ) {
        let peers = peers.into_iter().cloned();
        for peer in peers {
            let entry = &self.peers[&peer]
                .raft_log
                .slice(index, index + 1, None)
                .unwrap()[0];
            assert_eq!(entry.get_entry_type(), EntryType::EntryConfChange);
            let mut conf_change = ConfChange::default();
            conf_change.merge_from_bytes(&entry.data).unwrap();
            assert_eq!(conf_change.get_change_type(), entry_type);
        }
    }

    fn assert_can_apply_transition_entry_at_index<'a>(
        &mut self,
        peers: impl IntoIterator<Item = &'a u64>,
        index: u64,
        entry_type: ConfChangeType,
    ) {
        let peers = peers.into_iter().collect::<Vec<_>>();
        self.expect_apply_membership_change_entry(peers.clone(), entry_type)
            .unwrap();
        self.assert_membership_change_entry_at(peers, index, entry_type)
    }
    *******************************************/
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

/***************
fn build_propose_add_node_message(recipient: u64, added_id: u64, index: u64) -> Message {
    let add_nodes_entry = {
        let mut conf_change = ConfChange::default();
        conf_change.set_change_type(ConfChangeType::AddNode);
        conf_change.node_id = added_id;
        let data = conf_change.write_to_bytes().unwrap();
        let mut entry = Entry::default();
        entry.set_entry_type(EntryType::EntryConfChange);
        entry.data = data;
        entry.index = index;
        entry
    };
    let mut message = Message::default();
    message.to = recipient;
    message.set_msg_type(MessageType::MsgPropose);
    message.index = index;
    message.entries = vec![add_nodes_entry].into();
    message
}
***************/
