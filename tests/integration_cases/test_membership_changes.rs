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

use harness::testing_logger;
use harness::Network;
use hashbrown::{HashMap, HashSet};

use prost::Message as ProstMsg;
use raft::{
    eraftpb::{
        ConfChange, ConfChangeType, ConfState, Entry, EntryType, Message, MessageType, Snapshot,
    },
    storage::MemStorage,
    Config, Configuration, Raft, Result, INVALID_ID,
};

use crate::test_util::new_message;

// Test that the API itself works.
//
// * Errors are returned from misuse.
// * Happy path returns happy values.
mod api {
    use super::*;
    // Test that the cluster can transition from a single node to a whole cluster.
    #[test]
    fn can_transition() -> Result<()> {
        let l = testing_logger().new(o!("test" => "can_transition"));
        let mut raft = Raft::new(
            &Config {
                id: 1,
                tag: "1".into(),
                ..Default::default()
            },
            MemStorage::new_with_conf_state((vec![1], vec![])),
        )?
        .with_logger(&l);
        let begin_conf_change = begin_conf_change(&[1, 2, 3], &[4], raft.raft_log.last_index() + 1);
        raft.begin_membership_change(&begin_conf_change)?;
        let finalize_conf_change = finalize_conf_change();
        raft.finalize_membership_change(&finalize_conf_change)?;
        Ok(())
    }

    // Test if the process rejects an overlapping voter and learner set.
    #[test]
    fn checks_for_overlapping_membership() -> Result<()> {
        let l = testing_logger().new(o!("test" => "checks_for_overlapping_membership"));
        let mut raft = Raft::new(
            &Config {
                id: 1,
                tag: "1".into(),
                ..Default::default()
            },
            MemStorage::new_with_conf_state((vec![1], vec![])),
        )?
        .with_logger(&l);
        let begin_conf_change =
            begin_conf_change(&[1, 2, 3], &[1, 2, 3], raft.raft_log.last_index() + 1);
        assert!(raft.begin_membership_change(&begin_conf_change).is_err());
        Ok(())
    }

    // Test if the process rejects an voter demotion.
    #[test]
    fn checks_for_voter_demotion() -> Result<()> {
        let l = testing_logger().new(o!("test" => "checks_for_voter_demotion"));
        let config = Config {
            id: 1,
            tag: "1".into(),
            ..Default::default()
        };
        let store = MemStorage::new_with_conf_state((vec![1, 2, 3], vec![4]));
        let mut raft = Raft::new(&config, store)?.with_logger(&l);
        let begin_conf_change = begin_conf_change(&[1, 2], &[3, 4], raft.raft_log.last_index() + 1);
        assert!(raft.begin_membership_change(&begin_conf_change).is_err());
        Ok(())
    }

    // Test if the process rejects an voter demotion.
    #[test]
    fn finalize_before_begin_fails_gracefully() -> Result<()> {
        let l = testing_logger().new(o!("test" => "finalize_before_begin_fails_gracefully"));
        let mut raft = Raft::new(
            &Config {
                id: 1,
                tag: "1".into(),
                ..Default::default()
            },
            MemStorage::new_with_conf_state((vec![1, 2, 3], vec![4])),
        )?
        .with_logger(&l);
        let finalize_conf_change = finalize_conf_change();
        assert!(raft
            .finalize_membership_change(&finalize_conf_change)
            .is_err());
        Ok(())
    }
}

// Test that small cluster is able to progress through adding a voter.
mod three_peers_add_voter {
    use super::*;

    /// In a steady state transition should proceed without issue.
    #[test]
    fn stable() -> Result<()> {
        let l = testing_logger().new(o!("test" => "stable"));
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

// Test that small cluster is able to progress through adding a learner.
mod three_peers_add_learner {
    use super::*;

    /// In a steady state transition should proceed without issue.
    #[test]
    fn stable() -> Result<()> {
        let l = testing_logger().new(o!("test" => "stable"));
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

// Test that small cluster is able to progress through removing a learner.
mod remove_learner {
    use super::*;

    /// In a steady state transition should proceed without issue.
    #[test]
    fn stable() -> Result<()> {
        let l = testing_logger().new(o!("test" => "stable"));
        let leader = 1;
        let old_configuration = (vec![1, 2, 3], vec![4]);
        let new_configuration = (vec![1, 2, 3], vec![]);
        let mut scenario = Scenario::new(leader, old_configuration, new_configuration, &l)?;
        scenario.spawn_new_peers()?;
        scenario.propose_change_message()?;

        info!(l, "Allowing quorum to commit");
        scenario.expect_read_and_dispatch_messages_from(&[1, 2, 3, 4])?;

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
            &[2, 3, 4],
            3,
            ConfChangeType::BeginMembershipChange,
        );
        scenario.assert_in_membership_change(&[1, 2, 3]);

        info!(l, "Cluster leaving the joint.");
        scenario.expect_read_and_dispatch_messages_from(&[4, 3, 2, 1])?;
        scenario.assert_can_apply_transition_entry_at_index(
            &[1, 2, 3, 4],
            4,
            ConfChangeType::FinalizeMembershipChange,
        );
        scenario.assert_not_in_membership_change(&[1, 2, 3, 4]);

        Ok(())
    }
}

// Test that small cluster is able to progress through removing a voter.
mod remove_voter {
    use super::*;

    /// In a steady state transition should proceed without issue.
    #[test]
    fn stable() -> Result<()> {
        let l = testing_logger().new(o!("test" => "stable"));
        let leader = 1;
        let old_configuration = (vec![1, 2, 3], vec![]);
        let new_configuration = (vec![1, 2], vec![]);
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
        scenario.expect_read_and_dispatch_messages_from(&[2, 1])?;
        scenario.assert_can_apply_transition_entry_at_index(
            &[1, 2],
            4,
            ConfChangeType::FinalizeMembershipChange,
        );
        scenario.assert_not_in_membership_change(&[1, 2]);

        Ok(())
    }
}

// Test that small cluster is able to progress through removing a leader.
mod remove_leader {
    use super::*;

    /// In a steady state transition should proceed without issue.
    #[test]
    fn stable() -> Result<()> {
        let l = testing_logger().new(o!("test" => "stable"));
        let leader = 1;
        let old_configuration = (vec![1, 2, 3], vec![]);
        let new_configuration = (vec![2, 3], vec![]);
        let mut scenario = Scenario::new(leader, old_configuration, new_configuration.clone(), &l)?;
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
        scenario.assert_can_apply_transition_entry_at_index(
            &[1, 2, 3],
            4,
            ConfChangeType::FinalizeMembershipChange,
        );
        scenario.assert_not_in_membership_change(&[1, 2, 3]);
        let peer_leaders = scenario.peer_leaders();
        for id in 1..=3 {
            assert_eq!(peer_leaders[&id], INVALID_ID, "peer {}", id);
        }

        info!(l, "Prompting a new election.");
        {
            let new_leader = scenario.peers.get_mut(&2).unwrap();
            for _ in
                new_leader.election_elapsed..=(new_leader.get_randomized_election_timeout() + 1)
            {
                new_leader.tick();
            }
        }
        let messages = scenario.read_messages();
        scenario.send(messages);

        info!(l, "Verifying that all peers have the right peer group.");
        for (_, peer) in scenario.peers.iter() {
            assert_eq!(
                peer.prs().configuration(),
                &new_configuration.clone().into(),
            );
        }

        info!(l, "Verifying that old leader cannot disrupt the cluster.");
        {
            let old_leader = scenario.peers.get_mut(&1).unwrap();
            for _ in old_leader.get_heartbeat_elapsed()..=(old_leader.get_heartbeat_timeout() + 1) {
                old_leader.tick();
            }
        }
        let messages = scenario.read_messages();
        scenario.send(messages);
        Ok(())
    }

    /// If the leader fails after the `Begin`, then recovers after the `Finalize`, the group should ignore it.
    #[test]
    fn leader_fails_and_recovers() -> Result<()> {
        let l = testing_logger().new(o!("test" => "leader_fails_and_recovers"));
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
            for _ in
                new_leader.election_elapsed..=(new_leader.get_randomized_election_timeout() + 1)
            {
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
            for _ in old_leader.get_heartbeat_elapsed()..=(old_leader.get_heartbeat_timeout() + 1) {
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

    /// In a steady state transition should proceed without issue.
    #[test]
    fn stable() -> Result<()> {
        let l = testing_logger().new(o!("test" => "stable"));
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

    /// The leader power cycles before actually sending the messages.
    #[test]
    fn leader_power_cycles_no_compaction() -> Result<()> {
        let l = testing_logger().new(o!("test" => "leader_power_cycles_no_compaction"));
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
            let conf_state: ConfState = raft.prs().configuration().clone().into();
            let new_conf_state: ConfState = raft.prs().next_configuration().clone().unwrap().into();
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
            for _ in peer.get_heartbeat_elapsed()..=(peer.get_heartbeat_timeout() + 1) {
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
        let l = testing_logger().new(o!("test" => "leader_power_cycles_compacted_log"));
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
                ConfState::from(peer.prs().configuration().clone()).into(),
                peer.pending_membership_change().clone(),
            )?;
            let snapshot = peer.raft_log.snapshot()?;
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
            let snap = new_peer.raft_log.snapshot().unwrap();
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
        let l = testing_logger().new(o!("test" => "pending_delete_fails_after_begin"));
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
        let l = testing_logger().new(o!("test" => "pending_create_with_quorum_fails_after_begin"));
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
        let l = testing_logger().new(o!("test" => "pending_create_and_destroy_both_fail"));
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
        let l = testing_logger().new(o!("test" => "old_quorum_fails"));
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
        for _ in scenario.peers[&leader].get_heartbeat_elapsed()
            ..=scenario.peers[&leader].get_heartbeat_timeout()
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

        for _ in scenario.peers[&leader].get_heartbeat_elapsed()
            ..=scenario.peers[&leader].get_heartbeat_timeout()
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
        let l = testing_logger().new(o!("test" => "new_quorum_fails"));
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
        for _ in scenario.peers[&leader].get_heartbeat_elapsed()
            ..=scenario.peers[&leader].get_heartbeat_timeout()
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

        for _ in scenario.peers[&leader].get_heartbeat_elapsed()
            ..=scenario.peers[&leader].get_heartbeat_timeout()
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
        let l = testing_logger().new(o!("test" => "stable"));
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
        let l = testing_logger().new(o!("test" => "minority_old_followers_halt_at_start"));
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
            let ticks = leader.get_heartbeat_timeout();
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
        let l = testing_logger().new(o!("test" => "begin_then_add_node"));
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
                .entry_type(),
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
        let l = testing_logger().new(o!("test" => "begin_compact_then_finalize"));
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

/// A test harness providing some useful utility and shorthand functions appropriate for this test suite.
///
/// Since it derefs into `Network` it can be used the same way. So it acts as a transparent set of utilities over the standard `Network`.
/// The goal here is to boil down the test suite for Joint Consensus into the simplest terms possible, while allowing for control.
struct Scenario {
    old_configuration: Configuration,
    old_leader: u64,
    new_configuration: Configuration,
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

// TODO: Explore moving some functionality to `Network`.
impl Scenario {
    /// Create a new scenario with the given state.
    fn new(
        leader: u64,
        old_configuration: impl Into<Configuration>,
        new_configuration: impl Into<Configuration>,
        logger: &slog::Logger,
    ) -> Result<Scenario> {
        let logger = logger.new(o!());
        let old_configuration = old_configuration.into();
        let new_configuration = new_configuration.into();
        info!(
            logger,
            "Beginning scenario, old: {:?}, new: {:?}", old_configuration, new_configuration
        );
        let starting_peers = old_configuration
            .voters()
            .iter()
            .chain(old_configuration.learners().iter())
            .map(|&id| {
                Some(
                    Raft::new(
                        &Config {
                            id,
                            tag: id.to_string(),
                            ..Default::default()
                        },
                        MemStorage::new_with_conf_state(old_configuration.clone()),
                    )
                    .unwrap()
                    .with_logger(&logger)
                    .into(),
                )
            })
            .collect();
        let mut scenario = Scenario {
            old_leader: leader,
            old_configuration,
            new_configuration,
            network: Network::new(starting_peers, &logger),
            logger,
        };
        // Elect the leader.
        info!(
            scenario.logger,
            "Sending MsgHup to predetermined leader ({})", leader
        );
        let message = new_message(leader, leader, MessageType::MsgHup, 0);
        scenario.send(vec![message]);
        Ok(scenario)
    }

    /// Creates any peers which are pending creation.
    ///
    /// This *only* creates the peers and adds them to the `Network`. It does not take other
    /// action. Newly created peers are only aware of the leader and themself.
    fn spawn_new_peers(&mut self) -> Result<()> {
        let new_peers = self.new_peers();
        info!(self.logger, "Creating new peers. {:?}", new_peers);
        for &id in new_peers.voters() {
            let raft = Raft::new(
                &Config {
                    id,
                    tag: id.to_string(),
                    ..Default::default()
                },
                MemStorage::new_with_conf_state((vec![self.old_leader, id], vec![])),
            )?
            .with_logger(&self.logger);
            self.peers.insert(id, raft.into());
        }
        for &id in new_peers.learners() {
            let raft = Raft::new(
                &Config {
                    id,
                    tag: id.to_string(),
                    ..Default::default()
                },
                MemStorage::new_with_conf_state((vec![self.old_leader], vec![id])),
            )?
            .with_logger(&self.logger);
            self.peers.insert(id, raft.into());
        }
        Ok(())
    }

    /// Return the leader id according to each peer.
    fn peer_leaders(&self) -> HashMap<u64, u64> {
        self.peers
            .iter()
            .map(|(&id, peer)| (id, peer.leader_id))
            .collect()
    }

    /// Return a configuration containing only the peers pending creation.
    fn new_peers(&self) -> Configuration {
        let all_old = self
            .old_configuration
            .voters()
            .union(&self.old_configuration.learners())
            .cloned()
            .collect::<HashSet<_>>();
        Configuration::new(
            self.new_configuration
                .voters()
                .difference(&all_old)
                .cloned(),
            self.new_configuration
                .learners()
                .difference(&all_old)
                .cloned(),
        )
    }

    /// Send a message proposing a "one-by-one" style AddNode configuration.
    /// If the peers are in the midst joint consensus style (Begin/FinalizeMembershipChange) change they should reject it.
    fn propose_add_node_message(&mut self, id: u64) -> Result<()> {
        info!(self.logger, "Proposing add_node message. Target: {:?}", id,);
        let message = build_propose_add_node_message(
            self.old_leader,
            id,
            self.peers[&id].raft_log.last_index() + 1,
        );
        self.dispatch(vec![message])
    }

    /// Send the message which proposes the configuration change.
    fn propose_change_message(&mut self) -> Result<()> {
        info!(
            self.logger,
            "Proposing change message. Target: {:?}", self.new_configuration
        );
        let message = build_propose_change_message(
            self.old_leader,
            self.new_configuration.voters(),
            self.new_configuration.learners(),
            self.peers[&1].raft_log.last_index() + 1,
        );
        self.dispatch(vec![message])
    }

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
    fn expect_apply_membership_change_entry<'a>(
        &mut self,
        peers: impl IntoIterator<Item = &'a u64>,
        entry_type: ConfChangeType,
    ) -> Result<()> {
        for peer in peers {
            debug!(
                self.logger,
                "Advancing peer, expecting an entry.";
                "peer" => peer,
                "entry type" => ?entry_type,
            );
            let peer = self.network.peers.get_mut(peer).unwrap();
            if let Some(entries) = peer.raft_log.next_entries() {
                peer.mut_store().wl().append(&entries).unwrap();
                let mut found = false;
                for entry in &entries {
                    if entry.entry_type() == EntryType::EntryConfChange {
                        let conf_change = ConfChange::decode(&entry.data)?;
                        if conf_change.change_type() == entry_type {
                            found = true;
                            match entry_type {
                                ConfChangeType::BeginMembershipChange => {
                                    peer.begin_membership_change(&conf_change)?
                                }
                                ConfChangeType::FinalizeMembershipChange => {
                                    peer.finalize_membership_change(&conf_change)?
                                }
                                ConfChangeType::AddNode => peer.add_node(conf_change.node_id)?,
                                _ => panic!("Unexpected conf change"),
                            };
                        }
                    }
                    if found {
                        peer.raft_log.stable_to(entry.index, entry.term);
                        peer.raft_log.commit_to(entry.index);
                        peer.commit_apply(entry.index);
                        let hs = peer.hard_state();
                        peer.mut_store().wl().set_hardstate(hs);
                        peer.tick();
                        break;
                    }
                }
                assert!(
                    found,
                    "{:?} message not found for peer {}. Got: {:?}",
                    entry_type, peer.id, entries
                );
            } else {
                panic!("Didn't have any entries {}", peer.id);
            }
        }
        Ok(())
    }

    /// Reads messages from each peer in a given list, and dispatches their message before moving to the next peer.
    ///
    /// Expects each peer to have a message. If the message is not defintely sent use `read_and_dispatch_messages_from`.
    fn expect_read_and_dispatch_messages_from<'a>(
        &mut self,
        peers: impl IntoIterator<Item = &'a u64>,
    ) -> Result<()> {
        let peers = peers.into_iter().cloned();
        for (step, peer) in peers.enumerate() {
            info!(
                self.logger,
                "Expecting and dispatching messages from {peer} at step {step}.",
                peer = peer,
                step = step,
            );
            let messages = self.peers.get_mut(&peer).unwrap().read_messages();
            trace!(
                self.logger,
                "{peer} sends",
                peer = peer;
                "messages" => ?messages,
            );
            assert!(
                !messages.is_empty(),
                "Expected peer {} to have messages at step {}.",
                peer,
                step
            );
            self.dispatch(messages)?;
        }
        Ok(())
    }

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
                    tag: id.to_string(),
                    applied: applied,
                    ..Default::default()
                },
                store,
            )
            .expect("Could not create new Raft")
            .with_logger(&self.logger);

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
            assert_eq!(entry.entry_type(), EntryType::EntryConfChange);
            let conf_change = ConfChange::decode(&entry.data).unwrap();
            assert_eq!(conf_change.change_type(), entry_type);
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
}

fn conf_state<'a>(
    voters: impl IntoIterator<Item = &'a u64>,
    learners: impl IntoIterator<Item = &'a u64>,
) -> ConfState {
    let voters = voters.into_iter().cloned().collect::<Vec<_>>();
    let learners = learners.into_iter().cloned().collect::<Vec<_>>();
    let mut conf_state = ConfState::default();
    conf_state.set_nodes(voters);
    conf_state.set_learners(learners);
    conf_state
}

fn begin_conf_change<'a>(
    voters: impl IntoIterator<Item = &'a u64>,
    learners: impl IntoIterator<Item = &'a u64>,
    index: u64,
) -> ConfChange {
    let conf_state = conf_state(voters, learners);
    let mut conf_change = ConfChange::default();
    conf_change.set_change_type(ConfChangeType::BeginMembershipChange);
    conf_change.set_configuration(conf_state);
    conf_change.set_start_index(index);
    conf_change
}

fn finalize_conf_change<'a>() -> ConfChange {
    let mut conf_change = ConfChange::default();
    conf_change.set_change_type(ConfChangeType::FinalizeMembershipChange);
    conf_change
}

fn begin_entry<'a>(
    voters: impl IntoIterator<Item = &'a u64>,
    learners: impl IntoIterator<Item = &'a u64>,
    index: u64,
) -> Entry {
    let conf_change = begin_conf_change(voters, learners, index);
    let mut data = Vec::with_capacity(ProstMsg::encoded_len(&conf_change));
    conf_change.encode(&mut data).unwrap();
    let mut entry = Entry::default();
    entry.set_entry_type(EntryType::EntryConfChange);
    entry.set_data(data);
    entry.set_index(index);
    entry
}

fn build_propose_change_message<'a>(
    recipient: u64,
    voters: impl IntoIterator<Item = &'a u64>,
    learners: impl IntoIterator<Item = &'a u64>,
    index: u64,
) -> Message {
    let begin_entry = begin_entry(voters, learners, index);
    let mut message = Message::default();
    message.set_to(recipient);
    message.set_msg_type(MessageType::MsgPropose);
    message.set_index(index);
    message.set_entries(vec![begin_entry]);
    message
}

fn build_propose_add_node_message(recipient: u64, added_id: u64, index: u64) -> Message {
    let add_nodes_entry = {
        let mut conf_change = ConfChange::default();
        conf_change.set_change_type(ConfChangeType::AddNode);
        conf_change.set_node_id(added_id);
        let mut data = Vec::with_capacity(ProstMsg::encoded_len(&conf_change));
        conf_change.encode(&mut data).unwrap();
        let mut entry = Entry::default();
        entry.set_entry_type(EntryType::EntryConfChange);
        entry.set_data(data);
        entry.set_index(index);
        entry
    };
    let mut message = Message::default();
    message.set_to(recipient);
    message.set_msg_type(MessageType::MsgPropose);
    message.set_index(index);
    message.set_entries(vec![add_nodes_entry]);
    message
}
