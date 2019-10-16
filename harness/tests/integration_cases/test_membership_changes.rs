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
use hashbrown::{HashMap, HashSet};
use protobuf::{Message as PbMessage, RepeatedField};
use raft::{
    default_logger,
    eraftpb::{ConfChange, ConfChangeType, ConfState, Entry, EntryType, Message, MessageType},
    storage::MemStorage,
    Config, Configuration, Raft, Result,
};

use crate::test_util::new_message;

const MSG_BATCH_SIZE: u64 = 100;

// Test that small cluster is able to progress through adding a voter.
mod three_peers_add_voter {
    use super::*;

    /// In a steady state transition should proceed without issue.
    #[test]
    fn stable() -> Result<()> {
        let l = default_logger();
        let leader = 1;
        let old_configuration = (vec![1, 2, 3], vec![]);
        let new_configuration = (vec![1, 2, 3, 4], vec![]);
        let mut scenario = Scenario::new(leader, old_configuration, new_configuration, &l)?;
        scenario.spawn_new_peers()?;

        let index = scenario.propose_change_message()?;
        scenario.assert_in_membership_change(&[1]);

        info!(
            scenario.logger,
            "Allowing quorum to commit the BeginMembershipChange entry."
        );
        scenario.read_msgs_then_send();

        info!(
            scenario.logger,
            "Advancing leader, now can finalize the entry."
        );
        scenario.assert_can_apply_transition_entry_at_index(
            &[1, 2, 3, 4],
            index,
            ConfChangeType::BeginMembershipChange,
        );

        info!(scenario.logger, "Cluster leaving the joint.");
        scenario.assert_not_in_membership_change(&[1, 2, 3, 4]);
        assert!(scenario.peers.get(&4).unwrap().promotable());
        Ok(())
    }
}

// Test that small cluster is able to progress through adding a learner.
mod three_peers_add_learner {
    use super::*;

    /// In a steady state transition should proceed without issue.
    #[test]
    fn stable() -> Result<()> {
        let l = default_logger();
        let leader = 1;
        let old_configuration = (vec![1, 2, 3], vec![]);
        let new_configuration = (vec![1, 2, 3], vec![4]);
        let mut scenario = Scenario::new(leader, old_configuration, new_configuration, &l)?;
        scenario.spawn_new_peers()?;

        let index = scenario.propose_change_message()?;
        scenario.assert_in_membership_change(&[1]);

        info!(
            scenario.logger,
            "Allowing quorum to commit the BeginMembershipChange entry."
        );
        let messages = scenario.read_messages();
        scenario.send(messages);

        info!(
            scenario.logger,
            "Advancing leader, now can finalize the entry."
        );
        scenario.assert_can_apply_transition_entry_at_index(
            &[1, 2, 3, 4],
            index,
            ConfChangeType::BeginMembershipChange,
        );

        info!(scenario.logger, "Cluster leaving the joint.");
        scenario.assert_not_in_membership_change(&[1, 2, 3, 4]);
        assert!(!scenario.peers.get(&4).unwrap().promotable());
        Ok(())
    }
}

// Test that small cluster is able to progress through removing a learner.
mod remove_learner {
    use super::*;

    /// In a steady state transition should proceed without issue.
    #[test]
    fn stable() -> Result<()> {
        let l = default_logger();
        let leader = 1;
        let old_configuration = (vec![1, 2, 3], vec![4]);
        let new_configuration = (vec![1, 2, 3], vec![]);
        let mut scenario = Scenario::new(leader, old_configuration, new_configuration, &l)?;
        scenario.spawn_new_peers()?;

        let index = scenario.propose_change_message()?;
        scenario.assert_in_membership_change(&[1]);

        info!(
            scenario.logger,
            "Allowing quorum to commit the BeginMembershipChange entry."
        );
        let messages = scenario.read_messages();
        scenario.send(messages);

        info!(
            scenario.logger,
            "Advancing leader, now can finalize the entry."
        );
        scenario.assert_can_apply_transition_entry_at_index(
            &[1, 2, 3],
            index,
            ConfChangeType::BeginMembershipChange,
        );

        info!(scenario.logger, "Cluster leaving the joint.");
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
        let l = default_logger();
        let leader = 1;
        let old_configuration = (vec![1, 2, 3], vec![]);
        let new_configuration = (vec![1, 2], vec![]);
        let mut scenario = Scenario::new(leader, old_configuration, new_configuration, &l)?;
        scenario.spawn_new_peers()?;

        let index = scenario.propose_change_message()?;
        scenario.assert_in_membership_change(&[1]);

        info!(
            scenario.logger,
            "Allowing quorum to commit the BeginMembershipChange entry."
        );
        let messages = scenario.read_messages();
        scenario.send(messages);

        info!(
            scenario.logger,
            "Advancing leader, now can finalize the entry."
        );
        scenario.assert_can_apply_transition_entry_at_index(
            &[1, 2],
            index,
            ConfChangeType::BeginMembershipChange,
        );

        info!(scenario.logger, "Cluster leaving the joint.");
        scenario.assert_not_in_membership_change(&[1, 2, 3]);
        Ok(())
    }
}

// Test that small cluster is able to progress through removing a leader.
mod remove_leader {
    use super::*;

    /// In a steady state transition should proceed without issue.
    #[test]
    fn stable() -> Result<()> {
        let l = default_logger();
        let leader = 1;
        let old_configuration = (vec![1, 2, 3], vec![]);
        let new_configuration = (vec![2, 3], vec![]);
        let mut scenario = Scenario::new(leader, old_configuration, new_configuration.clone(), &l)?;
        scenario.spawn_new_peers()?;

        let index = scenario.propose_change_message()?;
        scenario.assert_in_membership_change(&[1]);

        info!(
            scenario.logger,
            "Allowing quorum to commit the BeginMembershipChange entry."
        );
        let messages = scenario.read_messages();
        scenario.send(messages);

        info!(
            scenario.logger,
            "Advancing leader, now can finalize the entry."
        );
        scenario.assert_can_apply_transition_entry_at_index(
            &[1, 2, 3],
            index,
            ConfChangeType::BeginMembershipChange,
        );

        info!(scenario.logger, "Cluster leaving the joint.");
        scenario.assert_not_in_membership_change(&[1, 2, 3]);

        info!(scenario.logger, "Prompting a new election.");
        {
            let new_leader = scenario.peers.get_mut(&2).unwrap();
            for _ in new_leader.election_elapsed..=(new_leader.randomized_election_timeout() + 1) {
                new_leader.tick();
            }
        }
        let messages = scenario.read_messages();
        scenario.send(messages);

        info!(
            scenario.logger,
            "Verifying that all peers have the right peer group."
        );
        for (_, peer) in scenario.peers.iter() {
            assert_eq!(
                peer.prs().configuration(),
                &new_configuration.clone().into(),
            );
        }

        info!(
            scenario.logger,
            "Verifying that old leader cannot disrupt the cluster."
        );
        {
            let old_leader = scenario.peers.get_mut(&1).unwrap();
            for _ in old_leader.heartbeat_elapsed()..=(old_leader.heartbeat_timeout() + 1) {
                old_leader.tick();
            }
        }
        let messages = scenario.read_messages();
        scenario.send(messages);
        Ok(())
    }

    /// If the leader fails after the `Begin`, then recovers after the `Finalize`, the group should ignore it.
    #[test]
    fn leader_fails_after_finalized_and_recovers() -> Result<()> {
        let l = default_logger();
        let leader = 1;
        let old_configuration = (vec![1, 2, 3], vec![]);
        let new_configuration = (vec![2, 3], vec![]);
        let mut scenario = Scenario::new(leader, old_configuration, new_configuration, &l)?;
        scenario.spawn_new_peers()?;

        let index = scenario.propose_change_message()?;
        scenario.assert_in_membership_change(&[1]);

        info!(
            scenario.logger,
            "Allowing quorum to commit the BeginMembershipChange entry."
        );
        let messages = scenario.read_messages();
        scenario.send(messages);

        info!(
            scenario.logger,
            "Advancing followers, now can finalize the entry."
        );
        scenario.assert_can_apply_transition_entry_at_index(
            &[1, 2, 3],
            index,
            ConfChangeType::BeginMembershipChange,
        );

        scenario.isolate(1); // Simulate the leader failing.
        info!(
            scenario.logger,
            "Promoting new leader, which already has received the finalize entry"
        );
        {
            let new_leader = scenario.peers.get_mut(&2).unwrap();
            for _ in new_leader.election_elapsed..=(new_leader.randomized_election_timeout() + 1) {
                new_leader.tick();
            }
        }
        let messages = scenario.read_messages();
        scenario.send(messages);

        let index = {
            let new_leader = scenario.peers.get(&2).unwrap();
            new_leader.raft_log.last_index()
        };
        scenario.assert_can_apply_transition_entry_at_index(
            &[2, 3],
            index - 1, // Because the last entry is an empty entry.
            ConfChangeType::FinalizeMembershipChange,
        );

        // Here we note that the old leader (1) has NOT applied the finalize operation and thus thinks it is still leader.
        //
        // The Raft paper notes that a removed leader should not disrupt the cluster.
        // It suggests doing this by ignoring any `RequestVote` when it has heard from the leader within the minimum election timeout.
        scenario.recover();
        info!(
            scenario.logger,
            "Verifying that old leader cannot disrupt the cluster."
        );
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

    /// In a steady state transition should proceed without issue.
    #[test]
    fn stable() -> Result<()> {
        let l = default_logger();
        let leader = 1;
        let old_configuration = (vec![1, 2, 3], vec![]);
        let new_configuration = (vec![1, 2, 4], vec![]);
        let mut scenario = Scenario::new(leader, old_configuration, new_configuration, &l)?;
        scenario.spawn_new_peers()?;

        let index = scenario.propose_change_message()?;
        scenario.assert_in_membership_change(&[1]);

        info!(
            scenario.logger,
            "Allowing quorum to commit the BeginMembershipChange entry."
        );
        let messages = scenario.read_messages();
        scenario.send(messages);

        info!(
            scenario.logger,
            "Advancing leader, now can finalize the entry."
        );
        scenario.assert_can_apply_transition_entry_at_index(
            &[1, 2, 4],
            index,
            ConfChangeType::BeginMembershipChange,
        );

        info!(scenario.logger, "Cluster leaving the joint.");
        scenario.assert_not_in_membership_change(&[1, 2, 3, 4]);
        assert!(scenario.peers.get(&4).unwrap().promotable());
        Ok(())
    }

    /// The leader power cycles before actually sending the FinalizeMembershipChange messages.
    #[test]
    fn leader_power_cycles_no_compaction() -> Result<()> {
        let l = default_logger();
        let leader = 1;
        let old_configuration = (vec![1, 2, 3], vec![]);
        let new_configuration = (vec![1, 2, 4], vec![]);
        let mut scenario = Scenario::new(leader, old_configuration, new_configuration, &l)?;
        scenario.spawn_new_peers()?;

        scenario.propose_change_message()?;
        scenario.assert_in_membership_change(&[1]);

        info!(
            scenario.logger,
            "Allowing quorum to commit the BeginMembershipChange entry."
        );
        let messages = scenario.read_messages();
        scenario.send(messages);

        info!(scenario.logger, "Leader power cycles.");
        scenario.power_cycle(&[1]);
        {
            let peer = scenario.peers.get_mut(&1).unwrap();
            for _ in peer.election_elapsed..=(peer.randomized_election_timeout() + 1) {
                peer.tick();
            }
        }
        let messages = scenario.read_messages();
        scenario.send(messages);

        info!(scenario.logger, "Cluster leaving the joint.");
        scenario.assert_not_in_membership_change(&[1, 2, 3, 4]);
        Ok(())
    }

    // Ensure if the old quorum fails during the joint state progress will halt until the peer group is recovered.
    #[test]
    fn leader_power_cycles_compacted_log() -> Result<()> {
        let l = default_logger();
        let leader = 1;
        let old_configuration = (vec![1, 2, 3], vec![]);
        let new_configuration = (vec![1, 2, 4], vec![]);
        let mut scenario = Scenario::new(leader, old_configuration, new_configuration, &l)?;
        scenario.spawn_new_peers()?;

        let index = scenario.propose_change_message()?;
        scenario.assert_in_membership_change(&[1]);

        info!(
            scenario.logger,
            "Allowing quorum to commit the BeginMembershipChange entry."
        );
        let messages = scenario.read_messages();
        scenario.send(messages);

        scenario.assert_not_in_membership_change(&[1, 2, 3, 4]);

        info!(scenario.logger, "Old quorum fails.");
        scenario.isolate(1); // Take 1 down.
        scenario.isolate(2); // Take 2 down.

        info!(
            scenario.logger,
            "Advancing leader, now can finalize the entry."
        );
        scenario.assert_can_apply_transition_entry_at_index(
            &[1, 2, 4],
            index,
            ConfChangeType::BeginMembershipChange,
        );

        info!(
            scenario.logger,
            "Spinning for awhile to ensure nothing spectacular happens"
        );
        for _ in scenario.peers[&leader].heartbeat_elapsed()
            ..=scenario.peers[&leader].heartbeat_timeout()
        {
            scenario.peers.iter_mut().for_each(|(_, peer)| {
                peer.tick();
            });
            let messages = scenario.read_messages();
            scenario.send(messages);
        }

        scenario.assert_not_in_membership_change(&[1, 3]);

        info!(scenario.logger, "Recovering new qourum.");
        scenario.recover();
        for _ in scenario.peers[&leader].heartbeat_elapsed()
            ..=scenario.peers[&leader].heartbeat_timeout()
        {
            scenario.peers.iter_mut().for_each(|(_, peer)| {
                peer.tick();
            });
        }
        let messages = scenario.read_messages();
        scenario.send(messages);

        info!(scenario.logger, "Cluster leaving the joint.");
        scenario.assert_not_in_membership_change(&[1, 2, 4]);
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

        let index = scenario.propose_change_message()?;
        scenario.assert_in_membership_change(&[1]);

        info!(
            scenario.logger,
            "Allowing quorum to commit the BeginMembershipChange entry."
        );
        let messages = scenario.read_messages();
        scenario.send(messages);

        info!(scenario.logger, "New quorum fails.");
        scenario.isolate(4); // Take 4 down.
        scenario.isolate(2); // Take 2 down.

        info!(
            scenario.logger,
            "Advancing leader, now can finalize the entry."
        );
        scenario.assert_can_apply_transition_entry_at_index(
            &[1, 2, 4],
            index,
            ConfChangeType::BeginMembershipChange,
        );

        scenario.assert_not_in_membership_change(&[1, 3]);

        info!(
            scenario.logger,
            "Spinning for awhile to ensure nothing spectacular happens"
        );
        for _ in scenario.peers[&leader].heartbeat_elapsed()
            ..=scenario.peers[&leader].heartbeat_timeout()
        {
            scenario.peers.iter_mut().for_each(|(_, peer)| {
                peer.tick();
            });
            let messages = scenario.read_messages();
            scenario.send(messages);
        }

        scenario.assert_not_in_membership_change(&[1, 3]);

        info!(scenario.logger, "Recovering new qourum.");
        scenario.recover();
        for _ in scenario.peers[&leader].heartbeat_elapsed()
            ..=scenario.peers[&leader].heartbeat_timeout()
        {
            scenario.peers.iter_mut().for_each(|(_, peer)| {
                peer.tick();
            });
        }
        let messages = scenario.read_messages();
        scenario.send(messages);

        info!(scenario.logger, "Cluster leaving the joint.");
        scenario.assert_not_in_membership_change(&[1, 2, 4]);
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

        let index = scenario.propose_change_message()?;
        scenario.assert_in_membership_change(&[1]);

        info!(
            scenario.logger,
            "Allowing quorum to commit the BeginMembershipChange entry."
        );
        let messages = scenario.read_messages();
        scenario.send(messages);

        info!(
            scenario.logger,
            "Advancing leader, now can finalize the entry."
        );
        scenario.assert_can_apply_transition_entry_at_index(
            &[1, 2, 3, 4, 5, 6],
            index,
            ConfChangeType::BeginMembershipChange,
        );

        assert!(scenario.peers.get(&4).unwrap().promotable());
        assert!(scenario.peers.get(&4).unwrap().promotable());
        assert!(!scenario.peers.get(&6).unwrap().promotable());

        info!(scenario.logger, "Cluster leaving the joint.");
        scenario.assert_not_in_membership_change(&[1, 2, 3, 4, 5, 6]);
        Ok(())
    }

    /// In this, a single node (of 3) halts during the transition.
    /// And an another node (of 4) halts during the finalize.
    #[test]
    fn minority_old_followers_halt_at_start() -> Result<()> {
        let l = default_logger();
        let leader = 1;
        let old_configuration = (vec![1, 2, 3], vec![]);
        let new_configuration = (vec![1, 2, 3, 4, 5], vec![6]);
        let mut scenario = Scenario::new(leader, old_configuration, new_configuration, &l)?;
        scenario.spawn_new_peers()?;
        scenario.isolate(3);

        let index = scenario.propose_change_message()?;
        scenario.assert_in_membership_change(&[1]);

        info!(
            scenario.logger,
            "Allowing quorum to commit the BeginMembershipChange entry."
        );
        let messages = scenario.read_messages();
        scenario.send(messages);
        scenario.isolate(4);

        info!(
            scenario.logger,
            "Advancing leader, now can finalize the entry."
        );
        scenario.assert_can_apply_transition_entry_at_index(
            &[1, 2, 4, 5, 6],
            index,
            ConfChangeType::BeginMembershipChange,
        );

        assert!(scenario.peers.get(&4).unwrap().promotable());
        assert!(scenario.peers.get(&4).unwrap().promotable());
        assert!(!scenario.peers.get(&6).unwrap().promotable());

        info!(scenario.logger, "Cluster leaving the joint.");
        scenario.assert_not_in_membership_change(&[1, 2, 5, 6]);

        info!(
            scenario.logger,
            "Ensure the finalize entry can be committed."
        );
        let messages = scenario.read_messages();
        scenario.send(messages);
        let peer = scenario.peers.get(&1).unwrap();
        assert_eq!(peer.raft_log.committed, peer.raft_log.last_index());
        Ok(())
    }
}

mod intermingled_config_changes {
    use super::*;

    // In this test, we make sure that if the peer group is sent a `BeginMembershipChange`, then
    // immediately a `AddNode` entry, that the `AddNode` is rejected by the leader.
    #[test]
    fn begin_then_add_node() -> Result<()> {
        let l = default_logger();
        let leader = 1;
        let old_configuration = (vec![1, 2, 3], vec![]);
        let new_configuration = (vec![1, 2, 3, 4], vec![]);
        let mut scenario = Scenario::new(leader, old_configuration, new_configuration, &l)?;
        scenario.spawn_new_peers()?;

        let index = scenario.propose_change_message()?;
        scenario.assert_in_membership_change(&[1]);

        info!(
            scenario.logger,
            "Allowing quorum to commit the BeginMembershipChange entry."
        );

        {
            info!(
                scenario.logger,
                "Add node should be replaced with an empty entry"
            );
            assert!(scenario.propose_add_node_message(4).is_ok());
            let peer = scenario.peers.get(&1).unwrap();
            let last_index = peer.raft_log.last_index();
            let last_entry = &peer.raft_log.entries(last_index, 1).unwrap()[0];
            assert_eq!(last_entry.get_entry_type(), EntryType::EntryNormal);
        }

        let messages = scenario.read_messages();
        scenario.send(messages);

        info!(
            scenario.logger,
            "Advancing leader, now can finalize the entry."
        );
        scenario.assert_can_apply_transition_entry_at_index(
            &[1, 2, 3, 4],
            index,
            ConfChangeType::BeginMembershipChange,
        );

        info!(scenario.logger, "Cluster leaving the joint.");
        scenario.assert_not_in_membership_change(&[1, 2, 3, 4]);
        assert!(scenario.peers.get(&4).unwrap().promotable());
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
        scenario.assert_in_membership_change(&[1]);

        info!(
            scenario.logger,
            "Allowing quorum to commit the BeginMembershipChange entry."
        );
        let messages = scenario.read_messages();
        scenario.send(messages);

        info!(
            scenario.logger,
            "Persist all peers, and then compact their logs."
        );
        for id in 1..=4 {
            scenario.persist(id);
            let applied = scenario.peers[&id].raft_log.committed;
            let store = scenario.peers.remove(&id).unwrap().raft_log.store.clone();
            store.wl().compact(applied).unwrap();
            let cfg = Config {
                id,
                applied,
                ..Default::default()
            };
            let raft = Raft::new(&cfg, store, &scenario.logger)?;
            scenario.peers.insert(id, raft.into());
        }
        {
            // Let node 1 become the new leader.
            let peer = scenario.peers.get_mut(&1).unwrap();
            for _ in peer.election_elapsed..=(peer.randomized_election_timeout() + 1) {
                peer.tick();
            }
        }
        let messages = scenario.read_messages();
        scenario.send(messages);

        info!(scenario.logger, "Cluster leaving the joint.");
        scenario.assert_not_in_membership_change(&[1, 2, 3, 4]);
        Ok(())
    }
}

mod overwrite {
    use super::*;

    // Ensure that followers' raft log can be overwritten by a new leader in different terms.
    #[test]
    fn overwrite_membership_change_on_followers() -> Result<()> {
        let l = default_logger();
        let leader = 1;
        let old_configuration = (vec![1, 2, 3, 4, 5], vec![]);
        let new_configuration = (vec![1, 2], vec![]);
        let mut scenario = Scenario::new(leader, old_configuration, new_configuration, &l)?;
        scenario.spawn_new_peers()?;

        // Isolate peer 3, 4, and 5.
        scenario.isolate(3);
        scenario.isolate(4);
        scenario.isolate(5);
        scenario.propose_change_message()?;
        scenario.read_msgs_then_send();

        // Peer 1 and 2 should have received the configuration change.
        scenario.assert_in_membership_change(&[1, 2]);
        scenario.assert_not_in_membership_change(&[3, 4, 5]);

        // Let peer 5 become the new leader.
        scenario.recover();
        scenario.isolate(1);
        scenario.isolate(2);
        let message = new_message(5, 5, MessageType::MsgHup, 0);
        scenario.send(vec![message]);
        scenario.read_msgs_then_send();
        scenario.old_leader = 5;

        // Let peer 1 and 2 become followers, and unpause them on the new leader.
        scenario.recover();
        for id in 1..3 {
            // Unpause peer 1 and 2.
            let message = new_message(id, id, MessageType::MsgHup, 0);
            scenario.send(vec![message]);
        }
        scenario.send(vec![new_message(5, 5, MessageType::MsgBeat, 0)]);

        {
            // Let the new leader commit and apply all entries so that it can propose a new
            // configuration change.
            let peer = scenario.network.peers.get_mut(&5).unwrap();
            let hard_state = peer.hard_state();
            peer.store().wl().set_hardstate(hard_state);

            let entries = peer.raft_log.unstable_entries().unwrap_or(&[]).to_vec();
            let committed_entries = peer.raft_log.next_entries().unwrap();
            peer.store().wl().append(&entries).unwrap();
            if let Some(entry) = entries.last() {
                peer.raft_log.stable_to(entry.index, entry.term);
                peer.raft_log.commit_to(entry.index);
            }
            if let Some(entry) = committed_entries.last() {
                peer.commit_apply(entry.index);
            }
        }

        scenario.propose_remove_node_message(1)?;
        let messages = scenario.read_messages();
        scenario.send(messages);
        scenario.assert_not_in_membership_change(&[1, 2, 3, 4, 5]);
        Ok(())
    }

    // Ensure that followers' raft log can be overwritten by a new leader in different terms.
    #[test]
    fn overwrite_conf_change_on_followers() -> Result<()> {
        let l = default_logger();
        let leader = 1;
        let old_configuration = (vec![1, 2, 3, 4, 5], vec![]);
        let new_configuration = (vec![6], vec![]);
        let mut scenario = Scenario::new(leader, old_configuration, new_configuration, &l)?;
        scenario.spawn_new_peers()?;

        // Isolate peer 3, 4, and 5.
        scenario.isolate(3);
        scenario.isolate(4);
        scenario.isolate(5);
        scenario.propose_add_node_message(6)?;
        scenario.read_msgs_then_send();
        for i in &[1, 2, 6] {
            let peer = scenario.network.peers.get_mut(&i).unwrap();
            let voters = peer.conf_states().last().unwrap().conf_state.get_voters();
            assert!(voters.contains(&1) && voters.contains(&6));
        }

        // Let peer 5 become the new leader.
        scenario.recover();
        scenario.isolate(1);
        scenario.isolate(2);
        let message = new_message(5, 5, MessageType::MsgHup, 0);
        scenario.send(vec![message]);
        scenario.read_msgs_then_send();
        scenario.old_leader = 5;

        // Let peer 1 and 2 become followers, and unpause them on the new leader.
        scenario.recover();
        for id in 1..3 {
            // Unpause peer 1 and 2.
            let message = new_message(id, id, MessageType::MsgHup, 0);
            scenario.send(vec![message]);
        }
        scenario.send(vec![new_message(5, 5, MessageType::MsgBeat, 0)]);

        {
            // Let the new leader commit and apply all entries so that it can propose a new
            // configuration change.
            let peer = scenario.network.peers.get_mut(&5).unwrap();
            let hard_state = peer.hard_state();
            peer.store().wl().set_hardstate(hard_state);

            let entries = peer.raft_log.unstable_entries().unwrap_or(&[]).to_vec();
            let committed_entries = peer.raft_log.next_entries().unwrap();
            peer.store().wl().append(&entries).unwrap();
            if let Some(entry) = entries.last() {
                peer.raft_log.stable_to(entry.index, entry.term);
                peer.raft_log.commit_to(entry.index);
            }
            if let Some(entry) = committed_entries.last() {
                peer.commit_apply(entry.index);
            }
        }

        scenario.propose_remove_node_message(1)?;
        let messages = scenario.read_messages();
        scenario.send(messages);
        for i in 1..6 {
            let peer = scenario.network.peers.get_mut(&i).unwrap();
            let voters = peer.conf_states().last().unwrap().conf_state.get_voters();
            assert!(!voters.contains(&1) && !voters.contains(&6));
        }
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

        // Must be sorted.
        let mut starting_peers: Vec<_> = old_configuration
            .voters()
            .iter()
            .chain(old_configuration.learners())
            .cloned()
            .collect();
        starting_peers.sort();

        let starting_peers = starting_peers
            .into_iter()
            .map(|id| {
                let cfg = Config {
                    id,
                    max_size_per_msg: MSG_BATCH_SIZE,
                    ..Default::default()
                };
                let cs = old_configuration.to_conf_state();
                let store = MemStorage::new_with_conf_state(cs);
                Some(Raft::new(&cfg, store, &logger).unwrap().into())
            })
            .collect();

        let mut config = Network::default_config();
        config.max_size_per_msg = MSG_BATCH_SIZE;

        let mut scenario = Scenario {
            old_leader: leader,
            old_configuration,
            new_configuration,
            network: Network::new_with_config(starting_peers, &config, &logger),
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
    /// action.
    fn spawn_new_peers(&mut self) -> Result<()> {
        let new_peers = self.new_peers();
        info!(self.logger, "Creating new peers {:?}", new_peers);
        for id in new_peers.voters().iter().chain(new_peers.learners()) {
            let cfg = Config {
                id: *id,
                max_size_per_msg: MSG_BATCH_SIZE,
                ..Default::default()
            };
            let raft = Raft::new(&cfg, MemStorage::new(), &self.logger)?;
            self.peers.insert(*id, raft.into());
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
        let leader_id = self.old_leader;
        let message = build_propose_add_node_message(
            self.old_leader,
            id,
            self.peers[&leader_id].raft_log.last_index() + 1,
        );
        self.dispatch(vec![message])
    }

    /// Send a message proposing a "one-by-one" style RemoveNode configuration.
    /// If the peers are in the midst joint consensus style (Begin/FinalizeMembershipChange) change they should reject it.
    fn propose_remove_node_message(&mut self, id: u64) -> Result<()> {
        info!(
            self.logger,
            "Proposing remove_node message. Target: {:?}", id,
        );
        let leader_id = self.old_leader;
        let message = build_propose_remove_node_message(
            self.old_leader,
            id,
            self.peers[&leader_id].raft_log.last_index() + 1,
        );
        self.dispatch(vec![message])
    }

    /// Send the message which proposes the configuration change.
    fn propose_change_message(&mut self) -> Result<u64> {
        info!(
            self.logger,
            "Proposing change message. Target: {:?}", self.new_configuration
        );
        let index = self.peers[&self.old_leader].raft_log.last_index() + 1;
        let message = build_propose_change_message(
            self.old_leader,
            self.new_configuration.voters(),
            self.new_configuration.learners(),
            index,
        );
        self.dispatch(vec![message])?;
        Ok(index)
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

    // Persist the peer states, but without applied index.
    fn persist(&mut self, peer: u64) {
        let peer = self.peers.get_mut(&peer).unwrap();
        let store = peer.store().clone();

        if peer.raft_log.unstable().snapshot.is_some() {
            let snap = peer.raft_log.unstable().snapshot.clone().unwrap();
            let idx = snap.get_metadata().index;
            store.wl().apply_snapshot(snap).unwrap();
            peer.raft_log.stable_snap_to(idx);
        }

        let entries = peer.raft_log.unstable_entries().unwrap_or(&[]).to_vec();
        if let Some(entry) = entries.last() {
            store.wl().append(&entries).unwrap();
            peer.raft_log.stable_to(entry.index, entry.term);
            peer.raft_log.commit_to(entry.index);
            let first_unstable_index = entries.first().unwrap().index;
            for i in (0..peer.conf_states().len()).rev() {
                if peer.conf_states()[i].index < first_unstable_index {
                    store.wl().append_conf_states(&peer.conf_states()[i + 1..]);
                    break;
                }
            }
        }

        let hard_state = peer.hard_state();
        store.wl().set_hardstate(hard_state);
    }

    /// Simulate a power cycle in the given nodes.
    ///
    /// This means that the MemStorage and `applied` is kept, but nothing else.
    fn power_cycle<'a>(&mut self, peers: impl IntoIterator<Item = &'a u64>) {
        let peers = peers.into_iter().cloned();
        for id in peers {
            debug!(self.logger, "Power cycling"; "id" => id);
            self.persist(id);

            // Treat all committed entries are applied, so that the leader can campaign
            // after power cycle, otherwise it will wait until they are committed.
            let applied = self.peers[&id].raft_log.committed;
            let store = self.peers.remove(&id).unwrap().raft_log.store.clone();
            let cfg = Config {
                id,
                applied,
                ..Default::default()
            };
            let raft = Raft::new(&cfg, store, &self.logger).unwrap();
            self.peers.insert(id, raft.into());
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
        self.assert_membership_change_entry_at(peers.clone(), index, entry_type);
        let mut messages = Vec::new();
        for peer in peers {
            let peer = self.network.peers.get_mut(peer).unwrap();
            let store = peer.store().clone();

            let hard_state = peer.hard_state();
            store.wl().set_hardstate(hard_state);

            if peer.raft_log.unstable().snapshot.is_some() {
                let snap = peer.raft_log.unstable().snapshot.clone().unwrap();
                let idx = snap.get_metadata().index;
                store.wl().apply_snapshot(snap).unwrap();
                peer.raft_log.stable_snap_to(idx);
            }

            let entries = peer.raft_log.unstable_entries().unwrap_or(&[]).to_vec();
            let committed_entries = peer.raft_log.next_entries().unwrap();
            match entries.last() {
                Some(entry) if entry.index >= index => {
                    store.wl().append(&entries).unwrap();
                    peer.raft_log.stable_to(entry.index, entry.term);
                    peer.raft_log.commit_to(entry.index);
                    let first_unstable_index = entries.first().unwrap().index;
                    for i in (0..peer.conf_states().len()).rev() {
                        if peer.conf_states()[i].index < first_unstable_index {
                            store.wl().append_conf_states(&peer.conf_states()[i + 1..]);
                            break;
                        }
                    }
                }
                _ => panic!("{} transition entry should be unstable", peer.id),
            }
            match committed_entries.last() {
                Some(entry) if entry.index >= index => {
                    peer.commit_apply(entry.index);
                }
                _ => panic!("{} transition entry should be committed", peer.id),
            }

            messages.extend_from_slice(&peer.msgs);
            peer.msgs.clear();
        }
        self.dispatch(messages).unwrap();
    }

    fn read_msgs_then_send(&mut self) {
        let msgs = self.network.read_messages();
        self.network.filter_and_send(msgs);
    }
}

fn conf_state<'a>(
    voters: impl IntoIterator<Item = &'a u64>,
    learners: impl IntoIterator<Item = &'a u64>,
) -> ConfState {
    let voters = voters.into_iter().cloned().collect::<Vec<_>>();
    let learners = learners.into_iter().cloned().collect::<Vec<_>>();
    let mut conf_state = ConfState::default();
    conf_state.nodes = voters;
    conf_state.learners = learners;
    conf_state
}

fn begin_entry<'a>(
    voters: impl IntoIterator<Item = &'a u64>,
    learners: impl IntoIterator<Item = &'a u64>,
    index: u64,
) -> Entry {
    let mut conf_change = ConfChange::default();
    conf_change.set_change_type(ConfChangeType::BeginMembershipChange);
    conf_change.set_configuration(conf_state(voters, learners));
    conf_change.set_start_index(index);
    let data = protobuf::Message::write_to_bytes(&conf_change).unwrap();
    let mut entry = Entry::default();
    entry.set_entry_type(EntryType::EntryConfChange);
    entry.data = data;
    entry.index = index;
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
    message.to = recipient;
    message.set_msg_type(MessageType::MsgPropose);
    message.index = index;
    message.entries = vec![begin_entry].into();
    message
}

fn build_propose_add_node_message(recipient: u64, added_id: u64, index: u64) -> Message {
    let add_nodes_entry = {
        let mut conf_change = ConfChange::default();
        conf_change.set_change_type(ConfChangeType::AddNode);
        conf_change.set_node_id(added_id);
        let data = protobuf::Message::write_to_bytes(&conf_change).unwrap();
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
    message.set_entries(RepeatedField::from(vec![add_nodes_entry]));
    message
}

fn build_propose_remove_node_message(recipient: u64, added_id: u64, index: u64) -> Message {
    let add_nodes_entry = {
        let mut conf_change = ConfChange::default();
        conf_change.set_change_type(ConfChangeType::RemoveNode);
        conf_change.set_node_id(added_id);
        let data = protobuf::Message::write_to_bytes(&conf_change).unwrap();
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
