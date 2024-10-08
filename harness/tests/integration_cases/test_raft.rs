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

use std::cmp;
use std::collections::HashMap;
use std::panic::{self, AssertUnwindSafe};

use harness::*;
use protobuf::Message as PbMessage;
use raft::eraftpb::*;
use raft::storage::MemStorage;
use raft::*;
use raft_proto::*;
use slog::Logger;

use crate::integration_cases::test_raft_paper::commit_noop_entry;
use crate::test_util::*;

type HashSet<K> = std::collections::HashSet<K, std::hash::BuildHasherDefault<fxhash::FxHasher>>;

fn read_messages<T: Storage>(raft: &mut Raft<T>) -> Vec<Message> {
    raft.msgs.drain(..).collect()
}

fn ents_with_config(
    terms: &[u64],
    pre_vote: bool,
    id: u64,
    peers: Vec<u64>,
    l: &Logger,
) -> Interface {
    let store = MemStorage::new_with_conf_state((peers.clone(), vec![]));
    for (i, term) in terms.iter().enumerate() {
        let mut e = Entry::default();
        e.index = i as u64 + 1;
        e.term = *term;
        store.wl().append(&[e]).expect("");
    }
    let mut raft = new_test_raft_with_prevote(id, peers, 5, 1, store, pre_vote, l);
    raft.reset(terms[terms.len() - 1]);
    raft
}

fn assert_raft_log(
    prefix: &str,
    raft_log: &RaftLog<MemStorage>,
    (committed, applied, last): (u64, u64, u64),
) {
    assert_eq!(
        raft_log.committed, committed,
        "{}committed = {}, want = {}",
        prefix, raft_log.committed, committed
    );
    assert_eq!(
        raft_log.applied, applied,
        "{}applied = {}, want = {}",
        prefix, raft_log.applied, applied
    );
    assert_eq!(
        raft_log.last_index(),
        last,
        "{}last_index = {}, want = {}",
        prefix,
        raft_log.last_index(),
        last
    );
}

// voted_with_config creates a raft state machine with vote and term set
// to the given value but no log entries (indicating that it voted in
// the given term but has not receive any logs).
fn voted_with_config(
    vote: u64,
    term: u64,
    pre_vote: bool,
    id: u64,
    peers: Vec<u64>,
    l: &Logger,
) -> Interface {
    let store = MemStorage::new_with_conf_state((peers.clone(), vec![]));
    store.wl().mut_hard_state().vote = vote;
    store.wl().mut_hard_state().term = term;
    let mut raft = new_test_raft_with_prevote(id, peers, 5, 1, store, pre_vote, l);
    raft.reset(term);
    raft
}

// Persist committed index and fetch next entries.
fn next_ents(r: &mut Raft<MemStorage>, s: &MemStorage) -> Vec<Entry> {
    let unstable = r.raft_log.unstable_entries().to_vec();
    if let Some(e) = unstable.last() {
        let (last_idx, last_term) = (e.get_index(), e.get_term());
        r.raft_log.stable_entries(last_idx, last_term);
        s.wl().append(&unstable).expect("");
        r.on_persist_entries(last_idx, last_term);
    }
    let ents = r.raft_log.next_entries(None);
    r.commit_apply(r.raft_log.committed);
    ents.unwrap_or_default()
}

#[test]
fn test_progress_committed_index() {
    let l = default_logger();
    let mut nt = Network::new(vec![None, None, None], &l);

    // set node 1 as Leader
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
    assert_eq!(nt.peers[&1].state, StateRole::Leader);

    assert_raft_log("#1: ", &nt.peers[&1].raft_log, (1, 0, 1));
    assert_raft_log("#2: ", &nt.peers[&2].raft_log, (1, 0, 1));
    assert_raft_log("#3: ", &nt.peers[&3].raft_log, (1, 0, 1));

    assert_eq!(
        (
            nt.peers[&1].prs().get(1).unwrap().committed_index,
            nt.peers[&1].prs().get(2).unwrap().committed_index,
            nt.peers[&1].prs().get(3).unwrap().committed_index
        ),
        (1, 1, 1)
    );

    // #1 test append entries
    // append entries between 1 and 2
    let mut test_entries = Entry::default();
    test_entries.data = (b"testdata" as &'static [u8]).into();
    let m = new_message_with_entries(1, 1, MessageType::MsgPropose, vec![test_entries]);
    nt.cut(1, 3);
    nt.send(vec![m.clone(), m]);
    nt.recover();

    assert_raft_log("#1: ", &nt.peers[&1].raft_log, (3, 0, 3));
    assert_raft_log("#2: ", &nt.peers[&2].raft_log, (3, 0, 3));
    assert_raft_log("#3: ", &nt.peers[&3].raft_log, (1, 0, 1));

    assert_eq!(
        (
            nt.peers[&1].prs().get(1).unwrap().committed_index,
            nt.peers[&1].prs().get(2).unwrap().committed_index,
            nt.peers[&1].prs().get(3).unwrap().committed_index
        ),
        (3, 3, 1)
    );

    // #2 test heartbeat
    let heartbeat = new_message(1, 1, MessageType::MsgBeat, 0);
    nt.send(vec![heartbeat]);

    assert_raft_log("#1: ", &nt.peers[&1].raft_log, (3, 0, 3));
    assert_raft_log("#2: ", &nt.peers[&2].raft_log, (3, 0, 3));
    assert_raft_log("#3: ", &nt.peers[&3].raft_log, (3, 0, 3));

    assert_eq!(
        (
            nt.peers[&1].prs().get(1).unwrap().committed_index,
            nt.peers[&1].prs().get(2).unwrap().committed_index,
            nt.peers[&1].prs().get(3).unwrap().committed_index
        ),
        (3, 3, 3)
    );

    // set node 2 as Leader
    nt.send(vec![new_message(2, 2, MessageType::MsgHup, 0)]);
    assert_eq!(nt.peers[&2].state, StateRole::Leader);

    assert_raft_log("#1: ", &nt.peers[&1].raft_log, (4, 0, 4));
    assert_raft_log("#2: ", &nt.peers[&2].raft_log, (4, 0, 4));
    assert_raft_log("#3: ", &nt.peers[&3].raft_log, (4, 0, 4));

    assert_eq!(
        (
            nt.peers[&2].prs().get(1).unwrap().committed_index,
            nt.peers[&2].prs().get(2).unwrap().committed_index,
            nt.peers[&2].prs().get(3).unwrap().committed_index
        ),
        (4, 4, 4)
    );

    // #3 test append entries rejection (fails to update committed index)
    nt.isolate(2);
    nt.send(vec![new_message(2, 2, MessageType::MsgPropose, 2)]);
    nt.recover();
    nt.dispatch(vec![new_message(2, 2, MessageType::MsgPropose, 1)])
        .expect("");

    // [msg_type: MsgAppend to: 1 from: 2 term: 2 log_term: 2 index: 6 entries {term: 2 index: 7 data: "somedata"} commit: 4,
    // msg_type: MsgAppend to: 3 from: 2 term: 2 log_term: 2 index: 6 entries {term: 2 index: 7 data: "somedata"} commit: 4]
    let msg_append = nt.read_messages();

    nt.dispatch(msg_append).expect("");

    // [msg_type: MsgAppendResponse to: 2 from: 1 term: 2 index: 6 commit: 4 reject: true reject_hint: 4,
    // msg_type: MsgAppendResponse to: 2 from: 3 term: 2 index: 6 commit: 4 reject: true reject_hint: 4]
    let msg_append_response = nt.read_messages();

    nt.dispatch(msg_append_response).expect("");

    // [msg_type: MsgAppend to: 3 from: 2 term: 2 log_term: 2 index: 4 entries {term: 2 index: 5 data: "somedata"} entries {term: 2 index: 6 data: "somedata"} entries {term: 2 index: 7 data: "somedata"} commit: 4,
    // msg_type: MsgAppend to: 1 from: 2 term: 2 log_term: 2 index: 4 entries {term: 2 index: 5 data: "somedata"} entries {term: 2 index: 6 data: "somedata"} entries {term: 2 index: 7 data: "somedata"} commit: 4]
    let msg_append = nt.read_messages();

    // committed index remain the same
    assert_eq!(
        (
            nt.peers[&2].prs().get(1).unwrap().committed_index,
            nt.peers[&2].prs().get(2).unwrap().committed_index,
            nt.peers[&2].prs().get(3).unwrap().committed_index
        ),
        (4, 4, 4)
    );

    // resend append
    nt.send(msg_append);

    // log is up-to-date
    assert_eq!(
        (
            nt.peers[&2].prs().get(1).unwrap().committed_index,
            nt.peers[&2].prs().get(2).unwrap().committed_index,
            nt.peers[&2].prs().get(3).unwrap().committed_index
        ),
        (7, 7, 7)
    );

    // set node 1 as Leader again
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
    assert_eq!(nt.peers[&1].state, StateRole::Leader);

    assert_raft_log("#1: ", &nt.peers[&1].raft_log, (8, 0, 8));
    assert_raft_log("#2: ", &nt.peers[&2].raft_log, (8, 0, 8));
    assert_raft_log("#3: ", &nt.peers[&3].raft_log, (8, 0, 8));

    // update to 8
    assert_eq!(
        (
            nt.peers[&1].prs().get(1).unwrap().committed_index,
            nt.peers[&1].prs().get(2).unwrap().committed_index,
            nt.peers[&1].prs().get(3).unwrap().committed_index
        ),
        (8, 8, 8)
    );

    // #4 pass a smaller committed index, it occurs when the append response delay

    nt.dispatch(vec![
        new_message(1, 1, MessageType::MsgPropose, 1),
        new_message(1, 1, MessageType::MsgPropose, 1),
    ])
    .expect("");
    let msg_append = nt.read_messages();
    nt.dispatch(msg_append).expect("");
    let msg_append_response = nt.read_messages();
    nt.dispatch(msg_append_response).expect("");
    let msg_append = nt.read_messages();
    nt.dispatch(msg_append).expect("");
    let mut msg_append_response = nt.read_messages();
    // m1: msg_type: MsgAppendResponse to: 1 from: 3 term: 3 index: 10 commit: 10
    // m2: msg_type: MsgAppendResponse to: 1 from: 2 term: 3 index: 10 commit: 10
    let m1 = msg_append_response.remove(1);
    let m2 = msg_append_response.remove(2);
    nt.send(vec![m1, m2]);

    assert_eq!(
        (
            nt.peers[&1].prs().get(1).unwrap().committed_index,
            nt.peers[&1].prs().get(2).unwrap().committed_index,
            nt.peers[&1].prs().get(3).unwrap().committed_index
        ),
        (10, 10, 10)
    );

    // committed index remain 10

    // msg_type: MsgAppendResponse to: 1 from: 2 term: 3 index: 10 commit: 9,
    // msg_type: MsgAppendResponse to: 1 from: 3 term: 3 index: 10 commit: 9
    nt.send(msg_append_response);
    assert_eq!(
        (
            nt.peers[&1].prs().get(1).unwrap().committed_index,
            nt.peers[&1].prs().get(2).unwrap().committed_index,
            nt.peers[&1].prs().get(3).unwrap().committed_index
        ),
        (10, 10, 10)
    );
}

#[test]
fn test_progress_leader() {
    let l = default_logger();
    let mut raft = new_test_raft(1, vec![1, 2], 5, 1, new_storage(), &l);
    raft.become_candidate();
    raft.become_leader();
    // For no-op entry
    raft.persist();
    raft.mut_prs().get_mut(2).unwrap().become_replicate();

    let prop_msg = new_message(1, 1, MessageType::MsgPropose, 1);
    for i in 0..5 {
        assert_eq!(
            raft.mut_prs().get_mut(1).unwrap().state,
            ProgressState::Replicate
        );

        let matched = raft.mut_prs().get_mut(1).unwrap().matched;
        let next_idx = raft.mut_prs().get_mut(1).unwrap().next_idx;
        assert_eq!(matched, i + 1);
        assert_eq!(next_idx, matched + 1);

        raft.step(prop_msg.clone()).unwrap();
        raft.persist();
    }
}

// test_progress_resume_by_heartbeat_resp ensures raft.heartbeat reset progress.paused by
// heartbeat response.
#[test]
fn test_progress_resume_by_heartbeat_resp() {
    let l = default_logger();
    let mut raft = new_test_raft(1, vec![1, 2], 5, 1, new_storage(), &l);
    raft.become_candidate();
    raft.become_leader();
    raft.mut_prs().get_mut(2).unwrap().paused = true;

    raft.step(new_message(1, 1, MessageType::MsgBeat, 0))
        .expect("");
    assert!(raft.prs().get(2).unwrap().paused);

    raft.mut_prs().get_mut(2).unwrap().become_replicate();
    raft.step(new_message(2, 1, MessageType::MsgHeartbeatResponse, 0))
        .expect("");
    assert!(!raft.prs().get(2).unwrap().paused);
}

#[test]
fn test_progress_paused() {
    let l = default_logger();
    let mut raft = new_test_raft(1, vec![1, 2], 5, 1, new_storage(), &l);
    raft.become_candidate();
    raft.become_leader();
    let mut m = Message::default();
    m.from = 1;
    m.to = 1;
    m.set_msg_type(MessageType::MsgPropose);
    let mut e = Entry::default();
    e.data = (b"some_data" as &'static [u8]).into();
    m.entries = vec![e].into();
    raft.step(m.clone()).expect("");
    raft.step(m.clone()).expect("");
    raft.step(m).expect("");
    let ms = read_messages(&mut raft);
    assert_eq!(ms.len(), 1);
}

#[test]
fn test_progress_flow_control() {
    let l = default_logger();
    let mut cfg = new_test_config(1, 5, 1);
    cfg.max_inflight_msgs = 3;
    cfg.max_size_per_msg = 2048;
    let s = MemStorage::new_with_conf_state((vec![1, 2], vec![]));
    let mut r = new_test_raft_with_config(&cfg, s, &l);
    r.become_candidate();
    r.become_leader();

    // Throw away all the messages relating to the initial election.
    r.read_messages();

    // While node 2 is in probe state, propose a bunch of entries.
    r.mut_prs().get_mut(2).unwrap().become_probe();
    let data: String = "a".repeat(1000);
    for _ in 0..10 {
        let msg = new_message_with_entries(
            1,
            1,
            MessageType::MsgPropose,
            vec![new_entry(0, 0, Some(&data))],
        );
        r.step(msg).unwrap();
    }

    let mut ms = r.read_messages();
    // First append has two entries: the empty entry to confirm the
    // election, and the first proposal (only one proposal gets sent
    // because we're in probe state).
    assert_eq!(ms.len(), 1);
    assert_eq!(ms[0].msg_type, MessageType::MsgAppend);
    assert_eq!(ms[0].entries.len(), 2);
    assert_eq!(ms[0].entries[0].data.len(), 0);
    assert_eq!(ms[0].entries[1].data.len(), 1000);

    // When this append is acked, we change to replicate state and can
    // send multiple messages at once.
    let mut msg = new_message(2, 1, MessageType::MsgAppendResponse, 0);
    msg.index = ms[0].entries[1].index;
    r.step(msg).unwrap();
    ms = r.read_messages();
    assert_eq!(ms.len(), 3);
    for (i, m) in ms.iter().enumerate() {
        if m.msg_type != MessageType::MsgAppend {
            panic!("{}: expected MsgAppend, got {:?}", i, m.msg_type);
        }
        if m.entries.len() != 2 {
            panic!("{}: expected 2 entries, got {}", i, m.entries.len());
        }
    }

    // Ack all three of those messages together and get the last two
    // messages (containing three entries).
    let mut msg = new_message(2, 1, MessageType::MsgAppendResponse, 0);
    msg.index = ms[2].entries[1].index;
    r.step(msg).unwrap();
    ms = r.read_messages();
    assert_eq!(ms.len(), 2);
    for (i, m) in ms.iter().enumerate() {
        if m.msg_type != MessageType::MsgAppend {
            panic!("{}: expected MsgAppend, got {:?}", i, m.msg_type);
        }
    }
    assert_eq!(ms[0].entries.len(), 2);
    assert_eq!(ms[1].entries.len(), 1);
}

#[test]
fn test_leader_election() {
    let l = default_logger();
    test_leader_election_with_config(false, &l);
}

#[test]
fn test_leader_election_pre_vote() {
    let l = default_logger();
    test_leader_election_with_config(true, &l);
}

fn test_leader_election_with_config(pre_vote: bool, l: &Logger) {
    let mut config = Network::default_config();
    config.pre_vote = pre_vote;
    let mut tests = vec![
        (
            Network::new_with_config(vec![None, None, None], &config, l),
            StateRole::Leader,
            1,
        ),
        (
            Network::new_with_config(vec![None, None, NOP_STEPPER], &config, l),
            StateRole::Leader,
            1,
        ),
        (
            Network::new_with_config(vec![None, NOP_STEPPER, NOP_STEPPER], &config, l),
            StateRole::Candidate,
            1,
        ),
        (
            Network::new_with_config(vec![None, NOP_STEPPER, NOP_STEPPER, None], &config, l),
            StateRole::Candidate,
            1,
        ),
        (
            Network::new_with_config(vec![None, NOP_STEPPER, NOP_STEPPER, None, None], &config, l),
            StateRole::Leader,
            1,
        ),
        // three logs further along than 0, but in the same term so rejection
        // are returned instead of the votes being ignored.
        (
            Network::new_with_config(
                vec![
                    None,
                    Some(ents_with_config(&[1], pre_vote, 2, vec![1, 2, 3, 4, 5], l)),
                    Some(ents_with_config(&[1], pre_vote, 3, vec![1, 2, 3, 4, 5], l)),
                    Some(ents_with_config(
                        &[1, 1],
                        pre_vote,
                        4,
                        vec![1, 2, 3, 4, 5],
                        l,
                    )),
                    None,
                ],
                &config,
                l,
            ),
            StateRole::Follower,
            1,
        ),
    ];

    for (i, &mut (ref mut network, state, term)) in tests.iter_mut().enumerate() {
        let mut m = Message::default();
        m.from = 1;
        m.to = 1;
        m.set_msg_type(MessageType::MsgHup);
        network.send(vec![m]);
        let raft = &network.peers[&1];
        let (exp_state, exp_term) = if state == StateRole::Candidate && pre_vote {
            // In pre-vote mode, an election that fails to complete
            // leaves the node in pre-candidate state without advancing
            // the term.
            (StateRole::PreCandidate, 0)
        } else {
            (state, term)
        };
        if raft.state != exp_state {
            panic!("#{}: state = {:?}, want {:?}", i, raft.state, exp_state);
        }
        if raft.term != exp_term {
            panic!("#{}: term = {}, want {}", i, raft.term, exp_term)
        }
    }
}

#[test]
fn test_leader_cycle() {
    let l = default_logger();
    test_leader_cycle_with_config(false, &l)
}

#[test]
fn test_leader_cycle_pre_vote() {
    let l = default_logger();
    test_leader_cycle_with_config(true, &l)
}

// test_leader_cycle verifies that each node in a cluster can campaign
// and be elected in turn. This ensures that elections (including
// pre-vote) work when not starting from a clean state (as they do in
// test_leader_election)
fn test_leader_cycle_with_config(pre_vote: bool, l: &Logger) {
    let mut config = Network::default_config();
    config.pre_vote = pre_vote;
    let mut network = Network::new_with_config(vec![None, None, None], &config, l);
    for campaigner_id in 1..4 {
        network.send(vec![new_message(
            campaigner_id,
            campaigner_id,
            MessageType::MsgHup,
            0,
        )]);

        for sm in network.peers.values() {
            if sm.id == campaigner_id && sm.state != StateRole::Leader {
                panic!(
                    "pre_vote={}: campaigning node {} state = {:?}, want Leader",
                    pre_vote, sm.id, sm.state
                );
            } else if sm.id != campaigner_id && sm.state != StateRole::Follower {
                panic!(
                    "pre_vote={}: after campaign of node {}, node {} had state = {:?}, want \
                     Follower",
                    pre_vote, campaigner_id, sm.id, sm.state
                );
            }
        }
    }
}

#[test]
fn test_leader_election_overwrite_newer_logs() {
    let l = default_logger();
    test_leader_election_overwrite_newer_logs_with_config(false, &l);
}

#[test]
fn test_leader_election_overwrite_newer_logs_pre_vote() {
    let l = default_logger();
    test_leader_election_overwrite_newer_logs_with_config(true, &l);
}

// test_leader_election_overwrite_newer_logs tests a scenario in which a
// newly-elected leader does *not* have the newest (i.e. highest term)
// log entries, and must overwrite higher-term log entries with
// lower-term ones.
fn test_leader_election_overwrite_newer_logs_with_config(pre_vote: bool, l: &Logger) {
    // This network represents the results of the following sequence of
    // events:
    // - Node 1 won the election in term 1.
    // - Node 1 replicated a log entry to node 2 but died before sending
    //   it to other nodes.
    // - Node 3 won the second election in term 2.
    // - Node 3 wrote an entry to its logs but died without sending it
    //   to any other nodes.
    //
    // At this point, nodes 1, 2, and 3 all have uncommitted entries in
    // their logs and could win an election at term 3. The winner's log
    // entry overwrites the loser's. (test_leader_sync_follower_log tests
    // the case where older log entries are overwritten, so this test
    // focuses on the case where the newer entries are lost).
    let peers = vec![1, 2, 3, 4, 5];
    let mut config = Network::default_config();
    config.pre_vote = pre_vote;
    let mut network = Network::new_with_config(
        vec![
            Some(ents_with_config(&[1], pre_vote, 1, peers.clone(), l)), // Node 1: Won first election
            Some(ents_with_config(&[1], pre_vote, 2, peers.clone(), l)), // Node 2: Get logs from node 1
            Some(ents_with_config(&[2], pre_vote, 3, peers.clone(), l)), // Node 3: Won second election
            Some(voted_with_config(3, 2, pre_vote, 4, peers.clone(), l)), // Node 4: Voted but didn't get logs
            Some(voted_with_config(3, 2, pre_vote, 5, peers, l)), // Node 5: Voted but didn't get logs
        ],
        &config,
        l,
    );

    // Node 1 campaigns. The election fails because a quorum of nodes
    // know about the election that already happened at term 2. Node 1's
    // term is pushed ahead to 2.
    network.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
    assert_eq!(network.peers[&1].state, StateRole::Follower);
    assert_eq!(network.peers[&1].term, 2);

    // Node 1 campaigns again with a higher term. this time it succeeds.
    network.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
    assert_eq!(network.peers[&1].state, StateRole::Leader);
    assert_eq!(network.peers[&1].term, 3);

    // Now all nodes agree on a log entry with term 1 at index 1 (and
    // term 3 at index 2).
    for (id, sm) in &network.peers {
        let entries = sm.raft_log.all_entries();
        assert_eq!(
            entries.len(),
            2,
            "node {}: entries.len() == {}, want 2",
            id,
            entries.len()
        );
        assert_eq!(
            entries[0].term, 1,
            "node {}: term at index 1 == {}, want 1",
            id, entries[0].term
        );
        assert_eq!(
            entries[1].term, 3,
            "node {}: term at index 2 == {}, want 3",
            id, entries[1].term
        );
    }
}

#[test]
fn test_vote_from_any_state() {
    let l = default_logger();
    test_vote_from_any_state_for_type(MessageType::MsgRequestVote, &l);
}

#[test]
fn test_prevote_from_any_state() {
    let l = default_logger();
    test_vote_from_any_state_for_type(MessageType::MsgRequestPreVote, &l);
}

fn test_vote_from_any_state_for_type(vt: MessageType, l: &Logger) {
    let all_states = vec![
        StateRole::Follower,
        StateRole::Candidate,
        StateRole::PreCandidate,
        StateRole::Leader,
    ];
    for state in all_states {
        let mut r = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage(), l);
        r.term = 1;
        match state {
            StateRole::Follower => {
                let term = r.term;
                r.become_follower(term, 3);
            }
            StateRole::PreCandidate => r.become_pre_candidate(),
            StateRole::Candidate => r.become_candidate(),
            StateRole::Leader => {
                r.become_candidate();
                r.become_leader();
            }
        }
        // Note that setting our state above may have advanced r.term
        // past its initial value.
        let orig_term = r.term;
        let new_term = r.term + 1;

        let mut msg = new_message(2, 1, vt, 0);
        msg.term = new_term;
        msg.log_term = new_term;
        msg.index = 42;
        r.step(msg)
            .unwrap_or_else(|_| panic!("{:?},{:?}: step failed", vt, state));
        assert_eq!(
            r.msgs.len(),
            1,
            "{:?},{:?}: {} response messages, want 1: {:?}",
            vt,
            state,
            r.msgs.len(),
            r.msgs
        );
        let resp = &r.msgs[0];
        assert_eq!(
            resp.get_msg_type(),
            vote_resp_msg_type(vt),
            "{:?},{:?}: response message is {:?}, want {:?}",
            vt,
            state,
            resp.get_msg_type(),
            vote_resp_msg_type(vt)
        );
        assert!(!resp.reject, "{:?},{:?}: unexpected rejection", vt, state);

        // If this was a real vote, we reset our state and term.
        if vt == MessageType::MsgRequestVote {
            assert_eq!(
                r.state,
                StateRole::Follower,
                "{:?},{:?}, state {:?}, want {:?}",
                vt,
                state,
                r.state,
                StateRole::Follower
            );
            assert_eq!(
                r.term, new_term,
                "{:?},{:?}, term {}, want {}",
                vt, state, r.term, new_term
            );
            assert_eq!(r.vote, 2, "{:?},{:?}, vote {}, want 2", vt, state, r.vote);
        } else {
            // In a pre-vote, nothing changes.
            assert_eq!(
                r.state, state,
                "{:?},{:?}, state {:?}, want {:?}",
                vt, state, r.state, state
            );
            assert_eq!(
                r.term, orig_term,
                "{:?},{:?}, term {}, want {}",
                vt, state, r.term, orig_term
            );
            // If state == Follower or PreCandidate, r hasn't voted yet.
            // In Candidate or Leader, it's voted for itself.
            assert!(
                r.vote == INVALID_ID || r.vote == 1,
                "{:?},{:?}, vote {}, want {:?} or 1",
                vt,
                state,
                r.vote,
                INVALID_ID
            );
        }
    }
}

#[test]
fn test_log_replication() {
    let l = default_logger();
    let mut tests = vec![
        (
            Network::new(vec![None, None, None], &l),
            vec![new_message(1, 1, MessageType::MsgPropose, 1)],
            2,
        ),
        (
            Network::new(vec![None, None, None], &l),
            vec![
                new_message(1, 1, MessageType::MsgPropose, 1),
                new_message(1, 2, MessageType::MsgHup, 0),
                new_message(1, 2, MessageType::MsgPropose, 1),
            ],
            4,
        ),
    ];

    for (i, &mut (ref mut network, ref msgs, wcommitted)) in tests.iter_mut().enumerate() {
        network.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
        for m in msgs {
            network.send(vec![m.clone()]);
        }

        for (j, x) in &mut network.peers {
            if x.raft_log.committed != wcommitted {
                panic!(
                    "#{}.{}: committed = {}, want {}",
                    i, j, x.raft_log.committed, wcommitted
                );
            }

            let mut ents = next_ents(x, &network.storage[j]);
            let ents: Vec<Entry> = ents.drain(..).filter(|e| !e.data.is_empty()).collect();
            for (k, m) in msgs
                .iter()
                .filter(|m| m.get_msg_type() == MessageType::MsgPropose)
                .enumerate()
            {
                if ents[k].data != m.entries[0].data {
                    panic!(
                        "#{}.{}: data = {:?}, want {:?}",
                        i, j, ents[k].data, m.entries[0].data
                    );
                }
            }
        }
    }
}

#[test]
fn test_single_node_commit() {
    let l = default_logger();
    let mut tt = Network::new(vec![None], &l);
    tt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
    tt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);
    tt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);
    assert_eq!(tt.peers[&1].raft_log.committed, 3);
}

// test_cannot_commit_without_new_term_entry tests the entries cannot be committed
// when leader changes, no new proposal comes in and ChangeTerm proposal is
// filtered.
#[test]
fn test_cannot_commit_without_new_term_entry() {
    let l = default_logger();
    let mut tt = Network::new(vec![None, None, None, None, None], &l);
    tt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    // 0 cannot reach 2, 3, 4
    tt.cut(1, 3);
    tt.cut(1, 4);
    tt.cut(1, 5);

    tt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);
    tt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);

    assert_eq!(tt.peers[&1].raft_log.committed, 1);

    // network recovery
    tt.recover();
    // avoid committing ChangeTerm proposal
    tt.ignore(MessageType::MsgAppend);

    // elect 2 as the new leader with term 2
    tt.send(vec![new_message(2, 2, MessageType::MsgHup, 0)]);

    // no log entries from previous term should be committed
    assert_eq!(tt.peers[&2].raft_log.committed, 1);

    tt.recover();
    // send heartbeat; reset wait
    tt.send(vec![new_message(2, 2, MessageType::MsgBeat, 0)]);
    // append an entry at current term
    tt.send(vec![new_message(2, 2, MessageType::MsgPropose, 1)]);
    // expect the committed to be advanced
    assert_eq!(tt.peers[&2].raft_log.committed, 5);
}

// test_commit_without_new_term_entry tests the entries could be committed
// when leader changes, no new proposal comes in.
#[test]
fn test_commit_without_new_term_entry() {
    let l = default_logger();
    let mut tt = Network::new(vec![None, None, None, None, None], &l);
    tt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    // 0 cannot reach 2, 3, 4
    tt.cut(1, 3);
    tt.cut(1, 4);
    tt.cut(1, 5);

    tt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);
    tt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);

    assert_eq!(tt.peers[&1].raft_log.committed, 1);

    // network recovery
    tt.recover();

    // elect 1 as the new leader with term 2
    // after append a ChangeTerm entry from the current term, all entries
    // should be committed
    tt.send(vec![new_message(2, 2, MessageType::MsgHup, 0)]);

    assert_eq!(tt.peers[&1].raft_log.committed, 4);
}

#[test]
fn test_dueling_candidates() {
    let l = default_logger();
    let a = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage(), &l);
    let b = new_test_raft(2, vec![1, 2, 3], 10, 1, new_storage(), &l);
    let c = new_test_raft(3, vec![1, 2, 3], 10, 1, new_storage(), &l);

    let mut nt = Network::new(vec![Some(a), Some(b), Some(c)], &l);
    nt.cut(1, 3);

    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
    nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);

    // 1 becomes leader since it receives votes from 1 and 2
    assert_eq!(nt.peers[&1].state, StateRole::Leader);

    // 3 stays as candidate since it receives a vote from 3 and a rejection from 2
    assert_eq!(nt.peers[&3].state, StateRole::Candidate);

    nt.recover();

    // Candidate 3 now increases its term and tries to vote again, we except it to
    // disrupt the leader 1 since it has a higher term, 3 will be follower again
    // since both 1 and 2 rejects its vote request since 3 does not have a long
    // enough log.
    nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);

    let tests = [
        // role, term, committed, applied, last index.
        (StateRole::Follower, 2, (1, 0, 1)),
        (StateRole::Follower, 2, (1, 0, 1)),
        (StateRole::Follower, 2, (0, 0, 0)),
    ];

    for (i, &(state, term, raft_log)) in tests.iter().enumerate() {
        let id = i as u64 + 1;
        if nt.peers[&id].state != state {
            panic!(
                "#{}: state = {:?}, want {:?}",
                i, nt.peers[&id].state, state
            );
        }
        if nt.peers[&id].term != term {
            panic!("#{}: term = {}, want {}", i, nt.peers[&id].term, term);
        }

        let prefix = format!("#{}: ", i);
        assert_raft_log(&prefix, &nt.peers[&id].raft_log, raft_log);
    }
}

#[test]
fn test_dueling_pre_candidates() {
    let l = default_logger();
    let a = new_test_raft_with_prevote(1, vec![1, 2, 3], 10, 1, new_storage(), true, &l);
    let b = new_test_raft_with_prevote(2, vec![1, 2, 3], 10, 1, new_storage(), true, &l);
    let c = new_test_raft_with_prevote(3, vec![1, 2, 3], 10, 1, new_storage(), true, &l);

    let mut config = Network::default_config();
    config.pre_vote = true;
    let mut nt = Network::new_with_config(vec![Some(a), Some(b), Some(c)], &config, &l);
    nt.cut(1, 3);

    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
    nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);

    // 1 becomes leader since it receives votes from 1 and 2
    assert_eq!(nt.peers[&1].state, StateRole::Leader);

    // 3 campaigns then reverts to follower when its pre_vote is rejected
    assert_eq!(nt.peers[&3].state, StateRole::Follower);

    nt.recover();

    // Candidate 3 now increases its term and tries to vote again.
    // With pre-vote, it does not disrupt the leader.
    nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);

    let tests = [
        // role, term, committed, applied, last index.
        (1, StateRole::Leader, 1, (1, 0, 1)),
        (2, StateRole::Follower, 1, (1, 0, 1)),
        (3, StateRole::Follower, 1, (0, 0, 0)),
    ];
    for (i, &(id, state, term, raft_log)) in tests.iter().enumerate() {
        if nt.peers[&id].state != state {
            panic!(
                "#{}: state = {:?}, want {:?}",
                i, nt.peers[&id].state, state
            );
        }
        if nt.peers[&id].term != term {
            panic!("#{}: term = {}, want {}", i, nt.peers[&id].term, term);
        }
        let prefix = format!("#{}: ", i);
        assert_raft_log(&prefix, &nt.peers[&id].raft_log, raft_log);
    }
}

#[test]
fn test_candidate_concede() {
    let l = default_logger();
    let mut tt = Network::new(vec![None, None, None], &l);
    tt.isolate(1);

    tt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
    tt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);

    // heal the partition
    tt.recover();
    // send heartbeat; reset wait
    tt.send(vec![new_message(3, 3, MessageType::MsgBeat, 0)]);

    // send a proposal to 3 to flush out a MsgAppend to 1
    let data = "force follower";
    let mut m = new_message(3, 3, MessageType::MsgPropose, 0);
    m.entries = vec![new_entry(0, 0, Some(data))].into();
    tt.send(vec![m]);
    // send heartbeat; flush out commit
    tt.send(vec![new_message(3, 3, MessageType::MsgBeat, 0)]);

    assert_eq!(tt.peers[&1].state, StateRole::Follower);
    assert_eq!(tt.peers[&1].term, 1);

    for p in tt.peers.values() {
        assert_eq!(p.raft_log.committed, 2); // All raft logs are committed.
        assert_eq!(p.raft_log.applied, 0); // Raft logs are based on a snapshot with index 1.
        assert_eq!(p.raft_log.last_index(), 2);
    }
}

#[test]
fn test_single_node_candidate() {
    let l = default_logger();
    let mut tt = Network::new(vec![None], &l);
    tt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    assert_eq!(tt.peers[&1].state, StateRole::Leader);
}

#[test]
fn test_sinle_node_pre_candidate() {
    let l = default_logger();
    let mut config = Network::default_config();
    config.pre_vote = true;
    let mut tt = Network::new_with_config(vec![None], &config, &l);
    tt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    assert_eq!(tt.peers[&1].state, StateRole::Leader);
}

#[test]
fn test_old_messages() {
    let l = default_logger();
    let mut tt = Network::new(vec![None, None, None], &l);
    // make 0 leader @ term 3
    tt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
    tt.send(vec![new_message(2, 2, MessageType::MsgHup, 0)]);
    tt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
    // pretend we're an old leader trying to make progress; this entry is expected to be ignored.
    let mut m = new_message(2, 1, MessageType::MsgAppend, 0);
    m.term = 2;
    m.entries = vec![empty_entry(2, 3)].into();
    tt.send(vec![m]);
    // commit a new entry
    tt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);

    for p in tt.peers.values() {
        let raft = p.raft.as_ref().unwrap();
        assert_eq!(raft.raft_log.committed, 4);
        assert_eq!(raft.raft_log.applied, 0);
        assert_eq!(raft.raft_log.last_index(), 4);
    }
}

// test_old_messages_reply - optimization - reply with new term.

#[test]
fn test_proposal() {
    let l = default_logger();
    let mut tests = vec![
        (Network::new(vec![None, None, None], &l), true),
        (Network::new(vec![None, None, NOP_STEPPER], &l), true),
        (
            Network::new(vec![None, NOP_STEPPER, NOP_STEPPER], &l),
            false,
        ),
        (
            Network::new(vec![None, NOP_STEPPER, NOP_STEPPER, None], &l),
            false,
        ),
        (
            Network::new(vec![None, NOP_STEPPER, NOP_STEPPER, None, None], &l),
            true,
        ),
    ];

    for (j, (mut nw, success)) in tests.drain(..).enumerate() {
        let send = |nw: &mut Network, m| {
            let res = panic::catch_unwind(AssertUnwindSafe(|| nw.send(vec![m])));
            assert!(res.is_ok() || !success);
        };

        // promote 0 the leader
        send(&mut nw, new_message(1, 1, MessageType::MsgHup, 0));
        send(&mut nw, new_message(1, 1, MessageType::MsgPropose, 1));

        // committed index, applied index and last index.
        let want_log = if success { (2, 0, 2) } else { (0, 0, 0) };

        for p in nw.peers.values() {
            if let Some(ref raft) = p.raft {
                let prefix = format!("#{}: ", j);
                assert_raft_log(&prefix, &raft.raft_log, want_log);
            }
        }
        if nw.peers[&1].term != 1 {
            panic!("#{}: term = {}, want: {}", j, nw.peers[&1].term, 1);
        }
    }
}

#[test]
fn test_proposal_by_proxy() {
    let l = default_logger();
    let mut tests = vec![
        Network::new(vec![None, None, None], &l),
        Network::new(vec![None, None, NOP_STEPPER], &l),
    ];
    for (j, tt) in tests.iter_mut().enumerate() {
        // promote 0 the leader
        tt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

        // propose via follower
        tt.send(vec![new_message(2, 2, MessageType::MsgPropose, 1)]);

        for p in tt.peers.values() {
            if p.raft.is_none() {
                continue;
            }
            if let Some(ref raft) = p.raft {
                let prefix = format!("#{}: ", j);
                assert_raft_log(&prefix, &raft.raft_log, (2, 0, 2));
            }
        }
        if tt.peers[&1].term != 1 {
            panic!("#{}: term = {}, want {}", j, tt.peers[&1].term, 1);
        }
    }
}

#[test]
fn test_commit() {
    let l = default_logger();
    let mut tests = vec![
        // single
        (vec![1], vec![empty_entry(1, 1)], 1, 1),
        (vec![1], vec![empty_entry(1, 1)], 2, 0),
        (vec![2], vec![empty_entry(1, 1), empty_entry(2, 2)], 2, 2),
        (vec![1], vec![empty_entry(2, 1)], 2, 1),
        // odd
        (
            vec![2, 1, 1],
            vec![empty_entry(1, 1), empty_entry(2, 2)],
            1,
            1,
        ),
        (
            vec![2, 1, 1],
            vec![empty_entry(1, 1), empty_entry(1, 2)],
            2,
            0,
        ),
        (
            vec![2, 1, 2],
            vec![empty_entry(1, 1), empty_entry(2, 2)],
            2,
            2,
        ),
        (
            vec![2, 1, 2],
            vec![empty_entry(1, 1), empty_entry(1, 2)],
            2,
            0,
        ),
        // even
        (
            vec![2, 1, 1, 1],
            vec![empty_entry(1, 1), empty_entry(2, 2)],
            1,
            1,
        ),
        (
            vec![2, 1, 1, 1],
            vec![empty_entry(1, 1), empty_entry(1, 2)],
            2,
            0,
        ),
        (
            vec![2, 1, 1, 2],
            vec![empty_entry(1, 1), empty_entry(2, 2)],
            1,
            1,
        ),
        (
            vec![2, 1, 1, 2],
            vec![empty_entry(1, 1), empty_entry(1, 2)],
            2,
            0,
        ),
        (
            vec![2, 1, 2, 2],
            vec![empty_entry(1, 1), empty_entry(2, 2)],
            2,
            2,
        ),
        (
            vec![2, 1, 2, 2],
            vec![empty_entry(1, 1), empty_entry(1, 2)],
            2,
            0,
        ),
    ];

    for (i, (matches, logs, sm_term, w)) in tests.drain(..).enumerate() {
        let store = MemStorage::new_with_conf_state((vec![1], vec![]));
        store.wl().append(&logs).unwrap();
        let mut hs = HardState::default();
        hs.term = sm_term;
        store.wl().set_hardstate(hs);
        let cfg = new_test_config(1, 5, 1);
        let mut sm = new_test_raft_with_config(&cfg, store, &l);

        for (j, v) in matches.iter().enumerate() {
            let id = j as u64 + 1;
            if sm.mut_prs().get(id).is_none() {
                sm.apply_conf_change(&add_node(id)).unwrap();
                let pr = sm.mut_prs().get_mut(id).unwrap();
                pr.matched = *v;
                pr.next_idx = *v + 1;
            }
        }
        sm.maybe_commit();
        if sm.raft_log.committed != w {
            panic!("#{}: committed = {}, want {}", i, sm.raft_log.committed, w);
        }
    }
}

#[test]
fn test_pass_election_timeout() {
    let l = default_logger();
    let tests = [
        (5, 0f64, false),
        (10, 0.1, true),
        (13, 0.4, true),
        (15, 0.6, true),
        (18, 0.9, true),
        (20, 1.0, false),
    ];

    for (i, &(elapse, wprobability, round)) in tests.iter().enumerate() {
        let mut sm = new_test_raft(1, vec![1], 10, 1, new_storage(), &l);
        sm.election_elapsed = elapse;
        let mut c = 0;
        for _ in 0..10_000 {
            sm.reset_randomized_election_timeout();
            if sm.pass_election_timeout() {
                c += 1;
            }
        }
        let mut got = f64::from(c) / 10000.0;
        if round {
            got = (got * 10.0 + 0.5).floor() / 10.0;
        }
        if (got - wprobability).abs() > 0.000_001 {
            panic!("#{}: probability = {}, want {}", i, got, wprobability);
        }
    }
}

// test_handle_msg_append ensures:
// 1. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm.
// 2. If an existing entry conflicts with a new one (same index but different terms),
//    delete the existing entry and all that follow it; append any new entries not already in the
//    log.
// 3. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry).
#[test]
fn test_handle_msg_append() {
    let l = default_logger();
    let nm = |term, log_term, index, commit, ents: Option<Vec<(u64, u64)>>| {
        let mut m = Message::default();
        m.set_msg_type(MessageType::MsgAppend);
        m.term = term;
        m.log_term = log_term;
        m.index = index;
        m.commit = commit;
        if let Some(ets) = ents {
            m.entries = ets.iter().map(|&(i, t)| empty_entry(t, i)).collect();
        }
        m
    };
    let mut tests = vec![
        // Ensure 1
        (nm(2, 3, 2, 3, None), 2, 0, true), // previous log mismatch
        (nm(2, 3, 3, 3, None), 2, 0, true), // previous log non-exist
        // Ensure 2
        (nm(2, 1, 1, 1, None), 2, 1, false),
        (nm(2, 0, 0, 1, Some(vec![(1, 2)])), 1, 1, false),
        (nm(2, 2, 2, 3, Some(vec![(3, 2), (4, 2)])), 4, 3, false),
        (nm(2, 2, 2, 4, Some(vec![(3, 2)])), 3, 3, false),
        (nm(2, 1, 1, 4, Some(vec![(2, 2)])), 2, 2, false),
        // Ensure 3
        (nm(1, 1, 1, 3, None), 2, 1, false), // match entry 1, commit up to last new entry 1
        (nm(1, 1, 1, 3, Some(vec![(2, 2)])), 2, 2, false), // match entry 1, commit up to last new
        // entry 2
        (nm(2, 2, 2, 3, None), 2, 2, false), // match entry 2, commit up to last new entry 2
        (nm(2, 2, 2, 4, None), 2, 2, false), // commit up to log.last()
    ];

    for (j, (m, w_index, w_commit, w_reject)) in tests.drain(..).enumerate() {
        let mut sm = new_test_raft_with_logs(
            1,
            vec![1],
            10,
            1,
            MemStorage::new(),
            &[empty_entry(1, 1), empty_entry(2, 2)],
            &l,
        );

        sm.become_follower(2, INVALID_ID);
        sm.handle_append_entries(&m);
        if sm.raft_log.last_index() != w_index {
            panic!(
                "#{}: last_index = {}, want {}",
                j,
                sm.raft_log.last_index(),
                w_index
            );
        }
        if sm.raft_log.committed != w_commit {
            panic!(
                "#{}: committed = {}, want {}",
                j, sm.raft_log.committed, w_commit
            );
        }
        let m = sm.read_messages();
        if m.len() != 1 {
            panic!("#{}: msg count = {}, want 1", j, m.len());
        }
        if m[0].reject != w_reject {
            panic!("#{}: reject = {}, want {}", j, m[0].reject, w_reject);
        }
    }
}

// test_handle_heartbeat ensures that the follower commits to the commit in the message.
#[test]
fn test_handle_heartbeat() {
    let l = default_logger();
    let commit = 2u64;
    let nw = |f, to, term, commit| {
        let mut m = new_message(f, to, MessageType::MsgHeartbeat, 0);
        m.term = term;
        m.commit = commit;
        m
    };
    let mut tests = vec![
        (nw(2, 1, 2, commit + 1), commit + 1),
        (nw(2, 1, 2, commit - 1), commit), // do not decrease commit
    ];
    for (i, (m, w_commit)) in tests.drain(..).enumerate() {
        let store = MemStorage::new_with_conf_state((vec![1, 2], vec![]));
        store
            .wl()
            .append(&[empty_entry(1, 1), empty_entry(2, 2), empty_entry(3, 3)])
            .unwrap();
        let cfg = new_test_config(1, 5, 1);
        let mut sm = new_test_raft_with_config(&cfg, store, &l);
        sm.become_follower(2, 2);
        sm.raft_log.commit_to(commit);
        sm.handle_heartbeat(m);
        if sm.raft_log.committed != w_commit {
            panic!(
                "#{}: committed = {}, want = {}",
                i, sm.raft_log.committed, w_commit
            );
        }
        let m = sm.read_messages();
        if m.len() != 1 {
            panic!("#{}: msg count = {}, want 1", i, m.len());
        }
        if m[0].get_msg_type() != MessageType::MsgHeartbeatResponse {
            panic!(
                "#{}: type = {:?}, want MsgHeartbeatResponse",
                i,
                m[0].get_msg_type()
            );
        }
    }
}

// test_handle_heartbeat_resp ensures that we re-send log entries when we get a heartbeat response.
#[test]
fn test_handle_heartbeat_resp() {
    let l = default_logger();
    let store = new_storage();
    store
        .wl()
        .append(&[empty_entry(1, 1), empty_entry(2, 2), empty_entry(3, 3)])
        .unwrap();
    let mut sm = new_test_raft(1, vec![1, 2], 5, 1, store, &l);
    sm.become_candidate();
    sm.become_leader();
    let last_index = sm.raft_log.last_index();
    sm.raft_log.commit_to(last_index);

    // A heartbeat response from a node that is behind; re-send MsgApp
    sm.step(new_message(2, 0, MessageType::MsgHeartbeatResponse, 0))
        .expect("");
    let mut msgs = sm.read_messages();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].get_msg_type(), MessageType::MsgAppend);

    // A second heartbeat response generates another MsgApp re-send
    sm.step(new_message(2, 0, MessageType::MsgHeartbeatResponse, 0))
        .expect("");
    msgs = sm.read_messages();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].get_msg_type(), MessageType::MsgAppend);

    // Once we have an MsgAppResp, heartbeats no longer send MsgApp.
    let mut m = new_message(2, 0, MessageType::MsgAppendResponse, 0);
    m.index = msgs[0].index + msgs[0].entries.len() as u64;
    sm.step(m).expect("");
    // Consume the message sent in response to MsgAppResp
    sm.read_messages();

    sm.step(new_message(2, 0, MessageType::MsgHeartbeatResponse, 0))
        .expect("");
    msgs = sm.read_messages();
    assert!(msgs.is_empty());
}

// test_raft_frees_read_only_mem ensures raft will free read request from
// ReadOnly read_index_queue and pending_read_index map.
// related issue: https://github.com/coreos/etcd/issues/7571
#[test]
fn test_raft_frees_read_only_mem() {
    let l = default_logger();
    let mut sm = new_test_raft(1, vec![1, 2], 5, 1, new_storage(), &l);
    sm.become_candidate();
    sm.become_leader();
    let last_index = sm.raft_log.last_index();
    sm.raft_log.commit_to(last_index);

    let ctx = "ctx";
    let vec_ctx = ctx.as_bytes().to_vec();

    // leader starts linearizable read request.
    // more info: raft dissertation 6.4, step 2.
    let m = new_message_with_entries(
        2,
        1,
        MessageType::MsgReadIndex,
        vec![new_entry(0, 0, Some(ctx))],
    );
    sm.step(m).expect("");
    let msgs = sm.read_messages();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].get_msg_type(), MessageType::MsgHeartbeat);
    assert_eq!(msgs[0].context, &vec_ctx[..]);
    assert_eq!(sm.read_only.read_index_queue.len(), 1);
    assert_eq!(sm.read_only.pending_read_index.len(), 1);
    assert!(sm.read_only.pending_read_index.contains_key(&vec_ctx));

    // heartbeat responses from majority of followers (1 in this case)
    // acknowledge the authority of the leader.
    // more info: raft dissertation 6.4, step 3.
    let mut m = new_message(2, 1, MessageType::MsgHeartbeatResponse, 0);
    m.context = vec_ctx.clone().into();
    sm.step(m).expect("");
    assert_eq!(sm.read_only.read_index_queue.len(), 0);
    assert_eq!(sm.read_only.pending_read_index.len(), 0);
    assert!(!sm.read_only.pending_read_index.contains_key(&vec_ctx));
}

// test_msg_append_response_wait_reset verifies the waitReset behavior of a leader
// MsgAppResp.
#[test]
fn test_msg_append_response_wait_reset() {
    let l = default_logger();
    let mut sm = new_test_raft(1, vec![1, 2, 3], 5, 1, new_storage(), &l);
    sm.become_candidate();
    sm.become_leader();
    // For no-op entry
    sm.persist();
    // The new leader has just emitted a new Term 4 entry; consume those messages
    // from the outgoing queue.
    sm.bcast_append();
    sm.read_messages();

    // Node 2 acks the first entry, making it committed.
    let mut m = new_message(2, 0, MessageType::MsgAppendResponse, 0);
    m.index = 1;
    sm.step(m).expect("");
    assert_eq!(sm.raft_log.committed, 1);
    // Also consume the MsgApp messages that update Commit on the followers.
    sm.read_messages();

    // A new command is now proposed on node 1.
    m = new_message(1, 0, MessageType::MsgPropose, 0);
    m.entries = vec![empty_entry(0, 0)].into();
    sm.step(m).expect("");
    sm.persist();

    // The command is broadcast to all nodes not in the wait state.
    // Node 2 left the wait state due to its MsgAppResp, but node 3 is still waiting.
    let mut msgs = sm.read_messages();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].get_msg_type(), MessageType::MsgAppend);
    assert_eq!(msgs[0].to, 2);
    assert_eq!(msgs[0].entries.len(), 1);
    assert_eq!(msgs[0].entries[0].index, 2);

    // Now Node 3 acks the first entry. This releases the wait and entry 2 is sent.
    m = new_message(3, 0, MessageType::MsgAppendResponse, 0);
    m.index = 1;
    sm.step(m).expect("");
    msgs = sm.read_messages();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].get_msg_type(), MessageType::MsgAppend);
    assert_eq!(msgs[0].to, 3);
    assert_eq!(msgs[0].entries.len(), 1);
    assert_eq!(msgs[0].entries[0].index, 2);
}

#[test]
fn test_recv_msg_request_vote() {
    let l = default_logger();
    test_recv_msg_request_vote_for_type(MessageType::MsgRequestVote, &l);
}

fn test_recv_msg_request_vote_for_type(msg_type: MessageType, l: &Logger) {
    let mut tests = vec![
        (StateRole::Follower, 0, 0, INVALID_ID, true),
        (StateRole::Follower, 0, 1, INVALID_ID, true),
        (StateRole::Follower, 0, 2, INVALID_ID, true),
        (StateRole::Follower, 0, 3, INVALID_ID, false),
        (StateRole::Follower, 1, 0, INVALID_ID, true),
        (StateRole::Follower, 1, 1, INVALID_ID, true),
        (StateRole::Follower, 1, 2, INVALID_ID, true),
        (StateRole::Follower, 1, 3, INVALID_ID, false),
        (StateRole::Follower, 2, 0, INVALID_ID, true),
        (StateRole::Follower, 2, 1, INVALID_ID, true),
        (StateRole::Follower, 2, 2, INVALID_ID, false),
        (StateRole::Follower, 2, 3, INVALID_ID, false),
        (StateRole::Follower, 3, 0, INVALID_ID, true),
        (StateRole::Follower, 3, 1, INVALID_ID, true),
        (StateRole::Follower, 3, 2, INVALID_ID, false),
        (StateRole::Follower, 3, 3, INVALID_ID, false),
        (StateRole::Follower, 3, 2, 2, false),
        (StateRole::Follower, 3, 2, 1, true),
        (StateRole::Leader, 3, 3, 1, true),
        (StateRole::PreCandidate, 3, 3, 1, true),
        (StateRole::Candidate, 3, 3, 1, true),
    ];

    for (j, (state, index, log_term, vote_for, w_reject)) in tests.drain(..).enumerate() {
        let store = MemStorage::new_with_conf_state((vec![1], vec![]));
        let ents = &[empty_entry(2, 1), empty_entry(2, 2)];
        store.wl().append(ents).unwrap();
        let mut sm = new_test_raft(1, vec![1], 10, 1, store, l);
        sm.state = state;
        sm.vote = vote_for;

        let mut m = new_message(2, 0, msg_type, 0);
        m.index = index;
        m.log_term = log_term;
        // raft.Term is greater than or equal to raft.raftLog.lastTerm. In this
        // test we're only testing MsgVote responses when the campaigning node
        // has a different raft log compared to the recipient node.
        // Additionally we're verifying behaviour when the recipient node has
        // already given out its vote for its current term. We're not testing
        // what the recipient node does when receiving a message with a
        // different term number, so we simply initialize both term numbers to
        // be the same.
        let term = cmp::max(sm.raft_log.last_term(), log_term);
        m.term = term;
        sm.term = term;
        sm.step(m).expect("");

        let msgs = sm.read_messages();
        if msgs.len() != 1 {
            panic!("#{}: msgs count = {}, want 1", j, msgs.len());
        }
        if msgs[0].get_msg_type() != vote_resp_msg_type(msg_type) {
            panic!(
                "#{}: m.type = {:?}, want {:?}",
                j,
                msgs[0].get_msg_type(),
                vote_resp_msg_type(msg_type)
            );
        }
        if msgs[0].reject != w_reject {
            panic!(
                "#{}: m.get_reject = {}, want {}",
                j, msgs[0].reject, w_reject
            );
        }
    }
}

#[test]
fn test_state_transition() {
    let l = default_logger();
    let mut tests = vec![
        (
            StateRole::Follower,
            StateRole::Follower,
            true,
            1,
            INVALID_ID,
        ),
        (
            StateRole::Follower,
            StateRole::PreCandidate,
            true,
            0,
            INVALID_ID,
        ),
        (
            StateRole::Follower,
            StateRole::Candidate,
            true,
            1,
            INVALID_ID,
        ),
        (StateRole::Follower, StateRole::Leader, false, 0, INVALID_ID),
        (
            StateRole::PreCandidate,
            StateRole::Follower,
            true,
            0,
            INVALID_ID,
        ),
        (
            StateRole::PreCandidate,
            StateRole::PreCandidate,
            true,
            0,
            INVALID_ID,
        ),
        (
            StateRole::PreCandidate,
            StateRole::Candidate,
            true,
            1,
            INVALID_ID,
        ),
        (StateRole::PreCandidate, StateRole::Leader, true, 0, 1),
        (
            StateRole::Candidate,
            StateRole::Follower,
            true,
            0,
            INVALID_ID,
        ),
        (
            StateRole::Candidate,
            StateRole::PreCandidate,
            true,
            0,
            INVALID_ID,
        ),
        (
            StateRole::Candidate,
            StateRole::Candidate,
            true,
            1,
            INVALID_ID,
        ),
        (StateRole::Candidate, StateRole::Leader, true, 0, 1),
        (StateRole::Leader, StateRole::Follower, true, 1, INVALID_ID),
        (
            StateRole::Leader,
            StateRole::PreCandidate,
            false,
            0,
            INVALID_ID,
        ),
        (
            StateRole::Leader,
            StateRole::Candidate,
            false,
            1,
            INVALID_ID,
        ),
        (StateRole::Leader, StateRole::Leader, true, 0, 1),
    ];
    for (i, (from, to, wallow, wterm, wlead)) in tests.drain(..).enumerate() {
        let sm: &mut Raft<MemStorage> = &mut new_test_raft(1, vec![1], 10, 1, new_storage(), &l);
        sm.state = from;

        let res = panic::catch_unwind(AssertUnwindSafe(|| match to {
            StateRole::Follower => sm.become_follower(wterm, wlead),
            StateRole::PreCandidate => sm.become_pre_candidate(),
            StateRole::Candidate => sm.become_candidate(),
            StateRole::Leader => sm.become_leader(),
        }));
        if res.is_ok() ^ wallow {
            panic!("#{}: allow = {}, want {}", i, res.is_ok(), wallow);
        }
        if res.is_err() {
            continue;
        }

        if sm.term != wterm {
            panic!("#{}: term = {}, want {}", i, sm.term, wterm);
        }
        if sm.leader_id != wlead {
            panic!("#{}: lead = {}, want {}", i, sm.leader_id, wlead);
        }
    }
}

#[test]
fn test_all_server_stepdown() {
    let l = default_logger();
    let mut tests = vec![
        // state, want_state, term, last_index, entry count.
        (StateRole::Follower, StateRole::Follower, 3, 0, 0),
        (StateRole::PreCandidate, StateRole::Follower, 3, 0, 0),
        (StateRole::Candidate, StateRole::Follower, 3, 0, 0),
        (StateRole::Leader, StateRole::Follower, 3, 1, 1),
    ];

    let tmsg_types = [MessageType::MsgRequestVote, MessageType::MsgAppend];
    let tterm = 3u64;

    for (i, (state, wstate, wterm, windex, entries)) in tests.drain(..).enumerate() {
        let mut sm = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage(), &l);
        match state {
            StateRole::Follower => sm.become_follower(1, INVALID_ID),
            StateRole::PreCandidate => sm.become_pre_candidate(),
            StateRole::Candidate => sm.become_candidate(),
            StateRole::Leader => {
                sm.become_candidate();
                sm.become_leader();
            }
        }

        for (j, &msg_type) in tmsg_types.iter().enumerate() {
            let mut m = new_message(2, 0, msg_type, 0);
            m.term = tterm;
            m.log_term = tterm;
            sm.step(m).expect("");

            if sm.state != wstate {
                panic!("{}.{} state = {:?}, want {:?}", i, j, sm.state, wstate);
            }
            if sm.term != wterm {
                panic!("{}.{} term = {}, want {}", i, j, sm.term, wterm);
            }
            if sm.raft_log.last_index() != windex {
                panic!(
                    "{}.{} index = {}, want {}",
                    i,
                    j,
                    sm.raft_log.last_index(),
                    windex
                );
            }
            let entry_count = sm.raft_log.all_entries().len() as u64;
            if entry_count != entries {
                panic!("{}.{} ents count = {}, want {}", i, j, entry_count, entries);
            }
            let wlead = if msg_type == MessageType::MsgRequestVote {
                INVALID_ID
            } else {
                2
            };
            if sm.leader_id != wlead {
                panic!("{}, sm.lead = {}, want {}", i, sm.leader_id, INVALID_ID);
            }
        }
    }
}

#[test]
fn test_candidate_reset_term_msg_heartbeat() {
    let l = default_logger();
    test_candidate_reset_term(MessageType::MsgHeartbeat, &l)
}

#[test]
fn test_candidate_reset_term_msg_append() {
    let l = default_logger();
    test_candidate_reset_term(MessageType::MsgAppend, &l)
}

// test_candidate_reset_term tests when a candidate receives a
// MsgHeartbeat or MsgAppend from leader, "step" resets the term
// with leader's and reverts back to follower.
fn test_candidate_reset_term(message_type: MessageType, l: &Logger) {
    let a = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage(), l);
    let b = new_test_raft(2, vec![1, 2, 3], 10, 1, new_storage(), l);
    let c = new_test_raft(3, vec![1, 2, 3], 10, 1, new_storage(), l);

    let mut nt = Network::new(vec![Some(a), Some(b), Some(c)], l);

    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    assert_eq!(nt.peers[&1].state, StateRole::Leader);
    assert_eq!(nt.peers[&2].state, StateRole::Follower);
    assert_eq!(nt.peers[&3].state, StateRole::Follower);

    // isolate 3 and increase term in rest
    nt.isolate(3);
    nt.send(vec![new_message(2, 2, MessageType::MsgHup, 0)]);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    assert_eq!(nt.peers[&1].state, StateRole::Leader);
    assert_eq!(nt.peers[&2].state, StateRole::Follower);
    assert_eq!(nt.peers[&3].state, StateRole::Follower);

    // trigger campaign in isolated c
    nt.peers
        .get_mut(&3)
        .unwrap()
        .reset_randomized_election_timeout();
    let timeout = nt.peers[&3].randomized_election_timeout();
    for _ in 0..timeout {
        nt.peers.get_mut(&3).unwrap().tick();
    }

    assert_eq!(nt.peers[&3].state, StateRole::Candidate);

    nt.recover();

    // leader sends to isolated candidate
    // and expects candidate to revert to follower
    let mut msg = new_message(1, 3, message_type, 0);
    msg.term = nt.peers[&1].term;
    nt.send(vec![msg]);

    assert_eq!(nt.peers[&3].state, StateRole::Follower);

    // follower c term is reset with leader's
    assert_eq!(
        nt.peers[&3].term, nt.peers[&1].term,
        "follower term expected same term as leader's {}, got {}",
        nt.peers[&1].term, nt.peers[&3].term,
    )
}

#[test]
fn test_leader_stepdown_when_quorum_active() {
    let l = default_logger();
    let mut sm = new_test_raft(1, vec![1, 2, 3], 5, 1, new_storage(), &l);
    sm.check_quorum = true;
    sm.become_candidate();
    sm.become_leader();

    for _ in 0..=sm.election_timeout() {
        let mut m = new_message(2, 0, MessageType::MsgHeartbeatResponse, 0);
        m.term = sm.term;
        sm.step(m).expect("");
        sm.tick();
    }

    assert_eq!(sm.state, StateRole::Leader);
}

#[test]
fn test_leader_stepdown_when_quorum_lost() {
    let l = default_logger();
    let mut sm = new_test_raft(1, vec![1, 2, 3], 5, 1, new_storage(), &l);

    sm.check_quorum = true;

    sm.become_candidate();
    sm.become_leader();

    for _ in 0..=sm.election_timeout() {
        sm.tick();
    }

    assert_eq!(sm.state, StateRole::Follower);
}

#[test]
fn test_leader_superseding_with_check_quorum() {
    let l = default_logger();
    let mut a = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage(), &l);
    let mut b = new_test_raft(2, vec![1, 2, 3], 10, 1, new_storage(), &l);
    let mut c = new_test_raft(3, vec![1, 2, 3], 10, 1, new_storage(), &l);

    a.check_quorum = true;
    b.check_quorum = true;
    c.check_quorum = true;

    let mut nt = Network::new(vec![Some(a), Some(b), Some(c)], &l);

    let b_election_timeout = nt.peers[&2].election_timeout();

    // prevent campaigning from b
    nt.peers
        .get_mut(&2)
        .unwrap()
        .set_randomized_election_timeout(b_election_timeout + 1);
    for _ in 0..b_election_timeout {
        nt.peers.get_mut(&2).unwrap().tick();
    }
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    assert_eq!(nt.peers[&1].state, StateRole::Leader);
    assert_eq!(nt.peers[&3].state, StateRole::Follower);

    nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);

    // Peer b rejected c's vote since its electionElapsed had not reached to electionTimeout
    assert_eq!(nt.peers[&3].state, StateRole::Candidate);

    // Letting b's electionElapsed reach to electionTimeout
    for _ in 0..b_election_timeout {
        nt.peers.get_mut(&2).unwrap().tick();
    }
    nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);
    assert_eq!(nt.peers[&3].state, StateRole::Leader);
}

#[test]
fn test_leader_election_with_check_quorum() {
    let l = default_logger();
    let mut a = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage(), &l);
    let mut b = new_test_raft(2, vec![1, 2, 3], 10, 1, new_storage(), &l);
    let mut c = new_test_raft(3, vec![1, 2, 3], 10, 1, new_storage(), &l);

    a.check_quorum = true;
    b.check_quorum = true;
    c.check_quorum = true;

    let mut nt = Network::new(vec![Some(a), Some(b), Some(c)], &l);

    // we can not let system choosing the value of randomizedElectionTimeout
    // otherwise it will introduce some uncertainty into this test case
    // we need to ensure randomizedElectionTimeout > electionTimeout here
    let a_election_timeout = nt.peers[&1].election_timeout();
    let b_election_timeout = nt.peers[&2].election_timeout();
    nt.peers
        .get_mut(&1)
        .unwrap()
        .set_randomized_election_timeout(a_election_timeout + 1);
    nt.peers
        .get_mut(&2)
        .unwrap()
        .set_randomized_election_timeout(b_election_timeout + 2);

    // Immediately after creation, votes are cast regardless of the election timeout

    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    assert_eq!(nt.peers[&1].state, StateRole::Leader);
    assert_eq!(nt.peers[&3].state, StateRole::Follower);

    // need to reset randomizedElectionTimeout larger than electionTimeout again,
    // because the value might be reset to electionTimeout since the last state changes
    let a_election_timeout = nt.peers[&1].election_timeout();
    let b_election_timeout = nt.peers[&2].election_timeout();
    nt.peers
        .get_mut(&1)
        .unwrap()
        .set_randomized_election_timeout(a_election_timeout + 1);
    nt.peers
        .get_mut(&2)
        .unwrap()
        .set_randomized_election_timeout(b_election_timeout + 2);

    for _ in 0..a_election_timeout {
        nt.peers.get_mut(&1).unwrap().tick();
    }
    for _ in 0..b_election_timeout {
        nt.peers.get_mut(&2).unwrap().tick();
    }
    nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);

    assert_eq!(nt.peers[&1].state, StateRole::Follower);
    assert_eq!(nt.peers[&3].state, StateRole::Leader);
}

// test_free_stuck_candidate_with_check_quorum ensures that a candidate with a higher term
// can disrupt the leader even if the leader still "officially" holds the lease, The
// leader is expected to step down and adopt the candidate's term
#[test]
fn test_free_stuck_candidate_with_check_quorum() {
    let l = default_logger();
    let mut a = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage(), &l);
    let mut b = new_test_raft(2, vec![1, 2, 3], 10, 1, new_storage(), &l);
    let mut c = new_test_raft(3, vec![1, 2, 3], 10, 1, new_storage(), &l);

    a.check_quorum = true;
    b.check_quorum = true;
    c.check_quorum = true;

    let mut nt = Network::new(vec![Some(a), Some(b), Some(c)], &l);

    // we can not let system choosing the value of randomizedElectionTimeout
    // otherwise it will introduce some uncertainty into this test case
    // we need to ensure randomizedElectionTimeout > electionTimeout here
    let b_election_timeout = nt.peers[&2].election_timeout();
    nt.peers
        .get_mut(&2)
        .unwrap()
        .set_randomized_election_timeout(b_election_timeout + 1);

    for _ in 0..b_election_timeout {
        nt.peers.get_mut(&2).unwrap().tick();
    }
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
    nt.isolate(1);
    nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);

    assert_eq!(nt.peers[&2].state, StateRole::Follower);
    assert_eq!(nt.peers[&3].state, StateRole::Candidate);
    assert_eq!(nt.peers[&3].term, &nt.peers[&2].term + 1);

    // Vote again for safety
    nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);

    assert_eq!(nt.peers[&2].state, StateRole::Follower);
    assert_eq!(nt.peers[&3].state, StateRole::Candidate);
    assert_eq!(nt.peers[&3].term, &nt.peers[&2].term + 2);

    nt.recover();
    let mut msg = new_message(1, 3, MessageType::MsgHeartbeat, 0);
    msg.term = nt.peers[&1].term;
    nt.send(vec![msg]);

    // Disrupt the leader so that the stuck peer is freed
    assert_eq!(nt.peers[&1].state, StateRole::Follower);
    assert_eq!(nt.peers[&3].term, nt.peers[&1].term);

    // Vote again, should become leader this time
    nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);
    assert_eq!(nt.peers[&3].state, StateRole::Leader);
}

#[test]
fn test_non_promotable_voter_with_check_quorum() {
    let l = default_logger();
    let mut a = new_test_raft(1, vec![1, 2], 10, 1, new_storage(), &l);
    let mut b = new_test_raft(2, vec![1], 10, 1, new_storage(), &l);

    a.check_quorum = true;
    b.check_quorum = true;

    let mut nt = Network::new(vec![Some(a), Some(b)], &l);

    // we can not let system choosing the value of randomizedElectionTimeout
    // otherwise it will introduce some uncertainty into this test case
    // we need to ensure randomizedElectionTimeout > electionTimeout here
    let b_election_timeout = nt.peers[&2].election_timeout();
    nt.peers
        .get_mut(&2)
        .unwrap()
        .set_randomized_election_timeout(b_election_timeout + 1);

    // Need to remove 2 again to make it a non-promotable node since newNetwork
    // overwritten some internal states
    nt.peers
        .get_mut(&2)
        .unwrap()
        .apply_conf_change(&remove_node(2))
        .unwrap();

    assert!(!nt.peers[&2].promotable());

    for _ in 0..b_election_timeout {
        nt.peers.get_mut(&2).unwrap().tick();
    }
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    assert_eq!(nt.peers[&1].state, StateRole::Leader);
    assert_eq!(nt.peers[&2].state, StateRole::Follower);
    assert_eq!(nt.peers[&2].leader_id, 1);
}

/// `test_disruptive_follower` tests isolated follower,
/// with slow network incoming from leader, election times out
/// to become a candidate with an increased term. Then, the
/// candiate's response to late leader heartbeat forces the leader
/// to step down.
#[test]
fn test_disruptive_follower() {
    let l = default_logger();
    let mut n1 = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage(), &l);
    let mut n2 = new_test_raft(2, vec![1, 2, 3], 10, 1, new_storage(), &l);
    let mut n3 = new_test_raft(3, vec![1, 2, 3], 10, 1, new_storage(), &l);

    n1.check_quorum = true;
    n2.check_quorum = true;
    n3.check_quorum = true;

    n1.become_follower(1, INVALID_ID);
    n2.become_follower(1, INVALID_ID);
    n3.become_follower(1, INVALID_ID);

    let mut nt = Network::new(vec![Some(n1), Some(n2), Some(n3)], &l);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    // check state
    assert_eq!(nt.peers[&1].state, StateRole::Leader);
    assert_eq!(nt.peers[&2].state, StateRole::Follower);
    assert_eq!(nt.peers[&3].state, StateRole::Follower);

    // etcd server "advanceTicksForElection" on restart;
    // this is to expedite campaign trigger when given larger
    // election timeouts (e.g. multi-datacenter deploy)
    // Or leader messages are being delayed while ticks elapse
    let timeout = nt.peers[&3].election_timeout();
    nt.peers
        .get_mut(&3)
        .unwrap()
        .set_randomized_election_timeout(timeout + 2);
    let timeout = nt.peers[&3].randomized_election_timeout();
    for _ in 0..timeout - 1 {
        nt.peers.get_mut(&3).unwrap().tick();
    }

    // ideally, before last election tick elapses,
    // the follower n3 receives "pb.MsgApp" or "pb.MsgHeartbeat"
    // from leader n1, and then resets its "electionElapsed"
    // however, last tick may elapse before receiving any
    // messages from leader, thus triggering campaign
    nt.peers.get_mut(&3).unwrap().tick();

    // n1 is still leader yet
    // while its heartbeat to candidate n3 is being delayed
    // check state
    assert_eq!(nt.peers[&1].state, StateRole::Leader);
    assert_eq!(nt.peers[&2].state, StateRole::Follower);
    assert_eq!(nt.peers[&3].state, StateRole::Candidate);

    // check term
    // n1.Term == 2
    // n2.Term == 2
    // n3.Term == 3
    assert_eq!(nt.peers[&1].term, 2);
    assert_eq!(nt.peers[&2].term, 2);
    assert_eq!(nt.peers[&3].term, 3);

    // while outgoing vote requests are still queued in n3,
    // leader heartbeat finally arrives at candidate n3
    // however, due to delayed network from leader, leader
    // heartbeat was sent with lower term than candidate's
    let mut msg = new_message(1, 3, MessageType::MsgHeartbeat, 0);
    msg.term = nt.peers[&1].term;
    nt.send(vec![msg]);

    // then candidate n3 responds with "pb.MsgAppResp" of higher term
    // and leader steps down from a message with higher term
    // this is to disrupt the current leader, so that candidate
    // with higher term can be freed with following election

    // check state
    assert_eq!(nt.peers[&1].state, StateRole::Follower);
    assert_eq!(nt.peers[&2].state, StateRole::Follower);
    assert_eq!(nt.peers[&3].state, StateRole::Candidate);

    // check term
    // n1.Term == 3
    // n2.Term == 2
    // n3.Term == 3
    assert_eq!(nt.peers[&1].term, 3);
    assert_eq!(nt.peers[&2].term, 2);
    assert_eq!(nt.peers[&3].term, 3);
}

/// `test_disruptive_follower_pre_vote` tests isolated follower,
/// with slow network incoming from leader, election times out
/// to become a pre-candidate with less log than current leader.
/// Then pre-vote phase prevents this isolated node from forcing
/// current leader to step down, thus less disruptions.
#[test]
fn test_disruptive_follower_pre_vote() {
    let l = default_logger();
    let mut n1 = new_test_raft_with_prevote(1, vec![1, 2, 3], 10, 1, new_storage(), true, &l);
    let mut n2 = new_test_raft_with_prevote(2, vec![1, 2, 3], 10, 1, new_storage(), true, &l);
    let mut n3 = new_test_raft_with_prevote(3, vec![1, 2, 3], 10, 1, new_storage(), true, &l);

    n1.check_quorum = true;
    n2.check_quorum = true;
    n3.check_quorum = true;

    n1.become_follower(1, INVALID_ID);
    n2.become_follower(1, INVALID_ID);
    n3.become_follower(1, INVALID_ID);

    let mut nt = Network::new(vec![Some(n1), Some(n2), Some(n3)], &l);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    // check state
    assert_eq!(nt.peers[&1].state, StateRole::Leader);
    assert_eq!(nt.peers[&2].state, StateRole::Follower);
    assert_eq!(nt.peers[&3].state, StateRole::Follower);

    nt.isolate(3);
    nt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);
    nt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);
    nt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);

    nt.recover();
    nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);

    // check state
    assert_eq!(nt.peers[&1].state, StateRole::Leader);
    assert_eq!(nt.peers[&2].state, StateRole::Follower);
    assert_eq!(nt.peers[&3].state, StateRole::PreCandidate);

    // check term
    // n1.Term == 2
    // n2.Term == 2
    // n3.Term == 2
    assert_eq!(nt.peers[&1].term, 2);
    assert_eq!(nt.peers[&2].term, 2);
    assert_eq!(nt.peers[&3].term, 2);

    // delayed leader heartbeat does not force current leader to step down
    let mut msg = new_message(1, 3, MessageType::MsgHeartbeat, 0);
    msg.term = nt.peers[&1].term;
    nt.send(vec![msg]);
    assert_eq!(nt.peers[&1].state, StateRole::Leader);
}

#[test]
fn test_read_only_option_safe() {
    let l = default_logger();
    let a = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage(), &l);
    let b = new_test_raft(2, vec![1, 2, 3], 10, 1, new_storage(), &l);
    let c = new_test_raft(3, vec![1, 2, 3], 10, 1, new_storage(), &l);

    let mut nt = Network::new(vec![Some(a), Some(b), Some(c)], &l);

    // we can not let system choose the value of randomizedElectionTimeout
    // otherwise it will introduce some uncertainty into this test case
    // we need to ensure randomizedElectionTimeout > electionTimeout here
    let b_election_timeout = nt.peers[&2].election_timeout();
    nt.peers
        .get_mut(&2)
        .unwrap()
        .set_randomized_election_timeout(b_election_timeout + 1);

    for _ in 0..b_election_timeout {
        nt.peers.get_mut(&2).unwrap().tick();
    }
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    assert_eq!(nt.peers[&1].state, StateRole::Leader);

    let mut tests = vec![
        (1, 10, 11, vec!["ctx1", "ctx11"], false),
        (2, 10, 21, vec!["ctx2", "ctx22"], false),
        (3, 10, 31, vec!["ctx3", "ctx33"], false),
        (1, 10, 41, vec!["ctx4", "ctx44"], true),
        (2, 10, 51, vec!["ctx5", "ctx55"], true),
        (3, 10, 61, vec!["ctx6", "ctx66"], true),
    ];

    for (i, (id, proposals, wri, wctx, pending)) in tests.drain(..).enumerate() {
        for _ in 0..proposals {
            nt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);
        }

        let msg1 = new_message_with_entries(
            id,
            id,
            MessageType::MsgReadIndex,
            vec![new_entry(0, 0, Some(wctx[0]))],
        );
        let msg2 = new_message_with_entries(
            id,
            id,
            MessageType::MsgReadIndex,
            vec![new_entry(0, 0, Some(wctx[1]))],
        );

        // `pending` indicates that a `ReadIndex` request will not get through quorum checking immediately
        // so that it remains in the `read_index_queue`
        if pending {
            // drop MsgHeartbeatResponse here to prevent leader handling pending ReadIndex request per round
            nt.ignore(MessageType::MsgHeartbeatResponse);
            nt.send(vec![msg1.clone(), msg1.clone(), msg2.clone()]);
            nt.recover();
            // send a ReadIndex request with the last ctx to notify leader to handle pending read requests
            nt.send(vec![msg2.clone()]);
        } else {
            nt.send(vec![msg1.clone(), msg1.clone(), msg2.clone()]);
        }

        let read_states: Vec<ReadState> = nt
            .peers
            .get_mut(&id)
            .unwrap()
            .read_states
            .drain(..)
            .collect();
        if read_states.is_empty() {
            panic!("#{}: read_states is empty, want non-empty", i);
        }
        assert_eq!(read_states.len(), wctx.len());
        for (rs, wctx) in read_states.iter().zip(wctx) {
            if rs.index != wri {
                panic!("#{}: read_index = {}, want {}", i, rs.index, wri)
            }
            let ctx_bytes = wctx.as_bytes().to_vec();
            if rs.request_ctx != ctx_bytes {
                panic!(
                    "#{}: request_ctx = {:?}, want {:?}",
                    i, rs.request_ctx, ctx_bytes
                )
            }
        }
    }
}

#[test]
fn test_read_only_with_learner() {
    let l = default_logger();
    let a = new_test_learner_raft(1, vec![1], vec![2], 10, 1, new_storage(), &l);
    let b = new_test_learner_raft(2, vec![1], vec![2], 10, 1, new_storage(), &l);

    let mut nt = Network::new(vec![Some(a), Some(b)], &l);

    // we can not let system choose the value of randomizedElectionTimeout
    // otherwise it will introduce some uncertainty into this test case
    // we need to ensure randomizedElectionTimeout > electionTimeout here
    let b_election_timeout = nt.peers[&2].election_timeout();
    nt.peers
        .get_mut(&2)
        .unwrap()
        .set_randomized_election_timeout(b_election_timeout + 1);

    for _ in 0..b_election_timeout {
        nt.peers.get_mut(&2).unwrap().tick();
    }
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    assert_eq!(nt.peers[&1].state, StateRole::Leader);
    assert_eq!(nt.peers[&2].state, StateRole::Follower);

    let mut tests = vec![
        (1, 10, 11, "ctx1"),
        (2, 10, 21, "ctx2"),
        (1, 10, 31, "ctx3"),
        (2, 10, 41, "ctx4"),
    ];

    for (i, (id, proposals, wri, wctx)) in tests.drain(..).enumerate() {
        for _ in 0..proposals {
            nt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);
        }

        let e = new_entry(0, 0, Some(wctx));
        nt.send(vec![new_message_with_entries(
            id,
            id,
            MessageType::MsgReadIndex,
            vec![e],
        )]);

        let read_states: Vec<ReadState> = nt
            .peers
            .get_mut(&id)
            .unwrap()
            .read_states
            .drain(..)
            .collect();
        assert!(
            !read_states.is_empty(),
            "#{}: read_states is empty, want non-empty",
            i
        );
        let rs = &read_states[0];
        assert_eq!(
            rs.index, wri,
            "#{}: read_index = {}, want {}",
            i, rs.index, wri
        );
        let vec_wctx = wctx.as_bytes().to_vec();
        assert_eq!(
            rs.request_ctx, vec_wctx,
            "#{}: request_ctx = {:?}, want {:?}",
            i, rs.request_ctx, vec_wctx
        );
    }
}

#[test]
fn test_read_only_option_lease() {
    let l = default_logger();
    let mut a = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage(), &l);
    let mut b = new_test_raft(2, vec![1, 2, 3], 10, 1, new_storage(), &l);
    let mut c = new_test_raft(3, vec![1, 2, 3], 10, 1, new_storage(), &l);
    a.read_only.option = ReadOnlyOption::LeaseBased;
    b.read_only.option = ReadOnlyOption::LeaseBased;
    c.read_only.option = ReadOnlyOption::LeaseBased;
    a.check_quorum = true;
    b.check_quorum = true;
    c.check_quorum = true;

    let mut nt = Network::new(vec![Some(a), Some(b), Some(c)], &l);

    // we can not let system choose the value of randomizedElectionTimeout
    // otherwise it will introduce some uncertainty into this test case
    // we need to ensure randomizedElectionTimeout > electionTimeout here
    let b_election_timeout = nt.peers[&2].election_timeout();
    nt.peers
        .get_mut(&2)
        .unwrap()
        .set_randomized_election_timeout(b_election_timeout + 1);

    for _ in 0..b_election_timeout {
        nt.peers.get_mut(&2).unwrap().tick();
    }
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    assert_eq!(nt.peers[&1].state, StateRole::Leader);

    let mut tests = vec![
        (1, 10, 11, "ctx1"),
        (2, 10, 21, "ctx2"),
        (3, 10, 31, "ctx3"),
        (1, 10, 41, "ctx4"),
        (2, 10, 51, "ctx5"),
        (3, 10, 61, "ctx6"),
    ];

    for (i, (id, proposals, wri, wctx)) in tests.drain(..).enumerate() {
        for _ in 0..proposals {
            nt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);
        }

        let entry = new_entry(0, 0, Some(wctx));
        nt.send(vec![new_message_with_entries(
            id,
            id,
            MessageType::MsgReadIndex,
            vec![entry],
        )]);

        let read_states: Vec<ReadState> = nt
            .peers
            .get_mut(&id)
            .unwrap()
            .read_states
            .drain(..)
            .collect();
        if read_states.is_empty() {
            panic!("#{}: read_states is empty, want non-empty", i);
        }
        let rs = &read_states[0];
        if rs.index != wri {
            panic!("#{}: read_index = {}, want {}", i, rs.index, wri);
        }
        let vec_wctx = wctx.as_bytes().to_vec();
        if rs.request_ctx != vec_wctx {
            panic!(
                "#{}: request_ctx = {:?}, want {:?}",
                i, rs.request_ctx, vec_wctx
            );
        }
    }
}

#[test]
fn test_read_only_option_lease_without_check_quorum() {
    let l = default_logger();
    let mut a = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage(), &l);
    let mut b = new_test_raft(2, vec![1, 2, 3], 10, 1, new_storage(), &l);
    let mut c = new_test_raft(3, vec![1, 2, 3], 10, 1, new_storage(), &l);
    a.read_only.option = ReadOnlyOption::LeaseBased;
    b.read_only.option = ReadOnlyOption::LeaseBased;
    c.read_only.option = ReadOnlyOption::LeaseBased;

    let mut nt = Network::new(vec![Some(a), Some(b), Some(c)], &l);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    let ctx = "ctx1";
    let entry = new_entry(0, 0, Some(ctx));
    nt.send(vec![new_message_with_entries(
        2,
        2,
        MessageType::MsgReadIndex,
        vec![entry],
    )]);

    let read_states = &nt.peers[&2].read_states;
    assert!(!read_states.is_empty());
    let rs = &read_states[0];
    assert_eq!(rs.index, 1);
    let vec_ctx = ctx.as_bytes().to_vec();
    assert_eq!(rs.request_ctx, vec_ctx);
}

// `test_read_only_for_new_leader` ensures that a leader only accepts MsgReadIndex message
// when it commits at least one log entry at it term.
#[test]
fn test_read_only_for_new_leader() {
    let l = default_logger();
    let heartbeat_ticks = 1;
    let node_configs = vec![(1, 1, 1, 0), (2, 2, 2, 2), (3, 2, 2, 2)];
    let mut peers = vec![];
    for (id, committed, applied, compact_index) in node_configs {
        let mut cfg = new_test_config(id, 10, heartbeat_ticks);
        cfg.applied = applied;
        let storage = MemStorage::new_with_conf_state((vec![1, 2, 3], vec![]));
        let entries = vec![empty_entry(1, 1), empty_entry(1, 2)];
        storage.wl().append(&entries).unwrap();
        let mut hs = HardState::default();
        hs.term = 1;
        hs.commit = committed;
        storage.wl().set_hardstate(hs);
        if compact_index != 0 {
            storage.wl().compact(compact_index).unwrap();
        }
        let i = new_test_raft_with_config(&cfg, storage, &l);
        peers.push(Some(i));
    }
    let mut nt = Network::new(peers, &l);

    // Drop MsgAppend to forbid peer 1 to commit any log entry at its term
    // after it becomes leader.
    nt.ignore(MessageType::MsgAppend);
    // Force peer 1 to become leader
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
    assert_eq!(nt.peers[&1].state, StateRole::Leader);

    // Ensure peer 1 drops read only request.
    let windex = 4;
    let wctx = "ctx";
    nt.send(vec![new_message_with_entries(
        1,
        1,
        MessageType::MsgReadIndex,
        vec![new_entry(0, 0, Some(wctx))],
    )]);
    assert_eq!(nt.peers[&1].read_states.len(), 0);

    nt.recover();

    // Force peer 1 to commit a log entry at its term.
    for _ in 0..heartbeat_ticks {
        nt.peers.get_mut(&1).unwrap().tick();
    }
    nt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);
    assert_eq!(nt.peers[&1].raft_log.committed, 4);
    assert_eq!(
        nt.peers[&1]
            .raft_log
            .term(nt.peers[&1].raft_log.committed)
            .unwrap_or(0),
        nt.peers[&1].term
    );

    // Ensure peer 1 accepts read only request after it commits a entry at its term.
    nt.send(vec![new_message_with_entries(
        1,
        1,
        MessageType::MsgReadIndex,
        vec![new_entry(0, 0, Some(wctx))],
    )]);
    let read_states: Vec<ReadState> = nt
        .peers
        .get_mut(&1)
        .unwrap()
        .read_states
        .drain(..)
        .collect();
    assert_eq!(read_states.len(), 1);
    let rs = &read_states[0];
    assert_eq!(rs.index, windex);
    assert_eq!(rs.request_ctx, wctx.as_bytes().to_vec());
}

// `test_advance_commit_index_by_read_index_response` ensures that read index response
// can advance the follower's commit index if it has new enough logs
#[test]
fn test_advance_commit_index_by_read_index_response() {
    let l = default_logger();
    let mut tt = Network::new(vec![None, None, None, None, None], &l);
    tt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    // don't commit entries
    tt.cut(1, 3);
    tt.cut(1, 4);
    tt.cut(1, 5);
    tt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);
    tt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);

    tt.recover();
    tt.cut(1, 2);

    // commit entries for leader but not node 2
    tt.send(vec![new_message(3, 1, MessageType::MsgReadIndex, 1)]);
    assert_eq!(tt.peers[&1].raft_log.committed, 3);
    assert_eq!(tt.peers[&2].raft_log.committed, 1);

    tt.recover();
    // use LeaseBased so leader won't send MsgHeartbeat to advance node 2's commit index
    tt.peers.get_mut(&1).unwrap().read_only.option = ReadOnlyOption::LeaseBased;
    tt.send(vec![new_message(2, 1, MessageType::MsgReadIndex, 1)]);
    assert_eq!(tt.peers[&2].raft_log.committed, 3);
}

#[test]
fn test_leader_append_response() {
    let l = default_logger();
    // Initial progress: match = 0, next = 4 on followers.
    let mut tests = vec![
        // Stale resp; no replies.
        (3, true, 0, 3, 0, 0, 0),
        // Denied resp; decrease next and send probing message.
        (2, true, 0, 2, 1, 1, 0),
        // Accepted resp; leader commits; broadcast with committed index.
        (2, false, 2, 4, 2, 2, 2),
        (0, false, 0, 3, 0, 0, 0),
    ];

    for (i, (index, reject, wmatch, wnext, wmsg_num, windex, wcommitted)) in
        tests.drain(..).enumerate()
    {
        // Initial raft logs: last index = 3, committed = 1.
        let store = MemStorage::new_with_conf_state((vec![1, 2, 3], vec![]));
        let ents = &[empty_entry(0, 1), empty_entry(1, 2)];
        store.wl().append(ents).unwrap();
        let mut sm = new_test_raft(1, vec![1, 2, 3], 10, 1, store, &l);

        // sm term is 2 after it becomes the leader.
        sm.become_candidate();
        sm.become_leader();

        sm.read_messages();
        let mut m = new_message(2, 0, MessageType::MsgAppendResponse, 0);
        m.index = index;
        m.term = sm.term;
        m.reject = reject;
        m.reject_hint = index;
        sm.step(m).expect("");

        if sm.prs().get(2).unwrap().matched != wmatch {
            panic!(
                "#{}: match = {}, want {}",
                i,
                sm.prs().get(2).unwrap().matched,
                wmatch
            );
        }
        if sm.prs().get(2).unwrap().next_idx != wnext {
            panic!(
                "#{}: next = {}, want {}",
                i,
                sm.prs().get(2).unwrap().next_idx,
                wnext
            );
        }

        let mut msgs = sm.read_messages();
        if msgs.len() != wmsg_num {
            panic!("#{} msg_num = {}, want {}", i, msgs.len(), wmsg_num);
        }
        for (j, msg) in msgs.drain(..).enumerate() {
            if msg.index != windex {
                panic!("#{}.{} index = {}, want {}", i, j, msg.index, windex);
            }
            if msg.commit != wcommitted {
                panic!("#{}.{} commit = {}, want {}", i, j, msg.commit, wcommitted);
            }
        }
    }
}

// When the leader receives a heartbeat tick, it should
// send a MsgApp with m.Index = 0, m.LogTerm=0 and empty entries.
#[test]
fn test_bcast_beat() {
    let l = default_logger();
    // make a state machine with log.offset = 1000
    let offset = 1000u64;
    let s = new_snapshot(offset, 1, vec![1, 2, 3]);
    let store = new_storage();
    store.wl().apply_snapshot(s).expect("");
    let mut sm = new_test_raft(1, vec![1, 2, 3], 10, 1, store, &l);
    sm.term = 1;

    sm.become_candidate();
    sm.become_leader();
    for i in 0..10 {
        let _ = sm.append_entry(&mut [empty_entry(0, offset + i + 1)]);
    }
    sm.persist();
    // slow follower
    let mut_pr = |sm: &mut Interface, n, matched, next_idx| {
        let m = sm.mut_prs().get_mut(n).unwrap();
        m.matched = matched;
        m.next_idx = next_idx;
    };
    // slow follower
    mut_pr(&mut sm, 2, offset + 5, offset + 6);
    // normal follower
    let last_index = sm.raft_log.last_index();
    mut_pr(&mut sm, 3, last_index, last_index + 1);

    sm.step(new_message(0, 0, MessageType::MsgBeat, 0))
        .expect("");
    let mut msgs = sm.read_messages();
    assert_eq!(msgs.len(), 2);

    let mut want_commit_map = HashMap::new();
    want_commit_map.insert(
        2,
        cmp::min(sm.raft_log.committed, sm.prs().get(2).unwrap().matched),
    );
    want_commit_map.insert(
        3,
        cmp::min(sm.raft_log.committed, sm.prs().get(3).unwrap().matched),
    );
    for (i, m) in msgs.drain(..).enumerate() {
        if m.get_msg_type() != MessageType::MsgHeartbeat {
            panic!(
                "#{}: type = {:?}, want = {:?}",
                i,
                m.get_msg_type(),
                MessageType::MsgHeartbeat
            );
        }
        if m.index != 0 {
            panic!("#{}: prev_index = {}, want {}", i, m.index, 0);
        }
        if m.log_term != 0 {
            panic!("#{}: prev_term = {}, want {}", i, m.log_term, 0);
        }
        if want_commit_map[&m.to] == 0 {
            panic!("#{}: unexpected to {}", i, m.to)
        } else {
            if m.commit != want_commit_map[&m.to] {
                panic!(
                    "#{}: commit = {}, want {}",
                    i, m.commit, want_commit_map[&m.to]
                );
            }
            want_commit_map.remove(&m.to);
        }
        if !m.entries.is_empty() {
            panic!("#{}: entries count = {}, want 0", i, m.entries.len());
        }
    }
}

// tests the output of the statemachine when receiving MsgBeat
#[test]
fn test_recv_msg_beat() {
    let l = default_logger();
    let mut tests = vec![
        (StateRole::Leader, 2),
        // candidate and follower should ignore MsgBeat
        (StateRole::Candidate, 0),
        (StateRole::Follower, 0),
    ];

    for (i, (state, w_msg)) in tests.drain(..).enumerate() {
        let store = MemStorage::new_with_conf_state((vec![1, 2, 3], vec![]));
        let ents = &[empty_entry(0, 1), empty_entry(1, 2)];
        store.wl().append(ents).unwrap();

        let mut sm = new_test_raft(1, vec![1, 2, 3], 10, 1, store, &l);
        sm.state = state;
        sm.step(new_message(1, 1, MessageType::MsgBeat, 0))
            .expect("");

        let msgs = sm.read_messages();
        if msgs.len() != w_msg {
            panic!("#{}: msg count = {}, want {}", i, msgs.len(), w_msg);
        }
        for m in msgs {
            if m.get_msg_type() != MessageType::MsgHeartbeat {
                panic!(
                    "#{}: msg.type = {:?}, want {:?}",
                    i,
                    m.get_msg_type(),
                    MessageType::MsgHeartbeat
                );
            }
        }
    }
}

#[test]
fn test_leader_increase_next() {
    let l = default_logger();
    let previous_ents = vec![empty_entry(1, 1), empty_entry(1, 2), empty_entry(1, 3)];
    let mut tests = vec![
        // state replicate; optimistically increase next
        // previous entries + noop entry + propose + 1
        (
            ProgressState::Replicate,
            2,
            previous_ents.len() as u64 + 1 + 1 + 1,
        ),
        // state probe, not optimistically increase next
        (ProgressState::Probe, 2, 2),
    ];
    for (i, (state, next_idx, wnext)) in tests.drain(..).enumerate() {
        let mut sm = new_test_raft(1, vec![1, 2], 10, 1, new_storage(), &l);
        sm.raft_log.append(&previous_ents);
        sm.persist();
        sm.become_candidate();
        sm.become_leader();
        sm.mut_prs().get_mut(2).unwrap().state = state;
        sm.mut_prs().get_mut(2).unwrap().next_idx = next_idx;
        sm.step(new_message(1, 1, MessageType::MsgPropose, 1))
            .expect("");

        if sm.prs().get(2).unwrap().next_idx != wnext {
            panic!(
                "#{}: next = {}, want {}",
                i,
                sm.prs().get(2).unwrap().next_idx,
                wnext
            );
        }
    }
}

#[test]
fn test_send_append_for_progress_probe() {
    let l = default_logger();
    let mut r = new_test_raft(1, vec![1, 2], 10, 1, new_storage(), &l);
    r.become_candidate();
    r.become_leader();
    r.read_messages();
    r.mut_prs().get_mut(2).unwrap().become_probe();

    // each round is a heartbeat
    for i in 0..3 {
        if i == 0 {
            // we expect that raft will only send out one msgAPP on the first
            // loop. After that, the follower is paused until a heartbeat response is
            // received.
            let _ = r.append_entry(&mut [new_entry(0, 0, SOME_DATA)]);
            r.send_append(2);
            let msg = r.read_messages();
            assert_eq!(msg.len(), 1);
            assert_eq!(msg[0].index, 0);
        }

        assert!(r.prs().get(2).unwrap().paused);
        for _ in 0..10 {
            let _ = r.append_entry(&mut [new_entry(0, 0, SOME_DATA)]);
            r.send_append(2);
            assert_eq!(r.read_messages().len(), 0);
        }

        // do a heartbeat
        for _ in 0..r.heartbeat_timeout() {
            r.step(new_message(1, 1, MessageType::MsgBeat, 0))
                .expect("");
        }
        assert!(r.prs().get(2).unwrap().paused);

        // consume the heartbeat
        let msg = r.read_messages();
        assert_eq!(msg.len(), 1);
        assert_eq!(msg[0].get_msg_type(), MessageType::MsgHeartbeat);
    }

    // a heartbeat response will allow another message to be sent
    r.step(new_message(2, 1, MessageType::MsgHeartbeatResponse, 0))
        .expect("");
    let msg = r.read_messages();
    assert_eq!(msg.len(), 1);
    assert_eq!(msg[0].index, 0);
    assert!(r.prs().get(2).unwrap().paused);
}

#[test]
fn test_send_append_for_progress_replicate() {
    let l = default_logger();
    let mut r = new_test_raft(1, vec![1, 2], 10, 1, new_storage(), &l);
    r.become_candidate();
    r.become_leader();
    r.read_messages();
    r.mut_prs().get_mut(2).unwrap().become_replicate();

    for _ in 0..10 {
        let _ = r.append_entry(&mut [new_entry(0, 0, SOME_DATA)]);
        r.send_append(2);
        assert_eq!(r.read_messages().len(), 1);
    }
}

#[test]
fn test_send_append_for_progress_snapshot() {
    let l = default_logger();
    let mut r = new_test_raft(1, vec![1, 2], 10, 1, new_storage(), &l);
    r.become_candidate();
    r.become_leader();
    r.read_messages();
    r.mut_prs().get_mut(2).unwrap().become_snapshot(10);

    for _ in 0..10 {
        let _ = r.append_entry(&mut [new_entry(0, 0, SOME_DATA)]);
        r.send_append(2);
        assert_eq!(r.read_messages().len(), 0);
    }
}

#[test]
fn test_recv_msg_unreachable() {
    let l = default_logger();
    let previous_ents = vec![empty_entry(1, 1), empty_entry(1, 2), empty_entry(1, 3)];
    let s = new_storage();
    s.wl().append(&previous_ents).unwrap();
    let mut r = new_test_raft(1, vec![1, 2], 10, 1, s, &l);
    r.become_candidate();
    r.become_leader();
    r.read_messages();
    // set node 2 to state replicate
    r.mut_prs().get_mut(2).unwrap().matched = 3;
    r.mut_prs().get_mut(2).unwrap().become_replicate();
    r.mut_prs().get_mut(2).unwrap().optimistic_update(5);

    r.step(new_message(2, 1, MessageType::MsgUnreachable, 0))
        .expect("");

    let peer_2 = r.prs().get(2).unwrap();
    assert_eq!(peer_2.state, ProgressState::Probe);
    assert_eq!(peer_2.matched + 1, peer_2.next_idx);
}

#[test]
fn test_restore() {
    let l = default_logger();
    // magic number
    let s = new_snapshot(11, 11, vec![1, 2, 3]);

    let mut sm = new_test_raft(1, vec![1, 2], 10, 1, new_storage(), &l);
    assert!(sm.restore(s.clone()));
    assert_eq!(sm.raft_log.last_index(), s.get_metadata().index);
    assert_eq!(
        sm.raft_log.term(s.get_metadata().index).unwrap(),
        s.get_metadata().term
    );
    assert_iter_eq!(
        o sm.prs().conf().voters().ids(),
        s.get_metadata()
            .get_conf_state()
            .voters
    );
    assert!(!sm.restore(s));
}

#[test]
fn test_restore_ignore_snapshot() {
    let l = default_logger();
    let previous_ents = vec![empty_entry(1, 1), empty_entry(1, 2), empty_entry(1, 3)];
    let commit = 1u64;
    let mut sm = new_test_raft(1, vec![1, 2], 10, 1, new_storage(), &l);
    sm.raft_log.append(&previous_ents);
    sm.raft_log.commit_to(commit);

    let mut s = new_snapshot(commit, 1, vec![1, 2]);

    // ingore snapshot
    assert!(!sm.restore(s.clone()));
    assert_eq!(sm.raft_log.committed, commit);

    // ignore snapshot and fast forward commit
    s.mut_metadata().index = commit + 1;
    assert!(!sm.restore(s));
    assert_eq!(sm.raft_log.committed, commit + 1);
}

#[test]
fn test_provide_snap() {
    let l = default_logger();
    // restore the state machine from a snapshot so it has a compacted log and a snapshot
    let s = new_snapshot(11, 11, vec![1, 2]); // magic number

    let mut sm = new_test_raft(1, vec![1], 10, 1, new_storage(), &l);
    sm.restore(s);
    sm.persist();

    sm.become_candidate();
    sm.become_leader();

    // force set the next of node 2, so that node 2 needs a snapshot
    sm.mut_prs().get_mut(2).unwrap().next_idx = sm.raft_log.first_index();
    let mut m = new_message(2, 1, MessageType::MsgAppendResponse, 0);
    m.index = sm.prs().get(2).unwrap().next_idx - 1;
    m.reject = true;
    sm.step(m).expect("");

    let msgs = sm.read_messages();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].get_msg_type(), MessageType::MsgSnapshot);
}

#[test]
fn test_ignore_providing_snapshot() {
    let l = default_logger();
    // restore the state machine from a snapshot so it has a compacted log and a snapshot
    let s = new_snapshot(11, 11, vec![1, 2]); // magic number
    let mut sm = new_test_raft(1, vec![1], 10, 1, new_storage(), &l);
    sm.restore(s);
    sm.persist();

    sm.become_candidate();
    sm.become_leader();

    // force set the next of node 2, so that node 2 needs a snapshot
    // change node 2 to be inactive, expect node 1 ignore sending snapshot to 2
    sm.mut_prs().get_mut(2).unwrap().next_idx = sm.raft_log.first_index() - 1;
    sm.mut_prs().get_mut(2).unwrap().recent_active = false;

    sm.step(new_message(1, 1, MessageType::MsgPropose, 1))
        .expect("");

    assert_eq!(sm.read_messages().len(), 0);
}

#[test]
fn test_restore_from_snap_msg() {
    let l = default_logger();
    let s = new_snapshot(11, 11, vec![1, 2]); // magic number
    let mut sm = new_test_raft(2, vec![1, 2], 10, 1, new_storage(), &l);
    let mut m = new_message(1, 0, MessageType::MsgSnapshot, 0);
    m.term = 2;
    m.set_snapshot(s);

    sm.step(m).expect("");

    assert_eq!(sm.leader_id, 1);

    // TODO: port the remaining if upstream completed this test.
}

#[test]
fn test_slow_node_restore() {
    let l = default_logger();
    let mut nt = Network::new(vec![None, None, None], &l);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    nt.isolate(3);
    for _ in 0..100 {
        nt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);
    }
    next_ents(nt.peers.get_mut(&1).unwrap(), &nt.storage[&1]);
    nt.storage[&1]
        .wl()
        .commit_to(nt.peers[&1].raft_log.applied)
        .unwrap();
    nt.storage[&1]
        .wl()
        .compact(nt.peers[&1].raft_log.applied)
        .unwrap();

    nt.recover();
    // send heartbeats so that the leader can learn everyone is active.
    // node 3 will only be considered as active when node 1 receives a reply from it.
    loop {
        nt.send(vec![new_message(1, 1, MessageType::MsgBeat, 0)]);
        if nt.peers[&1].prs().get(3).unwrap().recent_active {
            break;
        }
    }

    // trigger a snapshot
    nt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);

    // trigger a commit
    nt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);
    assert_eq!(
        nt.peers[&3].raft_log.committed,
        nt.peers[&1].raft_log.committed
    );
}

// test_step_config tests that when raft step msgProp in EntryConfChange type,
// it appends the entry to log and sets pendingConf to be true.
#[test]
fn test_step_config() {
    let l = default_logger();
    // a raft that cannot make progress
    let mut r = new_test_raft(1, vec![1, 2], 10, 1, new_storage(), &l);
    r.become_candidate();
    r.become_leader();
    let index = r.raft_log.last_index();
    let mut m = new_message(1, 1, MessageType::MsgPropose, 0);
    let mut e = Entry::default();
    e.set_entry_type(EntryType::EntryConfChange);
    m.mut_entries().push(e);
    r.step(m).expect("");
    assert_eq!(r.raft_log.last_index(), index + 1);
}

// test_step_ignore_config tests that if raft step the second msgProp in
// EntryConfChange type when the first one is uncommitted, the node will set
// the proposal to noop and keep its original state.
#[test]
fn test_step_ignore_config() {
    let l = default_logger();
    // a raft that cannot make progress
    let mut r = new_test_raft(1, vec![1, 2], 10, 1, new_storage(), &l);
    r.become_candidate();
    r.become_leader();
    assert!(!r.has_pending_conf());
    let mut m = new_message(1, 1, MessageType::MsgPropose, 0);
    let mut e = Entry::default();
    e.set_entry_type(EntryType::EntryConfChange);
    m.mut_entries().push(e);
    assert!(!r.has_pending_conf());
    r.step(m.clone()).expect("");
    assert!(r.has_pending_conf());
    let index = r.raft_log.last_index();
    let pending_conf_index = r.pending_conf_index;
    r.step(m).expect("");
    let mut we = empty_entry(1, 3);
    we.set_entry_type(EntryType::EntryNormal);
    let wents = vec![we];
    let entries = r
        .raft_log
        .entries(index + 1, None, GetEntriesContext::empty(false))
        .expect("");
    assert_eq!(entries, wents);
    assert_eq!(r.pending_conf_index, pending_conf_index);
}

// test_new_leader_pending_config tests that new leader sets its pending_conf_index
// based on uncommitted entries.
#[test]
fn test_new_leader_pending_config() {
    let l = default_logger();
    let mut tests = vec![(false, 0), (true, 1)];
    for (i, (add_entry, wpending_index)) in tests.drain(..).enumerate() {
        let mut r = new_test_raft(1, vec![1, 2], 10, 1, new_storage(), &l);
        let mut e = Entry::default();
        if add_entry {
            e.set_entry_type(EntryType::EntryNormal);
            let _ = r.append_entry(&mut [e]);
            r.persist();
        }
        r.become_candidate();
        r.become_leader();
        if r.pending_conf_index != wpending_index {
            panic!(
                "#{}: pending_conf_index = {}, want {}",
                i, r.pending_conf_index, wpending_index
            );
        }
        assert_eq!(r.has_pending_conf(), add_entry, "#{}: ", i);
    }
}

// test_add_node tests that add_node could update nodes correctly.
#[test]
fn test_add_node() -> Result<()> {
    let l = default_logger();
    let mut r = new_test_raft(1, vec![1], 10, 1, new_storage(), &l);
    r.apply_conf_change(&add_node(2))?;
    assert_iter_eq!(o r.prs().conf().voters().ids(),
        [1, 2]
    );

    Ok(())
}

#[test]
fn test_add_node_check_quorum() -> Result<()> {
    let l = default_logger();
    let mut r = new_test_raft(1, vec![1], 10, 1, new_storage(), &l);

    r.check_quorum = true;

    r.become_candidate();
    r.become_leader();

    for _ in 0..r.election_timeout() - 1 {
        r.tick();
    }

    r.apply_conf_change(&add_node(2))?;

    // This tick will reach electionTimeout, which triggers a quorum check.
    r.tick();

    // Node 1 should still be the leader after a single tick.
    assert_eq!(r.state, StateRole::Leader);

    // After another electionTimeout ticks without hearing from node 2,
    // node 1 should step down.
    for _ in 0..r.election_timeout() {
        r.tick();
    }

    assert_eq!(r.state, StateRole::Follower);

    Ok(())
}

// test_remove_node tests that removeNode could update pendingConf, nodes and
// and removed list correctly.
#[test]
fn test_remove_node() -> Result<()> {
    let l = default_logger();
    let mut r = new_test_raft(1, vec![1, 2], 10, 1, new_storage(), &l);
    r.apply_conf_change(&remove_node(2))?;
    assert_iter_eq!(o r.prs().conf().voters().ids(), [1]);

    // Removing all voters is not allowed.
    assert!(r.apply_conf_change(&remove_node(1)).is_err());
    assert_iter_eq!(o r.prs().conf().voters().ids(), [1]);

    Ok(())
}

#[test]
fn test_remove_node_itself() {
    let l = default_logger();
    let mut n1 = new_test_learner_raft(1, vec![1], vec![2], 10, 1, new_storage(), &l);

    assert!(n1.apply_conf_change(&remove_node(1)).is_err());
    assert_iter_eq!(n1.prs().conf().learners(), [2]);
    assert_iter_eq!(o n1.prs().conf().voters().ids(), [1]);
}

#[test]
fn test_promotable() {
    let l = default_logger();
    let id = 1u64;
    let mut tests = vec![
        (vec![1], true),
        (vec![1, 2, 3], true),
        (vec![], false),
        (vec![2, 3], false),
    ];
    for (i, (peers, wp)) in tests.drain(..).enumerate() {
        let r = new_test_raft(id, peers, 5, 1, new_storage(), &l);
        if r.promotable() != wp {
            panic!("#{}: promotable = {}, want {}", i, r.promotable(), wp);
        }
    }
}

#[test]
fn test_raft_nodes() {
    let l = default_logger();
    let mut tests = vec![
        (vec![1, 2, 3], vec![1, 2, 3]),
        (vec![3, 2, 1], vec![1, 2, 3]),
    ];
    for (i, (ids, wids)) in tests.drain(..).enumerate() {
        let r = new_test_raft(1, ids, 10, 1, new_storage(), &l);
        let voter_ids: HashSet<_> = r.prs().conf().voters().ids().iter().collect();
        let wids = wids.into_iter().collect::<HashSet<_>>();
        if voter_ids != wids {
            panic!("#{}: nodes = {:?}, want {:?}", i, voter_ids, wids);
        }
    }
}

#[test]
fn test_campaign_while_leader() {
    let l = default_logger();
    test_campaign_while_leader_with_pre_vote(false, &l);
}

#[test]
fn test_pre_campaign_while_leader() {
    let l = default_logger();
    test_campaign_while_leader_with_pre_vote(true, &l);
}

fn test_campaign_while_leader_with_pre_vote(pre_vote: bool, l: &Logger) {
    let mut r = new_test_raft_with_prevote(1, vec![1], 5, 1, new_storage(), pre_vote, l);
    assert_eq!(r.state, StateRole::Follower);
    // We don't call campaign() directly because it comes after the check
    // for our current state.
    r.step(new_message(1, 1, MessageType::MsgHup, 0)).expect("");
    assert_eq!(r.state, StateRole::Leader);
    let term = r.term;
    r.step(new_message(1, 1, MessageType::MsgHup, 0)).expect("");
    assert_eq!(r.state, StateRole::Leader);
    assert_eq!(r.term, term);
}

// test_commit_after_remove_node verifies that pending commands can become
// committed when a config change reduces the quorum requirements.
#[test]
fn test_commit_after_remove_node() -> Result<()> {
    let l = default_logger();
    // Create a cluster with two nodes.
    let s = new_storage();
    let mut r = new_test_raft(1, vec![1, 2], 5, 1, s.clone(), &l);
    r.become_candidate();
    r.become_leader();

    // Begin to remove the second node.
    let mut msg = new_message(0, 0, MessageType::MsgPropose, 0);
    let mut entry = Entry::default();
    entry.set_entry_type(EntryType::EntryConfChange);
    let mut cc = ConfChange::default();
    cc.set_change_type(ConfChangeType::RemoveNode);
    cc.node_id = 2;
    let ccdata = cc.write_to_bytes().unwrap();
    entry.data = ccdata.into();
    msg.mut_entries().push(entry);
    r.step(msg).expect("");
    // Stabilize the log and make sure nothing is committed yet.
    assert_eq!(next_ents(&mut r, &s).len(), 0);
    let cc_index = r.raft_log.last_index();

    // While the config change is pending, make another proposal.
    let mut msg = new_message(0, 0, MessageType::MsgPropose, 0);
    let mut entry = new_entry(0, 0, Some("hello"));
    entry.set_entry_type(EntryType::EntryNormal);
    msg.mut_entries().push(entry);
    r.step(msg).expect("");

    // Node 2 acknowledges the config change, committing it.
    let mut msg = new_message(2, 0, MessageType::MsgAppendResponse, 0);
    msg.index = cc_index;
    r.step(msg).expect("");
    let ents = next_ents(&mut r, &s);
    assert_eq!(ents.len(), 2);
    assert_eq!(ents[0].get_entry_type(), EntryType::EntryNormal);
    assert!(ents[0].data.is_empty());
    assert_eq!(ents[1].get_entry_type(), EntryType::EntryConfChange);

    // Apply the config change. This reduces quorum requirements so the
    // pending command can now commit.
    r.apply_conf_change(&remove_node(2))?;
    let ents = next_ents(&mut r, &s);
    assert_eq!(ents.len(), 1);
    assert_eq!(ents[0].get_entry_type(), EntryType::EntryNormal);
    assert_eq!(ents[0].data.as_ref(), b"hello");

    Ok(())
}

// test_leader_transfer_to_uptodate_node verifies transferring should succeed
// if the transferee has the most up-to-date log entries when transfer starts.
#[test]
fn test_leader_transfer_to_uptodate_node() {
    let l = default_logger();
    let mut nt = Network::new(vec![None, None, None], &l);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    let lead_id = nt.peers[&1].leader_id;
    assert_eq!(lead_id, 1);

    // Transfer leadership to peer 2.
    nt.send(vec![new_message(2, 1, MessageType::MsgTransferLeader, 0)]);
    check_leader_transfer_state(&nt.peers[&1], StateRole::Follower, 2);

    // After some log replication, transfer leadership back to peer 1.
    nt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);
    nt.send(vec![new_message(1, 2, MessageType::MsgTransferLeader, 0)]);
    check_leader_transfer_state(&nt.peers[&1], StateRole::Leader, 1);
}

// test_leader_transfer_to_uptodate_node_from_follower verifies transferring should succeed
// if the transferee has the most up-to-date log entries when transfer starts.
// Not like test_leader_transfer_to_uptodate_node, where the leader transfer message
// is sent to the leader, in this test case every leader transfer message is sent
// to the follower.
#[test]
fn test_leader_transfer_to_uptodate_node_from_follower() {
    let l = default_logger();
    let mut nt = Network::new(vec![None, None, None], &l);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    let lead_id = nt.peers[&1].leader_id;
    assert_eq!(lead_id, 1);

    // transfer leadership to peer 2.
    nt.send(vec![new_message(2, 2, MessageType::MsgTransferLeader, 0)]);
    check_leader_transfer_state(&nt.peers[&1], StateRole::Follower, 2);

    // After some log replication, transfer leadership back to peer 1.
    nt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);
    nt.send(vec![new_message(1, 1, MessageType::MsgTransferLeader, 0)]);
    check_leader_transfer_state(&nt.peers[&1], StateRole::Leader, 1);
}

// TestLeaderTransferWithCheckQuorum ensures transferring leader still works
// even the current leader is still under its leader lease
#[test]
fn test_leader_transfer_with_check_quorum() {
    let l = default_logger();
    let mut nt = Network::new(vec![None, None, None], &l);
    for i in 1..4 {
        let r = &mut nt.peers.get_mut(&i).unwrap();
        r.check_quorum = true;
        let election_timeout = r.election_timeout();
        r.set_randomized_election_timeout(election_timeout + i as usize);
    }

    let b_election_timeout = nt.peers[&2].election_timeout();
    nt.peers
        .get_mut(&2)
        .unwrap()
        .set_randomized_election_timeout(b_election_timeout + 1);

    // Letting peer 2 electionElapsed reach to timeout so that it can vote for peer 1
    for _ in 0..b_election_timeout {
        nt.peers.get_mut(&2).unwrap().tick();
    }
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    assert_eq!(nt.peers[&1].leader_id, 1);

    // Transfer leadership to 2.
    nt.send(vec![new_message(2, 1, MessageType::MsgTransferLeader, 0)]);
    check_leader_transfer_state(&nt.peers[&1], StateRole::Follower, 2);

    // After some log replication, transfer leadership back to 1.
    nt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);
    nt.send(vec![new_message(1, 2, MessageType::MsgTransferLeader, 0)]);
    check_leader_transfer_state(&nt.peers[&1], StateRole::Leader, 1);
}

#[test]
fn test_leader_transfer_to_slow_follower() {
    let l = default_logger();
    let mut nt = Network::new(vec![None, None, None], &l);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    nt.isolate(3);
    nt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);

    nt.recover();
    assert_eq!(nt.peers[&1].prs().get(3).unwrap().matched, 1);

    // Transfer leadership to 3 when node 3 is lack of log.
    nt.send(vec![new_message(3, 1, MessageType::MsgTransferLeader, 0)]);

    check_leader_transfer_state(&nt.peers[&1], StateRole::Follower, 3);
}

#[test]
fn test_leader_transfer_after_snapshot() {
    let l = default_logger();
    let mut nt = Network::new(vec![None, None, None], &l);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    nt.isolate(3);

    nt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);
    next_ents(nt.peers.get_mut(&1).unwrap(), &nt.storage[&1]);
    nt.storage[&1]
        .wl()
        .commit_to(nt.peers[&1].raft_log.applied)
        .unwrap();
    nt.storage[&1]
        .wl()
        .compact(nt.peers[&1].raft_log.applied)
        .unwrap();

    nt.recover();
    assert_eq!(nt.peers[&1].prs().get(3).unwrap().matched, 1);

    // Transfer leadership to 3 when node 3 is lack of snapshot.
    nt.send(vec![new_message(3, 1, MessageType::MsgTransferLeader, 0)]);
    // Send pb.MsgHeartbeatResp to leader to trigger a snapshot for node 3.
    nt.send(vec![new_message(
        3,
        1,
        MessageType::MsgHeartbeatResponse,
        0,
    )]);

    check_leader_transfer_state(&nt.peers[&1], StateRole::Follower, 3);
}

#[test]
fn test_leader_transfer_to_self() {
    let l = default_logger();
    let mut nt = Network::new(vec![None, None, None], &l);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    // Transfer leadership to self, there will be noop.
    nt.send(vec![new_message(1, 1, MessageType::MsgTransferLeader, 0)]);
    check_leader_transfer_state(&nt.peers[&1], StateRole::Leader, 1);
}

#[test]
fn test_leader_transfer_to_non_existing_node() {
    let l = default_logger();
    let mut nt = Network::new(vec![None, None, None], &l);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    // Transfer leadership to non-existing node, there will be noop.
    nt.send(vec![new_message(4, 1, MessageType::MsgTransferLeader, 0)]);
    check_leader_transfer_state(&nt.peers[&1], StateRole::Leader, 1);
}

#[test]
fn test_leader_transfer_to_learner() {
    let l = default_logger();
    let s = MemStorage::new_with_conf_state((vec![1], vec![2]));
    let c = new_test_config(1, 10, 1);
    let leader = new_test_raft_with_config(&c, s, &l);

    let s = MemStorage::new_with_conf_state((vec![1], vec![2]));
    let c = new_test_config(2, 10, 1);
    let learner = new_test_raft_with_config(&c, s, &l);

    let mut nt = Network::new(vec![Some(leader), Some(learner)], &l);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    // Transfer leadership to learner node, there will be noop.
    nt.send(vec![new_message(2, 1, MessageType::MsgTransferLeader, 0)]);
    check_leader_transfer_state(&nt.peers[&1], StateRole::Leader, 1);
}

#[test]
fn test_leader_transfer_timeout() {
    let l = default_logger();
    let mut nt = Network::new(vec![None, None, None], &l);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    nt.isolate(3);

    // Transfer leadership to isolated node, wait for timeout.
    nt.send(vec![new_message(3, 1, MessageType::MsgTransferLeader, 0)]);
    assert_eq!(nt.peers[&1].lead_transferee.unwrap(), 3);
    let heartbeat_timeout = nt.peers[&1].heartbeat_timeout();
    let election_timeout = nt.peers[&1].election_timeout();
    for _ in 0..heartbeat_timeout {
        nt.peers.get_mut(&1).unwrap().tick();
    }
    assert_eq!(nt.peers[&1].lead_transferee.unwrap(), 3);
    for _ in 0..election_timeout - heartbeat_timeout {
        nt.peers.get_mut(&1).unwrap().tick();
    }

    check_leader_transfer_state(&nt.peers[&1], StateRole::Leader, 1);
}

#[test]
fn test_leader_transfer_ignore_proposal() {
    let l = default_logger();
    let mut nt = Network::new(vec![None, None, None], &l);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    nt.isolate(3);

    // Transfer leadership to isolated node to let transfer pending, then send proposal.
    nt.send(vec![new_message(3, 1, MessageType::MsgTransferLeader, 0)]);
    assert_eq!(nt.peers[&1].lead_transferee.unwrap(), 3);

    nt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);
    assert_eq!(
        nt.peers
            .get_mut(&1)
            .unwrap()
            .step(new_message(1, 1, MessageType::MsgPropose, 1)),
        Err(Error::ProposalDropped),
        "should return drop proposal error while transferring"
    );

    assert_eq!(nt.peers[&1].prs().get(1).unwrap().matched, 1);
}

#[test]
fn test_leader_transfer_receive_higher_term_vote() {
    let l = default_logger();
    let mut nt = Network::new(vec![None, None, None], &l);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    nt.isolate(3);

    // Transfer leadership to isolated node to let transfer pending.
    nt.send(vec![new_message(3, 1, MessageType::MsgTransferLeader, 0)]);
    assert_eq!(nt.peers[&1].lead_transferee.unwrap(), 3);

    nt.send(vec![new_message_with_entries(
        2,
        2,
        MessageType::MsgHup,
        vec![new_entry(1, 2, None)],
    )]);

    check_leader_transfer_state(&nt.peers[&1], StateRole::Follower, 2);
}

#[test]
fn test_leader_transfer_remove_node() -> Result<()> {
    let l = default_logger();
    let mut nt = Network::new(vec![None, None, None], &l);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    nt.ignore(MessageType::MsgTimeoutNow);

    // The lead_transferee is removed when leadship transferring.
    nt.send(vec![new_message(3, 1, MessageType::MsgTransferLeader, 0)]);
    assert_eq!(nt.peers[&1].lead_transferee.unwrap(), 3);

    nt.peers
        .get_mut(&1)
        .unwrap()
        .apply_conf_change(&remove_node(3))?;

    check_leader_transfer_state(&nt.peers[&1], StateRole::Leader, 1);

    Ok(())
}

// test_leader_transfer_back verifies leadership can transfer
// back to self when last transfer is pending.
#[test]
fn test_leader_transfer_back() {
    let l = default_logger();
    let mut nt = Network::new(vec![None, None, None], &l);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    nt.isolate(3);

    nt.send(vec![new_message(3, 1, MessageType::MsgTransferLeader, 0)]);
    assert_eq!(nt.peers[&1].lead_transferee.unwrap(), 3);

    // Transfer leadership back to self.
    nt.send(vec![new_message(1, 1, MessageType::MsgTransferLeader, 0)]);

    check_leader_transfer_state(&nt.peers[&1], StateRole::Leader, 1);
}

// test_leader_transfer_second_transfer_to_another_node verifies leader can transfer to another node
// when last transfer is pending.
#[test]
fn test_leader_transfer_second_transfer_to_another_node() {
    let l = default_logger();
    let mut nt = Network::new(vec![None, None, None], &l);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    nt.isolate(3);

    nt.send(vec![new_message(3, 1, MessageType::MsgTransferLeader, 0)]);
    assert_eq!(nt.peers[&1].lead_transferee.unwrap(), 3);

    // Transfer leadership to another node.
    nt.send(vec![new_message(2, 1, MessageType::MsgTransferLeader, 0)]);

    check_leader_transfer_state(&nt.peers[&1], StateRole::Follower, 2);
}

// test_leader_transfer_second_transfer_to_same_node verifies second transfer leader request
// to the same node should not extend the timeout while the first one is pending.
#[test]
fn test_leader_transfer_second_transfer_to_same_node() {
    let l = default_logger();
    let mut nt = Network::new(vec![None, None, None], &l);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    nt.isolate(3);

    nt.send(vec![new_message(3, 1, MessageType::MsgTransferLeader, 0)]);
    assert_eq!(nt.peers[&1].lead_transferee.unwrap(), 3);

    let heartbeat_timeout = nt.peers[&1].heartbeat_timeout();
    for _ in 0..heartbeat_timeout {
        nt.peers.get_mut(&1).unwrap().tick();
    }

    // Second transfer leadership request to the same node.
    nt.send(vec![new_message(3, 1, MessageType::MsgTransferLeader, 0)]);

    let election_timeout = nt.peers[&1].election_timeout();
    for _ in 0..election_timeout - heartbeat_timeout {
        nt.peers.get_mut(&1).unwrap().tick();
    }

    check_leader_transfer_state(&nt.peers[&1], StateRole::Leader, 1);
}

fn check_leader_transfer_state(r: &Raft<MemStorage>, state: StateRole, lead: u64) {
    if r.state != state || r.leader_id != lead {
        panic!(
            "after transferring, node has state {:?} lead {}, want state {:?} lead {}",
            r.state, r.leader_id, state, lead
        );
    }
    assert_eq!(r.lead_transferee, None);
}

// test_transfer_non_member verifies that when a MsgTimeoutNow arrives at
// a node that has been removed from the group, nothing happens.
// (previously, if the node also got votes, it would panic as it
// transitioned to StateRole::Leader)
#[test]
fn test_transfer_non_member() {
    let l = default_logger();
    let mut raft = new_test_raft(1, vec![2, 3, 4], 5, 1, new_storage(), &l);
    raft.step(new_message(2, 1, MessageType::MsgTimeoutNow, 0))
        .expect("");

    raft.step(new_message(2, 1, MessageType::MsgRequestVoteResponse, 0))
        .expect("");
    raft.step(new_message(3, 1, MessageType::MsgRequestVoteResponse, 0))
        .expect("");
    assert_eq!(raft.state, StateRole::Follower);
}

// TestNodeWithSmallerTermCanCompleteElection tests the scenario where a node
// that has been partitioned away (and fallen behind) rejoins the cluster at
// about the same time the leader node gets partitioned away.
// Previously the cluster would come to a standstill when run with PreVote
// enabled.
#[test]
fn test_node_with_smaller_term_can_complete_election() {
    let l = default_logger();
    let mut n1 = new_test_raft_with_prevote(1, vec![1, 2, 3], 10, 1, new_storage(), true, &l);
    let mut n2 = new_test_raft_with_prevote(2, vec![1, 2, 3], 10, 1, new_storage(), true, &l);
    let mut n3 = new_test_raft_with_prevote(3, vec![1, 2, 3], 10, 1, new_storage(), true, &l);

    n1.become_follower(1, INVALID_ID);
    n2.become_follower(1, INVALID_ID);
    n3.become_follower(1, INVALID_ID);

    // cause a network partition to isolate node 3
    let mut config = Network::default_config();
    config.pre_vote = true;
    let mut nt = Network::new_with_config(vec![Some(n1), Some(n2), Some(n3)], &config, &l);
    nt.cut(1, 3);
    nt.cut(2, 3);

    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    assert_eq!(nt.peers[&1].state, StateRole::Leader);
    assert_eq!(nt.peers[&2].state, StateRole::Follower);

    nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);
    assert_eq!(nt.peers[&3].state, StateRole::PreCandidate);

    nt.send(vec![new_message(2, 2, MessageType::MsgHup, 0)]);

    // check whether the term values are expected
    // a.Term == 3
    // b.Term == 3
    // c.Term == 1
    assert_eq!(nt.peers[&1].term, 3);
    assert_eq!(nt.peers[&2].term, 3);
    assert_eq!(nt.peers[&3].term, 1);

    // check state
    // a == follower
    // b == leader
    // c == pre-candidate
    assert_eq!(nt.peers[&1].state, StateRole::Follower);
    assert_eq!(nt.peers[&2].state, StateRole::Leader);
    assert_eq!(nt.peers[&3].state, StateRole::PreCandidate);

    // recover the network then immediately isolate b which is currently
    // the leader, this is to emulate the crash of b.
    nt.recover();
    nt.cut(2, 1);
    nt.cut(2, 3);

    // call for election
    nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    // do we have a leader?
    assert!(
        nt.peers[&1].state == StateRole::Leader || nt.peers[&3].state == StateRole::Leader,
        "no leader"
    );
}

pub fn new_test_learner_raft(
    id: u64,
    peers: Vec<u64>,
    learners: Vec<u64>,
    election: usize,
    heartbeat: usize,
    storage: MemStorage,
    logger: &Logger,
) -> Interface {
    if storage.initial_state().unwrap().initialized() && peers.is_empty() {
        panic!("new_test_raft with empty peers on initialized store");
    }
    if !peers.is_empty() && !storage.initial_state().unwrap().initialized() {
        storage.initialize_with_conf_state((peers, learners));
    }
    let cfg = new_test_config(id, election, heartbeat);
    new_test_raft_with_config(&cfg, storage, logger)
}

pub fn new_test_learner_raft_with_prevote(
    id: u64,
    peers: Vec<u64>,
    learners: Vec<u64>,
    logger: &Logger,
    prevote: bool,
) -> Interface {
    let storage = new_storage();
    storage.initialize_with_conf_state((peers, learners));
    let mut cfg = new_test_config(id, 10, 1);
    cfg.pre_vote = prevote;
    new_test_raft_with_config(&cfg, storage, logger)
}

// TestLearnerElectionTimeout verifies that the leader should not start election
// even when times out.
#[test]
fn test_learner_election_timeout() {
    let l = default_logger();
    let mut n1 = new_test_learner_raft(1, vec![1], vec![2], 10, 1, new_storage(), &l);
    n1.become_follower(1, INVALID_ID);

    let mut n2 = new_test_learner_raft(2, vec![1], vec![2], 10, 1, new_storage(), &l);
    n2.become_follower(1, INVALID_ID);

    let timeout = n2.election_timeout();
    n2.set_randomized_election_timeout(timeout);

    // n2 is a learner. Learner should not start election even when time out.
    for _ in 0..timeout {
        n2.tick();
    }
    assert_eq!(n2.state, StateRole::Follower);
}

// TestLearnerPromotion verifies that the leaner should not election until
// it is promoted to a normal peer.
#[test]
fn test_learner_promotion() -> Result<()> {
    let l = default_logger();
    let mut n1 = new_test_learner_raft(1, vec![1], vec![2], 10, 1, new_storage(), &l);
    n1.become_follower(1, INVALID_ID);

    let mut n2 = new_test_learner_raft(2, vec![1], vec![2], 10, 1, new_storage(), &l);
    n2.become_follower(1, INVALID_ID);

    let mut network = Network::new(vec![Some(n1), Some(n2)], &l);
    assert_eq!(network.peers[&1].state, StateRole::Follower);

    // n1 should become leader.
    let timeout = network.peers[&1].election_timeout();
    network
        .peers
        .get_mut(&1)
        .unwrap()
        .set_randomized_election_timeout(timeout);
    for _ in 0..timeout {
        network.peers.get_mut(&1).unwrap().tick();
    }
    assert_eq!(network.peers[&1].state, StateRole::Leader);
    assert_eq!(network.peers[&2].state, StateRole::Follower);

    let mut heart_beat = new_message(1, 1, MessageType::MsgBeat, 0);
    network.send(vec![heart_beat.clone()]);

    // Promote n2 from learner to follower.
    network
        .peers
        .get_mut(&1)
        .unwrap()
        .apply_conf_change(&add_node(2))?;
    network
        .peers
        .get_mut(&2)
        .unwrap()
        .apply_conf_change(&add_node(2))?;
    assert_eq!(network.peers[&2].state, StateRole::Follower);
    assert!(network.peers[&2].promotable());

    let timeout = network.peers[&2].election_timeout();
    network
        .peers
        .get_mut(&2)
        .unwrap()
        .set_randomized_election_timeout(timeout);
    for _ in 0..timeout {
        network.peers.get_mut(&2).unwrap().tick();
    }

    heart_beat.to = 2;
    heart_beat.from = 2;
    network.send(vec![heart_beat]);
    assert_eq!(network.peers[&1].state, StateRole::Follower);
    assert_eq!(network.peers[&2].state, StateRole::Leader);

    Ok(())
}

// TestLearnerLogReplication tests that a learner can receive entries from the leader.
#[test]
fn test_learner_log_replication() {
    let l = default_logger();
    let n1 = new_test_learner_raft(1, vec![1], vec![2], 10, 1, new_storage(), &l);
    let n2 = new_test_learner_raft(2, vec![1], vec![2], 10, 1, new_storage(), &l);
    let mut network = Network::new(vec![Some(n1), Some(n2)], &l);

    network
        .peers
        .get_mut(&1)
        .unwrap()
        .become_follower(1, INVALID_ID);
    network
        .peers
        .get_mut(&2)
        .unwrap()
        .become_follower(1, INVALID_ID);

    let timeout = network.peers[&1].election_timeout();
    network
        .peers
        .get_mut(&1)
        .unwrap()
        .set_randomized_election_timeout(timeout);

    for _ in 0..timeout {
        network.peers.get_mut(&1).unwrap().tick();
    }

    let heart_beat = new_message(1, 1, MessageType::MsgBeat, 0);
    network.send(vec![heart_beat]);

    assert_eq!(network.peers[&1].state, StateRole::Leader);
    assert_eq!(network.peers[&2].state, StateRole::Follower);
    assert!(!network.peers[&2].promotable());

    let next_committed = network.peers[&1].raft_log.committed + 1;

    let msg = new_message(1, 1, MessageType::MsgPropose, 1);
    network.send(vec![msg]);

    assert_eq!(network.peers[&1].raft_log.committed, next_committed);
    assert_eq!(network.peers[&2].raft_log.committed, next_committed);

    let matched = network
        .peers
        .get_mut(&1)
        .unwrap()
        .prs()
        .get(2)
        .unwrap()
        .matched;
    assert_eq!(matched, network.peers[&2].raft_log.committed);
}

// TestRestoreWithLearner restores a snapshot which contains learners.
#[test]
fn test_restore_with_learner() {
    let l = default_logger();
    let mut s = new_snapshot(11, 11, vec![1, 2]);
    s.mut_metadata().mut_conf_state().mut_learners().push(3);

    let mut sm = new_test_learner_raft(3, vec![1, 2], vec![3], 10, 1, new_storage(), &l);
    assert!(!sm.promotable());
    assert!(sm.restore(s.clone()));
    assert_eq!(sm.raft_log.last_index(), 11);
    assert_eq!(sm.raft_log.term(11).unwrap(), 11);
    assert_iter_eq!(o sm.prs().conf().voters().ids(), [1, 2]);
    assert_iter_eq!(sm.prs().conf().learners(), [3]);

    let conf_state = s.get_metadata().get_conf_state();
    for node in &conf_state.voters {
        assert!(sm.prs().get(*node).is_some());
        assert!(!sm.prs().conf().learners().contains(node));
    }

    for node in &conf_state.learners {
        assert!(sm.prs().get(*node).is_some());
        assert!(sm.prs().conf().learners().contains(node));
    }

    assert!(!sm.restore(s));
}

// Tests if outgoing voters can restore snapshot correctly.
#[test]
fn test_restore_with_voters_outgoing() {
    let l = default_logger();
    // magic number
    let mut s = new_snapshot(11, 11, vec![2, 3, 4]);
    s.mut_metadata().mut_conf_state().voters_outgoing = vec![1, 2, 3];

    let mut sm = new_test_raft(1, vec![1, 2], 10, 1, new_storage(), &l);
    assert!(sm.restore(s.clone()));
    assert_eq!(sm.raft_log.last_index(), s.get_metadata().index);
    assert_eq!(
        sm.raft_log.term(s.get_metadata().index).unwrap(),
        s.get_metadata().term
    );
    assert_iter_eq!(
        o sm.prs().conf().voters().ids(),
        [1, 2, 3, 4]
    );
    assert!(!sm.restore(s));
}

// Verifies that a voter can be depromoted by snapshot.
#[test]
fn test_restore_depromote_voter() {
    let l = default_logger();
    let mut s = new_snapshot(11, 11, vec![1, 2]);
    s.mut_metadata().mut_conf_state().mut_learners().push(3);

    let mut sm = new_test_raft(3, vec![1, 2, 3], 10, 1, new_storage(), &l);
    assert!(sm.promotable());
    assert!(sm.restore(s));
}

#[test]
fn test_restore_learner() {
    let l = default_logger();
    let mut s = new_snapshot(11, 11, vec![1, 2]);
    s.mut_metadata().mut_conf_state().mut_learners().push(3);

    let mut sm = new_test_raft(3, vec![], 10, 1, new_storage(), &l);
    assert!(!sm.promotable()); // Uninitialized peers can't be promoted.
    assert!(sm.restore(s));
    assert!(!sm.promotable());
}

// TestRestoreLearnerPromotion checks that a learner can become to a follower after
// restoring snapshot.
#[test]
fn test_restore_learner_promotion() {
    let l = default_logger();
    let s = new_snapshot(11, 11, vec![1, 2, 3]);
    let mut sm = new_test_learner_raft(3, vec![1, 2], vec![3], 10, 1, new_storage(), &l);
    assert!(!sm.promotable());
    assert!(sm.restore(s));
    assert!(sm.promotable());
}

// TestLearnerReceiveSnapshot tests that a learner can receive a snapshot from leader.
#[test]
fn test_learner_receive_snapshot() {
    let l = default_logger();
    let mut s = new_snapshot(11, 11, vec![1]);
    s.mut_metadata().mut_conf_state().mut_learners().push(2);

    let mut n1 = new_test_learner_raft(1, vec![1], vec![2], 10, 1, new_storage(), &l);
    let n2 = new_test_learner_raft(2, vec![1], vec![2], 10, 1, new_storage(), &l);

    n1.restore(s);
    n1.persist();

    let committed = n1.raft_log.committed;
    n1.commit_apply(committed);

    let mut network = Network::new(vec![Some(n1), Some(n2)], &l);

    let timeout = network.peers[&1].election_timeout();
    network
        .peers
        .get_mut(&1)
        .unwrap()
        .set_randomized_election_timeout(timeout);

    for _ in 0..timeout {
        network.peers.get_mut(&1).unwrap().tick();
    }

    let mut msg = Message::default();
    msg.from = 1;
    msg.to = 1;
    msg.set_msg_type(MessageType::MsgBeat);
    network.send(vec![msg]);

    let n1_committed = network.peers[&1].raft_log.committed;
    let n2_committed = network.peers[&2].raft_log.committed;
    assert_eq!(n1_committed, n2_committed);
}

// TestAddLearner tests that addLearner could update nodes correctly.
#[test]
fn test_add_learner() -> Result<()> {
    let l = default_logger();
    let mut n1 = new_test_raft(1, vec![1], 10, 1, new_storage(), &l);
    n1.apply_conf_change(&add_learner(2))?;

    assert_iter_eq!(n1.prs().conf().learners(), [2]);
    assert!(n1.prs().conf().learners().contains(&2));

    Ok(())
}

// TestRemoveLearner tests that removeNode could update nodes and
// and removed list correctly.
#[test]
fn test_remove_learner() -> Result<()> {
    let l = default_logger();
    let mut n1 = new_test_learner_raft(1, vec![1], vec![2], 10, 1, new_storage(), &l);
    n1.apply_conf_change(&remove_node(2))?;
    assert_iter_eq!(o n1.prs().conf().voters().ids(), [1]);
    assert!(n1.prs().conf().learners().is_empty());

    // Remove all voters are not allowed.
    assert!(n1.apply_conf_change(&remove_node(1)).is_err());
    assert_iter_eq!(o n1.prs().conf().voters().ids(), [1]);
    assert!(n1.prs().conf().learners().is_empty());

    Ok(())
}

// simulate rolling update a cluster for Pre-Vote. cluster has 3 nodes [n1, n2, n3].
// n1 is leader with term 2
// n2 is follower with term 2
// n3 is partitioned, with term 4 and less log, state is candidate
fn new_prevote_migration_cluster(l: &Logger) -> Network {
    // We intentionally do not enable pre_vote for n3, this is done so in order
    // to simulate a rolling restart process where it's possible to have a mixed
    // version cluster with replicas with pre_vote enabled, and replicas without.
    let mut n1 = new_test_raft_with_prevote(1, vec![1, 2, 3], 10, 1, new_storage(), true, l);
    let mut n2 = new_test_raft_with_prevote(2, vec![1, 2, 3], 10, 1, new_storage(), true, l);
    let mut n3 = new_test_raft_with_prevote(3, vec![1, 2, 3], 10, 1, new_storage(), false, l);

    n1.become_follower(1, INVALID_ID);
    n2.become_follower(1, INVALID_ID);
    n3.become_follower(1, INVALID_ID);

    let mut nt = Network::new(vec![Some(n1), Some(n2), Some(n3)], l);

    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    // Cause a network partition to isolate n3.
    nt.isolate(3);
    nt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);

    nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);
    nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);

    // check state
    // n1.state == Leader
    // n2.state == Follower
    // n3.state == Candidate
    assert_eq!(nt.peers[&1].state, StateRole::Leader);
    assert_eq!(nt.peers[&2].state, StateRole::Follower);
    assert_eq!(nt.peers[&3].state, StateRole::Candidate);

    // check term
    // n1.Term == 2
    // n2.Term == 2
    // n3.Term == 4
    assert_eq!(nt.peers[&1].term, 2);
    assert_eq!(nt.peers[&2].term, 2);
    assert_eq!(nt.peers[&3].term, 4);

    // Enable prevote on n3, then recover the network
    nt.peers.get_mut(&3).unwrap().pre_vote = true;
    nt.recover();

    nt
}

#[test]
fn test_prevote_migration_can_complete_election() {
    let l = default_logger();
    // n1 is leader with term 2
    // n2 is follower with term 2
    // n3 is pre-candidate with term 4, and less log
    let mut nt = new_prevote_migration_cluster(&l);

    // simulate leader down
    nt.isolate(1);

    // Call for elections from both n2 and n3.
    nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);
    nt.send(vec![new_message(2, 2, MessageType::MsgHup, 0)]);

    // check state
    // n2.state == Follower
    // n3.state == PreCandidate
    assert_eq!(nt.peers[&2].state, StateRole::Follower);
    assert_eq!(nt.peers[&3].state, StateRole::PreCandidate);

    nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);
    nt.send(vec![new_message(2, 2, MessageType::MsgHup, 0)]);

    // Do we have a leader?
    assert!(
        (nt.peers[&2].state == StateRole::Leader) || (nt.peers[&3].state == StateRole::Follower)
    );
}

#[test]
fn test_prevote_migration_with_free_stuck_pre_candidate() {
    let l = default_logger();
    let mut nt = new_prevote_migration_cluster(&l);

    // n1 is leader with term 2
    // n2 is follower with term 2
    // n3 is pre-candidate with term 4, and less log
    nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);

    assert_eq!(nt.peers[&1].state, StateRole::Leader);
    assert_eq!(nt.peers[&2].state, StateRole::Follower);
    assert_eq!(nt.peers[&3].state, StateRole::PreCandidate);

    // Pre-Vote again for safety
    nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);
    assert_eq!(nt.peers[&1].state, StateRole::Leader);
    assert_eq!(nt.peers[&2].state, StateRole::Follower);
    assert_eq!(nt.peers[&3].state, StateRole::PreCandidate);

    let mut to_send = new_message(1, 3, MessageType::MsgHeartbeat, 0);
    to_send.term = nt.peers[&1].term;
    nt.send(vec![to_send]);

    // Disrupt the leader so that the stuck peer is freed
    assert_eq!(nt.peers[&1].state, StateRole::Follower);

    assert_eq!(nt.peers[&3].term, nt.peers[&1].term);
}

#[test]
fn test_learner_respond_vote() -> Result<()> {
    let l = default_logger();
    let mut n1 = new_test_learner_raft(1, vec![1, 2], vec![3], 10, 1, new_storage(), &l);
    n1.become_follower(1, INVALID_ID);
    n1.reset_randomized_election_timeout();

    let mut n3 = new_test_learner_raft(3, vec![1, 2], vec![3], 10, 1, new_storage(), &l);
    n3.become_follower(1, INVALID_ID);
    n3.reset_randomized_election_timeout();

    let do_campaign = |nw: &mut Network| {
        let msg = new_message(1, 1, MessageType::MsgHup, 0);
        nw.send(vec![msg]);
    };

    let mut network = Network::new(vec![Some(n1), None, Some(n3)], &l);
    network.isolate(2);

    // Can't elect new leader because 1 won't send MsgRequestVote to 3.
    do_campaign(&mut network);
    assert_eq!(network.peers[&1].state, StateRole::Candidate);

    // After promote 3 to voter, election should success.
    network
        .peers
        .get_mut(&1)
        .unwrap()
        .apply_conf_change(&add_node(3))?;
    do_campaign(&mut network);
    assert_eq!(network.peers[&1].state, StateRole::Leader);

    Ok(())
}

#[test]
fn test_election_tick_range() {
    let l = default_logger();
    let mut cfg = new_test_config(1, 10, 1);
    let s = MemStorage::new_with_conf_state((vec![1, 2, 3], vec![]));
    let mut raft = new_test_raft_with_config(&cfg, s, &l).raft.unwrap();
    for _ in 0..1000 {
        raft.reset_randomized_election_timeout();
        let randomized_timeout = raft.randomized_election_timeout();
        assert!(
            cfg.election_tick <= randomized_timeout && randomized_timeout < 2 * cfg.election_tick
        );
    }

    cfg.min_election_tick = cfg.election_tick;
    cfg.validate().unwrap();

    // Too small election tick.
    cfg.min_election_tick = cfg.election_tick - 1;
    cfg.validate().unwrap_err();

    // max_election_tick should be larger than min_election_tick
    cfg.min_election_tick = cfg.election_tick;
    cfg.max_election_tick = cfg.election_tick;
    cfg.validate().unwrap_err();

    cfg.max_election_tick = cfg.election_tick + 1;
    raft = new_test_raft_with_config(&cfg, new_storage(), &l)
        .raft
        .unwrap();
    for _ in 0..100 {
        raft.reset_randomized_election_timeout();
        let randomized_timeout = raft.randomized_election_timeout();
        assert_eq!(randomized_timeout, cfg.election_tick);
    }
}

// TestPreVoteWithSplitVote verifies that after split vote, cluster can complete
// election in next round.
#[test]
fn test_prevote_with_split_vote() {
    let l = default_logger();
    let peers = (1..=3).map(|id| {
        let mut raft =
            new_test_raft_with_prevote(id, vec![1, 2, 3], 10, 1, new_storage(), true, &l);
        raft.become_follower(1, INVALID_ID);
        Some(raft)
    });
    let mut network = Network::new(peers.collect(), &l);
    network.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    // simulate leader down. followers start split vote.
    network.isolate(1);
    network.send(vec![
        new_message(2, 2, MessageType::MsgHup, 0),
        new_message(3, 3, MessageType::MsgHup, 0),
    ]);

    // check whether the term values are expected
    assert_eq!(network.peers[&2].term, 3, "peer 2 term",);
    assert_eq!(network.peers[&3].term, 3, "peer 3 term",);

    // check state
    assert_eq!(
        network.peers[&2].state,
        StateRole::Candidate,
        "peer 2 state",
    );
    assert_eq!(
        network.peers[&3].state,
        StateRole::Candidate,
        "peer 3 state",
    );

    // node 2 election timeout first
    network.send(vec![new_message(2, 2, MessageType::MsgHup, 0)]);

    // check whether the term values are expected
    assert_eq!(network.peers[&2].term, 4, "peer 2 term",);
    assert_eq!(network.peers[&3].term, 4, "peer 3 term",);

    // check state
    assert_eq!(network.peers[&2].state, StateRole::Leader, "peer 2 state",);
    assert_eq!(network.peers[&3].state, StateRole::Follower, "peer 3 state",);
}

// ensure that after a node become pre-candidate, it will checkQuorum correctly.
#[test]
fn test_prevote_with_check_quorum() {
    let l = default_logger();
    let bootstrap = |id| {
        let mut cfg = new_test_config(id, 10, 1);
        cfg.pre_vote = true;
        cfg.check_quorum = true;
        let s = MemStorage::new_with_conf_state((vec![1, 2, 3], vec![]));
        let mut i = new_test_raft_with_config(&cfg, s, &l);
        i.become_follower(1, INVALID_ID);
        i
    };
    let (peer1, peer2, peer3) = (bootstrap(1), bootstrap(2), bootstrap(3));

    let mut network = Network::new(vec![Some(peer1), Some(peer2), Some(peer3)], &l);
    network.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    // cause a network partition to isolate node 3. node 3 has leader info
    network.cut(1, 3);
    network.cut(2, 3);

    assert_eq!(network.peers[&1].state, StateRole::Leader, "peer 1 state",);
    assert_eq!(network.peers[&2].state, StateRole::Follower, "peer 2 state",);

    network.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);

    assert_eq!(
        network.peers[&3].state,
        StateRole::PreCandidate,
        "peer 3 state",
    );

    // term + 2, so that node 2 will ignore node 3's PreVote
    network.send(vec![new_message(2, 1, MessageType::MsgTransferLeader, 0)]);
    network.send(vec![new_message(1, 2, MessageType::MsgTransferLeader, 0)]);

    // check whether the term values are expected
    assert_eq!(network.peers[&1].term, 4, "peer 1 term",);
    assert_eq!(network.peers[&2].term, 4, "peer 2 term",);
    assert_eq!(network.peers[&3].term, 2, "peer 3 term",);

    // check state
    assert_eq!(network.peers[&1].state, StateRole::Leader, "peer 1 state",);
    assert_eq!(network.peers[&2].state, StateRole::Follower, "peer 2 state",);
    assert_eq!(
        network.peers[&3].state,
        StateRole::PreCandidate,
        "peer 3 state",
    );

    // recover the network then immediately isolate node 1 which is currently
    // the leader, this is to emulate the crash of node 1.
    network.recover();
    network.cut(1, 2);
    network.cut(1, 3);

    // call for election. node 3 shouldn't ignore node 2's PreVote
    let timeout = network.peers[&3].randomized_election_timeout();
    for _ in 0..timeout {
        network.peers.get_mut(&3).unwrap().tick();
    }
    network.send(vec![new_message(2, 2, MessageType::MsgHup, 0)]);

    // check state
    assert_eq!(network.peers[&2].state, StateRole::Leader, "peer 2 state",);
    assert_eq!(network.peers[&3].state, StateRole::Follower, "peer 3 state",);
}

// ensure a new Raft returns a Error::ConfigInvalid with an invalid config
#[test]
fn test_new_raft_with_bad_config_errors() {
    let invalid_config = new_test_config(INVALID_ID, 1, 1);
    let s = MemStorage::new_with_conf_state((vec![1, 2], vec![]));
    let raft = Raft::new(&invalid_config, s, &default_logger());
    assert!(raft.is_err())
}

// tests whether MsgAppend are batched
#[test]
fn test_batch_msg_append() {
    let l = default_logger();
    let storage = new_storage();
    let mut raft = new_test_raft(1, vec![1, 2, 3], 10, 1, storage.clone(), &l);
    raft.become_candidate();
    raft.become_leader();
    raft.set_batch_append(true);
    commit_noop_entry(&mut raft, &storage);
    for _ in 0..10 {
        let prop_msg = new_message(1, 1, MessageType::MsgPropose, 1);
        raft.step(prop_msg).unwrap();
    }
    assert_eq!(raft.msgs.len(), 2);
    for msg in &raft.msgs {
        assert_eq!(msg.entries.len(), 10);
        assert_eq!(msg.index, 1);
    }
    // if the append entry is not continuous, raft should not batch the RPC
    let mut reject_msg = new_message(2, 1, MessageType::MsgAppendResponse, 0);
    reject_msg.reject = true;
    reject_msg.index = 2;
    raft.step(reject_msg).unwrap();
    assert_eq!(raft.msgs.len(), 3);
}

/// Tests if unapplied conf change is checked before campaign.
#[test]
fn test_conf_change_check_before_campaign() {
    let l = default_logger();
    let mut nt = Network::new(vec![None, None, None], &l);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
    assert_eq!(nt.peers[&1].state, StateRole::Leader);

    let mut m = new_message(1, 1, MessageType::MsgPropose, 0);
    let mut e = Entry::default();
    e.set_entry_type(EntryType::EntryConfChange);
    let mut cc = ConfChange::default();
    cc.set_change_type(ConfChangeType::RemoveNode);
    cc.node_id = 3;
    e.data = protobuf::Message::write_to_bytes(&cc).unwrap().into();
    m.mut_entries().push(e);
    nt.send(vec![m]);

    // trigger campaign in node 2
    nt.peers
        .get_mut(&2)
        .unwrap()
        .reset_randomized_election_timeout();
    let timeout = nt.peers[&2].randomized_election_timeout();
    for _ in 0..timeout {
        nt.peers.get_mut(&2).unwrap().tick();
    }
    // It's still follower because committed conf change is not applied.
    assert_eq!(nt.peers[&2].state, StateRole::Follower);

    // Transfer leadership to peer 2.
    nt.send(vec![new_message(2, 1, MessageType::MsgTransferLeader, 0)]);
    assert_eq!(nt.peers[&1].state, StateRole::Leader);
    // It's still follower because committed conf change is not applied.
    assert_eq!(nt.peers[&2].state, StateRole::Follower);
    // Abort transfer leader.
    nt.peers.get_mut(&1).unwrap().abort_leader_transfer();

    let committed = nt.peers[&2].raft_log.committed;
    nt.peers.get_mut(&2).unwrap().commit_apply(committed);
    nt.peers
        .get_mut(&2)
        .unwrap()
        .apply_conf_change(&remove_node(3))
        .unwrap();

    // transfer leadership to peer 2 again.
    nt.send(vec![new_message(2, 1, MessageType::MsgTransferLeader, 0)]);
    assert_eq!(nt.peers[&1].state, StateRole::Follower);
    assert_eq!(nt.peers[&2].state, StateRole::Leader);

    nt.peers.get_mut(&1).unwrap().commit_apply(committed);
    nt.peers
        .get_mut(&1)
        .unwrap()
        .apply_conf_change(&remove_node(3))
        .unwrap();

    // trigger campaign in node 1
    nt.peers
        .get_mut(&1)
        .unwrap()
        .reset_randomized_election_timeout();
    let timeout = nt.peers[&1].randomized_election_timeout();
    for _ in 0..timeout {
        nt.peers.get_mut(&1).unwrap().tick();
    }
    assert_eq!(nt.peers[&1].state, StateRole::Candidate);
}

fn test_advance_commit_index_by_vote_request(use_prevote: bool) {
    let l = default_logger();
    let mut cases: Vec<Box<dyn ConfChangeI>> = vec![
        Box::new(conf_change(ConfChangeType::AddNode, 4)),
        Box::new(conf_change_v2(vec![
            new_conf_change_single(3, ConfChangeType::AddLearnerNode),
            new_conf_change_single(4, ConfChangeType::AddNode),
        ])),
    ];
    for (i, cc) in cases.drain(..).enumerate() {
        let peers = (1..=4)
            .map(|id| {
                Some(new_test_learner_raft_with_prevote(
                    id,
                    vec![1, 2, 3],
                    vec![4],
                    &l,
                    use_prevote,
                ))
            })
            .collect();
        let mut nt = Network::new(peers, &l);
        nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
        let mut e = Entry::default();
        if let Some(v1) = cc.as_v1() {
            e.set_entry_type(EntryType::EntryConfChange);
            e.set_data(v1.write_to_bytes().unwrap().into());
        } else {
            e.set_entry_type(EntryType::EntryConfChangeV2);
            e.set_data(cc.as_v2().write_to_bytes().unwrap().into());
        }

        // propose a confchange entry but don't let it commit
        nt.ignore(MessageType::MsgAppendResponse);
        nt.send(vec![new_message_with_entries(
            1,
            1,
            MessageType::MsgPropose,
            vec![e],
        )]);
        let cc_index = nt.peers[&1].raft_log.last_index();

        // let node 4 have more up to data log than other voter
        nt.recover();
        nt.cut(1, 2);
        nt.cut(1, 3);
        nt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);

        // let the confchange entry commit but don't let node 4 know
        nt.recover();
        nt.cut(1, 4);
        nt.ignore(MessageType::MsgAppend);
        let mut msg = new_message(2, 1, MessageType::MsgAppendResponse, 0);
        msg.set_index(nt.peers[&2].raft_log.last_index());
        nt.send(vec![msg, new_message(1, 1, MessageType::MsgBeat, 0)]);

        // simulate the leader down
        nt.recover();
        nt.isolate(1);

        let p4 = nt.peers.get_mut(&4).unwrap();
        if p4.raft_log.committed >= cc_index {
            panic!(
                "#{} expected node 4 commit index less than {}, got {}",
                i, cc_index, p4.raft_log.committed
            );
        }
        // node 4 can't start new election because it thinks itself is a learner
        for _ in 0..p4.randomized_election_timeout() {
            p4.tick();
        }
        if p4.state != StateRole::Follower {
            panic!("#{} node 4 state: {:?}, want Follower", i, p4.state);
        }
        let p2 = nt.peers.get_mut(&2).unwrap();
        if p2.raft_log.committed < cc_index {
            panic!(
                "#{} expected node 2 commit index not less than {}, got {}",
                i, cc_index, p2.raft_log.committed
            );
        }
        p2.apply_conf_change(&cc.as_v2()).unwrap();
        p2.commit_apply(cc_index);

        // node 2 needs votes from both node 3 and node 4, but node 4 will reject it
        for _ in 0..p2.randomized_election_timeout() {
            p2.tick();
        }
        let want = if use_prevote {
            StateRole::PreCandidate
        } else {
            StateRole::Candidate
        };
        if p2.state != want {
            panic!("#{} node 2 state: {:?}, want {:?}", i, p2.state, want);
        }
        let msgs = nt.read_messages();
        nt.filter_and_send(msgs);
        if nt.peers[&2].state == StateRole::Leader {
            panic!("#{} node 2 can't campaign successfully.", i);
        }

        // node 4's commit index should be advanced by node 2's vote request
        let p4 = nt.peers.get_mut(&4).unwrap();
        if p4.raft_log.committed < cc_index {
            panic!(
                "#{} expected node 4 commit index not less than {}, got {}",
                i, cc_index, p4.raft_log.committed
            );
        }
        p4.apply_conf_change(&cc.as_v2()).unwrap();
        p4.commit_apply(cc_index);

        // now node 4 can start new election and become leader
        for _ in 0..p4.randomized_election_timeout() {
            p4.tick();
        }
        let msgs = nt.read_messages();
        nt.filter_and_send(msgs);
        if nt.peers[&4].state != StateRole::Leader {
            panic!("#{} node 4 state: {:?} want Leader", i, nt.peers[&4].state);
        }
    }
}

/// Tests the commit index can be advanced by direct vote request
#[test]
fn test_advance_commit_index_by_direct_vote_request() {
    test_advance_commit_index_by_vote_request(false)
}

/// Tests the commit index can be advanced by prevote request
#[test]
fn test_advance_commit_index_by_prevote_request() {
    test_advance_commit_index_by_vote_request(true)
}

fn test_advance_commit_index_by_vote_response(use_prevote: bool) {
    let l = default_logger();
    let mut cases: Vec<Box<dyn ConfChangeI>> = vec![
        Box::new(conf_change(ConfChangeType::RemoveNode, 4)),
        // Explicit leave joint
        Box::new(conf_change_v2(vec![])),
    ];
    // Enter joint confchange
    let mut enter_joint = conf_change_v2(vec![
        new_conf_change_single(3, ConfChangeType::AddNode),
        new_conf_change_single(4, ConfChangeType::AddLearnerNode),
    ]);
    enter_joint.set_transition(ConfChangeTransition::Explicit);
    for (i, cc) in cases.drain(..).enumerate() {
        let peers = (1..=4)
            .map(|id| {
                Some(new_test_raft_with_prevote(
                    id,
                    vec![1, 2, 3, 4],
                    10,
                    1,
                    new_storage(),
                    use_prevote,
                    &l,
                ))
            })
            .collect();
        let mut nt = Network::new(peers, &l);

        // Joint confchange, let's enter joint first
        if cc.as_v1().is_none() {
            for p in nt.peers.values_mut() {
                p.apply_conf_change(&enter_joint).unwrap();
            }
        }

        nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

        let mut e = Entry::default();
        if let Some(v1) = cc.as_v1() {
            e.set_entry_type(EntryType::EntryConfChange);
            e.set_data(v1.write_to_bytes().unwrap().into());
        } else {
            e.set_entry_type(EntryType::EntryConfChangeV2);
            e.set_data(cc.as_v2().write_to_bytes().unwrap().into());
        }

        // propose a confchange entry but don't let it commit
        nt.ignore(MessageType::MsgAppendResponse);
        nt.send(vec![new_message_with_entries(
            1,
            1,
            MessageType::MsgPropose,
            vec![e],
        )]);
        let cc_index = nt.peers[&1].raft_log.last_index();

        // let node 4 have more up to data log than other voter
        nt.recover();
        nt.cut(1, 2);
        nt.cut(1, 3);
        nt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);

        // A delayed MsgAppResp message make the confchange entry become committed
        let mut msg = new_message(2, 1, MessageType::MsgAppendResponse, 0);
        msg.set_index(nt.peers[&2].raft_log.last_index());
        nt.send(vec![msg, new_message(1, 1, MessageType::MsgBeat, 0)]);

        // simulate the leader down
        nt.recover();
        nt.isolate(1);

        let p4 = nt.peers.get_mut(&4).unwrap();
        if p4.raft_log.committed < cc_index {
            panic!(
                "#{} expected node 4 commit index larger than {}, got {}",
                i, cc_index, p4.raft_log.committed
            );
        }
        p4.apply_conf_change(&cc.as_v2()).unwrap();
        p4.commit_apply(cc_index);
        // node 4 can't start new election because it thinks itself is a learner
        for _ in 0..p4.randomized_election_timeout() {
            p4.tick();
        }
        if p4.state != StateRole::Follower {
            panic!("#{} node 4 state: {:?}, want Follower", i, p4.state);
        }
        let p2 = nt.peers.get_mut(&2).unwrap();
        if p2.raft_log.committed >= cc_index {
            panic!(
                "#{} expected node 2 commit index less than {}, got {}",
                i, cc_index, p2.raft_log.committed
            );
        }

        // node 2 needs votes from both node 3 and node 4, but node 4 will reject it
        for _ in 0..p2.randomized_election_timeout() {
            p2.tick();
        }
        let want = if use_prevote {
            StateRole::PreCandidate
        } else {
            StateRole::Candidate
        };
        if p2.state != want {
            panic!("#{} node 2 state: {:?}, want {:?}", i, p2.state, want);
        }
        let msgs = nt.read_messages();
        nt.filter_and_send(msgs);
        let p2 = nt.peers.get_mut(&2).unwrap();
        if p2.state != StateRole::Follower {
            panic!(
                "#{} node 2 should become follower by vote response, but got {:?}",
                i, p2.state
            );
        }

        // node 2's commit index should be advanced by vote response
        if p2.raft_log.committed < cc_index {
            panic!(
                "#{} expected node 2 commit index less than {}, got {}",
                i, cc_index, p2.raft_log.committed
            );
        }
        p2.apply_conf_change(&cc.as_v2()).unwrap();
        p2.commit_apply(cc_index);

        // now node 2 only need vote from node 3
        for _ in 0..p2.randomized_election_timeout() {
            p2.tick();
        }
        let msgs = nt.read_messages();
        nt.filter_and_send(msgs);
        if nt.peers[&2].state != StateRole::Leader {
            panic!("#{} node 2 state: {:?} want Leader", i, nt.peers[&2].state);
        }
    }
}

// Tests the commit index can be forwarded by direct vote response
#[test]
fn test_advance_commit_index_by_direct_vote_response() {
    test_advance_commit_index_by_vote_response(false)
}

// Tests the commit index can be forwarded by prevote response
#[test]
fn test_advance_commit_index_by_prevote_response() {
    test_advance_commit_index_by_vote_response(true)
}

fn prepare_request_snapshot() -> (Network, Snapshot) {
    let l = default_logger();

    fn index_term_11(id: u64, ids: Vec<u64>, l: &Logger) -> Interface {
        let store = MemStorage::new();
        store
            .wl()
            .apply_snapshot(new_snapshot(11, 11, ids.clone()))
            .unwrap();
        let mut raft = new_test_raft(id, ids, 5, 1, store, l);
        raft.reset(11);
        raft
    }

    let mut nt = Network::new(
        vec![
            Some(index_term_11(1, vec![1, 2, 3], &l)),
            Some(index_term_11(2, vec![1, 2, 3], &l)),
            Some(index_term_11(3, vec![1, 2, 3], &l)),
        ],
        &l,
    );

    // elect r1 as leader
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    let mut test_entries = Entry::default();
    test_entries.data = (b"testdata" as &'static [u8]).into();
    let msg = new_message_with_entries(1, 1, MessageType::MsgPropose, vec![test_entries]);
    nt.send(vec![msg.clone(), msg]);
    assert_eq!(nt.peers[&1].raft_log.committed, 14);
    assert_eq!(nt.peers[&2].raft_log.committed, 14);

    let ents = nt
        .peers
        .get_mut(&1)
        .unwrap()
        .raft_log
        .unstable_entries()
        .to_vec();
    nt.storage[&1].wl().append(&ents).unwrap();
    nt.storage[&1].wl().commit_to(14).unwrap();
    nt.peers.get_mut(&1).unwrap().raft_log.applied = 14;

    // Commit a new raft log.
    let mut test_entries = Entry::default();
    test_entries.data = (b"testdata" as &'static [u8]).into();
    let msg = new_message_with_entries(1, 1, MessageType::MsgPropose, vec![test_entries]);
    nt.send(vec![msg]);

    let s = nt.storage[&1].snapshot(0, 0).unwrap();
    (nt, s)
}

// Test if an up-to-date follower can request a snapshot from leader.
#[test]
fn test_follower_request_snapshot() {
    let (mut nt, s) = prepare_request_snapshot();

    // Request the latest snapshot.
    let prev_snapshot_idx = s.get_metadata().index;
    let request_idx = nt.peers[&1].raft_log.committed;
    assert!(prev_snapshot_idx < request_idx);
    nt.peers.get_mut(&2).unwrap().request_snapshot().unwrap();

    // Send the request snapshot message.
    let req_snap = nt.peers.get_mut(&2).unwrap().msgs.pop().unwrap();
    assert!(
        req_snap.get_msg_type() == MessageType::MsgAppendResponse
            && req_snap.reject
            && req_snap.request_snapshot == request_idx,
        "{:?}",
        req_snap
    );
    nt.peers.get_mut(&1).unwrap().step(req_snap).unwrap();

    // New proposes can not be replicated to peer 2.
    let mut test_entries = Entry::default();
    test_entries.data = (b"testdata" as &'static [u8]).into();
    let msg = new_message_with_entries(1, 1, MessageType::MsgPropose, vec![test_entries]);
    nt.send(vec![msg.clone()]);
    assert_eq!(nt.peers[&1].raft_log.committed, 16);
    assert_eq!(
        nt.peers[&1].prs().get(2).unwrap().state,
        ProgressState::Snapshot
    );
    assert_eq!(nt.peers[&2].raft_log.committed, 15);

    // Util snapshot success or fail.
    let report_ok = new_message(2, 1, MessageType::MsgSnapStatus, 0);
    nt.send(vec![report_ok]);
    let hb_resp = new_message(2, 1, MessageType::MsgHeartbeatResponse, 0);
    nt.send(vec![hb_resp]);
    nt.send(vec![msg]);

    assert_eq!(nt.peers[&1].raft_log.committed, 17);
    assert_eq!(nt.peers[&2].raft_log.committed, 17);
}

// Test if request snapshot can make progress when it meets SnapshotTemporarilyUnavailable.
#[test]
fn test_request_snapshot_unavailable() {
    let (mut nt, s) = prepare_request_snapshot();

    // Request the latest snapshot.
    let prev_snapshot_idx = s.get_metadata().index;
    let request_idx = nt.peers[&1].raft_log.committed;
    assert!(prev_snapshot_idx < request_idx);
    nt.peers.get_mut(&2).unwrap().request_snapshot().unwrap();

    // Send the request snapshot message.
    let req_snap = nt.peers.get_mut(&2).unwrap().msgs.pop().unwrap();
    assert!(
        req_snap.get_msg_type() == MessageType::MsgAppendResponse
            && req_snap.reject
            && req_snap.request_snapshot == request_idx,
        "{:?}",
        req_snap
    );

    // Peer 2 is still in probe state due to SnapshotTemporarilyUnavailable.
    nt.peers[&1].store().wl().trigger_snap_unavailable();
    nt.peers
        .get_mut(&1)
        .unwrap()
        .step(req_snap.clone())
        .unwrap();
    assert_eq!(
        nt.peers[&1].prs().get(2).unwrap().state,
        ProgressState::Probe
    );

    // Next index is decreased.
    nt.peers[&1].store().wl().trigger_snap_unavailable();
    nt.peers
        .get_mut(&1)
        .unwrap()
        .step(req_snap.clone())
        .unwrap();
    assert_eq!(
        nt.peers[&1].prs().get(2).unwrap().state,
        ProgressState::Probe
    );

    // Snapshot will be available if it requests again. This message must not
    // be considered stale even if `reject != next - 1`
    nt.peers.get_mut(&1).unwrap().step(req_snap).unwrap();
    assert_eq!(
        nt.peers[&1].prs().get(2).unwrap().state,
        ProgressState::Snapshot
    );
}

// Test if request snapshot can make progress when matched is advanced.
#[test]
fn test_request_snapshot_matched_change() {
    let (mut nt, _) = prepare_request_snapshot();
    // Let matched be greater than the committed.
    nt.peers.get_mut(&2).unwrap().raft_log.committed -= 1;

    // Request the latest snapshot.
    nt.peers.get_mut(&2).unwrap().request_snapshot().unwrap();
    let req_snap = nt.peers.get_mut(&2).unwrap().msgs.pop().unwrap();
    // The request snapshot is ignored because it is considered as out of order.
    nt.peers.get_mut(&1).unwrap().step(req_snap).unwrap();
    assert_eq!(
        nt.peers[&1].prs().get(2).unwrap().state,
        ProgressState::Replicate
    );

    // Heartbeat is responded with a request snapshot message.
    for _ in 0..nt.peers[&1].heartbeat_timeout() {
        nt.peers.get_mut(&1).unwrap().tick();
    }
    let msg_hb = nt
        .peers
        .get_mut(&1)
        .unwrap()
        .msgs
        .iter()
        .filter(|m| m.to == 2)
        .collect::<Vec<_>>()[0]
        .clone();
    nt.peers.get_mut(&2).unwrap().step(msg_hb).unwrap();
    let req_snap = nt.peers.get_mut(&2).unwrap().msgs.pop().unwrap();
    nt.peers.get_mut(&1).unwrap().step(req_snap).unwrap();
    assert_eq!(
        nt.peers[&1].prs().get(2).unwrap().state,
        ProgressState::Snapshot
    );
}

// Test if request snapshot can make progress when the peer is not Replicate.
#[test]
fn test_request_snapshot_none_replicate() {
    let (mut nt, _) = prepare_request_snapshot();
    nt.peers
        .get_mut(&1)
        .unwrap()
        .mut_prs()
        .get_mut(2)
        .unwrap()
        .state = ProgressState::Probe;

    // Request the latest snapshot.
    nt.peers.get_mut(&2).unwrap().request_snapshot().unwrap();
    let req_snap = nt.peers.get_mut(&2).unwrap().msgs.pop().unwrap();
    nt.peers.get_mut(&1).unwrap().step(req_snap).unwrap();
    assert!(nt.peers[&1].prs().get(2).unwrap().pending_request_snapshot != 0);
}

// Test if request snapshot can make progress when leader steps down.
#[test]
fn test_request_snapshot_step_down() {
    let (mut nt, _) = prepare_request_snapshot();

    // Commit a new entry and leader steps down while peer 2 is isolated.
    nt.isolate(2);
    let mut test_entries = Entry::default();
    test_entries.data = (b"testdata" as &'static [u8]).into();
    let msg = new_message_with_entries(1, 1, MessageType::MsgPropose, vec![test_entries]);
    nt.send(vec![msg]);
    nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);
    assert_eq!(nt.peers[&3].state, StateRole::Leader);

    // Recover and request the latest snapshot.
    nt.recover();
    nt.peers.get_mut(&2).unwrap().request_snapshot().unwrap();
    nt.send(vec![new_message(3, 3, MessageType::MsgBeat, 0)]);
    assert!(
        nt.peers[&2].pending_request_snapshot == INVALID_INDEX,
        "{}",
        nt.peers[&2].pending_request_snapshot
    );
}

// Abort request snapshot if it becomes leader or candidate.
#[test]
fn test_request_snapshot_on_role_change() {
    let (mut nt, _) = prepare_request_snapshot();

    nt.peers.get_mut(&2).unwrap().request_snapshot().unwrap();

    // Becoming follower does not reset pending_request_snapshot.
    let (term, id) = (nt.peers[&1].term, nt.peers[&1].id);
    nt.peers.get_mut(&2).unwrap().become_follower(term, id);
    assert!(
        nt.peers[&2].pending_request_snapshot != INVALID_INDEX,
        "{}",
        nt.peers[&2].pending_request_snapshot
    );

    // Becoming candidate resets pending_request_snapshot.
    nt.peers.get_mut(&2).unwrap().become_candidate();
    assert!(
        nt.peers[&2].pending_request_snapshot == INVALID_INDEX,
        "{}",
        nt.peers[&2].pending_request_snapshot
    );
}

// Abort request snapshot if term change.
#[test]
fn test_request_snapshot_after_term_change() {
    let (mut nt, _) = prepare_request_snapshot();

    nt.peers.get_mut(&2).unwrap().request_snapshot().unwrap();

    assert!(
        nt.peers[&2].pending_request_snapshot != INVALID_INDEX,
        "{}",
        nt.peers[&2].pending_request_snapshot
    );

    let term = nt.peers[&1].term;
    nt.peers.get_mut(&2).unwrap().reset(term + 1);
    assert!(
        nt.peers[&2].pending_request_snapshot == INVALID_INDEX,
        "{}",
        nt.peers[&2].pending_request_snapshot
    );
}

/// Tests group commit.
///
/// 1. Logs should be replicated to at least different groups before committed;
/// 2. all peers are configured to the same group, simple quorum should be used.
#[test]
fn test_group_commit() {
    let l = default_logger();
    let mut tests = vec![
        // Single
        (vec![1], vec![0], 1, 1),
        (vec![1], vec![1], 1, 1),
        // Odd
        (vec![2, 2, 1], vec![1, 2, 1], 2, 2),
        (vec![2, 2, 1], vec![1, 1, 2], 1, 2),
        (vec![2, 2, 1], vec![1, 0, 1], 1, 2),
        (vec![2, 2, 1], vec![0, 0, 0], 1, 2),
        // Even
        (vec![4, 2, 1, 3], vec![0, 0, 0, 0], 1, 2),
        (vec![4, 2, 1, 3], vec![1, 0, 0, 0], 1, 2),
        (vec![4, 2, 1, 3], vec![0, 1, 0, 2], 2, 2),
        (vec![4, 2, 1, 3], vec![0, 2, 1, 0], 1, 2),
        (vec![4, 2, 1, 3], vec![1, 1, 1, 1], 2, 2),
        (vec![4, 2, 1, 3], vec![1, 1, 2, 1], 1, 2),
        (vec![4, 2, 1, 3], vec![1, 2, 1, 1], 2, 2),
        (vec![4, 2, 1, 3], vec![4, 3, 2, 1], 2, 2),
    ];

    for (i, (matches, group_ids, g_w, q_w)) in tests.drain(..).enumerate() {
        let store = MemStorage::new_with_conf_state((vec![1], vec![]));
        let min_index = *matches.iter().min().unwrap();
        let max_index = *matches.iter().max().unwrap();
        let logs: Vec<_> = (min_index..=max_index).map(|i| empty_entry(1, i)).collect();
        store.wl().append(&logs).unwrap();
        let mut hs = HardState::default();
        hs.term = 1;
        store.wl().set_hardstate(hs);
        let cfg = new_test_config(1, 5, 1);
        let mut sm = new_test_raft_with_config(&cfg, store, &l);

        let mut groups = vec![];
        for (j, (m, g)) in matches.into_iter().zip(group_ids).enumerate() {
            let id = j as u64 + 1;
            if sm.mut_prs().get(id).is_none() {
                sm.apply_conf_change(&add_node(id)).unwrap();
                let pr = sm.mut_prs().get_mut(id).unwrap();
                pr.matched = m;
                pr.next_idx = m + 1;
            }
            if g != 0 {
                groups.push((id, g));
            }
        }
        sm.enable_group_commit(true);
        sm.assign_commit_groups(&groups);
        if sm.raft_log.committed != 0 {
            panic!(
                "#{}: follower group committed {}, want 0",
                i, sm.raft_log.committed
            );
        }
        sm.state = StateRole::Leader;
        sm.assign_commit_groups(&groups);
        if sm.raft_log.committed != g_w {
            panic!(
                "#{}: leader group committed {}, want {}",
                i, sm.raft_log.committed, g_w
            );
        }
        sm.enable_group_commit(false);
        if sm.raft_log.committed != q_w {
            panic!(
                "#{}: quorum committed {}, want {}",
                i, sm.raft_log.committed, q_w
            );
        }
    }
}

#[test]
fn test_group_commit_consistent() {
    let l = default_logger();
    let mut logs = vec![];
    for i in 1..6 {
        logs.push(empty_entry(1, i));
    }
    for i in 6..=8 {
        logs.push(empty_entry(2, i));
    }
    let mut tests = vec![
        // Single node is not using group commit
        (vec![8], vec![0], 8, 6, StateRole::Leader, Some(false)),
        (vec![8], vec![1], 8, 5, StateRole::Leader, None),
        (vec![8], vec![1], 8, 6, StateRole::Follower, None),
        // Not commit to current term should return None, as old leader may
        // have reach consistent.
        (vec![8, 2, 0], vec![1, 2, 1], 2, 2, StateRole::Leader, None),
        (
            vec![8, 2, 6],
            vec![1, 1, 2],
            6,
            6,
            StateRole::Leader,
            Some(true),
        ),
        // Not apply to current term should return None, as there maybe pending conf change.
        (vec![8, 2, 6], vec![1, 1, 2], 6, 5, StateRole::Leader, None),
        // It should be false when not using group commit.
        (
            vec![8, 6, 6],
            vec![0, 0, 0],
            6,
            6,
            StateRole::Leader,
            Some(false),
        ),
        // It should be false when there is only one group.
        (
            vec![8, 6, 6],
            vec![1, 1, 1],
            6,
            6,
            StateRole::Leader,
            Some(false),
        ),
        (
            vec![8, 6, 6],
            vec![1, 1, 0],
            6,
            6,
            StateRole::Leader,
            Some(false),
        ),
        // Only leader knows what's the current state.
        (
            vec![8, 2, 6],
            vec![1, 1, 2],
            6,
            6,
            StateRole::Follower,
            None,
        ),
        (
            vec![8, 2, 6],
            vec![1, 1, 2],
            6,
            6,
            StateRole::Candidate,
            None,
        ),
        (
            vec![8, 2, 6],
            vec![1, 1, 2],
            6,
            6,
            StateRole::PreCandidate,
            None,
        ),
    ];

    for (i, (matches, group_ids, committed, applied, role, exp)) in tests.drain(..).enumerate() {
        let store = MemStorage::new_with_conf_state((vec![1], vec![]));
        store.wl().append(&logs).unwrap();
        let mut hs = HardState::default();
        hs.term = 2;
        hs.commit = committed;
        store.wl().set_hardstate(hs);
        let mut cfg = new_test_config(1, 5, 1);
        cfg.applied = applied;
        let mut sm = new_test_raft_with_config(&cfg, store, &l);
        sm.state = role;

        let mut groups = vec![];
        for (j, (m, g)) in matches.into_iter().zip(group_ids).enumerate() {
            let id = j as u64 + 1;
            if sm.mut_prs().get(id).is_none() {
                sm.apply_conf_change(&add_node(id)).unwrap();
                let pr = sm.mut_prs().get_mut(id).unwrap();
                pr.matched = m;
                pr.next_idx = m + 1;
            }
            if g != 0 {
                groups.push((id, g));
            }
        }
        sm.assign_commit_groups(&groups);
        if Some(true) == exp {
            let is_consistent = sm.check_group_commit_consistent();
            if is_consistent != Some(false) {
                panic!(
                    "#{}: consistency = {:?}, want Some(false)",
                    i, is_consistent
                );
            }
        }
        sm.enable_group_commit(true);
        let is_consistent = sm.check_group_commit_consistent();
        if is_consistent != exp {
            panic!("#{}: consistency = {:?}, want {:?}", i, is_consistent, exp);
        }
    }
}

/// test_election_with_priority_log verifies the correctness
/// of the election with both priority and log.
#[test]
fn test_election_with_priority_log() {
    let tests = [
        // log is up to date or not 1..3, priority 1..3, id, state
        (true, false, false, 3, 1, 1, 1, StateRole::Leader),
        (true, false, false, 2, 2, 2, 1, StateRole::Leader),
        (true, false, false, 1, 3, 3, 1, StateRole::Leader),
        (true, true, true, 3, 1, 1, 1, StateRole::Leader),
        (true, true, true, 2, 2, 2, 1, StateRole::Leader),
        (true, true, true, 1, 3, 3, 1, StateRole::Follower),
        (false, true, true, 3, 1, 1, 1, StateRole::Follower),
        (false, true, true, 2, 2, 2, 1, StateRole::Follower),
        (false, true, true, 1, 3, 3, 1, StateRole::Follower),
        (false, false, true, 1, 3, 1, 1, StateRole::Follower),
        (false, false, true, 1, 1, 3, 1, StateRole::Leader),
    ];

    for (l1, l2, l3, p1, p2, p3, id, state) in tests {
        let l = default_logger();
        let mut n1 = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage(), &l);
        let mut n2 = new_test_raft(2, vec![1, 2, 3], 10, 1, new_storage(), &l);
        let mut n3 = new_test_raft(3, vec![1, 2, 3], 10, 1, new_storage(), &l);
        n1.set_priority(p1);
        n2.set_priority(p2);
        n3.set_priority(p3);
        let entries = vec![new_entry(1, 1, SOME_DATA), new_entry(1, 1, SOME_DATA)];
        if l1 {
            n1.raft_log.append(&entries);
        }
        if l2 {
            n2.raft_log.append(&entries);
        }
        if l3 {
            n3.raft_log.append(&entries);
        }

        let mut network = Network::new(vec![Some(n1), Some(n2), Some(n3)], &l);

        network.send(vec![new_message(id, id, MessageType::MsgHup, 0)]);

        assert_eq!(network.peers[&id].state, state);
    }
}

/// test_election_after_change_priority verifies that a peer can win an election
/// by raising its priority and lose election by lowering its priority.
#[test]
fn test_election_after_change_priority() {
    let l = default_logger();
    let mut n1 = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage(), &l);
    let mut n2 = new_test_raft(2, vec![1, 2, 3], 10, 1, new_storage(), &l);
    let mut n3 = new_test_raft(3, vec![1, 2, 3], 10, 1, new_storage(), &l);
    // priority of n1 is 0 in default.
    n2.set_priority(2);
    n3.set_priority(3);
    n1.become_follower(1, INVALID_ID);
    n2.become_follower(1, INVALID_ID);
    n3.become_follower(1, INVALID_ID);
    let mut network = Network::new(vec![Some(n1), Some(n2), Some(n3)], &l);

    assert_eq!(network.peers[&1].priority, 0, "peer 1 priority");
    network.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
    // check state
    assert_eq!(network.peers[&1].state, StateRole::Follower, "peer 1 state");

    let tests = [
        (1, 1, StateRole::Follower), //id, priority, state
        (1, 2, StateRole::Leader),
        (1, 3, StateRole::Leader),
        (1, 0, StateRole::Follower),
    ];

    for (i, &(id, p, state)) in tests.iter().enumerate() {
        network
            .peers
            .get_mut(&id)
            .unwrap()
            .become_follower((i + 2) as u64, INVALID_ID);
        network.peers.get_mut(&id).unwrap().set_priority(p);
        network.send(vec![new_message(id, id, MessageType::MsgHup, 0)]);

        // check state
        assert_eq!(network.peers[&id].state, state, "peer {} state", id);
    }
}

// `test_read_when_quorum_becomes_less` tests read requests could be handled earlier
// if quorum becomes less in configuration changes.
#[test]
fn test_read_when_quorum_becomes_less() {
    let l = default_logger();
    let mut network = Network::new(vec![None, None], &l);

    let mut m = Message::default();
    m.from = 1;
    m.to = 1;
    m.set_msg_type(MessageType::MsgHup);
    network.send(vec![m]);
    assert_eq!(network.peers[&1].raft_log.committed, 1);

    // Read index on the peer.
    let mut m = Message::default();
    m.to = 1;
    m.set_msg_type(MessageType::MsgReadIndex);
    let mut e = Entry::default();
    e.data = (b"abcdefg" as &'static [u8]).into();
    m.set_entries(vec![e].into());
    network.dispatch(vec![m]).unwrap();

    // Broadcast heartbeats.
    let heartbeats = network.read_messages();
    network.dispatch(heartbeats).unwrap();

    // Drop heartbeat response from peer 2.
    let heartbeat_responses = network.read_messages();
    assert_eq!(heartbeat_responses.len(), 1);

    network
        .peers
        .get_mut(&1)
        .unwrap()
        .apply_conf_change(&remove_node(2))
        .unwrap();
    assert!(!network.peers[&1].read_states.is_empty());
}

#[test]
fn test_uncommitted_entries_size_limit() {
    let l = default_logger();
    let config = &Config {
        id: 1,
        max_uncommitted_size: 12,
        ..Config::default()
    };
    let mut nt = Network::new_with_config(vec![None, None, None], config, &l);
    let data = b"hello world!".to_vec();
    let mut entry = Entry::default();
    entry.data = data.to_vec().into();
    let msg = new_message_with_entries(1, 1, MessageType::MsgPropose, vec![entry]);

    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    // should return ok
    nt.dispatch([msg.clone()]).unwrap();

    // then next proposal should be dropped
    let result = nt.dispatch([msg]);
    assert_eq!(result.unwrap_err(), raft::Error::ProposalDropped);

    // but entry with empty size should be accepted
    let entry = Entry::default();
    let empty_msg = new_message_with_entries(1, 1, MessageType::MsgPropose, vec![entry]);
    nt.dispatch([empty_msg]).unwrap();

    // after reduce, new proposal should be accepted
    let mut entry = Entry::default();
    entry.data = data.into();
    entry.index = 3;
    nt.peers
        .get_mut(&1)
        .unwrap()
        .reduce_uncommitted_size(&[entry]);
    assert_eq!(nt.peers.get_mut(&1).unwrap().uncommitted_size(), 0);

    // a huge proposal should be accepted when there is no uncommitted entry,
    // even it's bigger than max_uncommitted_size
    let mut entry = Entry::default();
    entry.data = (b"hello world and raft" as &'static [u8]).into();
    let long_msg = new_message_with_entries(1, 1, MessageType::MsgPropose, vec![entry]);
    nt.dispatch([long_msg]).unwrap();

    // but another huge one will be dropped
    let mut entry = Entry::default();
    entry.data = (b"hello world and raft" as &'static [u8]).into();
    let long_msg = new_message_with_entries(1, 1, MessageType::MsgPropose, vec![entry]);
    nt.dispatch([long_msg]).unwrap_err();

    // entry with empty size should still be accepted
    let entry = Entry::default();
    let empty_msg = new_message_with_entries(1, 1, MessageType::MsgPropose, vec![entry]);
    nt.dispatch([empty_msg]).unwrap();
}

#[test]
fn test_uncommitted_entry_after_leader_election() {
    let l = default_logger();
    let config = &Config {
        id: 1,
        max_uncommitted_size: 12,
        ..Config::default()
    };
    let mut nt = Network::new_with_config(vec![None, None, None, None, None], config, &l);
    let data = b"hello world!".to_vec();
    let mut entry = Entry::default();
    entry.data = data.into();
    let msg = new_message_with_entries(1, 1, MessageType::MsgPropose, vec![entry]);

    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    // create a uncommitted entry on node2
    nt.cut(1, 3);
    nt.cut(1, 4);
    nt.cut(1, 5);
    nt.send(vec![msg]);

    // now isolate master and make node2 as master
    nt.isolate(1);
    // ignore message append, cluster only work on election
    nt.ignore(MessageType::MsgAppend);
    nt.send(vec![new_message(2, 2, MessageType::MsgHup, 0)]);

    // uncommitted log size should be 0 on node2,
    // because we set uncommitted size to 0 rather than re-computing it,
    // which means max_uncommitted_size is a soft limit
    assert_eq!(nt.peers.get_mut(&2).unwrap().state, raft::StateRole::Leader);
    assert_eq!(nt.peers.get_mut(&2).unwrap().uncommitted_size(), 0);
}

#[test]
fn test_uncommitted_state_advance_ready_from_last_term() {
    let l = default_logger();
    let config = &Config {
        id: 1,
        max_uncommitted_size: 12,
        ..Config::default()
    };
    let mut nt = Network::new_with_config(vec![None, None, None, None, None], config, &l);

    let data = b"hello world!".to_vec();
    let mut ent = Entry::default();
    ent.data = data.clone().into();

    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    nt.send(vec![new_message_with_entries(
        1,
        1,
        MessageType::MsgPropose,
        vec![ent.clone()],
    )]);
    nt.send(vec![new_message_with_entries(
        1,
        1,
        MessageType::MsgPropose,
        vec![ent.clone()],
    )]);

    // now node2 has 2 committed entries
    // make node2 leader
    nt.send(vec![new_message(2, 2, MessageType::MsgHup, 0)]);
    assert_eq!(nt.peers.get_mut(&2).unwrap().state, raft::StateRole::Leader);

    nt.isolate(2);
    // create one uncommitted entry
    nt.send(vec![new_message_with_entries(
        2,
        2,
        MessageType::MsgPropose,
        vec![ent.clone()],
    )]);

    let mut ent1 = ent.clone();
    ent1.index = 1;
    let mut ent2 = ent;
    ent2.index = 2;

    // simulate advance 2 entries when node2 is follower
    nt.peers
        .get_mut(&2)
        .unwrap()
        .reduce_uncommitted_size(&[ent1, ent2]);

    // uncommitted size should be 12(remain unchanged since there's only one uncommitted entries)
    assert_eq!(nt.peers.get_mut(&2).unwrap().uncommitted_size(), data.len());
}

#[test]
fn test_fast_log_rejection() {
    let mut tests = vec![
        // This case tests that leader can find the conflict index quickly.
        // Firstly leader appends (type=MsgApp,index=7,logTerm=4, entries=...);
        // After rejected leader appends (type=MsgApp,index=3,logTerm=2).
        (
            vec![
                empty_entry(1, 1),
                empty_entry(2, 2),
                empty_entry(2, 3),
                empty_entry(4, 4),
                empty_entry(4, 5),
                empty_entry(4, 6),
                empty_entry(4, 7),
            ],
            vec![
                empty_entry(1, 1),
                empty_entry(2, 2),
                empty_entry(2, 3),
                empty_entry(3, 4),
                empty_entry(3, 5),
                empty_entry(3, 6),
                empty_entry(3, 7),
                empty_entry(3, 8),
                empty_entry(3, 9),
                empty_entry(3, 10),
                empty_entry(3, 11),
            ],
            3,
            7,
            2,
            3,
        ),
        // This case tests that leader can find the conflict index quickly.
        // Firstly leader appends (type=MsgApp,index=8,logTerm=5, entries=...);
        // After rejected leader appends (type=MsgApp,index=4,logTerm=3).
        (
            vec![
                empty_entry(1, 1),
                empty_entry(2, 2),
                empty_entry(2, 3),
                empty_entry(3, 4),
                empty_entry(4, 5),
                empty_entry(4, 6),
                empty_entry(4, 7),
                empty_entry(5, 8),
            ],
            vec![
                empty_entry(1, 1),
                empty_entry(2, 2),
                empty_entry(2, 3),
                empty_entry(3, 4),
                empty_entry(3, 5),
                empty_entry(3, 6),
                empty_entry(3, 7),
                empty_entry(3, 8),
                empty_entry(3, 9),
                empty_entry(3, 10),
                empty_entry(3, 11),
            ],
            3,
            8,
            3,
            4,
        ),
        // This case tests that follower can find the conflict index quickly.
        // Firstly leader appends (type=MsgApp,index=4,logTerm=1, entries=...);
        // After rejected leader appends (type=MsgApp,index=1,logTerm=1).
        (
            vec![
                empty_entry(1, 1),
                empty_entry(1, 2),
                empty_entry(1, 3),
                empty_entry(1, 4),
            ],
            vec![
                empty_entry(1, 1),
                empty_entry(2, 2),
                empty_entry(2, 3),
                empty_entry(4, 4),
            ],
            1,
            1,
            1,
            1,
        ),
        // This case is similar to the previous case. However, this time, the
        // leader has a longer uncommitted log tail than the follower.
        // Firstly leader appends (type=MsgApp,index=6,logTerm=1, entries=...);
        // After rejected leader appends (type=MsgApp,index=1,logTerm=1).
        (
            vec![
                empty_entry(1, 1),
                empty_entry(1, 2),
                empty_entry(1, 3),
                empty_entry(1, 4),
                empty_entry(1, 5),
                empty_entry(1, 6),
            ],
            vec![
                empty_entry(1, 1),
                empty_entry(2, 2),
                empty_entry(2, 3),
                empty_entry(4, 4),
            ],
            1,
            1,
            1,
            1,
        ),
        // This case is similar to the previous case. However, this time, the
        // follower has a longer uncommitted log tail than the leader.
        // Firstly leader appends (type=MsgApp,index=4,logTerm=1, entries=...);
        // After rejected leader appends (type=MsgApp,index=1,logTerm=1).
        (
            vec![
                empty_entry(1, 1),
                empty_entry(1, 2),
                empty_entry(1, 3),
                empty_entry(1, 4),
            ],
            vec![
                empty_entry(1, 1),
                empty_entry(2, 2),
                empty_entry(2, 3),
                empty_entry(4, 4),
                empty_entry(4, 5),
                empty_entry(4, 6),
            ],
            1,
            1,
            1,
            1,
        ),
        // An normal case that there are no log conflicts.
        // Firstly leader appends (type=MsgApp,index=5,logTerm=5, entries=...);
        // After rejected leader appends (type=MsgApp,index=4,logTerm=4).
        (
            vec![
                empty_entry(1, 1),
                empty_entry(1, 2),
                empty_entry(1, 3),
                empty_entry(4, 4),
                empty_entry(5, 5),
            ],
            vec![
                empty_entry(1, 1),
                empty_entry(1, 2),
                empty_entry(1, 3),
                empty_entry(4, 4),
            ],
            4,
            4,
            4,
            4,
        ),
        // Test case from example comment in stepLeader (on leader).
        (
            vec![
                empty_entry(2, 1),
                empty_entry(5, 2),
                empty_entry(5, 3),
                empty_entry(5, 4),
                empty_entry(5, 5),
                empty_entry(5, 6),
                empty_entry(5, 7),
                empty_entry(5, 8),
                empty_entry(5, 9),
            ],
            vec![
                empty_entry(2, 1),
                empty_entry(4, 2),
                empty_entry(4, 3),
                empty_entry(4, 4),
                empty_entry(4, 5),
                empty_entry(4, 6),
            ],
            4,
            6,
            2,
            1,
        ),
        // Test case from example comment in handleAppendEntries (on follower).
        (
            vec![
                empty_entry(2, 1),
                empty_entry(2, 2),
                empty_entry(2, 3),
                empty_entry(2, 4),
                empty_entry(2, 5),
            ],
            vec![
                empty_entry(2, 1),
                empty_entry(4, 2),
                empty_entry(4, 3),
                empty_entry(4, 4),
                empty_entry(4, 5),
                empty_entry(4, 6),
                empty_entry(4, 7),
                empty_entry(4, 8),
            ],
            2,
            1,
            2,
            1,
        ),
    ];
    for (
        i,
        (
            leader_log,
            follower_log,
            reject_hint_term,
            reject_hint_index,
            next_append_term,
            next_append_index,
        ),
    ) in tests.drain(..).enumerate()
    {
        let l = default_logger();
        let s1 = MemStorage::new_with_conf_state((vec![1, 2, 3], vec![]));
        s1.wl().append(&leader_log).unwrap();
        let s2 = MemStorage::new_with_conf_state((vec![1, 2, 3], vec![]));
        s2.wl().append(&follower_log).unwrap();
        let mut n1 = new_test_raft(1, vec![1, 2, 3], 10, 1, s1, &l);
        let mut n2 = new_test_raft(2, vec![1, 2, 3], 10, 1, s2, &l);
        n1.become_candidate();
        n1.become_leader();
        n2.step(new_message(2, 2, MessageType::MsgHeartbeat, 0))
            .unwrap();

        let mut msgs = n2.read_messages();
        assert_eq!(msgs.len(), 1, "#{}", i);
        assert_eq!(
            msgs[0].get_msg_type(),
            MessageType::MsgHeartbeatResponse,
            "#{}",
            i
        );
        // move Vec item by pop
        n1.step(msgs.pop().unwrap()).unwrap();

        let mut msgs = n1.read_messages();
        assert_eq!(msgs.len(), 1, "#{}", i);
        assert_eq!(msgs[0].get_msg_type(), MessageType::MsgAppend, "#{}", i);
        n2.step(msgs.pop().unwrap()).unwrap();

        let mut msgs = n2.read_messages();
        assert_eq!(msgs.len(), 1, "#{}", i);
        assert_eq!(
            msgs[0].get_msg_type(),
            MessageType::MsgAppendResponse,
            "#{}",
            i
        );
        assert!(msgs[0].reject, "#{}", i);
        assert_eq!(msgs[0].reject_hint, reject_hint_index, "#{}", i);
        assert_eq!(msgs[0].log_term, reject_hint_term, "#{}", i);
        n1.step(msgs.pop().unwrap()).unwrap();

        let msgs = n1.read_messages();
        assert_eq!(msgs.len(), 1, "#{}", i);
        assert_eq!(msgs[0].log_term, next_append_term, "#{}", i);
        assert_eq!(msgs[0].index, next_append_index, "#{}", i);
    }
}

#[test]
fn test_switching_check_quorum() {
    let l = default_logger();
    let mut sm = new_test_raft(1, vec![1, 2, 3], 5, 1, new_storage(), &l);

    sm.set_check_quorum(true);
    sm.become_candidate();
    sm.become_leader();
    for _ in 0..=sm.election_timeout() {
        sm.tick();
    }
    assert_ne!(sm.state, StateRole::Leader);

    sm.persist();
    sm.set_check_quorum(false);
    sm.become_candidate();
    sm.become_leader();

    for _ in 0..=sm.election_timeout() {
        sm.tick();
    }
    assert_eq!(sm.state, StateRole::Leader);
}

#[test]
fn test_disable_proposal_forwarding() {
    let l = default_logger();

    let n1 = new_test_raft_with_config(
        &Config {
            id: 1,
            heartbeat_tick: 1,
            election_tick: 10,
            disable_proposal_forwarding: true,
            ..Default::default()
        },
        MemStorage::new_with_conf_state((vec![1, 2, 3], vec![])),
        &l,
    );

    let n2 = new_test_raft_with_config(
        &Config {
            id: 2,
            heartbeat_tick: 1,
            election_tick: 10,
            disable_proposal_forwarding: true,
            ..Default::default()
        },
        MemStorage::new_with_conf_state((vec![1, 2, 3], vec![])),
        &l,
    );

    let n3 = new_test_raft_with_config(
        &Config {
            id: 3,
            heartbeat_tick: 1,
            election_tick: 10,
            disable_proposal_forwarding: true,
            ..Default::default()
        },
        MemStorage::new_with_conf_state((vec![1, 2, 3], vec![])),
        &l,
    );

    let mut network = Network::new(vec![Some(n1), Some(n2), Some(n3)], &l);

    // node 1 starts campaign to become leader.
    network.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    assert_eq!(network.peers.get(&1).unwrap().state, StateRole::Leader);
    assert_eq!(network.peers.get(&2).unwrap().state, StateRole::Follower);
    assert_eq!(network.peers.get(&3).unwrap().state, StateRole::Follower);

    assert_eq!(network.peers.get(&2).unwrap().leader_id, 1);
    assert_eq!(network.peers.get(&3).unwrap().leader_id, 1);

    let committed_index_1 = network.peers.get(&1).unwrap().raft_log.committed;

    // send a proposal to follower 2
    network.send(vec![new_message(2, 2, MessageType::MsgPropose, 1)]);

    // send a proposal to follower 3
    network.send(vec![new_message(3, 3, MessageType::MsgPropose, 1)]);

    // assert no proposals are forwarded from follower and committed.
    assert_eq!(
        network.peers.get(&1).unwrap().raft_log.committed,
        committed_index_1
    );
    assert_eq!(
        network.peers.get(&2).unwrap().raft_log.committed,
        committed_index_1
    );
    assert_eq!(
        network.peers.get(&3).unwrap().raft_log.committed,
        committed_index_1
    );

    // nodes 2 and 3 are follower, with `disable_proposal_forwarding` enable, proposal will be dropped.
    // assert error returned in `step`.
    assert_eq!(
        network
            .peers
            .get_mut(&2)
            .unwrap()
            .step(new_message(2, 2, MessageType::MsgPropose, 1)),
        Err(Error::ProposalDropped)
    );

    assert_eq!(
        network
            .peers
            .get_mut(&3)
            .unwrap()
            .step(new_message(3, 3, MessageType::MsgPropose, 1)),
        Err(Error::ProposalDropped)
    );

    // send proposal to leader node.
    network.send(vec![new_message(1, 1, MessageType::MsgPropose, 5)]);

    let committed_index_2 = network.peers.get(&1).unwrap().raft_log.committed;
    if committed_index_1 >= committed_index_2 {
        panic!(
            "expect committed_index_1 < committed_index_2, got {:?} > {:?}",
            committed_index_1, committed_index_2
        );
    }

    // assert proposals are accepted on leader node.
    assert_eq!(
        network.peers.get(&1).unwrap().raft_log.committed,
        committed_index_2
    );
    assert_eq!(
        network.peers.get(&2).unwrap().raft_log.committed,
        committed_index_2
    );
    assert_eq!(
        network.peers.get(&3).unwrap().raft_log.committed,
        committed_index_2
    );
}
