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

use harness::Network;
use protobuf::{Message as PbMessage, ProtobufEnum as _};
use raft::eraftpb::*;
use raft::storage::MemStorage;
use raft::*;
use slog::Logger;

use crate::test_util::*;

fn conf_change(t: ConfChangeType, node_id: u64) -> ConfChange {
    let mut cc = ConfChange::default();
    cc.set_change_type(t);
    cc.node_id = node_id;
    cc
}

#[allow(clippy::too_many_arguments)]
fn must_cmp_ready(
    r: &Ready,
    ss: &Option<SoftState>,
    hs: &Option<HardState>,
    entries: &[Entry],
    committed_entries: &[Entry],
    snapshot: &Option<Snapshot>,
    msg_is_empty: bool,
    must_sync: bool,
) {
    assert_eq!(r.ss(), ss.as_ref());
    assert_eq!(r.hs(), hs.as_ref());
    assert_eq!(r.entries().as_slice(), entries);
    assert_eq!(r.committed_entries().as_slice(), committed_entries);
    assert_eq!(r.must_sync(), must_sync);
    assert!(r.read_states().is_empty());
    assert_eq!(
        r.snapshot(),
        snapshot.as_ref().unwrap_or(&Snapshot::default())
    );
    assert_eq!(r.messages().is_empty(), msg_is_empty);
}

fn new_raw_node(
    id: u64,
    peers: Vec<u64>,
    election: usize,
    heartbeat: usize,
    storage: MemStorage,
    logger: &Logger,
) -> RawNode<MemStorage> {
    let config = new_test_config(id, election, heartbeat);
    if storage.initial_state().unwrap().initialized() && peers.is_empty() {
        panic!("new_raw_node with empty peers on initialized store");
    }
    if !peers.is_empty() && !storage.initial_state().unwrap().initialized() {
        storage
            .wl()
            .apply_snapshot(new_snapshot(1, 1, peers))
            .unwrap();
    }
    RawNode::new(&config, storage, logger).unwrap()
}

// test_raw_node_step ensures that RawNode.Step ignore local message.
#[test]
fn test_raw_node_step() {
    let l = default_logger();
    for msg_t in MessageType::values() {
        let mut raw_node = new_raw_node(1, vec![1], 10, 1, new_storage(), &l);
        let res = raw_node.step(new_message(0, 0, *msg_t, 0));
        // local msg should be ignored.
        if vec![
            MessageType::MsgBeat,
            MessageType::MsgHup,
            MessageType::MsgUnreachable,
            MessageType::MsgSnapStatus,
        ]
        .contains(&msg_t)
        {
            assert_eq!(res, Err(Error::StepLocalMsg));
        }
    }
}

// test_raw_node_read_index_to_old_leader ensures that MsgReadIndex to old leader gets
// forward to the new leader and 'send' method does not attach its term
#[test]
fn test_raw_node_read_index_to_old_leader() {
    let l = default_logger();
    let r1 = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage(), &l);
    let r2 = new_test_raft(2, vec![1, 2, 3], 10, 1, new_storage(), &l);
    let r3 = new_test_raft(3, vec![1, 2, 3], 10, 1, new_storage(), &l);

    let mut nt = Network::new(vec![Some(r1), Some(r2), Some(r3)], &l);

    // elect r1 as leader
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
    let mut test_entries = Entry::default();
    test_entries.data = b"testdata".to_vec();

    // send readindex request to r2(follower)
    let _ = nt.peers.get_mut(&2).unwrap().step(new_message_with_entries(
        2,
        2,
        MessageType::MsgReadIndex,
        vec![test_entries.clone()],
    ));

    // verify r2(follower) forwards this message to r1(leader) with term not set
    assert_eq!(nt.peers[&2].msgs.len(), 1);
    let read_index_msg1 =
        new_message_with_entries(2, 1, MessageType::MsgReadIndex, vec![test_entries.clone()]);
    assert_eq!(read_index_msg1, nt.peers[&2].msgs[0]);

    // send readindex request to r3(follower)
    let _ = nt.peers.get_mut(&3).unwrap().step(new_message_with_entries(
        3,
        3,
        MessageType::MsgReadIndex,
        vec![test_entries.clone()],
    ));

    // verify r3(follower) forwards this message to r1(leader) with term not set as well.
    assert_eq!(nt.peers[&3].msgs.len(), 1);

    let read_index_msg2 =
        new_message_with_entries(3, 1, MessageType::MsgReadIndex, vec![test_entries.clone()]);
    assert_eq!(nt.peers[&3].msgs[0], read_index_msg2);

    // now elect r3 as leader
    nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);

    // let r1 steps the two messages previously we got from r2, r3
    let _ = nt.peers.get_mut(&1).unwrap().step(read_index_msg1);
    let _ = nt.peers.get_mut(&1).unwrap().step(read_index_msg2);

    // verify r1(follower) forwards these messages again to r3(new leader)
    assert_eq!(nt.peers[&1].msgs.len(), 2);

    let read_index_msg3 =
        new_message_with_entries(1, 3, MessageType::MsgReadIndex, vec![test_entries]);

    assert_eq!(nt.peers[&1].msgs[0], read_index_msg3);
    assert_eq!(nt.peers[&1].msgs[1], read_index_msg3);
}

// test_raw_node_propose_and_conf_change ensures that RawNode.propose and
// RawNode.propose_conf_change send the given proposal and ConfChange to the underlying raft.
#[test]
fn test_raw_node_propose_and_conf_change() {
    let l = default_logger();
    let s = new_storage();
    let mut raw_node = new_raw_node(1, vec![1], 10, 1, s.clone(), &l);
    raw_node.campaign().expect("");
    let mut proposed = false;
    let mut last_index;
    let mut ccdata = vec![];
    loop {
        let rd = raw_node.ready();
        println!("rd {:?}", rd);
        s.wl().append(rd.entries()).expect("");
        let is_leader = rd.ss().map_or(false, |ss| ss.leader_id == raw_node.raft.id);
        let _ = raw_node.advance(rd);
        // Once we are the leader, propose a command and a ConfChange.
        if !proposed && is_leader {
            raw_node.propose(vec![], b"somedata".to_vec()).expect("");

            let cc = conf_change(ConfChangeType::AddNode, 2);
            ccdata.reserve_exact(cc.compute_size() as usize);
            cc.write_to_vec(&mut ccdata).unwrap();
            raw_node.propose_conf_change(vec![], cc).expect("");

            proposed = true;
        }

        // Exit when we have 3 entries: one initial configuration, one no-op for the election
        // our proposed command and proposed ConfChange.
        last_index = s.last_index().unwrap();
        if last_index >= 3 {
            break;
        }
    }

    let entries = s.entries(last_index - 1, last_index + 1, None).unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].data, b"somedata");
    assert_eq!(entries[1].get_entry_type(), EntryType::EntryConfChange);
    assert_eq!(entries[1].data, &*ccdata);
}

// test_raw_node_propose_add_duplicate_node ensures that two proposes to add the same node should
// not affect the later propose to add new node.
#[test]
fn test_raw_node_propose_add_duplicate_node() {
    let l = default_logger();
    let s = new_storage();
    let mut raw_node = new_raw_node(1, vec![1], 10, 1, s.clone(), &l);
    raw_node.campaign().expect("");
    loop {
        let rd = raw_node.ready();
        s.wl().append(rd.entries()).expect("");
        if rd.ss().map_or(false, |ss| ss.leader_id == raw_node.raft.id) {
            let _ = raw_node.advance(rd);
            break;
        }
        let _ = raw_node.advance(rd);
    }

    let mut propose_conf_change_and_apply = |cc| {
        raw_node.propose_conf_change(vec![], cc).expect("");
        let mut rd = raw_node.ready();
        s.wl().append(rd.entries()).expect("");
        let handle_committed_entries =
            |rn: &mut RawNode<MemStorage>, committed_entries: Vec<Entry>| {
                for e in committed_entries {
                    if e.get_entry_type() == EntryType::EntryConfChange {
                        let mut conf_change = ConfChange::default();
                        conf_change.merge_from_bytes(&e.data).unwrap();
                        rn.apply_conf_change(&conf_change).ok();
                    }
                }
            };
        handle_committed_entries(&mut raw_node, rd.take_committed_entries());

        let mut light_rd = raw_node.advance(rd);
        handle_committed_entries(&mut raw_node, light_rd.take_committed_entries());
        raw_node.advance_apply();
    };

    let cc1 = conf_change(ConfChangeType::AddNode, 1);
    let ccdata1 = cc1.write_to_bytes().unwrap();
    propose_conf_change_and_apply(cc1.clone());

    // try to add the same node again
    propose_conf_change_and_apply(cc1);

    // the new node join should be ok
    let cc2 = conf_change(ConfChangeType::AddNode, 2);
    let ccdata2 = cc2.write_to_bytes().unwrap();
    propose_conf_change_and_apply(cc2);

    let last_index = s.last_index().unwrap();

    // the last three entries should be: ConfChange cc1, cc1, cc2
    let mut entries = s.entries(last_index - 2, last_index + 1, None).unwrap();
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].take_data(), ccdata1);
    assert_eq!(entries[2].take_data(), ccdata2);
}

#[test]
fn test_raw_node_propose_add_learner_node() -> Result<()> {
    let l = default_logger();
    let s = new_storage();
    let mut raw_node = new_raw_node(1, vec![1], 10, 1, s.clone(), &l);
    let rd = raw_node.ready();
    must_cmp_ready(&rd, &None, &None, &[], &[], &None, true, false);
    let _ = raw_node.advance(rd);

    raw_node.campaign().expect("");
    loop {
        let rd = raw_node.ready();
        s.wl().append(rd.entries()).expect("");
        if rd.ss().map_or(false, |ss| ss.leader_id == raw_node.raft.id) {
            let _ = raw_node.advance(rd);
            break;
        }
        let _ = raw_node.advance(rd);
    }

    // propose add learner node and check apply state
    let cc = conf_change(ConfChangeType::AddLearnerNode, 2);
    raw_node.propose_conf_change(vec![], cc).expect("");

    let rd = raw_node.ready();
    s.wl().append(rd.entries()).expect("");

    let light_rd = raw_node.advance(rd);

    assert_eq!(
        light_rd.committed_entries().len(),
        1,
        "should committed the conf change entry"
    );

    let e = &light_rd.committed_entries()[0];
    assert_eq!(e.get_entry_type(), EntryType::EntryConfChange);
    let mut conf_change = ConfChange::default();
    conf_change.merge_from_bytes(&e.data).unwrap();
    let conf_state = raw_node.apply_conf_change(&conf_change)?;
    assert_eq!(conf_state.voters, vec![1]);
    assert_eq!(conf_state.learners, vec![2]);

    Ok(())
}

// test_raw_node_read_index ensures that RawNode.read_index sends the MsgReadIndex message
// to the underlying raft. It also ensures that ReadState can be read out.
#[test]
fn test_raw_node_read_index() {
    let l = default_logger();
    let wrequest_ctx = b"somedata".to_vec();
    let wrs = vec![ReadState {
        index: 2u64,
        request_ctx: wrequest_ctx.clone(),
    }];

    let s = new_storage();
    let mut raw_node = new_raw_node(1, vec![1], 10, 1, s.clone(), &l);
    raw_node.campaign().expect("");
    loop {
        let rd = raw_node.ready();
        s.wl().append(rd.entries()).expect("");
        if rd.ss().map_or(false, |ss| ss.leader_id == raw_node.raft.id) {
            let _ = raw_node.advance(rd);

            // Once we are the leader, issue a read index request
            raw_node.read_index(wrequest_ctx);
            break;
        }
        let _ = raw_node.advance(rd);
    }

    // ensure the read_states can be read out
    assert!(!raw_node.raft.read_states.is_empty());
    assert!(raw_node.has_ready());
    let rd = raw_node.ready();
    assert_eq!(*rd.read_states(), wrs);
    s.wl().append(rd.entries()).expect("");
    let _ = raw_node.advance(rd);

    // ensure raft.read_states is reset after advance
    assert!(!raw_node.has_ready());
    assert!(raw_node.raft.read_states.is_empty());
}

// test_raw_node_start ensures that a node can be started correctly.
#[test]
fn test_raw_node_start() {
    let l = default_logger();
    let store = new_storage();
    let mut raw_node = new_raw_node(1, vec![1], 10, 1, store.clone(), &l);

    let rd = raw_node.ready();
    must_cmp_ready(&rd, &None, &None, &[], &[], &None, true, false);
    let _ = raw_node.advance(rd);

    raw_node.campaign().expect("");
    let rd = raw_node.ready();
    must_cmp_ready(
        &rd,
        &Some(soft_state(1, StateRole::Leader)),
        &Some(hard_state(2, 1, 1)),
        &[new_entry(2, 2, None)],
        &[],
        &None,
        true,
        true,
    );
    store.wl().append(rd.entries()).expect("");
    let light_rd = raw_node.advance(rd);
    assert_eq!(light_rd.commit_index(), Some(2));
    assert_eq!(*light_rd.committed_entries(), vec![new_entry(2, 2, None)]);
    assert!(!raw_node.has_ready());

    raw_node.propose(vec![], b"somedata".to_vec()).expect("");
    let rd = raw_node.ready();
    must_cmp_ready(
        &rd,
        &None,
        &None,
        &[new_entry(2, 3, SOME_DATA)],
        &[],
        &None,
        true,
        true,
    );
    store.wl().append(rd.entries()).expect("");
    let light_rd = raw_node.advance(rd);
    assert_eq!(light_rd.commit_index(), Some(3));
    assert_eq!(
        *light_rd.committed_entries(),
        vec![new_entry(2, 3, SOME_DATA)]
    );

    assert!(!raw_node.has_ready());
}

#[test]
fn test_raw_node_restart() {
    let l = default_logger();
    let entries = vec![empty_entry(1, 1), new_entry(1, 2, Some("foo"))];

    let mut raw_node = {
        let store = new_storage();
        store.wl().set_hardstate(hard_state(1, 1, 0));
        store.wl().append(&entries).unwrap();
        new_raw_node(1, vec![], 10, 1, store, &l)
    };

    let rd = raw_node.ready();
    must_cmp_ready(&rd, &None, &None, &[], &entries[..1], &None, true, false);
    let _ = raw_node.advance(rd);
    assert!(!raw_node.has_ready());
}

#[test]
fn test_raw_node_restart_from_snapshot() {
    let l = default_logger();
    let snap = new_snapshot(2, 1, vec![1, 2]);
    let entries = vec![new_entry(1, 3, Some("foo"))];

    let mut raw_node = {
        let store = new_storage();
        store.wl().apply_snapshot(snap).unwrap();
        store.wl().append(&entries).unwrap();
        store.wl().set_hardstate(hard_state(1, 3, 0));
        RawNode::new(&new_test_config(1, 10, 1), store, &l).unwrap()
    };

    let rd = raw_node.ready();
    must_cmp_ready(&rd, &None, &None, &[], &entries, &None, true, false);
    let _ = raw_node.advance(rd);
    assert!(!raw_node.has_ready());
}

// test_skip_bcast_commit ensures that empty commit message is not sent out
// when skip_bcast_commit is true.
#[test]
fn test_skip_bcast_commit() {
    let l = default_logger();
    let mut config = new_test_config(1, 10, 1);
    config.skip_bcast_commit = true;
    let s = MemStorage::new_with_conf_state((vec![1, 2, 3], vec![]));
    let r1 = new_test_raft_with_config(&config, s, &l);
    let r2 = new_test_raft(2, vec![1, 2, 3], 10, 1, new_storage(), &l);
    let r3 = new_test_raft(3, vec![1, 2, 3], 10, 1, new_storage(), &l);
    let mut nt = Network::new(vec![Some(r1), Some(r2), Some(r3)], &l);

    // elect r1 as leader
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    // Without bcast commit, followers will not update its commit index immediately.
    let mut test_entries = Entry::default();
    test_entries.data = b"testdata".to_vec();
    let msg = new_message_with_entries(1, 1, MessageType::MsgPropose, vec![test_entries]);
    nt.send(vec![msg.clone()]);
    assert_eq!(nt.peers[&1].raft_log.committed, 2);
    assert_eq!(nt.peers[&2].raft_log.committed, 1);
    assert_eq!(nt.peers[&3].raft_log.committed, 1);

    // After bcast heartbeat, followers will be informed the actual commit index.
    for _ in 0..nt.peers[&1].randomized_election_timeout() {
        nt.peers.get_mut(&1).unwrap().tick();
    }
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
    assert_eq!(nt.peers[&2].raft_log.committed, 2);
    assert_eq!(nt.peers[&3].raft_log.committed, 2);

    // The feature should be able to be adjusted at run time.
    nt.peers.get_mut(&1).unwrap().skip_bcast_commit(false);
    nt.send(vec![msg.clone()]);
    assert_eq!(nt.peers[&1].raft_log.committed, 3);
    assert_eq!(nt.peers[&2].raft_log.committed, 3);
    assert_eq!(nt.peers[&3].raft_log.committed, 3);

    nt.peers.get_mut(&1).unwrap().skip_bcast_commit(true);

    // Later proposal should commit former proposal.
    nt.send(vec![msg.clone()]);
    nt.send(vec![msg]);
    assert_eq!(nt.peers[&1].raft_log.committed, 5);
    assert_eq!(nt.peers[&2].raft_log.committed, 4);
    assert_eq!(nt.peers[&3].raft_log.committed, 4);

    // When committing conf change, leader should always bcast commit.
    let mut cc = ConfChange::default();
    cc.set_change_type(ConfChangeType::RemoveNode);
    cc.node_id = 3;
    let data = cc.write_to_bytes().unwrap();
    let mut cc_entry = Entry::default();
    cc_entry.set_entry_type(EntryType::EntryConfChange);
    cc_entry.data = data;
    nt.send(vec![new_message_with_entries(
        1,
        1,
        MessageType::MsgPropose,
        vec![cc_entry],
    )]);
    assert!(nt.peers[&1].should_bcast_commit());
    assert!(nt.peers[&2].should_bcast_commit());
    assert!(nt.peers[&3].should_bcast_commit());

    assert_eq!(nt.peers[&1].raft_log.committed, 6);
    assert_eq!(nt.peers[&2].raft_log.committed, 6);
    assert_eq!(nt.peers[&3].raft_log.committed, 6);
}

#[test]
fn test_raw_node_with_async_apply() {
    let l = default_logger();
    let s = new_storage();
    s.wl().apply_snapshot(new_snapshot(1, 1, vec![1])).unwrap();

    let mut raw_node = new_raw_node(1, vec![1], 10, 1, s.clone(), &l);
    raw_node.campaign().unwrap();
    let rd = raw_node.ready();
    // Single node should become leader.
    assert!(rd
        .ss()
        .map_or(false, |ss| ss.leader_id == raw_node.raft.leader_id));
    s.wl().append(&rd.entries()).unwrap();
    let _ = raw_node.advance(rd);

    let mut last_index = raw_node.raft.raft_log.last_index();

    let data = b"hello world!";

    for _ in 1..10 {
        let cnt = rand::random::<u64>() % 10 + 1;
        for _ in 0..cnt {
            raw_node.propose(vec![], data.to_vec()).unwrap();
        }

        let rd = raw_node.ready();
        let entries = rd.entries().clone();
        assert_eq!(entries.first().unwrap().get_index(), last_index + 1);
        assert_eq!(entries.last().unwrap().get_index(), last_index + cnt);
        must_cmp_ready(&rd, &None, &None, &entries, &[], &None, true, true);

        s.wl().append(&entries).unwrap();

        let light_rd = raw_node.advance_append(rd);
        assert_eq!(entries, *light_rd.committed_entries());
        assert_eq!(light_rd.commit_index(), Some(last_index + cnt));

        // No matter how applied index changes, the index of next committed
        // entries should be the same.
        raw_node.advance_apply_to(last_index + 1);
        assert!(!raw_node.has_ready());

        last_index += cnt;
    }
}

/// Test if the ready process is expected when a follower receives a snapshot
/// and some committed entries after its snapshot.
#[test]
fn test_raw_node_entries_after_snapshot() {
    let l = default_logger();
    let s = new_storage();
    s.wl()
        .apply_snapshot(new_snapshot(1, 1, vec![1, 2]))
        .unwrap();

    let mut raw_node = new_raw_node(1, vec![1, 2], 10, 1, s.clone(), &l);

    let snapshot = new_snapshot(10, 2, vec![1, 2]);
    let mut snapshot_msg = new_message(2, 1, MessageType::MsgSnapshot, 0);
    snapshot_msg.set_term(2);
    snapshot_msg.set_snapshot(snapshot.clone());
    raw_node.step(snapshot_msg).unwrap();

    let entries = [
        new_entry(2, 11, Some("hello")),
        new_entry(2, 12, Some("hello")),
        new_entry(2, 13, Some("hello")),
    ];
    let mut append_msg = new_message_with_entries(2, 1, MessageType::MsgAppend, entries.to_vec());
    append_msg.set_term(2);
    append_msg.set_index(10);
    append_msg.set_log_term(2);
    append_msg.set_commit(12);
    raw_node.step(append_msg).unwrap();

    let rd = raw_node.ready();
    // If there is a snapshot, the committed entries should be empty.
    must_cmp_ready(
        &rd,
        &Some(soft_state(2, StateRole::Follower)),
        &Some(hard_state(2, 12, 0)),
        &entries,
        &[],
        &Some(snapshot),
        true,
        true,
    );
    s.wl().set_hardstate(rd.hs().unwrap().clone());
    s.wl().apply_snapshot(rd.snapshot().clone()).unwrap();
    s.wl().append(rd.entries()).unwrap();

    let light_rd = raw_node.advance(rd);
    assert_eq!(light_rd.commit_index(), None);
    assert_eq!(light_rd.committed_entries().as_slice(), &entries[..2]);
    // Should have a MsgAppendResponse
    assert_eq!(
        light_rd.messages()[0][0].get_msg_type(),
        MessageType::MsgAppendResponse
    );
}

/// Test if the given committed entries are persisted when some persisted
/// entries are overwritten by a new leader.
#[test]
fn test_raw_node_overwrite_entries() {
    let l = default_logger();
    let s = new_storage();
    s.wl()
        .apply_snapshot(new_snapshot(1, 1, vec![1, 2, 3]))
        .unwrap();

    let mut raw_node = new_raw_node(1, vec![1, 2, 3], 10, 1, s.clone(), &l);

    let entries = [
        new_entry(2, 2, Some("hello")),
        new_entry(2, 3, Some("hello")),
        new_entry(2, 4, Some("hello")),
    ];
    let mut append_msg = new_message_with_entries(2, 1, MessageType::MsgAppend, entries.to_vec());
    append_msg.set_term(2);
    append_msg.set_index(1);
    append_msg.set_log_term(1);
    append_msg.set_commit(1);
    raw_node.step(append_msg).unwrap();

    let rd = raw_node.ready();
    must_cmp_ready(
        &rd,
        &Some(soft_state(2, StateRole::Follower)),
        &Some(hard_state(2, 1, 0)),
        &entries,
        &[],
        &None,
        true,
        true,
    );
    s.wl().set_hardstate(rd.hs().unwrap().clone());
    s.wl().append(rd.entries()).unwrap();

    let light_rd = raw_node.advance(rd);
    assert_eq!(light_rd.commit_index(), None);
    assert!(light_rd.committed_entries().is_empty());
    // Append entries response
    assert!(!light_rd.messages().is_empty());

    let entries_2 = [
        new_entry(3, 4, Some("hello")),
        new_entry(3, 5, Some("hello")),
        new_entry(3, 6, Some("hello")),
    ];
    let mut append_msg = new_message_with_entries(3, 1, MessageType::MsgAppend, entries_2.to_vec());
    append_msg.set_term(3);
    append_msg.set_index(3);
    append_msg.set_log_term(2);
    append_msg.set_commit(5);
    raw_node.step(append_msg).unwrap();

    let rd = raw_node.ready();
    must_cmp_ready(
        &rd,
        &Some(soft_state(3, StateRole::Follower)),
        &Some(hard_state(3, 5, 0)),
        &entries_2,
        &entries[..2],
        &None,
        true,
        true,
    );
    s.wl().set_hardstate(rd.hs().unwrap().clone());
    s.wl().append(rd.entries()).unwrap();

    let light_rd = raw_node.advance(rd);
    assert_eq!(light_rd.commit_index(), None);
    assert_eq!(light_rd.committed_entries().as_slice(), &entries_2[..2]);
    // Append entries response
    assert!(!light_rd.messages().is_empty());
}

/// Test if async ready process is expected when a leader receives
/// the append response and persist its entries.
#[test]
fn test_async_ready_leader() {
    let l = default_logger();
    let s = new_storage();
    s.wl()
        .apply_snapshot(new_snapshot(1, 1, vec![1, 2, 3]))
        .unwrap();

    let mut raw_node = new_raw_node(1, vec![1, 2, 3], 10, 1, s.clone(), &l);
    raw_node.raft.become_candidate();
    raw_node.raft.become_leader();
    let rd = raw_node.ready();
    assert!(rd
        .ss()
        .map_or(false, |ss| ss.leader_id == raw_node.raft.leader_id));
    s.wl().append(&rd.entries()).unwrap();
    let _ = raw_node.advance(rd);

    assert_eq!(raw_node.raft.term, 2);
    let mut first_index = raw_node.raft.raft_log.last_index();

    let data = b"hello world!";

    // Set node 2 progress to replicate
    raw_node.raft.mut_prs().get_mut(2).unwrap().matched = 1;
    raw_node
        .raft
        .mut_prs()
        .get_mut(2)
        .unwrap()
        .become_replicate();
    for i in 0..10 {
        for _ in 0..10 {
            raw_node.propose(vec![], data.to_vec()).unwrap();
        }

        let mut rd = raw_node.ready();
        assert_eq!(rd.number(), i + 2);
        let entries = rd.entries().clone();
        assert_eq!(
            entries.first().unwrap().get_index(),
            first_index + i * 10 + 1
        );
        assert_eq!(
            entries.last().unwrap().get_index(),
            first_index + i * 10 + 10
        );
        // Leader‘s msg can be sent immediately.
        must_cmp_ready(&rd, &None, &None, &entries, &[], &None, false, true);
        for vec_msg in rd.take_messages() {
            for msg in vec_msg {
                assert_eq!(msg.get_msg_type(), MessageType::MsgAppend);
            }
        }

        s.wl().append(&entries).unwrap();
        raw_node.advance_append_async(rd);
    }
    // Unpersisted Ready number in range [2, 11]
    raw_node.on_persist_ready(4);
    // No new committed entries due to two nodes in this cluster
    assert!(!raw_node.has_ready());

    // The index of uncommitted entries in range [first_index, first_index + 100]
    let mut append_response = new_message(2, 1, MessageType::MsgAppendResponse, 0);
    append_response.set_term(2);
    append_response.set_index(first_index + 100);

    raw_node.step(append_response).unwrap();

    // Forward commit index due to append response
    let rd = raw_node.ready();
    assert_eq!(rd.hs(), Some(&hard_state(2, first_index + 30, 1)));
    assert_eq!(
        rd.committed_entries().first().unwrap().get_index(),
        first_index
    );
    assert_eq!(
        rd.committed_entries().last().unwrap().get_index(),
        first_index + 30
    );
    assert!(!rd.messages().is_empty());
    s.wl().set_hardstate(rd.hs().unwrap().clone());
    raw_node.advance_append_async(rd);

    // Forward commit index due to persist ready
    raw_node.on_persist_ready(8);
    let rd = raw_node.ready();
    assert_eq!(rd.hs(), Some(&hard_state(2, first_index + 70, 1)));
    assert_eq!(
        rd.committed_entries().first().unwrap().get_index(),
        first_index + 31
    );
    assert_eq!(
        rd.committed_entries().last().unwrap().get_index(),
        first_index + 70
    );
    assert!(!rd.messages().is_empty());
    s.wl().set_hardstate(rd.hs().unwrap().clone());

    // Forward commit index due to persist last ready
    let light_rd = raw_node.advance_append(rd);
    assert_eq!(light_rd.commit_index(), Some(first_index + 100));
    assert_eq!(
        light_rd.committed_entries().first().unwrap().get_index(),
        first_index + 71
    );
    assert_eq!(
        light_rd.committed_entries().last().unwrap().get_index(),
        first_index + 100
    );
    assert!(!light_rd.messages().is_empty());

    // Test when 2 followers response the append entries msg and leader has
    // not persisted them yet.
    first_index += 100;
    for _ in 0..10 {
        raw_node.propose(vec![], data.to_vec()).unwrap();
    }

    let mut rd = raw_node.ready();
    assert_eq!(rd.number(), 14);
    let entries = rd.entries().clone();
    assert_eq!(entries.first().unwrap().get_index(), first_index + 1);
    assert_eq!(entries.last().unwrap().get_index(), first_index + 10);
    // Leader‘s msg can be sent immediately.
    must_cmp_ready(&rd, &None, &None, &entries, &[], &None, false, true);
    for vec_msg in rd.take_messages() {
        for msg in vec_msg {
            assert_eq!(msg.get_msg_type(), MessageType::MsgAppend);
        }
    }
    s.wl().append(&entries).unwrap();
    raw_node.advance_append_async(rd);

    let mut append_response = new_message(2, 1, MessageType::MsgAppendResponse, 0);
    append_response.set_term(2);
    append_response.set_index(first_index + 9);

    raw_node.step(append_response).unwrap();

    let mut append_response = new_message(3, 1, MessageType::MsgAppendResponse, 0);
    append_response.set_term(2);
    append_response.set_index(first_index + 10);

    raw_node.step(append_response).unwrap();

    let mut rd = raw_node.ready();
    // It should has some append msgs and its commit index should be first_index + 9.
    must_cmp_ready(
        &rd,
        &None,
        &Some(hard_state(2, first_index + 9, 1)),
        &[],
        &[],
        &None,
        false,
        false,
    );
    for vec_msg in rd.take_messages() {
        for msg in vec_msg {
            assert_eq!(msg.get_msg_type(), MessageType::MsgAppend);
            assert_eq!(msg.get_commit(), first_index + 9);
        }
    }

    // Forward commit index due to peer 1's append response and persisted entries
    let light_rd = raw_node.advance_append(rd);
    assert_eq!(light_rd.commit_index(), Some(first_index + 10));
    assert_eq!(
        light_rd.committed_entries().first().unwrap().get_index(),
        first_index + 1
    );
    assert_eq!(
        light_rd.committed_entries().last().unwrap().get_index(),
        first_index + 10
    );
    assert!(!light_rd.messages().is_empty());
}

/// Test if async ready process is expected when a follower receives
/// some append msg.
#[test]
fn test_async_ready_follower() {
    let l = default_logger();
    let s = new_storage();
    s.wl()
        .apply_snapshot(new_snapshot(1, 1, vec![1, 2]))
        .unwrap();

    let mut raw_node = new_raw_node(1, vec![1, 2], 10, 1, s.clone(), &l);
    let mut first_index = 1;
    let mut rd_number = 1;
    for cnt in 0..3 {
        for i in 0..10 {
            let entries = [
                new_entry(2, first_index + i * 3 + 1, Some("hello")),
                new_entry(2, first_index + i * 3 + 2, Some("hello")),
                new_entry(2, first_index + i * 3 + 3, Some("hello")),
            ];
            let mut append_msg =
                new_message_with_entries(2, 1, MessageType::MsgAppend, entries.to_vec());
            append_msg.set_term(2);
            append_msg.set_index(first_index + i * 3);
            if cnt == 0 && i == 0 {
                append_msg.set_log_term(1);
            } else {
                append_msg.set_log_term(2);
            }
            append_msg.set_commit(first_index + i * 3 + 3);
            raw_node.step(append_msg).unwrap();

            let rd = raw_node.ready();
            assert_eq!(rd.number(), rd_number + i);
            assert_eq!(rd.hs(), Some(&hard_state(2, first_index + i * 3 + 3, 0)));
            assert_eq!(rd.entries(), &entries);
            assert_eq!(rd.committed_entries().as_slice(), &[]);
            assert!(rd.messages().is_empty());

            s.wl().set_hardstate(rd.hs().unwrap().clone());
            s.wl().append(rd.entries()).unwrap();
            raw_node.advance_append_async(rd);
        }
        // Unpersisted Ready number in range [1, 10]
        raw_node.on_persist_ready(rd_number + 3);
        let mut rd = raw_node.ready();
        assert_eq!(rd.hs(), None);
        assert_eq!(
            rd.committed_entries().first().unwrap().get_index(),
            first_index + 1
        );
        assert_eq!(
            rd.committed_entries().last().unwrap().get_index(),
            first_index + 3 * 3 + 3
        );
        let mut msg_num = 0;
        for vec_msg in rd.take_messages() {
            for msg in vec_msg {
                assert_eq!(msg.get_msg_type(), MessageType::MsgAppendResponse);
                msg_num += 1;
            }
        }
        assert_eq!(msg_num, 4);

        let mut light_rd = raw_node.advance_append(rd);
        assert_eq!(light_rd.commit_index(), None);
        assert_eq!(
            light_rd.committed_entries().first().unwrap().get_index(),
            first_index + 3 * 3 + 4
        );
        assert_eq!(
            light_rd.committed_entries().last().unwrap().get_index(),
            first_index + 10 * 3
        );
        let mut msg_num = 0;
        for vec_msg in light_rd.take_messages() {
            for msg in vec_msg {
                assert_eq!(msg.get_msg_type(), MessageType::MsgAppendResponse);
                msg_num += 1;
            }
        }
        assert_eq!(msg_num, 6);

        first_index += 10 * 3;
        rd_number += 11;
    }
}

/// Test if a new leader immediately sends all messages recorded before without
/// persisting.
#[test]
fn test_async_ready_become_leader() {
    let l = default_logger();
    let s = new_storage();
    s.wl()
        .apply_snapshot(new_snapshot(5, 5, vec![1, 2, 3]))
        .unwrap();

    let mut raw_node = new_raw_node(1, vec![1, 2, 3], 10, 1, s.clone(), &l);
    for _ in 1..raw_node.raft.election_timeout() * 2 {
        raw_node.raft.tick_election();
    }
    let rd = raw_node.ready();
    assert_eq!(rd.number(), 1);
    must_cmp_ready(
        &rd,
        &Some(soft_state(0, StateRole::Candidate)),
        &Some(hard_state(6, 5, 1)),
        &[],
        &[],
        &None,
        true,
        true,
    );
    s.wl().set_hardstate(rd.hs().unwrap().clone());

    let mut light_rd = raw_node.advance_append(rd);
    for vec_msg in light_rd.take_messages() {
        for msg in vec_msg {
            assert_eq!(msg.get_msg_type(), MessageType::MsgRequestVote);
        }
    }

    // Peer 1 should reject to vote to peer 2
    let mut vote_request_2 = new_message(2, 1, MessageType::MsgRequestVote, 0);
    vote_request_2.set_term(6);
    vote_request_2.set_log_term(4);
    vote_request_2.set_index(4);
    raw_node.step(vote_request_2).unwrap();

    let rd = raw_node.ready();
    assert_eq!(rd.number(), 2);
    must_cmp_ready(&rd, &None, &None, &[], &[], &None, true, false);
    raw_node.advance_append_async(rd);

    // Peer 1 should reject to vote to peer 2
    let mut vote_request_3 = new_message(3, 1, MessageType::MsgRequestVote, 0);
    vote_request_3.set_term(6);
    vote_request_3.set_log_term(4);
    vote_request_3.set_index(4);
    raw_node.step(vote_request_3).unwrap();

    let rd = raw_node.ready();
    assert_eq!(rd.number(), 3);
    must_cmp_ready(&rd, &None, &None, &[], &[], &None, true, false);
    raw_node.advance_append_async(rd);

    // Peer 1 receives the vote from peer 2
    let mut vote_response_2 = new_message(2, 1, MessageType::MsgRequestVoteResponse, 0);
    vote_response_2.set_term(6);
    vote_response_2.set_reject(false);
    raw_node.step(vote_response_2).unwrap();

    let mut rd = raw_node.ready();
    assert_eq!(rd.number(), 4);
    assert_eq!(rd.entries().len(), 1);
    must_cmp_ready(
        &rd,
        &Some(soft_state(1, StateRole::Leader)),
        &None,
        rd.entries(),
        &[],
        &None,
        false,
        true,
    );
    // 2 vote reject + 2 append entries
    let mut count = 0;
    for vec_msg in rd.take_messages() {
        for msg in vec_msg {
            let msg_type = match count {
                0 | 1 => MessageType::MsgRequestVoteResponse,
                _ => MessageType::MsgAppend,
            };
            assert_eq!(msg.get_msg_type(), msg_type);
            count += 1;
        }
    }
    assert_eq!(count, 4);

    let light_rd = raw_node.advance_append(rd);
    assert_eq!(light_rd.commit_index(), None);
    assert!(light_rd.committed_entries().is_empty());
    assert!(light_rd.messages().is_empty());
}
