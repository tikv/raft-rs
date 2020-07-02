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

fn must_cmp_ready(
    r: &Ready,
    ss: &Option<SoftState>,
    hs: &Option<HardState>,
    entries: &[Entry],
    committed_entries: Vec<Entry>,
    must_sync: bool,
) {
    assert_eq!(r.ss(), ss.as_ref());
    assert_eq!(r.hs(), hs.as_ref());
    assert_eq!(r.entries(), entries);
    assert_eq!(r.committed_entries, Some(committed_entries));
    assert_eq!(r.must_sync(), must_sync);
    assert!(r.read_states().is_empty());
    assert_eq!(r.snapshot(), &Snapshot::default());
    assert!(r.messages.is_empty());
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
    assert_eq!(
        nt.peers[&1].msgs[0],
        new_message_with_entries(2, 3, MessageType::MsgReadIndex, vec![test_entries.clone()])
    );
    assert_eq!(
        nt.peers[&1].msgs[1],
        new_message_with_entries(3, 3, MessageType::MsgReadIndex, vec![test_entries])
    );
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
        s.wl().append(rd.entries()).expect("");
        // Once we are the leader, propose a command and a ConfChange.
        if !proposed && rd.ss().is_some() && rd.ss().unwrap().leader_id == raw_node.raft.id {
            raw_node.propose(vec![], b"somedata".to_vec()).expect("");

            let cc = conf_change(ConfChangeType::AddNode, 2);
            ccdata.reserve_exact(cc.compute_size() as usize);
            cc.write_to_vec(&mut ccdata).unwrap();
            raw_node.propose_conf_change(vec![], cc).expect("");

            proposed = true;
        }
        raw_node.advance(rd);

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
        if rd.ss().is_some() && rd.ss().unwrap().leader_id == raw_node.raft.id {
            raw_node.advance(rd);
            break;
        }
        raw_node.advance(rd);
    }

    let mut propose_conf_change_and_apply = |cc| {
        raw_node.propose_conf_change(vec![], cc).expect("");
        let rd = raw_node.ready();
        s.wl().append(rd.entries()).expect("");
        for e in rd.committed_entries.as_ref().unwrap() {
            if e.get_entry_type() == EntryType::EntryConfChange {
                let mut conf_change = ConfChange::default();
                conf_change.merge_from_bytes(&e.data).unwrap();
                raw_node.apply_conf_change(&conf_change).ok();
            }
        }
        raw_node.advance(rd);
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
    s.wl().append(rd.entries()).expect("");
    raw_node.advance(rd);

    raw_node.campaign().expect("");
    loop {
        let rd = raw_node.ready();
        s.wl().append(rd.entries()).expect("");
        if rd.ss().is_some() && rd.ss().unwrap().leader_id == raw_node.raft.id {
            raw_node.advance(rd);
            break;
        }
        raw_node.advance(rd);
    }

    // propose add learner node and check apply state
    let cc = conf_change(ConfChangeType::AddLearnerNode, 2);
    raw_node.propose_conf_change(vec![], cc).expect("");

    let rd = raw_node.ready();
    s.wl().append(rd.entries()).expect("");

    assert!(
        rd.committed_entries.is_some() && rd.committed_entries.as_ref().unwrap().len() == 1,
        "should committed the conf change entry"
    );

    let e = &rd.committed_entries.as_ref().unwrap()[0];
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
            raw_node.advance(rd);

            // Once we are the leader, issue a read index request
            raw_node.read_index(wrequest_ctx);
            break;
        }
        raw_node.advance(rd);
    }

    // ensure the read_states can be read out
    assert!(!raw_node.raft.read_states.is_empty());
    assert!(raw_node.has_ready());
    let rd = raw_node.ready();
    assert_eq!(rd.read_states(), wrs.as_slice());
    s.wl().append(&rd.entries()).expect("");
    raw_node.advance(rd);

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
    must_cmp_ready(&rd, &None, &None, &[], vec![], false);
    store.wl().append(rd.entries()).unwrap();
    raw_node.advance(rd);

    raw_node.campaign().expect("");
    let rd = raw_node.ready();
    must_cmp_ready(
        &rd,
        &Some(soft_state(1, StateRole::Leader)),
        &Some(hard_state(2, 2, 1)),
        &[new_entry(2, 2, None)],
        vec![new_entry(2, 2, None)],
        true,
    );
    store.wl().append(rd.entries()).expect("");
    raw_node.advance(rd);

    raw_node.propose(vec![], b"somedata".to_vec()).expect("");
    let rd = raw_node.ready();
    must_cmp_ready(
        &rd,
        &None,
        &Some(hard_state(2, 3, 1)),
        &[new_entry(2, 3, SOME_DATA)],
        vec![new_entry(2, 3, SOME_DATA)],
        true,
    );
    store.wl().append(rd.entries()).expect("");
    raw_node.advance(rd);

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
    must_cmp_ready(&rd, &None, &None, &[], entries[..1].to_vec(), false);
    raw_node.advance(rd);
    assert!(!raw_node.has_ready());
}

#[test]
fn test_raw_node_restart_from_snapshot() {
    let l = default_logger();
    let snap = new_snapshot(2, 1, vec![1, 2]);
    let entries = vec![new_entry(1, 3, Some("foo"))];

    let mut raw_node = {
        let raw_node = new_raw_node(1, vec![], 10, 1, new_storage(), &l);
        let store = raw_node.raft.r.raft_log.store;
        store.wl().apply_snapshot(snap).unwrap();
        store.wl().append(&entries).unwrap();
        store.wl().set_hardstate(hard_state(1, 3, 0));
        RawNode::new(&new_test_config(1, 10, 1), store, &l).unwrap()
    };

    let rd = raw_node.ready();
    must_cmp_ready(&rd, &None, &None, &[], entries, false);
    raw_node.advance(rd);
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

/// test_set_priority checks the set_priority function in RawNode.
#[test]
fn test_set_priority() {
    let l = default_logger();
    let mut raw_node = new_raw_node(1, vec![1], 10, 1, new_storage(), &l);
    let priorities = vec![0, 1, 5, 10, 10000];
    for p in priorities {
        raw_node.set_priority(p);
        assert_eq!(raw_node.raft.priority, p);
    }
}
