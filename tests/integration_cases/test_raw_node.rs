// Copyright 2016 PingCAP, Inc.
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

use crate::test_util::*;
use harness::*;
use protobuf::Message;
use protobuf::{self, ProtobufEnum};
use raft::eraftpb::*;
use raft::raw_node::new_mem_raw_node;
use raft::storage::MemStorage;
use raft::*;

fn conf_change(t: ConfChangeType, node_id: u64) -> ConfChange {
    let mut cc = ConfChange::new();
    cc.set_change_type(t);
    cc.set_node_id(node_id);
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
) -> RawNode<MemStorage> {
    let mut config = new_test_config(id, peers, election, heartbeat);
    new_mem_raw_node(&mut config, storage).unwrap()
}

// test_raw_node_step ensures that RawNode.Step ignore local message.
#[test]
fn test_raw_node_step() {
    setup_for_test();
    for msg_t in MessageType::values() {
        let mut raw_node = new_raw_node(1, vec![1], 10, 1, new_storage());
        let res = raw_node.step(new_message(0, 0, *msg_t, 0));
        // local msg should be ignored.
        if vec![
            MessageType::MsgBeat,
            MessageType::MsgHup,
            MessageType::MsgUnreachable,
            MessageType::MsgSnapStatus,
            MessageType::MsgRequestVote,
            MessageType::MsgRequestPreVote,
        ]
        .contains(msg_t)
        {
            assert_eq!(res, Err(Error::StepLocalMsg));
        }
    }
}

// test_raw_node_read_index_to_old_leader ensures that MsgReadIndex to old leader gets
// forward to the new leader and 'send' method does not attach its term
#[test]
fn test_raw_node_read_index_to_old_leader() {
    setup_for_test();
    let r1 = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage());
    let r2 = new_test_raft(2, vec![1, 2, 3], 10, 1, new_storage());
    let r3 = new_test_raft(3, vec![1, 2, 3], 10, 1, new_storage());

    let mut nt = Network::new(vec![Some(r1), Some(r2), Some(r3)]);

    // elect r1 as leader
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
    let mut test_entries = Entry::new();
    test_entries.set_data(b"testdata".to_vec());

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
        new_message_with_entries(1, 3, MessageType::MsgReadIndex, vec![test_entries.clone()]);

    assert_eq!(nt.peers[&1].msgs[0], read_index_msg3);
    assert_eq!(nt.peers[&1].msgs[1], read_index_msg3);
}

// test_raw_node_propose_and_conf_change ensures that RawNode.propose and
// RawNode.propose_conf_change send the given proposal and ConfChange to the underlying raft.
#[test]
fn test_raw_node_propose_and_conf_change() {
    setup_for_test();
    let s = new_storage();
    let mut raw_node = new_raw_node(1, vec![1], 10, 1, s.clone());
    raw_node.campaign().expect("");
    let mut proposed = false;
    let mut last_index;
    let mut ccdata = vec![];
    loop {
        let rd = raw_node.ready();
        s.wl().append(rd.entries());
        // Once we are the leader, propose a command and a ConfChange.
        if !proposed && rd.ss().is_some() && rd.ss().unwrap().leader_id == raw_node.raft.id {
            raw_node.propose(vec![], b"somedata".to_vec()).expect("");

            let cc = conf_change(ConfChangeType::AddNode, 2);
            ccdata = protobuf::Message::write_to_bytes(&cc).unwrap();
            raw_node.propose_conf_change(vec![], cc).expect("");

            proposed = true;
        }
        raw_node.advance(rd);

        // Exit when we have four entries: one ConfChange, one no-op for the election,
        // our proposed command and proposed ConfChange.
        last_index = s.last_index().unwrap();
        if last_index >= 3 {
            break;
        }
    }

    let entries = s.entries(last_index - 1, last_index + 1, None).unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].get_data(), b"somedata");
    assert_eq!(entries[1].get_entry_type(), EntryType::EntryConfChange);
    assert_eq!(entries[1].get_data(), &*ccdata);
}

// test_raw_node_propose_add_duplicate_node ensures that two proposes to add the same node should
// not affect the later propose to add new node.
#[test]
fn test_raw_node_propose_add_duplicate_node() {
    setup_for_test();
    let s = new_storage();
    let mut raw_node = new_raw_node(1, vec![1], 10, 1, s.clone());
    raw_node.campaign().expect("");
    loop {
        let rd = raw_node.ready();
        s.wl().append(rd.entries());
        if rd.ss().is_some() && rd.ss().unwrap().leader_id == raw_node.raft.id {
            raw_node.advance(rd);
            break;
        }
        raw_node.advance(rd);
    }

    let mut propose_conf_change_and_apply = |cc| {
        if raw_node.propose_conf_change(vec![], cc).is_ok() {
            let rd = raw_node.ready();
            s.wl().append(rd.entries());
            if let Some(entry) = rd.committed_entries.as_ref().and_then(|v| v.last()) {
                raw_node.raft.commit_apply(entry.get_index());
            }
            raw_node.advance(rd);
        }
    };

    let cc1 = conf_change(ConfChangeType::AddNode, 1);
    let ccdata1 = protobuf::Message::write_to_bytes(&cc1).unwrap();
    propose_conf_change_and_apply(cc1.clone());

    // try to add the same node again
    propose_conf_change_and_apply(cc1);

    // the new node join should be ok
    let cc2 = conf_change(ConfChangeType::AddNode, 2);
    let ccdata2 = protobuf::Message::write_to_bytes(&cc2).unwrap();
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
    setup_for_test();
    let s = new_storage();
    let mut raw_node = new_raw_node(1, vec![1], 10, 1, s.clone());
    let rd = raw_node.ready();
    s.wl().append(rd.entries());
    raw_node.advance(rd);

    raw_node.campaign().expect("");
    loop {
        let rd = raw_node.ready();
        s.wl().append(rd.entries());
        if rd.ss().is_some() && rd.ss().unwrap().leader_id == raw_node.raft.id {
            raw_node.advance(rd);
            break;
        }
        raw_node.advance(rd);
    }

    // propose add learner node and check apply state
    let cc = conf_change(ConfChangeType::AddLearnerNode, 2);
    raw_node.propose_conf_change(vec![], cc).expect("");

    let cs = raw_node.raft.conf_states().last().unwrap();
    assert_eq!(cs.conf_state.nodes, vec![1]);
    assert_eq!(cs.conf_state.learners, vec![2]);
    Ok(())
}

// test_raw_node_read_index ensures that RawNode.read_index sends the MsgReadIndex message
// to the underlying raft. It also ensures that ReadState can be read out.
#[test]
fn test_raw_node_read_index() {
    setup_for_test();
    let wrequest_ctx = b"somedata".to_vec();
    let wrs = vec![ReadState {
        index: 2u64,
        request_ctx: wrequest_ctx.clone(),
    }];

    let s = new_storage();
    let mut raw_node = new_raw_node(1, vec![1], 10, 1, s.clone());
    raw_node.campaign().expect("");
    loop {
        let rd = raw_node.ready();
        s.wl().append(rd.entries());
        if rd.ss().map_or(false, |ss| ss.leader_id == raw_node.raft.id) {
            raw_node.advance(rd);

            // Once we are the leader, issue a read index request
            raw_node.read_index(wrequest_ctx.clone());
            break;
        }
        raw_node.advance(rd);
    }

    // ensure the read_states can be read out
    assert!(!raw_node.raft.read_states.is_empty());
    assert!(raw_node.has_ready());
    let rd = raw_node.ready();
    assert_eq!(rd.read_states(), wrs.as_slice());
    s.wl().append(&rd.entries());
    raw_node.advance(rd);

    // ensure raft.read_states is reset after advance
    assert!(!raw_node.has_ready());
    assert!(raw_node.raft.read_states.is_empty());
}

// test_raw_node_start ensures that a node can be started correctly.
#[test]
fn test_raw_node_start() {
    setup_for_test();
    let store = new_storage();
    let mut raw_node = new_raw_node(1, vec![1], 10, 1, store.clone());

    let rd = raw_node.ready();
    must_cmp_ready(&rd, &None, &None, &[], vec![], false);

    store.wl().append(rd.entries());
    raw_node.advance(rd);

    raw_node.campaign().expect("");
    let rd = raw_node.ready();
    store.wl().append(rd.entries());
    raw_node.advance(rd);

    raw_node.propose(vec![], b"foo".to_vec()).expect("");
    let rd = raw_node.ready();
    must_cmp_ready(
        &rd,
        &None,
        &Some(hard_state(1, 3, 1)),
        &[new_entry(1, 3, Some("foo"))],
        vec![new_entry(1, 3, Some("foo"))],
        false,
    );
    store.wl().append(rd.entries());
    raw_node.advance(rd);
    assert!(!raw_node.has_ready());
}

#[test]
fn test_raw_node_restart() {
    setup_for_test();
    let entries = vec![empty_entry(1, 1), new_entry(1, 2, Some("foo"))];

    let mut raw_node = {
        let raw_node = new_raw_node(1, vec![], 10, 1, new_storage());
        let store = raw_node.raft.raft_log.store;
        store.wl().set_hardstate(hard_state(1, 1, 0));
        store.wl().append(&entries);
        new_raw_node(1, vec![], 10, 1, store)
    };

    let rd = raw_node.ready();
    must_cmp_ready(&rd, &None, &None, &[], entries[..1].to_vec(), false);
    raw_node.advance(rd);
    assert!(!raw_node.has_ready());
}

#[test]
fn test_raw_node_restart_from_snapshot() {
    setup_for_test();
    let snap = new_snapshot(2, 1, vec![1, 2]);
    let entries = vec![new_entry(1, 3, Some("foo"))];

    let mut raw_node = {
        let raw_node = new_raw_node(1, vec![], 10, 1, new_storage());
        let store = raw_node.raft.raft_log.store;
        store.wl().apply_snapshot(snap);
        store.wl().append(&entries);
        store.wl().set_hardstate(hard_state(1, 3, 0));
        new_raw_node(1, vec![], 10, 1, store)
    };

    let rd = raw_node.ready();
    must_cmp_ready(&rd, &None, &None, &[], entries.clone(), false);
    raw_node.advance(rd);
    assert!(!raw_node.has_ready());
}

// test_skip_bcast_commit ensures that empty commit message is not sent out
// when skip_bcast_commit is true.
#[test]
fn test_skip_bcast_commit() {
    setup_for_test();
    let mut config = new_test_config(1, vec![1, 2, 3], 10, 1);
    config.skip_bcast_commit = true;
    let r1 = new_test_raft_with_config(&config, new_storage());
    let r2 = new_test_raft(2, vec![1, 2, 3], 10, 1, new_storage());
    let r3 = new_test_raft(3, vec![1, 2, 3], 10, 1, new_storage());
    let mut nt = Network::new(vec![Some(r1), Some(r2), Some(r3)]);

    // elect r1 as leader
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    // Without bcast commit, followers will not update its commit index immediately.
    let mut test_entries = Entry::new();
    test_entries.set_data(b"testdata".to_vec());
    let msg = new_message_with_entries(1, 1, MessageType::MsgPropose, vec![test_entries.clone()]);
    nt.send(vec![msg.clone()]);
    assert_eq!(nt.peers[&1].raft_log.committed, 3);
    assert_eq!(nt.peers[&2].raft_log.committed, 2);
    assert_eq!(nt.peers[&3].raft_log.committed, 2);

    // After bcast heartbeat, followers will be informed the actual commit index.
    for _ in 0..nt.peers[&1].get_randomized_election_timeout() {
        nt.peers.get_mut(&1).unwrap().tick();
    }
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
    assert_eq!(nt.peers[&2].raft_log.committed, 3);
    assert_eq!(nt.peers[&3].raft_log.committed, 3);

    // The feature should be able to be adjusted at run time.
    nt.peers.get_mut(&1).unwrap().skip_bcast_commit(false);
    nt.send(vec![msg.clone()]);
    assert_eq!(nt.peers[&1].raft_log.committed, 4);
    assert_eq!(nt.peers[&2].raft_log.committed, 4);
    assert_eq!(nt.peers[&3].raft_log.committed, 4);

    nt.peers.get_mut(&1).unwrap().skip_bcast_commit(true);

    // Later proposal should commit former proposal.
    nt.send(vec![msg.clone()]);
    nt.send(vec![msg]);
    assert_eq!(nt.peers[&1].raft_log.committed, 6);
    assert_eq!(nt.peers[&2].raft_log.committed, 5);
    assert_eq!(nt.peers[&3].raft_log.committed, 5);

    // When committing conf change, leader should always bcast commit.
    let mut cc = ConfChange::new();
    cc.set_change_type(ConfChangeType::RemoveNode);
    cc.set_node_id(3);
    let data = cc.write_to_bytes().unwrap();
    let mut cc_entry = Entry::new();
    cc_entry.set_entry_type(EntryType::EntryConfChange);
    cc_entry.set_data(data);
    nt.send(vec![new_message_with_entries(
        1,
        1,
        MessageType::MsgPropose,
        vec![cc_entry],
    )]);
    assert!(nt.peers[&1].should_bcast_commit());
    assert!(nt.peers[&2].should_bcast_commit());
    assert!(nt.peers[&3].should_bcast_commit());

    assert_eq!(nt.peers[&1].raft_log.committed, 7);
    assert_eq!(nt.peers[&2].raft_log.committed, 7);
}
