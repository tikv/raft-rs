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
use raft_proto::*;
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

/// Ensures that RawNode::step ignore local message.
#[test]
fn test_raw_node_step() {
    let l = default_logger();
    for msg_t in MessageType::values() {
        let s = new_storage();
        s.wl().set_hardstate(hard_state(1, 1, 0));
        // Append an empty entry to make sure the non-local messages (like
        // vote requests) are ignored and don't trigger assertions.
        s.wl().append(&[new_entry(1, 1, None)]).unwrap();
        s.wl().apply_snapshot(new_snapshot(1, 1, vec![1])).unwrap();

        let mut raw_node = new_raw_node(1, vec![1], 10, 1, new_storage(), &l);
        let res = raw_node.step(new_message(0, 0, *msg_t, 0));
        // LocalMsg should be ignored.
        if raw_node::is_local_msg(*msg_t) {
            assert_eq!(res, Err(Error::StepLocalMsg), "{:?}", msg_t);
        }
    }
}

/// Ensures that MsgReadIndex to old leader gets forwarded to the new leader and
/// 'send' method does not attach its term.
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

/// Tests the configuration change mechanism. Each test case sends a configuration
/// change which is either simple or joint, verifies that it applies and that the
/// resulting ConfState matches expectations, and for joint configurations makes
/// sure that they are exited successfully.
#[test]
fn test_raw_node_propose_and_conf_change() {
    let l = default_logger();
    let mut test_cases: Vec<(Box<dyn ConfChangeI>, _, _)> = vec![];
    // V1 config change.
    test_cases.push((
        Box::new(conf_change(ConfChangeType::AddNode, 2)),
        conf_state(vec![1, 2], vec![]),
        None,
    ));

    // Proposing the same as a V2 change works just the same, without entering
    // a joint config.
    let single = new_conf_change_single(2, ConfChangeType::AddNode);
    test_cases.push((
        Box::new(conf_change_v2(vec![single])),
        conf_state(vec![1, 2], vec![]),
        None,
    ));

    // Ditto if we add it as a learner instead.
    let single = new_conf_change_single(2, ConfChangeType::AddLearnerNode);
    test_cases.push((
        Box::new(conf_change_v2(vec![single])),
        conf_state(vec![1], vec![2]),
        None,
    ));

    // We can ask explicitly for joint consensus if we want it.
    let single = new_conf_change_single(2, ConfChangeType::AddLearnerNode);
    let mut cc = conf_change_v2(vec![single]);
    cc.set_transition(ConfChangeTransition::Explicit);
    let cs = conf_state_v2(vec![1], vec![2], vec![1], vec![], false);
    test_cases.push((Box::new(cc), cs, Some(conf_state(vec![1], vec![2]))));

    // Ditto, but with implicit transition (the harness checks this).
    let single = new_conf_change_single(2, ConfChangeType::AddLearnerNode);
    let mut cc = conf_change_v2(vec![single]);
    cc.set_transition(ConfChangeTransition::Implicit);
    let cs = conf_state_v2(vec![1], vec![2], vec![1], vec![], true);
    test_cases.push((Box::new(cc), cs, Some(conf_state(vec![1], vec![2]))));

    // Add a new node and demote n1. This exercises the interesting case in
    // which we really need joint config changes and also need LearnersNext.
    let cc = conf_change_v2(vec![
        new_conf_change_single(2, ConfChangeType::AddNode),
        new_conf_change_single(1, ConfChangeType::AddLearnerNode),
        new_conf_change_single(3, ConfChangeType::AddLearnerNode),
    ]);
    let cs = conf_state_v2(vec![2], vec![3], vec![1], vec![1], true);
    test_cases.push((Box::new(cc), cs, Some(conf_state(vec![2], vec![1, 3]))));

    // Ditto explicit.
    let mut cc = conf_change_v2(vec![
        new_conf_change_single(2, ConfChangeType::AddNode),
        new_conf_change_single(1, ConfChangeType::AddLearnerNode),
        new_conf_change_single(3, ConfChangeType::AddLearnerNode),
    ]);
    cc.set_transition(ConfChangeTransition::Explicit);
    let cs = conf_state_v2(vec![2], vec![3], vec![1], vec![1], false);
    test_cases.push((Box::new(cc), cs, Some(conf_state(vec![2], vec![1, 3]))));

    // Ditto implicit.
    let mut cc = conf_change_v2(vec![
        new_conf_change_single(2, ConfChangeType::AddNode),
        new_conf_change_single(1, ConfChangeType::AddLearnerNode),
        new_conf_change_single(3, ConfChangeType::AddLearnerNode),
    ]);
    cc.set_transition(ConfChangeTransition::Implicit);
    let cs = conf_state_v2(vec![2], vec![3], vec![1], vec![1], true);
    test_cases.push((Box::new(cc), cs, Some(conf_state(vec![2], vec![1, 3]))));

    for (cc, exp, exp2) in test_cases {
        let s = new_storage();
        let mut raw_node = new_raw_node(1, vec![1], 10, 1, s.clone(), &l);
        raw_node.campaign().unwrap();
        let mut proposed = false;
        let mut ccdata = vec![];
        // Propose the ConfChange, wait until it applies, save the resulting ConfState.
        let mut cs = None;
        while cs.is_none() {
            let rd = raw_node.ready();
            s.wl().append(rd.entries()).unwrap();
            for e in rd.committed_entries.as_ref().unwrap() {
                if e.get_entry_type() == EntryType::EntryConfChange {
                    let mut cc = ConfChange::default();
                    cc.merge_from_bytes(e.get_data()).unwrap();
                    cs = Some(raw_node.apply_conf_change(&cc).unwrap());
                } else if e.get_entry_type() == EntryType::EntryConfChangeV2 {
                    let mut cc = ConfChangeV2::default();
                    cc.merge_from_bytes(e.get_data()).unwrap();
                    cs = Some(raw_node.apply_conf_change(&cc).unwrap());
                }
            }
            let is_leader = rd.ss().map_or(false, |ss| ss.leader_id == raw_node.raft.id);
            raw_node.advance(rd);
            // Once we are the leader, propose a command and a ConfChange.
            if !proposed && is_leader {
                raw_node.propose(vec![], b"somedata".to_vec()).unwrap();

                if let Some(v1) = cc.as_v1() {
                    ccdata = v1.write_to_bytes().unwrap();
                    raw_node.propose_conf_change(vec![], v1.clone()).unwrap();
                } else {
                    let v2 = cc.as_v2().clone().into_owned();
                    ccdata = v2.write_to_bytes().unwrap();
                    raw_node.propose_conf_change(vec![], v2).unwrap();
                }

                proposed = true;
            }
        }

        // Check that the last index is exactly the conf change we put in,
        // down to the bits. Note that this comes from the Storage, which
        // will not reflect any unstable entries that we'll only be presented
        // with in the next Ready.
        let last_index = s.last_index().unwrap();
        let entries = s.entries(last_index - 1, last_index + 1, NO_LIMIT).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].get_data(), b"somedata");
        if cc.as_v1().is_some() {
            assert_eq!(entries[1].get_entry_type(), EntryType::EntryConfChange);
        } else {
            assert_eq!(entries[1].get_entry_type(), EntryType::EntryConfChangeV2);
        }
        assert_eq!(ccdata, entries[1].get_data());
        assert_eq!(exp, cs.unwrap());

        let conf_index = if cc.as_v2().enter_joint() == Some(true) {
            // If this is an auto-leaving joint conf change, it will have
            // appended the entry that auto-leaves, so add one to the last
            // index that forms the basis of our expectations on
            // pendingConfIndex. (Recall that lastIndex was taken from stable
            // storage, but this auto-leaving entry isn't on stable storage
            // yet).
            last_index + 1
        } else {
            last_index
        };
        assert_eq!(conf_index, raw_node.raft.pending_conf_index);

        // Move the RawNode along. If the ConfChange was simple, nothing else
        // should happen. Otherwise, we're in a joint state, which is either
        // left automatically or not. If not, we add the proposal that leaves
        // it manually.
        let mut rd = raw_node.ready();
        let mut context = vec![];
        if !exp.auto_leave {
            assert!(rd.entries().is_empty());
            if exp2.is_none() {
                continue;
            }
            context = b"manual".to_vec();
            let mut cc = conf_change_v2(vec![]);
            cc.set_context(context.clone());
            raw_node.propose_conf_change(vec![], cc).unwrap();
            rd = raw_node.ready();
        }

        // Check that the right ConfChange comes out.
        assert_eq!(rd.entries().len(), 1);
        assert_eq!(
            rd.entries()[0].get_entry_type(),
            EntryType::EntryConfChangeV2
        );
        let mut leave_cc = ConfChangeV2::default();
        leave_cc
            .merge_from_bytes(rd.entries()[0].get_data())
            .unwrap();
        assert_eq!(context, leave_cc.get_context(), "{:?}", cc.as_v2());
        // Lie and pretend the ConfChange applied. It won't do so because now
        // we require the joint quorum and we're only running one node.
        let cs = raw_node.apply_conf_change(&leave_cc).unwrap();
        assert_eq!(cs, exp2.unwrap());
    }
}

/// Tests the configuration change auto leave even leader lost leadership.
#[test]
fn test_raw_node_joint_auto_leave() {
    let l = default_logger();

    let single = new_conf_change_single(2, ConfChangeType::AddLearnerNode);
    let mut test_cc = conf_change_v2(vec![single]);
    test_cc.set_transition(ConfChangeTransition::Implicit);
    let exp_cs = conf_state_v2(vec![1], vec![2], vec![1], vec![], true);
    let exp_cs2 = conf_state(vec![1], vec![2]);

    let s = new_storage();
    let mut raw_node = new_raw_node(1, vec![1], 10, 1, s.clone(), &l);
    raw_node.campaign().unwrap();
    let mut proposed = false;
    let ccdata = test_cc.write_to_bytes().unwrap();
    // Propose the ConfChange, wait until it applies, save the resulting ConfState.
    let mut cs = None;
    while cs.is_none() {
        let rd = raw_node.ready();
        s.wl().append(rd.entries()).unwrap();
        for e in rd.committed_entries.as_ref().unwrap() {
            if e.get_entry_type() == EntryType::EntryConfChangeV2 {
                let mut cc = ConfChangeV2::default();
                cc.merge_from_bytes(e.get_data()).unwrap();

                // Force it step down.
                let mut msg = new_message(1, 1, MessageType::MsgHeartbeatResponse, 0);
                msg.term = raw_node.raft.term + 1;
                raw_node.step(msg).unwrap();

                cs = Some(raw_node.apply_conf_change(&cc).unwrap());
            }
        }
        let is_leader = rd.ss().map_or(false, |ss| ss.leader_id == raw_node.raft.id);
        raw_node.advance(rd);
        // Once we are the leader, propose a command and a ConfChange.
        if !proposed && is_leader {
            raw_node.propose(vec![], b"somedata".to_vec()).unwrap();
            raw_node
                .propose_conf_change(vec![], test_cc.clone())
                .unwrap();
            proposed = true;
        }
    }

    // Check that the last index is exactly the conf change we put in,
    // down to the bits. Note that this comes from the Storage, which
    // will not reflect any unstable entries that we'll only be presented
    // with in the next Ready.
    let last_index = s.last_index().unwrap();
    let entries = s.entries(last_index - 1, last_index + 1, NO_LIMIT).unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].get_data(), b"somedata");
    assert_eq!(entries[1].get_entry_type(), EntryType::EntryConfChangeV2);
    assert_eq!(ccdata, entries[1].get_data());
    assert_eq!(exp_cs, cs.unwrap());
    assert_eq!(0, raw_node.raft.pending_conf_index);

    // Move the RawNode along. It should not leave joint because it's follower.
    let mut rd = raw_node.ready();
    assert!(rd.entries().is_empty());

    // Make it leader again. It should leave joint automatically after moving apply index.
    raw_node.campaign().unwrap();
    rd = raw_node.ready();
    s.wl().append(rd.entries()).unwrap();
    raw_node.advance(rd);
    rd = raw_node.ready();
    s.wl().append(rd.entries()).unwrap();

    // Check that the right ConfChange comes out.
    assert_eq!(rd.entries().len(), 1);
    assert_eq!(
        rd.entries()[0].get_entry_type(),
        EntryType::EntryConfChangeV2
    );
    let mut leave_cc = ConfChangeV2::default();
    leave_cc
        .merge_from_bytes(rd.entries()[0].get_data())
        .unwrap();
    assert!(leave_cc.get_context().is_empty());
    // Lie and pretend the ConfChange applied. It won't do so because now
    // we require the joint quorum and we're only running one node.
    let cs = raw_node.apply_conf_change(&leave_cc).unwrap();
    assert_eq!(cs, exp_cs2);
}

/// Ensures that two proposes to add the same node should not affect the later propose
/// to add new node.
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
                raw_node.apply_conf_change(&conf_change).unwrap();
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
        if rd.ss().map_or(false, |ss| ss.leader_id == raw_node.raft.id) {
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

    assert_eq!(
        rd.committed_entries.as_ref().unwrap().len(),
        1,
        "should committed the conf change entry"
    );

    let e = &rd.committed_entries.as_ref().unwrap()[0];
    assert_eq!(e.get_entry_type(), EntryType::EntryConfChange);
    let mut conf_change = ConfChange::default();
    conf_change.merge_from_bytes(&e.data).unwrap();
    let conf_state = raw_node.apply_conf_change(&conf_change)?;
    assert_eq!(conf_state.voters, vec![1]);
    assert_eq!(conf_state.learners, vec![2]);

    Ok(())
}

/// Ensures that RawNode.read_index sends the MsgReadIndex message to the underlying
/// raft. It also ensures that ReadState can be read out.
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

/// Ensures that a node can be started correctly. Note that RawNode requires the
/// application to bootstrap the state, i.e. it does not accept peers and will not
/// create faux configuration change entries.
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
