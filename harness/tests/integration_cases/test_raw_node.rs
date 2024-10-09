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

#[allow(clippy::too_many_arguments)]
fn must_cmp_ready(
    r: &Ready,
    ss: &Option<SoftState>,
    hs: &Option<HardState>,
    entries: &[Entry],
    committed_entries: &[Entry],
    snapshot: &Option<Snapshot>,
    msg_is_empty: bool,
    persisted_msg_is_empty: bool,
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
    assert_eq!(r.persisted_messages().is_empty(), persisted_msg_is_empty);
}

fn new_raw_node(
    id: u64,
    peers: Vec<u64>,
    election_tick: usize,
    heartbeat_tick: usize,
    storage: MemStorage,
    logger: &Logger,
) -> RawNode<MemStorage> {
    let config = new_test_config(id, election_tick, heartbeat_tick);
    new_raw_node_with_config(peers, &config, storage, logger)
}

fn new_raw_node_with_config(
    peers: Vec<u64>,
    config: &Config,
    storage: MemStorage,
    logger: &Logger,
) -> RawNode<MemStorage> {
    if storage.initial_state().unwrap().initialized() && peers.is_empty() {
        panic!("new_raw_node with empty peers on initialized store");
    }
    if !peers.is_empty() && !storage.initial_state().unwrap().initialized() {
        storage
            .wl()
            .apply_snapshot(new_snapshot(1, 1, peers))
            .unwrap();
    }
    RawNode::new(config, storage, logger).unwrap()
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
    test_entries.data = (b"testdata" as &'static [u8]).into();

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
    let mut test_cases: Vec<(Box<dyn ConfChangeI>, _, _)> = vec![
        // V1 config change.
        (
            Box::new(conf_change(ConfChangeType::AddNode, 2)),
            conf_state(vec![1, 2], vec![]),
            None,
        ),
    ];

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
            let mut rd = raw_node.ready();
            s.wl().append(rd.entries()).unwrap();
            let mut handle_committed_entries =
                |rn: &mut RawNode<MemStorage>, committed_entries: Vec<Entry>| {
                    for e in committed_entries {
                        if e.get_entry_type() == EntryType::EntryConfChange {
                            let mut cc = ConfChange::default();
                            cc.merge_from_bytes(e.get_data()).unwrap();
                            cs = Some(rn.apply_conf_change(&cc).unwrap());
                        } else if e.get_entry_type() == EntryType::EntryConfChangeV2 {
                            let mut cc = ConfChangeV2::default();
                            cc.merge_from_bytes(e.get_data()).unwrap();
                            cs = Some(rn.apply_conf_change(&cc).unwrap());
                        }
                    }
                };
            handle_committed_entries(&mut raw_node, rd.take_committed_entries());
            let is_leader = rd.ss().map_or(false, |ss| ss.leader_id == raw_node.raft.id);

            let mut light_rd = raw_node.advance(rd);
            handle_committed_entries(&mut raw_node, light_rd.take_committed_entries());
            raw_node.advance_apply();

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
        let entries = s
            .entries(
                last_index - 1,
                last_index + 1,
                NO_LIMIT,
                GetEntriesContext::empty(false),
            )
            .unwrap();
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
            cc.set_context(context.clone().into());
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
        let mut rd = raw_node.ready();
        s.wl().append(rd.entries()).unwrap();
        let mut handle_committed_entries =
            |rn: &mut RawNode<MemStorage>, committed_entries: Vec<Entry>| {
                for e in committed_entries {
                    if e.get_entry_type() == EntryType::EntryConfChangeV2 {
                        let mut cc = ConfChangeV2::default();
                        cc.merge_from_bytes(e.get_data()).unwrap();

                        // Force it step down.
                        let mut msg = new_message(1, 1, MessageType::MsgHeartbeatResponse, 0);
                        msg.term = rn.raft.term + 1;
                        rn.step(msg).unwrap();

                        cs = Some(rn.apply_conf_change(&cc).unwrap());
                    }
                }
            };
        handle_committed_entries(&mut raw_node, rd.take_committed_entries());
        let is_leader = rd.ss().map_or(false, |ss| ss.leader_id == raw_node.raft.id);

        let mut light_rd = raw_node.advance(rd);
        handle_committed_entries(&mut raw_node, light_rd.take_committed_entries());
        raw_node.advance_apply();

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
    let entries = s
        .entries(
            last_index - 1,
            last_index + 1,
            NO_LIMIT,
            GetEntriesContext::empty(false),
        )
        .unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].get_data(), b"somedata");
    assert_eq!(entries[1].get_entry_type(), EntryType::EntryConfChangeV2);
    assert_eq!(ccdata, entries[1].get_data());
    assert_eq!(exp_cs, cs.unwrap());
    assert_eq!(0, raw_node.raft.pending_conf_index);

    // Move the RawNode along. It should not leave joint because it's follower.
    let mut rd = raw_node.ready();
    assert!(rd.entries().is_empty());
    let _ = raw_node.advance(rd);

    // Make it leader again. It should leave joint automatically after moving apply index.
    raw_node.campaign().unwrap();
    rd = raw_node.ready();
    s.wl().append(rd.entries()).unwrap();
    let _ = raw_node.advance(rd);

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
        s.wl().append(rd.entries()).unwrap();
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
                        rn.apply_conf_change(&conf_change).unwrap();
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
    let mut entries = s
        .entries(
            last_index - 2,
            last_index + 1,
            None,
            GetEntriesContext::empty(false),
        )
        .unwrap();
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
    must_cmp_ready(&rd, &None, &None, &[], &[], &None, true, true, false);
    let _ = raw_node.advance(rd);

    raw_node.campaign().expect("");
    loop {
        let rd = raw_node.ready();
        s.wl().append(rd.entries()).unwrap();
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
        s.wl().append(rd.entries()).unwrap();
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

/// Ensures that a node can be started correctly. Note that RawNode requires the
/// application to bootstrap the state, i.e. it does not accept peers and will not
/// create faux configuration change entries.
#[test]
fn test_raw_node_start() {
    let l = default_logger();
    let store = new_storage();
    let mut raw_node = new_raw_node(1, vec![1], 10, 1, store.clone(), &l);

    let rd = raw_node.ready();
    must_cmp_ready(&rd, &None, &None, &[], &[], &None, true, true, false);
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
    must_cmp_ready(
        &rd,
        &None,
        &None,
        &[],
        &entries[..1],
        &None,
        true,
        true,
        false,
    );
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
    must_cmp_ready(&rd, &None, &None, &[], &entries, &None, true, true, false);
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
    test_entries.data = (b"testdata" as &'static [u8]).into();
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
    cc_entry.data = data.into();
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

// TestNodeBoundedLogGrowthWithPartition tests a scenario where a leader is
// partitioned from a quorum of nodes. It verifies that the leader's log is
// protected from unbounded growth even as new entries continue to be proposed.
// This protection is provided by the max_uncommitted_size configuration.
#[test]
fn test_bounded_uncommitted_entries_growth_with_partition() {
    let l = default_logger();
    let config = &Config {
        id: 1,
        max_uncommitted_size: 12,
        ..Config::default()
    };
    let s = new_storage();
    let mut raw_node = new_raw_node_with_config(vec![1], config, s.clone(), &l);

    // wait raw_node to be leader
    raw_node.campaign().unwrap();
    loop {
        let rd = raw_node.ready();
        s.wl().set_hardstate(rd.hs().unwrap().clone());
        s.wl().append(rd.entries()).unwrap();
        if rd
            .ss()
            .map_or(false, |ss| ss.leader_id == raw_node.raft.leader_id)
        {
            let _ = raw_node.advance(rd);
            break;
        }

        let _ = raw_node.advance(rd);
    }

    // should be accepted
    let data = b"hello world!";
    raw_node.propose(vec![], data.to_vec()).unwrap();

    // shoule be dropped
    let result = raw_node.propose(vec![], data.to_vec());
    assert_eq!(result.unwrap_err(), Error::ProposalDropped);

    // should be accepted when previous data has been committed
    let rd = raw_node.ready();
    s.wl().append(rd.entries()).unwrap();
    let _ = raw_node.advance(rd);

    let data = b"hello world!".to_vec();
    raw_node.propose(vec![], data).unwrap();
}

fn prepare_async_entries(raw_node: &mut RawNode<MemStorage>, s: &MemStorage) {
    raw_node.raft.become_candidate();
    raw_node.raft.become_leader();

    let rd = raw_node.ready();
    s.wl().append(rd.entries()).unwrap();
    let _ = raw_node.advance(rd);

    let data: Vec<u8> = vec![1; 1000];
    for _ in 0..10 {
        raw_node.propose(vec![], data.to_vec()).unwrap();
    }

    let rd = raw_node.ready();
    let entries = rd.entries().clone();
    assert_eq!(entries.len(), 10);
    s.wl().append(&entries).unwrap();
    let msgs = rd.messages();
    // First append has two entries: the empty entry to confirm the
    // election, and the first proposal (only one proposal gets sent
    // because we're in probe state).
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].msg_type, MessageType::MsgAppend);
    assert_eq!(msgs[0].entries.len(), 2);
    let _ = raw_node.advance_append(rd);

    s.wl().trigger_log_unavailable(true);

    // Become replicate state
    let mut append_response = new_message(2, 1, MessageType::MsgAppendResponse, 0);
    append_response.set_term(2);
    append_response.set_index(2);
    raw_node.step(append_response).unwrap();
}

// Test entries are handled properly when they are fetched asynchronously
#[test]
fn test_raw_node_with_async_entries() {
    let l = default_logger();
    let mut cfg = new_test_config(1, 10, 1);
    cfg.max_size_per_msg = 2048;
    let s = new_storage();
    let mut raw_node = new_raw_node_with_config(vec![1, 2], &cfg, s.clone(), &l);

    prepare_async_entries(&mut raw_node, &s);

    // No entries are sent because the entries are temporarily unavailable
    let rd = raw_node.ready();
    let entries = rd.entries().clone();
    s.wl().append(&entries).unwrap();
    let msgs = rd.messages();
    assert_eq!(msgs.len(), 0);
    let _ = raw_node.advance_append(rd);

    // Entries are sent when the entries are ready which is informed by `on_entries_fetched`.
    s.wl().trigger_log_unavailable(false);
    let context = s.wl().take_get_entries_context().unwrap();
    raw_node.on_entries_fetched(context);
    let rd = raw_node.ready();
    let entries = rd.entries().clone();
    s.wl().append(&entries).unwrap();
    let msgs = rd.messages();
    assert_eq!(msgs.len(), 5);
    assert_eq!(msgs[0].msg_type, MessageType::MsgAppend);
    assert_eq!(msgs[0].entries.len(), 2);
    let _ = raw_node.advance_append(rd);
}

// Test if async fetch entries works well when there is a remove node conf-change.
#[test]
fn test_raw_node_with_async_entries_to_removed_node() {
    let l = default_logger();
    let mut cfg = new_test_config(1, 10, 1);
    cfg.max_size_per_msg = 2048;
    let s = new_storage();
    let mut raw_node = new_raw_node_with_config(vec![1, 2], &cfg, s.clone(), &l);

    prepare_async_entries(&mut raw_node, &s);

    raw_node.apply_conf_change(&remove_node(2)).unwrap();

    // Entries are not sent due to the node is removed.
    s.wl().trigger_log_unavailable(false);
    let context = s.wl().take_get_entries_context().unwrap();
    raw_node.on_entries_fetched(context);
    let rd = raw_node.ready();
    assert_eq!(rd.entries().len(), 0);
    assert_eq!(rd.messages().len(), 0);
    let _ = raw_node.advance_append(rd);
}

// Test if async fetch entries works well when there is a leader step-down.
#[test]
fn test_raw_node_with_async_entries_on_follower() {
    let l = default_logger();
    let mut cfg = new_test_config(1, 10, 1);
    cfg.max_size_per_msg = 2048;
    let s = new_storage();
    let mut raw_node = new_raw_node_with_config(vec![1, 2], &cfg, s.clone(), &l);

    prepare_async_entries(&mut raw_node, &s);

    // Set recent inactive to step down leader
    raw_node.raft.mut_prs().get_mut(2).unwrap().recent_active = false;
    let mut msg = Message::new();
    msg.set_to(1);
    msg.set_msg_type(MessageType::MsgCheckQuorum);
    raw_node.raft.step(msg).unwrap();
    assert_ne!(raw_node.raft.state, StateRole::Leader);

    // Entries are not sent due to the leader is changed.
    s.wl().trigger_log_unavailable(false);
    let context = s.wl().take_get_entries_context().unwrap();
    raw_node.on_entries_fetched(context);
    let rd = raw_node.ready();
    assert_eq!(rd.entries().len(), 0);
    assert_eq!(rd.messages().len(), 0);
    let _ = raw_node.advance_append(rd);
}

#[test]
fn test_raw_node_async_entries_with_leader_change() {
    let l = default_logger();
    let mut cfg = new_test_config(1, 10, 1);
    cfg.max_size_per_msg = 2048;
    let s = new_storage();
    let mut raw_node = new_raw_node_with_config(vec![1, 2], &cfg, s.clone(), &l);

    raw_node.raft.become_candidate();
    raw_node.raft.become_leader();

    let rd = raw_node.ready();
    s.wl().append(rd.entries()).unwrap();
    let _ = raw_node.advance(rd);

    let data: Vec<u8> = vec![1; 1000];
    for _ in 0..10 {
        raw_node.propose(vec![], data.to_vec()).unwrap();
    }

    let rd = raw_node.ready();
    let entries = rd.entries().clone();
    assert_eq!(entries.len(), 10);
    s.wl().append(&entries).unwrap();
    let msgs = rd.messages();
    // First append has two entries: the empty entry to confirm the
    // election, and the first proposal (only one proposal gets sent
    // because we're in probe state).
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].msg_type, MessageType::MsgAppend);
    assert_eq!(msgs[0].entries.len(), 2);
    let _ = raw_node.advance_append(rd);

    s.wl().trigger_log_unavailable(true);

    // Become replicate state
    let mut append_response = new_message(2, 1, MessageType::MsgAppendResponse, 0);
    append_response.set_term(2);
    append_response.set_index(2);
    raw_node.step(append_response).unwrap();

    raw_node.raft.become_follower(raw_node.raft.term + 1, 2);
    raw_node.raft.become_candidate();
    raw_node.raft.become_leader();

    // Entries are not sent due to the leadership or the term is changed.
    s.wl().trigger_log_unavailable(false);
    let context = s.wl().take_get_entries_context().unwrap();
    raw_node.on_entries_fetched(context);
    let rd = raw_node.ready();
    assert_eq!(rd.entries().len(), 1); // no-op entry
    assert_eq!(rd.messages().len(), 0);
    let _ = raw_node.advance_append(rd);
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
    s.wl().append(rd.entries()).unwrap();
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
        must_cmp_ready(&rd, &None, &None, &entries, &[], &None, true, true, true);

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

    let mut entries = vec![];
    for i in 2..20 {
        entries.push(new_entry(2, i, Some("hello")));
    }
    let mut append_msg = new_message_with_entries(2, 1, MessageType::MsgAppend, entries.to_vec());
    append_msg.set_term(2);
    append_msg.set_index(1);
    append_msg.set_log_term(1);
    append_msg.set_commit(5);
    raw_node.step(append_msg).unwrap();

    let rd = raw_node.ready();
    must_cmp_ready(
        &rd,
        &Some(soft_state(2, StateRole::Follower)),
        &Some(hard_state(2, 5, 0)),
        &entries,
        &[],
        &None,
        true,
        false,
        true,
    );
    s.wl().set_hardstate(rd.hs().unwrap().clone());
    s.wl().append(rd.entries()).unwrap();
    let light_rd = raw_node.advance(rd);
    assert_eq!(light_rd.commit_index(), None);
    assert_eq!(light_rd.committed_entries().as_slice(), &entries[..4]);
    assert!(light_rd.messages().is_empty());

    let snapshot = new_snapshot(10, 3, vec![1, 2]);
    let mut snapshot_msg = new_message(2, 1, MessageType::MsgSnapshot, 0);
    snapshot_msg.set_term(3);
    snapshot_msg.set_snapshot(snapshot.clone());
    raw_node.step(snapshot_msg).unwrap();

    let mut entries = vec![];
    for i in 11..14 {
        entries.push(new_entry(3, i, Some("hello")));
    }
    let mut append_msg = new_message_with_entries(2, 1, MessageType::MsgAppend, entries.to_vec());
    append_msg.set_term(3);
    append_msg.set_index(10);
    append_msg.set_log_term(3);
    append_msg.set_commit(12);
    raw_node.step(append_msg).unwrap();

    let rd = raw_node.ready();
    // If there is a snapshot, the committed entries should be empty.
    must_cmp_ready(
        &rd,
        &None,
        &Some(hard_state(3, 12, 0)),
        &entries,
        &[],
        &Some(snapshot),
        true,
        false,
        true,
    );
    // Should have a MsgAppendResponse
    assert_eq!(
        rd.persisted_messages()[0].get_msg_type(),
        MessageType::MsgAppendResponse
    );
    s.wl().set_hardstate(rd.hs().unwrap().clone());
    s.wl().apply_snapshot(rd.snapshot().clone()).unwrap();
    s.wl().append(rd.entries()).unwrap();

    let light_rd = raw_node.advance(rd);
    assert_eq!(light_rd.commit_index(), None);
    assert_eq!(light_rd.committed_entries().as_slice(), &entries[..2]);
    assert!(light_rd.messages().is_empty());
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
        false,
        true,
    );
    // Should have a MsgAppendResponse
    assert_eq!(
        rd.persisted_messages()[0].get_msg_type(),
        MessageType::MsgAppendResponse
    );
    s.wl().set_hardstate(rd.hs().unwrap().clone());
    s.wl().append(rd.entries()).unwrap();

    let light_rd = raw_node.advance(rd);
    assert_eq!(light_rd.commit_index(), None);
    assert!(light_rd.committed_entries().is_empty());
    assert!(light_rd.messages().is_empty());

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
        false,
        true,
    );
    // Should have a MsgAppendResponse
    assert_eq!(
        rd.persisted_messages()[0].get_msg_type(),
        MessageType::MsgAppendResponse
    );
    s.wl().set_hardstate(rd.hs().unwrap().clone());
    s.wl().append(rd.entries()).unwrap();

    let light_rd = raw_node.advance(rd);
    assert_eq!(light_rd.commit_index(), None);
    assert_eq!(light_rd.committed_entries().as_slice(), &entries_2[..2]);
    assert!(light_rd.messages().is_empty());
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
    s.wl().append(rd.entries()).unwrap();
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
        must_cmp_ready(&rd, &None, &None, &entries, &[], &None, false, true, true);
        for msg in rd.take_messages() {
            assert_eq!(msg.get_msg_type(), MessageType::MsgAppend);
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
    assert!(rd.persisted_messages().is_empty());
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
    must_cmp_ready(&rd, &None, &None, &entries, &[], &None, false, true, true);
    for msg in rd.take_messages() {
        assert_eq!(msg.get_msg_type(), MessageType::MsgAppend);
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
        true,
        false,
    );
    for msg in rd.take_messages() {
        assert_eq!(msg.get_msg_type(), MessageType::MsgAppend);
        assert_eq!(msg.get_commit(), first_index + 9);
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
/// some append msg and snapshot.
#[test]
fn test_async_ready_follower() {
    let l = default_logger();
    let s = new_storage();
    s.wl()
        .apply_snapshot(new_snapshot(1, 1, vec![1, 2]))
        .unwrap();

    let mut raw_node = new_raw_node(1, vec![1, 2], 10, 1, s.clone(), &l);
    let mut first_index = 1;
    let mut rd_number = 0;
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
            assert_eq!(rd.number(), rd_number + i + 1);
            assert_eq!(rd.hs(), Some(&hard_state(2, first_index + i * 3 + 3, 0)));
            assert_eq!(rd.entries(), &entries);
            assert_eq!(rd.committed_entries().as_slice(), &[]);
            assert!(rd.messages().is_empty());
            assert_eq!(
                rd.persisted_messages()[0].get_msg_type(),
                MessageType::MsgAppendResponse
            );

            s.wl().set_hardstate(rd.hs().unwrap().clone());
            s.wl().append(rd.entries()).unwrap();
            raw_node.advance_append_async(rd);
        }
        // Unpersisted Ready number in range [1, 10]
        raw_node.on_persist_ready(rd_number + 4);
        let rd = raw_node.ready();
        assert_eq!(rd.hs(), None);
        assert_eq!(
            rd.committed_entries().first().unwrap().get_index(),
            first_index + 1
        );
        assert_eq!(
            rd.committed_entries().last().unwrap().get_index(),
            first_index + 3 * 3 + 3
        );
        assert!(rd.messages().is_empty());
        assert!(rd.persisted_messages().is_empty());

        let light_rd = raw_node.advance_append(rd);
        assert_eq!(light_rd.commit_index(), None);
        assert_eq!(
            light_rd.committed_entries().first().unwrap().get_index(),
            first_index + 3 * 3 + 4
        );
        assert_eq!(
            light_rd.committed_entries().last().unwrap().get_index(),
            first_index + 10 * 3
        );
        assert!(light_rd.messages().is_empty());

        first_index += 10 * 3;
        rd_number += 11;
    }

    let snapshot = new_snapshot(first_index + 5, 2, vec![1, 2]);
    let mut snapshot_msg = new_message(2, 1, MessageType::MsgSnapshot, 0);
    snapshot_msg.set_term(2);
    snapshot_msg.set_snapshot(snapshot.clone());
    raw_node.step(snapshot_msg).unwrap();

    let rd = raw_node.ready();
    assert_eq!(rd.number(), rd_number + 1);
    must_cmp_ready(
        &rd,
        &None,
        &Some(hard_state(2, first_index + 5, 0)),
        &[],
        &[],
        &Some(snapshot.clone()),
        true,
        false,
        true,
    );

    s.wl().set_hardstate(rd.hs().unwrap().clone());
    s.wl().apply_snapshot(snapshot).unwrap();
    s.wl().append(rd.entries()).unwrap();
    raw_node.advance_append_async(rd);

    let mut entries = vec![];
    for i in 1..10 {
        entries.push(new_entry(2, first_index + 5 + i, Some("hello")));
    }
    let mut append_msg = new_message_with_entries(2, 1, MessageType::MsgAppend, entries.to_vec());
    append_msg.set_term(2);
    append_msg.set_index(first_index + 5);
    append_msg.set_log_term(2);
    append_msg.set_commit(first_index + 5 + 3);
    raw_node.step(append_msg).unwrap();

    let rd = raw_node.ready();
    assert_eq!(rd.number(), rd_number + 2);
    must_cmp_ready(
        &rd,
        &None,
        &Some(hard_state(2, first_index + 5 + 3, 0)),
        &entries,
        &[],
        &None,
        true,
        false,
        true,
    );
    s.wl().set_hardstate(rd.hs().unwrap().clone());
    s.wl().append(rd.entries()).unwrap();
    raw_node.advance_append_async(rd);

    raw_node.on_persist_ready(rd_number + 1);
    assert_eq!(raw_node.raft.raft_log.persisted, first_index + 5);
    raw_node.advance_apply_to(first_index + 5);

    raw_node.on_persist_ready(rd_number + 2);

    let rd = raw_node.ready();
    must_cmp_ready(
        &rd,
        &None,
        &None,
        &[],
        &entries[..3],
        &None,
        true,
        true,
        false,
    );
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
        false,
        true,
    );
    s.wl().set_hardstate(rd.hs().unwrap().clone());

    for msg in rd.persisted_messages() {
        assert_eq!(msg.get_msg_type(), MessageType::MsgRequestVote);
    }

    let _ = raw_node.advance_append(rd);

    // Peer 1 should reject to vote to peer 2
    let mut vote_request_2 = new_message(2, 1, MessageType::MsgRequestVote, 0);
    vote_request_2.set_term(6);
    vote_request_2.set_log_term(4);
    vote_request_2.set_index(4);
    raw_node.step(vote_request_2).unwrap();

    let rd = raw_node.ready();
    assert_eq!(rd.number(), 2);
    must_cmp_ready(&rd, &None, &None, &[], &[], &None, true, false, false);
    assert_eq!(
        rd.persisted_messages()[0].get_msg_type(),
        MessageType::MsgRequestVoteResponse
    );
    raw_node.advance_append_async(rd);

    // Peer 1 should reject to vote to peer 3
    let mut vote_request_3 = new_message(3, 1, MessageType::MsgRequestVote, 0);
    vote_request_3.set_term(6);
    vote_request_3.set_log_term(4);
    vote_request_3.set_index(4);
    raw_node.step(vote_request_3).unwrap();

    let rd = raw_node.ready();
    assert_eq!(rd.number(), 3);
    must_cmp_ready(&rd, &None, &None, &[], &[], &None, true, false, false);
    assert_eq!(
        rd.persisted_messages()[0].get_msg_type(),
        MessageType::MsgRequestVoteResponse
    );
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
        true,
    );
    assert_eq!(rd.messages().len(), 2);
    for msg in rd.take_messages().iter() {
        assert_eq!(msg.get_msg_type(), MessageType::MsgAppend);
    }

    s.wl().append(rd.entries()).unwrap();

    s.wl().append(rd.entries()).unwrap();

    let light_rd = raw_node.advance_append(rd);
    assert_eq!(light_rd.commit_index(), None);
    assert!(light_rd.committed_entries().is_empty());
    assert!(light_rd.messages().is_empty());
}

#[test]
fn test_async_ready_multiple_snapshot() {
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

    let mut entries = vec![];
    for i in 11..14 {
        entries.push(new_entry(2, i, Some("hello")));
    }
    let mut append_msg = new_message_with_entries(2, 1, MessageType::MsgAppend, entries.to_vec());
    append_msg.set_term(2);
    append_msg.set_index(10);
    append_msg.set_log_term(2);
    append_msg.set_commit(12);
    raw_node.step(append_msg).unwrap();

    let rd = raw_node.ready();
    assert_eq!(rd.number(), 1);
    // If there is a snapshot, the committed entries should be empty.
    must_cmp_ready(
        &rd,
        &Some(soft_state(2, StateRole::Follower)),
        &Some(hard_state(2, 12, 0)),
        &entries,
        &[],
        &Some(snapshot),
        true,
        false,
        true,
    );
    s.wl().set_hardstate(rd.hs().unwrap().clone());
    s.wl().apply_snapshot(rd.snapshot().clone()).unwrap();
    s.wl().append(rd.entries()).unwrap();

    raw_node.advance_append_async(rd);

    let snapshot = new_snapshot(20, 1, vec![1, 2]);
    let mut snapshot_msg = new_message(2, 1, MessageType::MsgSnapshot, 0);
    snapshot_msg.set_term(2);
    snapshot_msg.set_snapshot(snapshot.clone());
    raw_node.step(snapshot_msg).unwrap();

    raw_node.on_persist_ready(1);

    assert_eq!(raw_node.raft.raft_log.persisted, 13);

    raw_node.advance_apply_to(10);

    let rd = raw_node.ready();
    assert_eq!(rd.number(), 2);
    must_cmp_ready(
        &rd,
        &None,
        &Some(hard_state(2, 20, 0)),
        &[],
        &[],
        &Some(snapshot),
        true,
        false,
        true,
    );
    s.wl().set_hardstate(rd.hs().unwrap().clone());
    s.wl().apply_snapshot(rd.snapshot().clone()).unwrap();

    let light_rd = raw_node.advance_append(rd);
    assert_eq!(light_rd.commit_index(), None);
    assert!(light_rd.committed_entries().is_empty());
    assert!(light_rd.messages().is_empty());

    raw_node.advance_apply_to(20);
}

#[test]
fn test_committed_entries_pagination() {
    let l = default_logger();
    let s = new_storage();
    let mut raw_node = new_raw_node(1, vec![1, 2, 3], 10, 1, s, &l);

    let mut entries = vec![];
    for i in 2..10 {
        entries.push(new_entry(1, i, None));
    }
    let mut msg = new_message_with_entries(3, 1, MessageType::MsgAppend, entries.to_vec());
    msg.set_term(1);
    msg.set_index(1);
    msg.set_log_term(1);
    msg.set_commit(9);
    raw_node.step(msg).unwrap();

    // Test unpersisted entries won't be fetched.
    // NOTE: maybe it's better to allow fetching unpersisted committed entries.
    let rd = raw_node.ready();
    assert!(rd.committed_entries().is_empty());
    assert!(raw_node.has_ready());

    // Persist entries.
    assert!(!rd.entries().is_empty());
    raw_node.store().wl().append(rd.entries()).unwrap();

    // Advance the ready, and we can get committed_entries as expected.
    // Test using 0 as `committed_entries_max_size` works as expected.
    raw_node.raft.set_max_committed_size_per_ready(0);
    let rd = raw_node.advance(rd);
    // `MemStorage::entries` uses `util::limit_size` to limit size of committed entries.
    // So there will be at least one entry.
    assert_eq!(rd.committed_entries().len(), 1);

    // Fetch a `Ready` again without size limit for committed entries.
    assert!(raw_node.has_ready());
    raw_node.raft.set_max_committed_size_per_ready(u64::MAX);
    let rd = raw_node.ready();
    assert_eq!(rd.committed_entries().len(), 7);

    // No more `Ready`s.
    assert!(!raw_node.has_ready());
}

/// Test with `commit_since_index`, committed entries can be fetched correctly after restart.
///
/// Case steps:
/// - Node learns that index 10 is committed
/// - `next_entries` returns entries [2..11) in committed_entries (but index 10 already
///   exceeds maxBytes), which isn't noticed internally by Raft
/// - Commit index gets bumped to 10
/// - The node persists the `HardState`, but crashes before applying the entries
/// - Upon restart, the storage returns the same entries, but `slice` takes a
///   different code path and removes the last entry.
/// - Raft does not emit a HardState, but when the app calls advance(), it bumps
///   its internal applied index cursor to 10 (when it should be 9)
/// - The next `Ready` asks the app to apply index 11 (omitting index 10), losing a
///   write.
#[test]
fn test_committed_entries_pagination_after_restart() {
    let l = default_logger();
    let s = IgnoreSizeHintMemStorage::default();
    s.inner
        .wl()
        .apply_snapshot(new_snapshot(1, 1, vec![1, 2, 3]))
        .unwrap();

    let (mut entries, mut size) = (vec![], 0);
    for i in 2..=10 {
        let e = new_entry(1, i, Some("test data"));
        size += e.compute_size() as u64;
        entries.push(e);
    }
    s.inner.wl().append(&entries).unwrap();
    s.inner.wl().mut_hard_state().commit = 10;

    s.inner
        .wl()
        .append(&[new_entry(1, 11, Some("boom"))])
        .unwrap();

    let config = new_test_config(1, 10, 1);
    let mut raw_node = RawNode::new(&config, s, &l).unwrap();

    // `IgnoreSizeHintMemStorage` will ignore `max_committed_size_per_ready` but
    // `RaftLog::slice won't.`
    raw_node.raft.set_max_committed_size_per_ready(size - 1);

    let mut highest_applied = 1;
    while highest_applied != 11 {
        let mut rd = raw_node.ready();
        let committed_entries = rd.take_committed_entries();
        let next = committed_entries.first().map(|x| x.index).unwrap();
        assert_eq!(highest_applied + 1, next);

        highest_applied = committed_entries.last().map(|x| x.index).unwrap();
        raw_node.raft.raft_log.commit_to(11);
    }
}

#[test]
fn test_disable_proposal_forwarding() {
    let l = default_logger();

    let n1 = new_test_raft_with_config(
        &Config {
            id: 1,
            heartbeat_tick: 1,
            election_tick: 10,
            disable_proposal_forwarding: false,
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
            disable_proposal_forwarding: false,
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

    // send proposal to n2(follower) where DisableProposalForwarding is false
    assert_eq!(
        network
            .peers
            .get_mut(&2)
            .unwrap()
            .step(new_message(2, 2, MessageType::MsgPropose, 1)),
        Ok(())
    );

    // verify n2(follower) does forward the proposal when DisableProposalForwarding is false
    assert_eq!(network.peers.get(&2).unwrap().msgs.len(), 1);

    // send proposal to n3(follower) where DisableProposalForwarding is true
    assert_eq!(
        network
            .peers
            .get_mut(&3)
            .unwrap()
            .step(new_message(3, 3, MessageType::MsgPropose, 1)),
        Err(Error::ProposalDropped)
    );

    assert_eq!(network.peers.get(&3).unwrap().msgs.is_empty(), true);
}

#[derive(Default)]
struct IgnoreSizeHintMemStorage {
    inner: MemStorage,
}

impl Storage for IgnoreSizeHintMemStorage {
    fn initial_state(&self) -> Result<RaftState> {
        self.inner.initial_state()
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        _max_size: impl Into<Option<u64>>,
        context: GetEntriesContext,
    ) -> Result<Vec<Entry>> {
        self.inner.entries(low, high, u64::MAX, context)
    }

    fn term(&self, idx: u64) -> Result<u64> {
        self.inner.term(idx)
    }

    fn first_index(&self) -> Result<u64> {
        self.inner.first_index()
    }

    fn last_index(&self) -> Result<u64> {
        self.inner.last_index()
    }

    fn snapshot(&self, request_index: u64, to: u64) -> Result<Snapshot> {
        self.inner.snapshot(request_index, to)
    }
}
