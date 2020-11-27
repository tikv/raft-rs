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

use crate::test_util::*;
use harness::Network;
use raft::eraftpb::*;
use raft::{default_logger, Error, ProgressState, INVALID_INDEX};

fn testing_snap() -> Snapshot {
    new_snapshot(11, 11, vec![1, 2])
}

#[test]
fn test_sending_snapshot_set_pending_snapshot() {
    let l = default_logger();
    let mut sm = new_test_raft(1, vec![1, 2], 10, 1, new_storage(), &l);
    sm.restore(testing_snap());
    sm.persist();

    sm.become_candidate();
    sm.become_leader();

    // force set the next of node 1, so that
    // node 1 needs a snapshot
    sm.mut_prs().get_mut(2).unwrap().next_idx = sm.raft_log.first_index();

    let mut m = new_message(2, 1, MessageType::MsgAppendResponse, 0);
    {
        let voter_2 = sm.prs().get(2).unwrap();
        m.index = voter_2.next_idx - 1;
        m.reject = true;
    };
    sm.step(m).expect("");
    assert_eq!(sm.prs().get(2).unwrap().pending_snapshot, 11);
}

#[test]
fn test_pending_snapshot_pause_replication() {
    let l = default_logger();
    let mut sm = new_test_raft(1, vec![1, 2], 10, 1, new_storage(), &l);
    sm.restore(testing_snap());
    sm.persist();

    sm.become_candidate();
    sm.become_leader();
    sm.mut_prs().get_mut(2).unwrap().become_snapshot(11);

    sm.step(new_message(1, 1, MessageType::MsgPropose, 1))
        .expect("");
    let msgs = sm.read_messages();
    assert!(msgs.is_empty());
}

#[test]
fn test_snapshot_failure() {
    let l = default_logger();
    let mut sm = new_test_raft(1, vec![1, 2], 10, 1, new_storage(), &l);
    sm.restore(testing_snap());
    sm.persist();

    sm.become_candidate();
    sm.become_leader();

    sm.mut_prs().get_mut(2).unwrap().next_idx = 1;
    sm.mut_prs().get_mut(2).unwrap().become_snapshot(11);

    let mut m = new_message(2, 1, MessageType::MsgSnapStatus, 0);
    m.reject = true;
    sm.step(m).expect("");
    let voter_2 = sm.prs().get(2).unwrap();
    assert_eq!(voter_2.pending_snapshot, 0);
    assert_eq!(voter_2.next_idx, 1);
    assert!(voter_2.paused);
}

#[test]
fn test_snapshot_succeed() {
    let l = default_logger();
    let mut sm = new_test_raft(1, vec![1, 2], 10, 1, new_storage(), &l);
    sm.restore(testing_snap());
    sm.persist();

    sm.become_candidate();
    sm.become_leader();

    sm.mut_prs().get_mut(2).unwrap().next_idx = 1;
    sm.mut_prs().get_mut(2).unwrap().become_snapshot(11);

    let mut m = new_message(2, 1, MessageType::MsgSnapStatus, 0);
    m.reject = false;
    sm.step(m).expect("");
    let voter_2 = sm.prs().get(2).unwrap();
    assert_eq!(voter_2.pending_snapshot, 0);
    assert_eq!(voter_2.next_idx, 12);
    assert!(voter_2.paused);
}

#[test]
fn test_snapshot_abort() {
    let l = default_logger();
    let mut sm = new_test_raft(1, vec![1, 2], 10, 1, new_storage(), &l);
    sm.restore(testing_snap());
    sm.persist();

    sm.become_candidate();
    sm.become_leader();

    sm.mut_prs().get_mut(2).unwrap().next_idx = 1;
    sm.mut_prs().get_mut(2).unwrap().become_snapshot(11);

    let mut m = new_message(2, 1, MessageType::MsgAppendResponse, 0);
    m.index = 11;
    // A successful MsgAppendResponse that has a higher/equal index than the
    // pending snapshot should abort the pending snapshot.
    sm.step(m).expect("");
    assert_eq!(sm.prs().get(2).unwrap().pending_snapshot, 0);
    assert_eq!(sm.prs().get(2).unwrap().next_idx, 12);
}

// Initialized storage should be at term 1 instead of 0. Otherwise the case will fail.
#[test]
fn test_snapshot_with_min_term() {
    let l = default_logger();
    let do_test = |pre_vote: bool| {
        let s = new_storage();
        s.wl()
            .apply_snapshot(new_snapshot(1, 1, vec![1, 2]))
            .unwrap();
        let n1 = new_test_raft_with_prevote(1, vec![1, 2], 10, 1, s, pre_vote, &l);
        let n2 = new_test_raft_with_prevote(2, vec![], 10, 1, new_storage(), pre_vote, &l);
        let mut nt = Network::new(vec![Some(n1), Some(n2)], &l);
        nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
        // 1 will be elected as leader, and then send a snapshot and an empty entry to 2.
        assert_eq!(nt.peers[&2].raft_log.first_index(), 2);
        assert_eq!(nt.peers[&2].raft_log.last_index(), 2);
    };
    do_test(true);
    do_test(false);
}

#[test]
fn test_request_snapshot() {
    let l = default_logger();
    let mut sm = new_test_raft(1, vec![1, 2], 10, 1, new_storage(), &l);
    sm.restore(testing_snap());
    sm.persist();

    // Raft can not step request snapshot if there is no leader.
    assert_eq!(
        sm.raft
            .as_mut()
            .unwrap()
            .request_snapshot(INVALID_INDEX + 1)
            .unwrap_err(),
        Error::RequestSnapshotDropped
    );

    sm.become_candidate();
    sm.become_leader();

    // Raft can not step request snapshot if itself is a leader.
    assert_eq!(
        sm.raft
            .as_mut()
            .unwrap()
            .request_snapshot(INVALID_INDEX + 1)
            .unwrap_err(),
        Error::RequestSnapshotDropped
    );

    // Advance matched.
    let mut m = new_message(2, 1, MessageType::MsgAppendResponse, 0);
    m.index = 11;
    sm.step(m).unwrap();
    assert_eq!(sm.prs().get(2).unwrap().state, ProgressState::Replicate);

    let request_snapshot_idx = sm.raft_log.committed;
    let mut m = new_message(2, 1, MessageType::MsgAppendResponse, 0);
    m.index = 11;
    m.reject = true;
    m.reject_hint = INVALID_INDEX;
    m.request_snapshot = request_snapshot_idx;

    // Ignore out of order request snapshot messages.
    let mut out_of_order = m.clone();
    out_of_order.index = 9;
    sm.step(out_of_order).unwrap();
    assert_eq!(sm.prs().get(2).unwrap().state, ProgressState::Replicate);

    // Request snapshot.
    sm.step(m).unwrap();
    assert_eq!(sm.prs().get(2).unwrap().state, ProgressState::Snapshot);
    assert_eq!(sm.prs().get(2).unwrap().pending_snapshot, 11);
    assert_eq!(sm.prs().get(2).unwrap().next_idx, 12);
    assert!(sm.prs().get(2).unwrap().is_paused());
    let snap = sm.msgs.pop().unwrap();
    assert!(
        snap.get_msg_type() == MessageType::MsgSnapshot
            && snap.get_snapshot().get_metadata().index == request_snapshot_idx,
        "{:?}",
        snap
    );

    // Append/heartbeats does not set the state from snapshot to probe.
    let mut m = new_message(2, 1, MessageType::MsgAppendResponse, 0);
    m.index = 11;
    sm.step(m).unwrap();
    assert_eq!(sm.prs().get(2).unwrap().state, ProgressState::Snapshot);
    assert_eq!(sm.prs().get(2).unwrap().pending_snapshot, 11);
    assert_eq!(sm.prs().get(2).unwrap().next_idx, 12);
    assert!(sm.prs().get(2).unwrap().is_paused());

    // However snapshot status report does set the stat to probe.
    let m = new_message(2, 1, MessageType::MsgSnapStatus, 0);
    sm.step(m).unwrap();
    assert_eq!(sm.prs().get(2).unwrap().state, ProgressState::Probe);
    assert_eq!(sm.prs().get(2).unwrap().pending_snapshot, 0);
    assert_eq!(sm.prs().get(2).unwrap().next_idx, 12);
    assert!(sm.prs().get(2).unwrap().is_paused());
}
