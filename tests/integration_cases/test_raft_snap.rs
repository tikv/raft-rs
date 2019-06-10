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

use raft::eraftpb::*;
use raft::{Error, ProgressState, INVALID_INDEX};
use test_util::*;

fn testing_snap() -> Snapshot {
    new_snapshot(11, 11, vec![1, 2])
}

#[test]
fn test_sending_snapshot_set_pending_snapshot() {
    setup_for_test();
    let mut sm = new_test_raft(1, vec![1], 10, 1, new_storage());
    sm.restore(testing_snap());

    sm.become_candidate();
    sm.become_leader();

    // force set the next of node 1, so that
    // node 1 needs a snapshot
    sm.mut_prs().get_mut(2).unwrap().next_idx = sm.raft_log.first_index();

    let mut m = new_message(2, 1, MessageType::MsgAppendResponse, 0);
    m.set_index(sm.prs().voters()[&2].next_idx - 1);
    m.set_reject(true);
    sm.step(m).expect("");
    assert_eq!(sm.prs().voters()[&2].pending_snapshot, 11);
}

#[test]
fn test_pending_snapshot_pause_replication() {
    setup_for_test();
    let mut sm = new_test_raft(1, vec![1, 2], 10, 1, new_storage());
    sm.restore(testing_snap());

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
    setup_for_test();
    let mut sm = new_test_raft(1, vec![1, 2], 10, 1, new_storage());
    sm.restore(testing_snap());

    sm.become_candidate();
    sm.become_leader();

    sm.mut_prs().get_mut(2).unwrap().next_idx = 1;
    sm.mut_prs().get_mut(2).unwrap().become_snapshot(11);

    let mut m = new_message(2, 1, MessageType::MsgSnapStatus, 0);
    m.set_reject(true);
    sm.step(m).expect("");
    assert_eq!(sm.prs().voters()[&2].pending_snapshot, 0);
    assert_eq!(sm.prs().voters()[&2].next_idx, 1);
    assert!(sm.prs().voters()[&2].paused);
}

#[test]
fn test_snapshot_succeed() {
    setup_for_test();
    let mut sm = new_test_raft(1, vec![1, 2], 10, 1, new_storage());
    sm.restore(testing_snap());

    sm.become_candidate();
    sm.become_leader();

    sm.mut_prs().get_mut(2).unwrap().next_idx = 1;
    sm.mut_prs().get_mut(2).unwrap().become_snapshot(11);

    let mut m = new_message(2, 1, MessageType::MsgSnapStatus, 0);
    m.set_reject(false);
    sm.step(m).expect("");
    assert_eq!(sm.prs().voters()[&2].pending_snapshot, 0);
    assert_eq!(sm.prs().voters()[&2].next_idx, 12);
    assert!(sm.prs().voters()[&2].paused);
}

#[test]
fn test_snapshot_abort() {
    setup_for_test();
    let mut sm = new_test_raft(1, vec![1, 2], 10, 1, new_storage());
    sm.restore(testing_snap());

    sm.become_candidate();
    sm.become_leader();

    sm.mut_prs().get_mut(2).unwrap().next_idx = 1;
    sm.mut_prs().get_mut(2).unwrap().become_snapshot(11);

    let mut m = new_message(2, 1, MessageType::MsgAppendResponse, 0);
    m.set_index(11);
    // A successful MsgAppendResponse that has a higher/equal index than the
    // pending snapshot should abort the pending snapshot.
    sm.step(m).expect("");
    assert_eq!(sm.prs().voters()[&2].pending_snapshot, 0);
    assert_eq!(sm.prs().voters()[&2].next_idx, 12);
}

#[test]
fn test_request_snapshot() {
    setup_for_test();
    let mut sm = new_test_raft(1, vec![1, 2], 10, 1, new_storage());
    sm.restore(testing_snap());

    // Raft can not step request snapshot if there is no leader.
    let m = new_message(2, 1, MessageType::MsgRequestSnapshot, 0);
    assert_eq!(sm.step(m).unwrap_err(), Error::RequestSnapshotDropped);

    sm.become_candidate();
    sm.become_leader();

    // Raft can not step request snapshot if itself is a leader.
    let m = new_message(1, 1, MessageType::MsgRequestSnapshot, 0);
    assert_eq!(sm.step(m).unwrap_err(), Error::RequestSnapshotDropped);

    // Advance matched.
    let mut m = new_message(2, 1, MessageType::MsgAppendResponse, 0);
    m.set_index(11);
    sm.step(m).unwrap();
    assert_eq!(sm.prs().voters()[&2].state, ProgressState::Replicate);

    // Ignore out of order request snapshot messages.
    let mut m = new_message(2, 1, MessageType::MsgAppendResponse, 0);
    m.set_index(9);
    m.set_reject(true);
    m.set_reject_hint(INVALID_INDEX);
    sm.step(m.clone()).unwrap();
    assert_eq!(sm.prs().voters()[&2].state, ProgressState::Replicate);

    // Request snapshot.
    m.set_index(11);
    sm.step(m.clone()).unwrap();
    assert_eq!(sm.prs().voters()[&2].state, ProgressState::Probe);
    // Become snapshot if it receives a request snapshot messages again.
    sm.step(m.clone()).unwrap();
    assert_eq!(sm.prs().voters()[&2].state, ProgressState::Snapshot);
    assert_eq!(sm.prs().voters()[&2].pending_snapshot, 11);
    assert_eq!(sm.prs().voters()[&2].next_idx, 1);
    assert!(sm.prs().voters()[&2].is_paused());
    let snap = sm.msgs.pop().unwrap();
    assert_eq!(snap.get_msg_type(), MessageType::MsgSnapshot, "{:?}", snap);

    // Append/heartbeats does not set the state from snapshot to probe.
    let mut m = new_message(2, 1, MessageType::MsgAppendResponse, 0);
    m.set_index(11);
    sm.step(m).unwrap();
    assert_eq!(sm.prs().voters()[&2].state, ProgressState::Snapshot);
    assert_eq!(sm.prs().voters()[&2].pending_snapshot, 11);
    assert_eq!(sm.prs().voters()[&2].next_idx, 12);
    assert!(sm.prs().voters()[&2].is_paused());

    // However snapshot status report does set the stat to probe.
    let m = new_message(2, 1, MessageType::MsgSnapStatus, 0);
    sm.step(m).unwrap();
    assert_eq!(sm.prs().voters()[&2].state, ProgressState::Probe);
    assert_eq!(sm.prs().voters()[&2].pending_snapshot, 0);
    assert_eq!(sm.prs().voters()[&2].next_idx, 12);
    assert!(sm.prs().voters()[&2].is_paused());
}
