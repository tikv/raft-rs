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
use crate::testing_logger;
use harness::Network;
use raft::eraftpb::*;

fn testing_snap() -> Snapshot {
    new_snapshot(11, 11, vec![1, 2])
}

#[test]
fn test_sending_snapshot_set_pending_snapshot() {
    let l = testing_logger().new(o!("test" => "sending_snapshot_set_pending_snapshot"));
    let mut sm = new_test_raft(1, vec![1, 2], 10, 1, new_storage(), &l);
    sm.restore(testing_snap());

    sm.become_candidate();
    sm.become_leader();

    // force set the next of node 1, so that
    // node 1 needs a snapshot
    sm.mut_prs().get_mut(2).unwrap().next_idx = sm.raft_log.first_index();

    let mut m = new_message(2, 1, MessageType::MsgAppendResponse, 0);
    {
        let voter_2 = sm.prs().get(2).unwrap();
        m.set_index(voter_2.next_idx - 1);
        m.set_reject(true);
    };
    sm.step(m).expect("");
    assert_eq!(sm.prs().get(2).unwrap().pending_snapshot, 11);
}

#[test]
fn test_pending_snapshot_pause_replication() {
    let l = testing_logger().new(o!("test" => "pending_snapshot_pause_replication"));
    let mut sm = new_test_raft(1, vec![1, 2], 10, 1, new_storage(), &l);
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
    let l = testing_logger().new(o!("test" => "snapshot_failure"));
    let mut sm = new_test_raft(1, vec![1, 2], 10, 1, new_storage(), &l);
    sm.restore(testing_snap());

    sm.become_candidate();
    sm.become_leader();

    sm.mut_prs().get_mut(2).unwrap().next_idx = 1;
    sm.mut_prs().get_mut(2).unwrap().become_snapshot(11);

    let mut m = new_message(2, 1, MessageType::MsgSnapStatus, 0);
    m.set_reject(true);
    sm.step(m).expect("");
    let voter_2 = sm.prs().get(2).unwrap();
    assert_eq!(voter_2.pending_snapshot, 0);
    assert_eq!(voter_2.next_idx, 1);
    assert!(voter_2.paused);
}

#[test]
fn test_snapshot_succeed() {
    let l = testing_logger().new(o!("test" => "snapshot_succeed"));
    let mut sm = new_test_raft(1, vec![1, 2], 10, 1, new_storage(), &l);
    sm.restore(testing_snap());

    sm.become_candidate();
    sm.become_leader();

    sm.mut_prs().get_mut(2).unwrap().next_idx = 1;
    sm.mut_prs().get_mut(2).unwrap().become_snapshot(11);

    let mut m = new_message(2, 1, MessageType::MsgSnapStatus, 0);
    m.set_reject(false);
    sm.step(m).expect("");
    let voter_2 = sm.prs().get(2).unwrap();
    assert_eq!(voter_2.pending_snapshot, 0);
    assert_eq!(voter_2.next_idx, 12);
    assert!(voter_2.paused);
}

#[test]
fn test_snapshot_abort() {
    let l = testing_logger().new(o!("test" => "snapshot_abort"));
    let mut sm = new_test_raft(1, vec![1, 2], 10, 1, new_storage(), &l);
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
    assert_eq!(sm.prs().get(2).unwrap().pending_snapshot, 0);
    assert_eq!(sm.prs().get(2).unwrap().next_idx, 12);
}

// Initialized storage should be at term 1 instead of 0. Otherwise the case will fail.
#[test]
fn test_snapshot_with_min_term() {
    let l = testing_logger().new(o!("test" => "snapshot_with_min_term"));
    let do_test = |pre_vote: bool| {
        let n1 = new_test_raft_with_prevote(1, vec![1, 2], 10, 1, new_storage(), pre_vote, &l);
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
