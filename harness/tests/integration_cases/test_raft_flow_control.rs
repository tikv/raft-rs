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
use raft::{default_logger, eraftpb::*, Raft, Storage};

// Force progress `pr` to be in replicate state at `i`.
fn progress_become_replicate<T>(r: &mut Raft<T>, pr: u64, i: u64)
where
    T: Storage,
{
    r.mut_prs().get_mut(pr).unwrap().maybe_update(i - 1);
    r.mut_prs().get_mut(pr).unwrap().become_replicate();
}

// test_msg_app_flow_control_full ensures:
// 1. msgApp can fill the sending window until full
// 2. when the window is full, no more msgApp can be sent.
#[test]
fn test_msg_app_flow_control_full() {
    let l = default_logger();
    let mut r = new_test_raft(1, vec![1, 2], 5, 1, new_storage(), &l);
    r.become_candidate();
    r.become_leader();

    // The configuration is initialized at 1 and the leader's empty entry is at 2.
    assert_eq!(r.raft_log.last_index(), 2);

    // force the progress to be in replicate state
    progress_become_replicate(&mut r, 2, 2);
    // fill in the inflights window
    for i in 0..r.max_inflight {
        r.step(new_message(1, 1, MessageType::MsgPropose, 1))
            .expect("");
        let ms = r.read_messages();
        if ms.len() != 1 {
            panic!("#{}: ms count = {}, want 1", i, ms.len());
        }
    }

    // ensure 1
    assert!(r.prs().get(2).unwrap().ins.full());

    // ensure 2
    for i in 0..10 {
        r.step(new_message(1, 1, MessageType::MsgPropose, 1))
            .expect("");
        let ms = r.read_messages();
        if !ms.is_empty() {
            panic!("#{}: ms count = {}, want 0", i, ms.len());
        }
    }
}

// test_msg_app_flow_control_move_forward ensures msgAppResp can move
// forward the sending window correctly:
// 1. valid msgAppResp.index moves the windows to pass all smaller or equal index.
// 2. out-of-dated msgAppResp has no effect on the sliding window.
#[test]
fn test_msg_app_flow_control_move_forward() {
    let l = default_logger();
    let mut r = new_test_raft(1, vec![1, 2], 5, 1, new_storage(), &l);
    r.become_candidate();
    r.become_leader();

    // The configuration is initialized at 1 and the leader's empty entry is at 2.
    assert_eq!(r.raft_log.last_index(), 2);

    // force the progress to be in replicate state
    progress_become_replicate(&mut r, 2, 2);
    // fill in the inflights window
    for _ in 0..r.max_inflight {
        r.step(new_message(1, 1, MessageType::MsgPropose, 1))
            .expect("");
        r.read_messages();
    }

    // 2 is noop, 3 is the first proposal we just sent.
    // so we start with 3.
    for tt in 3..r.max_inflight {
        // move forward the window
        let mut m = new_message(2, 1, MessageType::MsgAppendResponse, 0);
        m.index = tt as u64;
        r.step(m).expect("");
        r.read_messages();

        // fill in the inflights window again
        r.step(new_message(1, 1, MessageType::MsgPropose, 1))
            .expect("");
        let ms = r.read_messages();
        if ms.len() != 1 {
            panic!("#{}: ms count = {}, want 1", tt, ms.len());
        }

        // ensure 1
        assert!(r.prs().get(2).unwrap().ins.full());

        // ensure 2
        for i in 0..tt {
            let mut m = new_message(2, 1, MessageType::MsgAppendResponse, 0);
            m.index = i as u64;
            r.step(m).expect("");
            if !r.prs().get(2).unwrap().ins.full() {
                panic!(
                    "#{}: inflights.full = {}, want true",
                    tt,
                    r.prs().get(2).unwrap().ins.full()
                );
            }
        }
    }
}

// test_msg_app_flow_control_recv_heartbeat ensures a heartbeat response
// frees one slot if the window is full.
#[test]
fn test_msg_app_flow_control_recv_heartbeat() {
    let l = default_logger();
    let mut r = new_test_raft(1, vec![1, 2], 5, 1, new_storage(), &l);
    r.become_candidate();
    r.become_leader();

    // force the progress to be in replicate state
    progress_become_replicate(&mut r, 2, 2);
    // fill in the inflights window
    for _ in 0..r.max_inflight {
        r.step(new_message(1, 1, MessageType::MsgPropose, 1))
            .expect("");
        r.read_messages();
    }

    for tt in 1..5 {
        if !r.prs().get(2).unwrap().ins.full() {
            panic!(
                "#{}: inflights.full = {}, want true",
                tt,
                r.prs().get(2).unwrap().ins.full()
            );
        }

        // recv tt MsgHeartbeatResp and expect one free slot
        for i in 0..tt {
            r.step(new_message(2, 1, MessageType::MsgHeartbeatResponse, 0))
                .expect("");
            r.read_messages();
            if r.prs().get(2).unwrap().ins.full() {
                panic!(
                    "#{}.{}: inflights.full = {}, want false",
                    tt,
                    i,
                    r.prs().get(2).unwrap().ins.full()
                );
            }
        }

        // one slot
        r.step(new_message(1, 1, MessageType::MsgPropose, 1))
            .expect("");
        let ms = r.read_messages();
        if ms.len() != 1 {
            panic!("#{}: free slot = 0, want 1", tt);
        }

        // and just one slot
        for i in 0..10 {
            r.step(new_message(1, 1, MessageType::MsgPropose, 1))
                .expect("");
            let ms1 = r.read_messages();
            if !ms1.is_empty() {
                panic!("#{}.{}, ms1 should be empty.", tt, i);
            }
        }

        // clear all pending messages
        r.step(new_message(2, 1, MessageType::MsgHeartbeatResponse, 0))
            .expect("");
        r.read_messages();
    }
}
