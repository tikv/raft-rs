// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::test_util::*;
use fail;
use raft::{eraftpb::MessageType, util::default_logger};
use std::sync::*;

// test_reject_stale_term_message tests that if a server receives a request with
// a stale term number, it rejects the request.
// Our implementation ignores the request instead.
// Reference: section 5.1
#[test]
fn test_reject_stale_term_message() {
    let scenario = fail::FailScenario::setup();
    let l = default_logger();
    let mut r = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage(), &l);
    fail::cfg("before_step", "panic").unwrap();
    r.load_state(&hard_state(2, 0, 0));

    let mut m = new_message(0, 0, MessageType::MsgAppend, 0);
    m.term = r.term - 1;
    r.step(m).expect("");
    scenario.teardown();
}

// ensure that the Step function ignores the message from old term and does not pass it to the
// actual stepX function.
#[test]
fn test_step_ignore_old_term_msg() {
    let scenario = fail::FailScenario::setup();
    let l = default_logger();
    let mut sm = new_test_raft(1, vec![1], 10, 1, new_storage(), &l);
    fail::cfg("before_step", "panic").unwrap();
    sm.term = 2;
    let mut m = new_message(0, 0, MessageType::MsgAppend, 0);
    m.term = 1;
    sm.step(m).expect("");
    scenario.teardown();
}
