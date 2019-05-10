// Copyright 2018 PingCAP, Inc.
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

use crate::test_util::*;
use fail;
use harness::setup_for_test;
use raft::eraftpb::MessageType;
use std::sync::*;

lazy_static! {
    /// Failpoints are global structs, hence rules set in different cases
    /// may affect each other. So use a global lock to synchronize them.
    static ref LOCK: Mutex<()> = {
        Mutex::new(())
    };
}

fn setup<'a>() -> MutexGuard<'a, ()> {
    setup_for_test();
    // We don't want a failed test breaks others.
    let guard = LOCK.lock().unwrap_or_else(|e| e.into_inner());
    fail::teardown();
    fail::setup();
    guard
}

// test_reject_stale_term_message tests that if a server receives a request with
// a stale term number, it rejects the request.
// Our implementation ignores the request instead.
// Reference: section 5.1
#[test]
fn test_reject_stale_term_message() {
    let _guard = setup();
    let mut r = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage());
    fail::cfg("before_step", "panic").unwrap();
    r.load_state(&hard_state(2, 1, 0));

    let mut m = new_message(0, 0, MessageType::MsgAppend, 0);
    m.set_term(r.term - 1);
    r.step(m).expect("");
}

// ensure that the Step function ignores the message from old term and does not pass it to the
// actual stepX function.
#[test]
fn test_step_ignore_old_term_msg() {
    let _guard = setup();
    let mut sm = new_test_raft(1, vec![1], 10, 1, new_storage());
    fail::cfg("before_step", "panic").unwrap();
    sm.term = 2;
    let mut m = new_message(0, 0, MessageType::MsgAppend, 0);
    m.set_term(1);
    sm.step(m).expect("");
}
