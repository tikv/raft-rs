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
use harness::*;
use raft::eraftpb::*;
use raft::storage::MemStorage;
use raft::*;
use slog::Logger;

pub fn commit_noop_entry(r: &mut Interface, s: &MemStorage) {
    assert_eq!(r.state, StateRole::Leader);
    r.bcast_append();
    // simulate the response of MsgAppend
    let msgs = r.read_messages();
    for m in msgs {
        assert_eq!(m.get_msg_type(), MessageType::MsgAppend);
        assert_eq!(m.entries.len(), 1);
        assert!(m.entries[0].data.is_empty());
        r.step(accept_and_reply(&m)).expect("");
    }
    // ignore further messages to refresh followers' commit index
    r.read_messages();
    s.wl()
        .append(r.raft_log.unstable_entries().unwrap_or(&[]))
        .expect("");
    let committed = r.raft_log.committed;
    r.commit_apply(committed);
    let (last_index, last_term) = (r.raft_log.last_index(), r.raft_log.last_term());
    r.raft_log.stable_to(last_index, last_term);
}

fn accept_and_reply(m: &Message) -> Message {
    assert_eq!(m.get_msg_type(), MessageType::MsgAppend);
    let mut reply = new_message(m.to, m.from, MessageType::MsgAppendResponse, 0);
    reply.term = m.term;
    reply.index = m.index + m.entries.len() as u64;
    reply
}

#[test]
fn test_follower_update_term_from_message() {
    let l = default_logger();
    test_update_term_from_message(StateRole::Follower, &l);
}

#[test]
fn test_candidate_update_term_from_message() {
    let l = default_logger();
    test_update_term_from_message(StateRole::Candidate, &l);
}

#[test]
fn test_leader_update_term_from_message() {
    let l = default_logger();
    test_update_term_from_message(StateRole::Leader, &l);
}

// test_update_term_from_message tests that if one server’s current term is
// smaller than the other’s, then it updates its current term to the larger
// value. If a candidate or leader discovers that its term is out of date,
// it immediately reverts to follower state.
// Reference: section 5.1
fn test_update_term_from_message(state: StateRole, l: &Logger) {
    let mut r = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage(), &l);
    match state {
        StateRole::Follower => r.become_follower(1, 2),
        StateRole::PreCandidate => r.become_pre_candidate(),
        StateRole::Candidate => r.become_candidate(),
        StateRole::Leader => {
            r.become_candidate();
            r.become_leader();
        }
    }

    let mut m = new_message(0, 0, MessageType::MsgAppend, 0);
    m.term = 2;
    r.step(m).expect("");

    assert_eq!(r.term, 2);
    assert_eq!(r.state, StateRole::Follower);
}

// test_start_as_follower tests that when servers start up, they begin as followers.
// Reference: section 5.2
#[test]
fn test_start_as_follower() {
    let l = default_logger();
    let r = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage(), &l);
    assert_eq!(r.state, StateRole::Follower);
}

// test_leader_bcast_beat tests that if the leader receives a heartbeat tick,
// it will send a msgApp with m.Index = 0, m.LogTerm=0 and empty entries as
// heartbeat to all followers.
// Reference: section 5.2
#[test]
fn test_leader_bcast_beat() {
    let l = default_logger();
    // heartbeat interval
    let hi = 1;
    let mut r = new_test_raft(1, vec![1, 2, 3], 10, hi, new_storage(), &l);
    r.become_candidate();
    r.become_leader();
    for i in 0..10 {
        let _ = r.append_entry(&mut [empty_entry(0, i as u64 + 1)]);
    }

    for _ in 0..hi {
        r.tick();
    }

    let mut msgs = r.read_messages();
    msgs.sort_by_key(|m| format!("{:?}", m));

    let new_message_ext = |f, to| {
        let mut m = new_message(f, to, MessageType::MsgHeartbeat, 0);
        m.term = 1;
        m.commit = 0;
        m
    };

    let expect_msgs = vec![new_message_ext(1, 2), new_message_ext(1, 3)];
    assert_eq!(msgs, expect_msgs);
}

#[test]
fn test_follower_start_election() {
    let l = default_logger();
    test_nonleader_start_election(StateRole::Follower, &l);
}

#[test]
fn test_candidate_start_new_election() {
    let l = default_logger();
    test_nonleader_start_election(StateRole::Candidate, &l);
}

// test_nonleader_start_election tests that if a follower receives no communication
// over election timeout, it begins an election to choose a new leader. It
// increments its current term and transitions to candidate state. It then
// votes for itself and issues RequestVote RPCs in parallel to each of the
// other servers in the cluster.
// Reference: section 5.2
// Also if a candidate fails to obtain a majority, it will time out and
// start a new election by incrementing its term and initiating another
// round of RequestVote RPCs.
// Reference: section 5.2
fn test_nonleader_start_election(state: StateRole, l: &Logger) {
    // election timeout
    let et = 10;
    let mut r = new_test_raft(1, vec![1, 2, 3], et, 1, new_storage(), l);
    match state {
        StateRole::Follower => r.become_follower(1, 2),
        StateRole::Candidate => r.become_candidate(),
        _ => panic!("Only non-leader role is accepted."),
    }

    for _ in 1..2 * et {
        r.tick();
    }

    assert_eq!(r.term, 2);
    assert_eq!(r.state, StateRole::Candidate);
    assert!(r.prs().votes()[&r.id]);
    let mut msgs = r.read_messages();
    msgs.sort_by_key(|m| format!("{:?}", m));
    let new_message_ext = |f, to| {
        let mut m = new_message(f, to, MessageType::MsgRequestVote, 0);
        m.term = 2;
        m
    };
    let expect_msgs = vec![new_message_ext(1, 2), new_message_ext(1, 3)];
    assert_eq!(msgs, expect_msgs);
}

// test_leader_election_in_one_round_rpc tests all cases that may happen in
// leader election during one round of RequestVote RPC:
// a) it wins the election
// b) it loses the election
// c) it is unclear about the result
// Reference: section 5.2
#[test]
fn test_leader_election_in_one_round_rpc() {
    let l = default_logger();
    let mut tests = vec![
        // win the election when receiving votes from a majority of the servers
        (1, map!(), StateRole::Leader),
        (3, map!(2 => true, 3 => true), StateRole::Leader),
        (3, map!(2 => true), StateRole::Leader),
        (
            5,
            map!(2 => true, 3 => true, 4 => true, 5 => true),
            StateRole::Leader,
        ),
        (5, map!(2 => true, 3 => true, 4 => true), StateRole::Leader),
        (5, map!(2 => true, 3 => true), StateRole::Leader),
        // return to follower state if it receives vote denial from a majority
        (3, map!(2 => false, 3 => false), StateRole::Follower),
        (
            5,
            map!(2 => false, 3 => false, 4 => false, 5 => false),
            StateRole::Follower,
        ),
        (
            5,
            map!(2 => true, 3 => false, 4 => false, 5 => false),
            StateRole::Follower,
        ),
        // stay in candidate if it does not obtain the majority
        (3, map!(), StateRole::Candidate),
        (5, map!(2 => true), StateRole::Candidate),
        (5, map!(2 => false, 3 => false), StateRole::Candidate),
        (5, map!(), StateRole::Candidate),
    ];

    for (i, (size, votes, state)) in tests.drain(..).enumerate() {
        let mut r = new_test_raft(1, (1..=size as u64).collect(), 10, 1, new_storage(), &l);

        r.step(new_message(1, 1, MessageType::MsgHup, 0)).expect("");
        for (id, vote) in votes {
            let mut m = new_message(id, 1, MessageType::MsgRequestVoteResponse, 0);
            m.term = r.term;
            m.reject = !vote;
            r.step(m).expect("");
        }

        if r.state != state {
            panic!("#{}: state = {:?}, want {:?}", i, r.state, state);
        }
        if r.term != 1 {
            panic!("#{}: term = {}, want {}", i, r.term, 1);
        }
    }
}

// test_follower_vote tests that each follower will vote for at most one
// candidate in a given term, on a first-come-first-served basis.
// Reference: section 5.2
#[test]
fn test_follower_vote() {
    let l = default_logger();
    let mut tests = vec![
        (INVALID_ID, 1, false),
        (INVALID_ID, 2, false),
        (1, 1, false),
        (2, 2, false),
        (1, 2, true),
        (2, 1, true),
    ];

    for (i, (vote, nvote, wreject)) in tests.drain(..).enumerate() {
        let mut r = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage(), &l);
        r.load_state(&hard_state(1, 0, vote));

        let mut m = new_message(nvote, 1, MessageType::MsgRequestVote, 0);
        m.term = 1;
        r.step(m).expect("");

        let msgs = r.read_messages();
        let mut m = new_message(1, nvote, MessageType::MsgRequestVoteResponse, 0);
        m.term = 1;
        m.reject = wreject;
        let expect_msgs = vec![m];
        if msgs != expect_msgs {
            panic!("#{}: msgs = {:?}, want {:?}", i, msgs, expect_msgs);
        }
    }
}

// test_candidate_fallback tests that while waiting for votes,
// if a candidate receives an AppendEntries RPC from another server claiming
// to be leader whose term is at least as large as the candidate's current term,
// it recognizes the leader as legitimate and returns to follower state.
// Reference: section 5.2
#[test]
fn test_candidate_fallback() {
    let l = default_logger();
    let new_message_ext = |f, to, term| {
        let mut m = new_message(f, to, MessageType::MsgAppend, 0);
        m.term = term;
        m
    };
    let mut tests = vec![new_message_ext(2, 1, 2), new_message_ext(2, 1, 3)];
    for (i, m) in tests.drain(..).enumerate() {
        let mut r = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage(), &l);
        r.step(new_message(1, 1, MessageType::MsgHup, 0)).expect("");
        assert_eq!(r.state, StateRole::Candidate);

        let term = m.term;
        r.step(m).expect("");

        if r.state != StateRole::Follower {
            panic!(
                "#{}: state = {:?}, want {:?}",
                i,
                r.state,
                StateRole::Follower
            );
        }
        if r.term != term {
            panic!("#{}: term = {}, want {}", i, r.term, term);
        }
    }
}

#[test]
fn test_follower_election_timeout_randomized() {
    let l = default_logger();
    test_non_leader_election_timeout_randomized(StateRole::Follower, &l);
}

#[test]
fn test_candidate_election_timeout_randomized() {
    let l = default_logger();
    test_non_leader_election_timeout_randomized(StateRole::Candidate, &l);
}

// test_non_leader_election_timeout_randomized tests that election timeout for
// follower or candidate is randomized.
// Reference: section 5.2
fn test_non_leader_election_timeout_randomized(state: StateRole, l: &Logger) {
    let et = 10;
    let mut r = new_test_raft(1, vec![1, 2, 3], et, 1, new_storage(), l);
    let mut timeouts = map!();
    for _ in 0..1000 * et {
        let term = r.term;
        match state {
            StateRole::Follower => r.become_follower(term + 1, 2),
            StateRole::Candidate => r.become_candidate(),
            _ => panic!("only non leader state is accepted!"),
        }

        let mut time = 0;
        while r.read_messages().is_empty() {
            r.tick();
            time += 1;
        }
        timeouts.insert(time, true);
    }

    assert!(timeouts.len() <= et && timeouts.len() >= et - 1);
    for d in et + 1..2 * et {
        assert!(timeouts[&d]);
    }
}

#[test]
fn test_follower_election_timeout_nonconflict() {
    let l = default_logger();
    test_nonleaders_election_timeout_nonconfict(StateRole::Follower, &l);
}

#[test]
fn test_candidates_election_timeout_nonconf() {
    let l = default_logger();
    test_nonleaders_election_timeout_nonconfict(StateRole::Candidate, &l);
}

// test_nonleaders_election_timeout_nonconfict tests that in most cases only a
// single server(follower or candidate) will time out, which reduces the
// likelihood of split vote in the new election.
// Reference: section 5.2
fn test_nonleaders_election_timeout_nonconfict(state: StateRole, l: &Logger) {
    let et = 10;
    let size = 5;
    let mut rs = Vec::with_capacity(size);
    let ids: Vec<u64> = (1..=size as u64).collect();
    for id in ids.iter().take(size) {
        rs.push(new_test_raft(*id, ids.clone(), et, 1, new_storage(), l));
    }
    let mut conflicts = 0;
    for _ in 0..1000 {
        for r in &mut rs {
            let term = r.term;
            match state {
                StateRole::Follower => r.become_follower(term + 1, INVALID_ID),
                StateRole::Candidate => r.become_candidate(),
                _ => panic!("non leader state is expect!"),
            }
        }

        let mut timeout_num = 0;
        while timeout_num == 0 {
            for r in &mut rs {
                r.tick();
                if !r.read_messages().is_empty() {
                    timeout_num += 1;
                }
            }
        }
        // several rafts time out at the same tick
        if timeout_num > 1 {
            conflicts += 1;
        }
    }

    assert!(f64::from(conflicts) / 1000.0 <= 0.3);
}

// test_leader_start_replication tests that when receiving client proposals,
// the leader appends the proposal to its log as a new entry, then issues
// AppendEntries RPCs in parallel to each of the other servers to replicate
// the entry. Also, when sending an AppendEntries RPC, the leader includes
// the index and term of the entry in its log that immediately precedes
// the new entries.
// Also, it writes the new entry into stable storage.
// Reference: section 5.3
#[test]
fn test_leader_start_replication() {
    let l = default_logger();
    let s = new_storage();
    let mut r = new_test_raft(1, vec![1, 2, 3], 10, 1, s.clone(), &l);
    r.become_candidate();
    r.become_leader();
    commit_noop_entry(&mut r, &s);
    let li = r.raft_log.last_index();

    r.step(new_message(1, 1, MessageType::MsgPropose, 1))
        .expect("");

    assert_eq!(r.raft_log.last_index(), li + 1);
    assert_eq!(r.raft_log.committed, li);
    let mut msgs = r.read_messages();
    msgs.sort_by_key(|m| format!("{:?}", m));
    let wents = vec![new_entry(1, li + 1, SOME_DATA)];
    let new_message_ext = |f, to, ents| {
        let mut m = new_message(f, to, MessageType::MsgAppend, 0);
        m.term = 1;
        m.index = li;
        m.log_term = 1;
        m.commit = li;
        m.entries = ents;
        m
    };
    let expect_msgs = vec![
        new_message_ext(1, 2, wents.clone().into()),
        new_message_ext(1, 3, wents.clone().into()),
    ];
    assert_eq!(msgs, expect_msgs);
    assert_eq!(r.raft_log.unstable_entries(), Some(&*wents));
}

// test_leader_commit_entry tests that when the entry has been safely replicated,
// the leader gives out the applied entries, which can be applied to its state
// machine.
// Also, the leader keeps track of the highest index it knows to be committed,
// and it includes that index in future AppendEntries RPCs so that the other
// servers eventually find out.
// Reference: section 5.3
#[test]
fn test_leader_commit_entry() {
    let l = default_logger();
    let s = new_storage();
    let mut r = new_test_raft(1, vec![1, 2, 3], 10, 1, s.clone(), &l);
    r.become_candidate();
    r.become_leader();
    commit_noop_entry(&mut r, &s);
    let li = r.raft_log.last_index();
    r.step(new_message(1, 1, MessageType::MsgPropose, 1))
        .expect("");

    for m in r.read_messages() {
        r.step(accept_and_reply(&m)).expect("");
    }

    assert_eq!(r.raft_log.committed, li + 1);
    let wents = vec![new_entry(1, li + 1, SOME_DATA)];
    assert_eq!(r.raft_log.next_entries(), Some(wents));
    let mut msgs = r.read_messages();
    msgs.sort_by_key(|m| format!("{:?}", m));
    for (i, m) in msgs.drain(..).enumerate() {
        assert_eq!(i as u64 + 2, m.to);
        assert_eq!(m.get_msg_type(), MessageType::MsgAppend);
        assert_eq!(m.commit, li + 1);
    }
}

// test_leader_acknowledge_commit tests that a log entry is committed once the
// leader that created the entry has replicated it on a majority of the servers.
// Reference: section 5.3
#[test]
fn test_leader_acknowledge_commit() {
    let l = default_logger();
    let mut tests = vec![
        (1, map!(), true),
        (3, map!(), false),
        (3, map!(2 => true), true),
        (3, map!(2 => true, 3 => true), true),
        (5, map!(), false),
        (5, map!(2 => true), false),
        (5, map!(2 => true, 3 => true), true),
        (5, map!(2 => true, 3 => true, 4 => true), true),
        (5, map!(2 => true, 3 => true, 4 => true, 5 => true), true),
    ];
    for (i, (size, acceptors, wack)) in tests.drain(..).enumerate() {
        let s = new_storage();
        let mut r = new_test_raft(1, (1..=size).collect(), 10, 1, s.clone(), &l);
        r.become_candidate();
        r.become_leader();
        commit_noop_entry(&mut r, &s);
        let li = r.raft_log.last_index();
        r.step(new_message(1, 1, MessageType::MsgPropose, 1))
            .expect("");

        for m in r.read_messages() {
            if acceptors.contains_key(&m.to) && acceptors[&m.to] {
                r.step(accept_and_reply(&m)).expect("");
            }
        }

        let g = r.raft_log.committed > li;
        if g ^ wack {
            panic!("#{}: ack commit = {}, want {}", i, g, wack);
        }
    }
}

// test_leader_commit_preceding_entries tests that when leader commits a log entry,
// it also commits all preceding entries in the leader’s log, including
// entries created by previous leaders.
// Also, it applies the entry to its local state machine (in log order).
// Reference: section 5.3
#[test]
fn test_leader_commit_preceding_entries() {
    let l = default_logger();
    let mut tests = vec![
        vec![],
        vec![empty_entry(2, 1)],
        vec![empty_entry(1, 1), empty_entry(2, 2)],
        vec![empty_entry(1, 1)],
    ];

    for (i, mut tt) in tests.drain(..).enumerate() {
        let mut r = {
            let store = MemStorage::new_with_conf_state((vec![1, 2, 3], vec![]));
            store.wl().append(&tt).unwrap();
            let cfg = new_test_config(1, 10, 1);
            new_test_raft_with_config(&cfg, store, &l)
        };
        r.load_state(&hard_state(2, 0, 0));
        r.become_candidate();
        r.become_leader();

        r.step(new_message(1, 1, MessageType::MsgPropose, 1))
            .expect("");

        for m in r.read_messages() {
            r.step(accept_and_reply(&m)).expect("");
        }

        let li = tt.len() as u64;
        tt.append(&mut vec![
            empty_entry(3, li + 1),
            new_entry(3, li + 2, SOME_DATA),
        ]);
        let g = r.raft_log.next_entries();
        let wg = Some(tt);
        if g != wg {
            panic!("#{}: ents = {:?}, want {:?}", i, g, wg);
        }
    }
}

// test_follower_commit_entry tests that once a follower learns that a log entry
// is committed, it applies the entry to its local state machine (in log order).
// Reference: section 5.3
#[test]
fn test_follower_commit_entry() {
    let l = default_logger();
    let mut tests = vec![
        (vec![new_entry(1, 1, SOME_DATA)], 1),
        (
            vec![
                new_entry(1, 1, SOME_DATA),
                new_entry(1, 2, Some("somedata2")),
            ],
            2,
        ),
        (
            vec![
                new_entry(1, 1, Some("somedata2")),
                new_entry(1, 2, SOME_DATA),
            ],
            2,
        ),
        (
            vec![
                new_entry(1, 1, SOME_DATA),
                new_entry(1, 2, Some("somedata2")),
            ],
            1,
        ),
    ];

    for (i, (ents, commit)) in tests.drain(..).enumerate() {
        let mut r = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage(), &l);
        r.become_follower(1, 2);

        let mut m = new_message(2, 1, MessageType::MsgAppend, 0);
        m.term = 1;
        m.commit = commit;
        m.entries = ents.clone().into();
        r.step(m).expect("");

        if r.raft_log.committed != commit {
            panic!(
                "#{}: committed = {}, want {}",
                i, r.raft_log.committed, commit
            );
        }
        let wents = Some(ents[..commit as usize].to_vec());
        let g = r.raft_log.next_entries();
        if g != wents {
            panic!("#{}: next_ents = {:?}, want {:?}", i, g, wents);
        }
    }
}

// test_follower_check_msg_append tests that if the follower does not find an
// entry in its log with the same index and term as the one in AppendEntries RPC,
// then it refuses the new entries. Otherwise it replies that it accepts the
// append entries.
// Reference: section 5.3
#[test]
fn test_follower_check_msg_append() {
    let l = default_logger();
    let ents = vec![empty_entry(1, 1), empty_entry(2, 2)];
    let mut tests = vec![
        // match with committed entries
        (0, 0, 1, false, 0, 1),
        (ents[0].term, ents[0].index, 1, false, 0, 1),
        // match with uncommitted entries
        (ents[1].term, ents[1].index, 2, false, 0, 1),
        // unmatch with existing entry
        (ents[0].term, ents[1].index, ents[1].index, true, 2, 1),
        // unexisting entry
        (
            ents[1].term + 1,
            ents[1].index + 1,
            ents[1].index + 1,
            true,
            2,
            1,
        ),
    ];
    for (i, (term, index, windex, wreject, wreject_hint, w_commit)) in tests.drain(..).enumerate() {
        let mut r = {
            let store = MemStorage::new_with_conf_state((vec![1, 2, 3], vec![]));
            store.wl().append(&ents).unwrap();
            let cfg = new_test_config(1, 10, 1);
            new_test_raft_with_config(&cfg, store, &l)
        };
        r.load_state(&hard_state(0, 1, 0));
        r.become_follower(2, 2);

        let mut m = new_message(2, 1, MessageType::MsgAppend, 0);
        m.term = 2;
        m.log_term = term;
        m.index = index;
        r.step(m).expect("");

        let msgs = r.read_messages();
        let mut wm = new_message(1, 2, MessageType::MsgAppendResponse, 0);
        wm.term = 2;
        wm.index = windex;
        wm.commit = w_commit;
        if wreject {
            wm.reject = wreject;
            wm.reject_hint = wreject_hint;
        }
        let expect_msgs = vec![wm];
        if msgs != expect_msgs {
            panic!("#{}: msgs = {:?}, want {:?}", i, msgs, expect_msgs);
        }
    }
}

// test_follower_append_entries tests that when AppendEntries RPC is valid,
// the follower will delete the existing conflict entry and all that follow it,
// and append any new entries not already in the log.
// Also, it writes the new entry into stable storage.
// Reference: section 5.3
#[test]
fn test_follower_append_entries() {
    let l = default_logger();
    let mut tests = vec![
        (
            2,
            2,
            vec![empty_entry(3, 3)],
            vec![empty_entry(1, 1), empty_entry(2, 2), empty_entry(3, 3)],
            vec![empty_entry(3, 3)],
        ),
        (
            1,
            1,
            vec![empty_entry(3, 2), empty_entry(4, 3)],
            vec![empty_entry(1, 1), empty_entry(3, 2), empty_entry(4, 3)],
            vec![empty_entry(3, 2), empty_entry(4, 3)],
        ),
        (
            0,
            0,
            vec![empty_entry(1, 1)],
            vec![empty_entry(1, 1), empty_entry(2, 2)],
            vec![],
        ),
        (
            0,
            0,
            vec![empty_entry(3, 1)],
            vec![empty_entry(3, 1)],
            vec![empty_entry(3, 1)],
        ),
    ];
    for (i, (index, term, ents, wents, wunstable)) in tests.drain(..).enumerate() {
        let mut r = {
            let store = MemStorage::new_with_conf_state((vec![1, 2, 3], vec![]));
            store
                .wl()
                .append(&[empty_entry(1, 1), empty_entry(2, 2)])
                .unwrap();
            let cfg = new_test_config(1, 10, 1);
            new_test_raft_with_config(&cfg, store, &l)
        };
        r.become_follower(2, 2);

        let mut m = new_message(2, 1, MessageType::MsgAppend, 0);
        m.term = 2;
        m.log_term = term;
        m.index = index;
        m.entries = ents.into();
        r.step(m).expect("");

        let g = r.raft_log.all_entries();
        if g != wents {
            panic!("#{}: ents = {:?}, want {:?}", i, g, wents);
        }
        let g = r.raft_log.unstable_entries();
        let wunstable = if wunstable.is_empty() {
            None
        } else {
            Some(&*wunstable)
        };
        if g != wunstable {
            panic!("#{}: unstable_entries = {:?}, want {:?}", i, g, wunstable);
        }
    }
}

// test_leader_sync_follower_log tests that the leader could bring a follower's log
// into consistency with its own.
// Reference: section 5.3, figure 7
#[test]
fn test_leader_sync_follower_log() {
    let l = default_logger();
    let ents = vec![
        empty_entry(1, 1),
        empty_entry(1, 2),
        empty_entry(1, 3),
        empty_entry(4, 4),
        empty_entry(4, 5),
        empty_entry(5, 6),
        empty_entry(5, 7),
        empty_entry(6, 8),
        empty_entry(6, 9),
        empty_entry(6, 10),
    ];
    let term = 8u64;
    let mut tests = vec![
        vec![
            empty_entry(1, 1),
            empty_entry(1, 2),
            empty_entry(1, 3),
            empty_entry(4, 4),
            empty_entry(4, 5),
            empty_entry(5, 6),
            empty_entry(5, 7),
            empty_entry(6, 8),
            empty_entry(6, 9),
        ],
        vec![
            empty_entry(1, 1),
            empty_entry(1, 2),
            empty_entry(1, 3),
            empty_entry(4, 4),
        ],
        vec![
            empty_entry(1, 1),
            empty_entry(1, 2),
            empty_entry(1, 3),
            empty_entry(4, 4),
            empty_entry(4, 5),
            empty_entry(5, 6),
            empty_entry(5, 7),
            empty_entry(6, 8),
            empty_entry(6, 9),
            empty_entry(6, 10),
            empty_entry(6, 11),
        ],
        vec![
            empty_entry(1, 1),
            empty_entry(1, 2),
            empty_entry(1, 3),
            empty_entry(4, 4),
            empty_entry(4, 5),
            empty_entry(5, 6),
            empty_entry(5, 7),
            empty_entry(6, 8),
            empty_entry(6, 9),
            empty_entry(6, 10),
            empty_entry(7, 11),
            empty_entry(7, 12),
        ],
        vec![
            empty_entry(1, 1),
            empty_entry(1, 2),
            empty_entry(1, 3),
            empty_entry(4, 4),
            empty_entry(4, 5),
            empty_entry(4, 6),
            empty_entry(4, 7),
        ],
        vec![
            empty_entry(1, 1),
            empty_entry(1, 2),
            empty_entry(1, 3),
            empty_entry(2, 4),
            empty_entry(2, 5),
            empty_entry(2, 6),
            empty_entry(3, 7),
            empty_entry(3, 8),
            empty_entry(3, 9),
            empty_entry(3, 10),
            empty_entry(3, 11),
        ],
    ];
    for (i, tt) in tests.drain(..).enumerate() {
        let mut lead = {
            let store = MemStorage::new_with_conf_state((vec![1, 2, 3], vec![]));
            store.wl().append(&ents).unwrap();
            let cfg = new_test_config(1, 10, 1);
            new_test_raft_with_config(&cfg, store, &l)
        };
        let last_index = lead.raft_log.last_index();
        lead.load_state(&hard_state(term, last_index, 0));

        let mut follower = {
            let store = MemStorage::new_with_conf_state((vec![1, 2, 3], vec![]));
            store.wl().append(&tt).unwrap();
            let cfg = new_test_config(2, 10, 1);
            new_test_raft_with_config(&cfg, store, &l)
        };
        follower.load_state(&hard_state(term - 1, 0, 0));

        // It is necessary to have a three-node cluster.
        // The second may have more up-to-date log than the first one, so the
        // first node needs the vote from the third node to become the leader.
        let mut n = Network::new(vec![Some(lead), Some(follower), NOP_STEPPER], &l);
        n.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
        // The election occurs in the term after the one we loaded with
        // lead.load_state above.
        let mut m = new_message(3, 1, MessageType::MsgRequestVoteResponse, 0);
        m.term = term + 1;
        n.send(vec![m]);

        let mut m = new_message(1, 1, MessageType::MsgPropose, 0);
        m.entries = vec![Entry::default()].into();
        n.send(vec![m]);
        let lead_str = ltoa(&n.peers[&1].raft_log);
        let follower_str = ltoa(&n.peers[&2].raft_log);
        if lead_str != follower_str {
            panic!(
                "#{}: lead str: {}, follower_str: {}",
                i, lead_str, follower_str
            );
        }
    }
}

// test_vote_request tests that the vote request includes information about the candidate’s log
// and are sent to all of the other nodes.
// Reference: section 5.4.1
#[test]
fn test_vote_request() {
    let l = default_logger();
    let mut tests = vec![
        (vec![empty_entry(1, 1)], 2),
        (vec![empty_entry(1, 1), empty_entry(2, 2)], 3),
    ];
    for (j, (ents, wterm)) in tests.drain(..).enumerate() {
        let mut r = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage(), &l);
        let mut m = new_message(2, 1, MessageType::MsgAppend, 0);
        m.term = wterm - 1;
        m.log_term = 0;
        m.index = 0;
        m.entries = ents.clone().into();
        r.step(m).expect("");
        r.read_messages();

        for _ in 1..r.election_timeout() * 2 {
            r.tick_election();
        }

        let mut msgs = r.read_messages();
        msgs.sort_by_key(|m| format!("{:?}", m));
        if msgs.len() != 2 {
            panic!("#{}: msg count = {}, want 2", j, msgs.len());
        }
        for (i, m) in msgs.iter().enumerate() {
            if m.get_msg_type() != MessageType::MsgRequestVote {
                panic!(
                    "#{}.{}: msg_type = {:?}, want {:?}",
                    j,
                    i,
                    m.get_msg_type(),
                    MessageType::MsgRequestVote
                );
            }
            if m.to != i as u64 + 2 {
                panic!("#{}.{}: to = {}, want {}", j, i, m.to, i + 2);
            }
            if m.term != wterm {
                panic!("#{}.{}: term = {}, want {}", j, i, m.term, wterm);
            }
            let windex = ents.last().unwrap().index;
            let wlogterm = ents.last().unwrap().term;
            if m.index != windex {
                panic!("#{}.{}: index = {}, want {}", j, i, m.index, windex);
            }
            if m.log_term != wlogterm {
                panic!("#{}.{}: log_term = {}, want {}", j, i, m.log_term, wlogterm);
            }
        }
    }
}

// test_voter tests the voter denies its vote if its own log is more up-to-date
// than that of the candidate.
// Reference: section 5.4.1
#[test]
fn test_voter() {
    let l = default_logger();
    let mut tests = vec![
        // same logterm
        (vec![empty_entry(1, 1)], 1, 1, false),
        (vec![empty_entry(1, 1)], 1, 2, false),
        (vec![empty_entry(1, 1), empty_entry(1, 2)], 1, 1, true),
        // candidate higher logterm
        (vec![empty_entry(1, 1)], 2, 1, false),
        (vec![empty_entry(1, 1)], 2, 2, false),
        (vec![empty_entry(1, 1), empty_entry(1, 2)], 2, 1, false),
        // voter higher logterm
        (vec![empty_entry(2, 1)], 1, 1, true),
        (vec![empty_entry(2, 1)], 1, 2, true),
        (vec![empty_entry(2, 1), empty_entry(1, 2)], 1, 1, true),
    ];
    for (i, (ents, log_term, index, wreject)) in tests.drain(..).enumerate() {
        let s = MemStorage::new_with_conf_state((vec![1, 2], vec![]));
        s.wl().append(&ents).unwrap();
        let cfg = new_test_config(1, 10, 1);
        let mut r = new_test_raft_with_config(&cfg, s, &l);

        let mut m = new_message(2, 1, MessageType::MsgRequestVote, 0);
        m.term = 3;
        m.log_term = log_term;
        m.index = index;
        r.step(m).expect("");

        let msgs = r.read_messages();
        if msgs.len() != 1 {
            panic!("#{}: msg count = {}, want {}", i, msgs.len(), 1);
        }
        if msgs[0].get_msg_type() != MessageType::MsgRequestVoteResponse {
            panic!(
                "#{}: msg_type = {:?}, want {:?}",
                i,
                msgs[0].get_msg_type(),
                MessageType::MsgRequestVoteResponse
            );
        }
        if msgs[0].reject != wreject {
            panic!("#{}: reject = {}, want {}", i, msgs[0].reject, wreject);
        }
    }
}

// TestLeaderOnlyCommitsLogFromCurrentTerm tests that only log entries from the leader’s
// current term are committed by counting replicas.
// Reference: section 5.4.2
#[test]
fn test_leader_only_commits_log_from_current_term() {
    let l = default_logger();
    let ents = vec![empty_entry(1, 1), empty_entry(2, 2)];
    let mut tests = vec![
        // do not commit log entries in previous terms
        (1, 0),
        (2, 0),
        // commit log in current term
        (3, 3),
    ];
    for (i, (index, wcommit)) in tests.drain(..).enumerate() {
        let mut r = {
            let store = MemStorage::new_with_conf_state((vec![1, 2], vec![]));
            store.wl().append(&ents).unwrap();
            let cfg = new_test_config(1, 10, 1);
            new_test_raft_with_config(&cfg, store, &l)
        };
        r.load_state(&hard_state(2, 0, 0));

        // become leader at term 3
        r.become_candidate();
        r.become_leader();
        r.read_messages();

        // propose a entry to current term
        r.step(new_message(1, 1, MessageType::MsgPropose, 1))
            .expect("");

        let mut m = new_message(2, 1, MessageType::MsgAppendResponse, 0);
        m.term = r.term;
        m.index = index;
        r.step(m).expect("");
        if r.raft_log.committed != wcommit {
            panic!(
                "#{}: commit = {}, want {}",
                i, r.raft_log.committed, wcommit
            );
        }
    }
}
