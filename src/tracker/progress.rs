// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::{Inflights, ProgressState, INVALID_INDEX};
use std::cmp;

/// The progress of catching up from a restart.
#[derive(Debug, Clone, PartialEq)]
pub struct Progress {
    /// How much state is matched.
    pub matched: u64,
    /// The next index to apply
    pub next_idx: u64,
    /// When in ProgressStateProbe, leader sends at most one replication message
    /// per heartbeat interval. It also probes actual progress of the follower.
    ///
    /// When in ProgressStateReplicate, leader optimistically increases next
    /// to the latest entry sent after sending replication message. This is
    /// an optimized state for fast replicating log entries to the follower.
    ///
    /// When in ProgressStateSnapshot, leader should have sent out snapshot
    /// before and stop sending any replication message.
    pub state: ProgressState,
    /// Paused is used in ProgressStateProbe.
    /// When Paused is true, raft should pause sending replication message to this peer.
    pub paused: bool,
    /// This field is used in ProgressStateSnapshot.
    /// If there is a pending snapshot, the pendingSnapshot will be set to the
    /// index of the snapshot. If pendingSnapshot is set, the replication process of
    /// this Progress will be paused. raft will not resend snapshot until the pending one
    /// is reported to be failed.
    pub pending_snapshot: u64,
    /// This field is used in request snapshot.
    /// If there is a pending request snapshot, this will be set to the request
    /// index of the snapshot.
    pub pending_request_snapshot: u64,

    /// This is true if the progress is recently active. Receiving any messages
    /// from the corresponding follower indicates the progress is active.
    /// RecentActive can be reset to false after an election timeout.
    pub recent_active: bool,

    /// Inflights is a sliding window for the inflight messages.
    /// When inflights is full, no more message should be sent.
    /// When a leader sends out a message, the index of the last
    /// entry should be added to inflights. The index MUST be added
    /// into inflights in order.
    /// When a leader receives a reply, the previous inflights should
    /// be freed by calling inflights.freeTo.
    pub ins: Inflights,

    /// Only logs replicated to different group will be committed if any group is configured.
    pub commit_group_id: u64,

    /// Committed index in raft_log
    pub committed_index: u64,
}

impl Progress {
    /// Creates a new progress with the given settings.
    pub fn new(next_idx: u64, ins_size: usize) -> Self {
        Progress {
            matched: 0,
            next_idx,
            state: ProgressState::default(),
            paused: false,
            pending_snapshot: 0,
            pending_request_snapshot: 0,
            recent_active: false,
            ins: Inflights::new(ins_size),
            commit_group_id: 0,
            committed_index: 0,
        }
    }

    fn reset_state(&mut self, state: ProgressState) {
        self.paused = false;
        self.pending_snapshot = 0;
        self.state = state;
        self.ins.reset();
    }

    pub(crate) fn reset(&mut self, next_idx: u64) {
        self.matched = 0;
        self.next_idx = next_idx;
        self.state = ProgressState::default();
        self.paused = false;
        self.pending_snapshot = 0;
        self.pending_request_snapshot = INVALID_INDEX;
        self.recent_active = false;
        debug_assert!(self.ins.cap() != 0);
        self.ins.reset();
    }

    /// Changes the progress to a probe.
    pub fn become_probe(&mut self) {
        // If the original state is ProgressStateSnapshot, progress knows that
        // the pending snapshot has been sent to this peer successfully, then
        // probes from pendingSnapshot + 1.
        if self.state == ProgressState::Snapshot {
            let pending_snapshot = self.pending_snapshot;
            self.reset_state(ProgressState::Probe);
            self.next_idx = cmp::max(self.matched + 1, pending_snapshot + 1);
        } else {
            self.reset_state(ProgressState::Probe);
            self.next_idx = self.matched + 1;
        }
    }

    /// Changes the progress to a Replicate.
    #[inline]
    pub fn become_replicate(&mut self) {
        self.reset_state(ProgressState::Replicate);
        self.next_idx = self.matched + 1;
    }

    /// Changes the progress to a snapshot.
    #[inline]
    pub fn become_snapshot(&mut self, snapshot_idx: u64) {
        self.reset_state(ProgressState::Snapshot);
        self.pending_snapshot = snapshot_idx;
    }

    /// Sets the snapshot to failure.
    #[inline]
    pub fn snapshot_failure(&mut self) {
        self.pending_snapshot = 0;
    }

    /// Unsets pendingSnapshot if Match is equal or higher than
    /// the pendingSnapshot
    #[inline]
    pub fn maybe_snapshot_abort(&self) -> bool {
        self.state == ProgressState::Snapshot && self.matched >= self.pending_snapshot
    }

    /// Returns false if the given n index comes from an outdated message.
    /// Otherwise it updates the progress and returns true.
    pub fn maybe_update(&mut self, n: u64) -> bool {
        let need_update = self.matched < n;
        if need_update {
            self.matched = n;
            self.resume();
        };

        if self.next_idx < n + 1 {
            self.next_idx = n + 1
        }

        need_update
    }

    /// update committed_index.
    pub fn update_committed(&mut self, committed_index: u64) {
        if committed_index > self.committed_index {
            self.committed_index = committed_index
        }
    }

    /// Optimistically advance the index
    #[inline]
    pub fn optimistic_update(&mut self, n: u64) {
        self.next_idx = n + 1;
    }

    /// Returns false if the given index comes from an out of order message.
    /// Otherwise it decreases the progress next index to min(rejected, last)
    /// and returns true.
    pub fn maybe_decr_to(&mut self, rejected: u64, last: u64, request_snapshot: u64) -> bool {
        if self.state == ProgressState::Replicate {
            // the rejection must be stale if the progress has matched and "rejected"
            // is smaller than "match".
            // Or rejected equals to matched and request_snapshot is the INVALID_INDEX.
            if rejected < self.matched
                || (rejected == self.matched && request_snapshot == INVALID_INDEX)
            {
                return false;
            }
            if request_snapshot == INVALID_INDEX {
                self.next_idx = self.matched + 1;
            } else {
                self.pending_request_snapshot = request_snapshot;
            }
            return true;
        }

        // The rejection must be stale if "rejected" does not match next - 1.
        // Do not consider it stale if it is a request snapshot message.
        if (self.next_idx == 0 || self.next_idx - 1 != rejected)
            && request_snapshot == INVALID_INDEX
        {
            return false;
        }

        // Do not decrease next index if it's requesting snapshot.
        if request_snapshot == INVALID_INDEX {
            self.next_idx = cmp::min(rejected, last + 1);
            if self.next_idx < 1 {
                self.next_idx = 1;
            }
        } else if self.pending_request_snapshot == INVALID_INDEX {
            // Allow requesting snapshot even if it's not Replicate.
            self.pending_request_snapshot = request_snapshot;
        }
        self.resume();
        true
    }

    /// Determine whether progress is paused.
    #[inline]
    pub fn is_paused(&self) -> bool {
        match self.state {
            ProgressState::Probe => self.paused,
            ProgressState::Replicate => self.ins.full(),
            ProgressState::Snapshot => true,
        }
    }

    /// Resume progress
    #[inline]
    pub fn resume(&mut self) {
        self.paused = false;
    }

    /// Pause progress.
    #[inline]
    pub fn pause(&mut self) {
        self.paused = true;
    }

    /// Update inflight msgs and next_idx
    pub fn update_state(&mut self, last: u64) {
        match self.state {
            ProgressState::Replicate => {
                self.optimistic_update(last);
                self.ins.add(last);
            }
            ProgressState::Probe => self.pause(),
            ProgressState::Snapshot => panic!(
                "updating progress state in unhandled state {:?}",
                self.state
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn new_progress(
        state: ProgressState,
        matched: u64,
        next_idx: u64,
        pending_snapshot: u64,
        ins_size: usize,
    ) -> Progress {
        let mut p = Progress::new(next_idx, ins_size);
        p.state = state;
        p.matched = matched;
        p.pending_snapshot = pending_snapshot;
        p
    }

    #[test]
    fn test_progress_is_paused() {
        let tests = vec![
            (ProgressState::Probe, false, false),
            (ProgressState::Probe, true, true),
            (ProgressState::Replicate, false, false),
            (ProgressState::Replicate, true, false),
            (ProgressState::Snapshot, false, true),
            (ProgressState::Snapshot, true, true),
        ];
        for (i, &(state, paused, w)) in tests.iter().enumerate() {
            let mut p = new_progress(state, 0, 0, 0, 256);
            p.paused = paused;
            if p.is_paused() != w {
                panic!("#{}: shouldwait = {}, want {}", i, p.is_paused(), w)
            }
        }
    }

    // test_progress_resume ensures that progress.maybeUpdate and progress.maybeDecrTo
    // will reset progress.paused.
    #[test]
    fn test_progress_resume() {
        let mut p = Progress::new(2, 256);
        p.paused = true;
        p.maybe_decr_to(1, 1, INVALID_INDEX);
        assert!(!p.paused, "paused= true, want false");
        p.paused = true;
        p.maybe_update(2);
        assert!(!p.paused, "paused= true, want false");
    }

    #[test]
    fn test_progress_become_probe() {
        let matched = 1u64;
        let mut tests = vec![
            (
                new_progress(ProgressState::Replicate, matched, 5, 0, 256),
                2,
            ),
            // snapshot finish
            (
                new_progress(ProgressState::Snapshot, matched, 5, 10, 256),
                11,
            ),
            // snapshot failure
            (new_progress(ProgressState::Snapshot, matched, 5, 0, 256), 2),
        ];
        for (i, &mut (ref mut p, wnext)) in tests.iter_mut().enumerate() {
            p.become_probe();
            if p.state != ProgressState::Probe {
                panic!(
                    "#{}: state = {:?}, want {:?}",
                    i,
                    p.state,
                    ProgressState::Probe
                );
            }
            if p.matched != matched {
                panic!("#{}: match = {:?}, want {:?}", i, p.matched, matched);
            }
            if p.next_idx != wnext {
                panic!("#{}: next = {}, want {}", i, p.next_idx, wnext);
            }
        }
    }

    #[test]
    fn test_progress_become_replicate() {
        let mut p = new_progress(ProgressState::Probe, 1, 5, 0, 256);
        p.become_replicate();

        assert_eq!(p.state, ProgressState::Replicate);
        assert_eq!(p.matched, 1);
        assert_eq!(p.matched + 1, p.next_idx);
    }

    #[test]
    fn test_progress_become_snapshot() {
        let mut p = new_progress(ProgressState::Probe, 1, 5, 0, 256);
        p.become_snapshot(10);
        assert_eq!(p.state, ProgressState::Snapshot);
        assert_eq!(p.matched, 1);
        assert_eq!(p.pending_snapshot, 10);
    }

    #[test]
    fn test_progress_update() {
        let (prev_m, prev_n) = (3u64, 5u64);
        let tests = vec![
            (prev_m - 1, prev_m, prev_n, false),
            (prev_m, prev_m, prev_n, false),
            (prev_m + 1, prev_m + 1, prev_n, true),
            (prev_m + 2, prev_m + 2, prev_n + 1, true),
        ];
        for (i, &(update, wm, wn, wok)) in tests.iter().enumerate() {
            let mut p = Progress::new(prev_n, 256);
            p.matched = prev_m;
            let ok = p.maybe_update(update);
            if ok != wok {
                panic!("#{}: ok= {}, want {}", i, ok, wok);
            }
            if p.matched != wm {
                panic!("#{}: match= {}, want {}", i, p.matched, wm);
            }
            if p.next_idx != wn {
                panic!("#{}: next= {}, want {}", i, p.next_idx, wn);
            }
        }
    }

    #[test]
    fn test_progress_maybe_decr() {
        let tests = vec![
            // state replicate and rejected is not greater than match
            (ProgressState::Replicate, 5, 10, 5, 5, false, 10),
            // state replicate and rejected is not greater than match
            (ProgressState::Replicate, 5, 10, 4, 4, false, 10),
            // state replicate and rejected is greater than match
            // directly decrease to match+1
            (ProgressState::Replicate, 5, 10, 9, 9, true, 6),
            // next-1 != rejected is always false
            (ProgressState::Probe, 0, 0, 0, 0, false, 0),
            // next-1 != rejected is always false
            (ProgressState::Probe, 0, 10, 5, 5, false, 10),
            // next>1 = decremented by 1
            (ProgressState::Probe, 0, 10, 9, 9, true, 9),
            // next>1 = decremented by 1
            (ProgressState::Probe, 0, 2, 1, 1, true, 1),
            // next<=1 = reset to 1
            (ProgressState::Probe, 0, 1, 0, 0, true, 1),
            // decrease to min(rejected, last+1)
            (ProgressState::Probe, 0, 10, 9, 2, true, 3),
            // rejected < 1, reset to 1
            (ProgressState::Probe, 0, 10, 9, 0, true, 1),
        ];
        for (i, &(state, m, n, rejected, last, w, wn)) in tests.iter().enumerate() {
            let mut p = new_progress(state, m, n, 0, 0);
            if p.maybe_decr_to(rejected, last, 0) != w {
                panic!("#{}: maybeDecrTo= {}, want {}", i, !w, w);
            }
            if p.matched != m {
                panic!("#{}: match= {}, want {}", i, p.matched, m);
            }
            if p.next_idx != wn {
                panic!("#{}: next= {}, want {}", i, p.next_idx, wn);
            }
        }
    }
}
