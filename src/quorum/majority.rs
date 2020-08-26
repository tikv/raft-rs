// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::{AckedIndexer, Index, VoteResult};
use crate::{DefaultHashBuilder, HashSet};
use std::cmp::Ordering;
use std::mem::MaybeUninit;
use std::ops::{Deref, DerefMut};
use std::{cmp, slice, u64};

/// A set of IDs that uses majority quorums to make decisions.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Configuration {
    voters: HashSet<u64>,
}

impl Configuration {
    /// Creates a new configuration using the given IDs.
    pub fn new(voters: HashSet<u64>) -> Configuration {
        Configuration { voters }
    }

    /// Creates an empty configuration with given capacity.
    pub fn with_capacity(cap: usize) -> Configuration {
        Configuration {
            voters: HashSet::with_capacity_and_hasher(cap, DefaultHashBuilder::default()),
        }
    }

    /// Returns the MajorityConfig as a sorted slice.
    pub fn slice(&self) -> Vec<u64> {
        let mut voters = self.raw_slice();
        voters.sort();
        voters
    }

    /// Returns the MajorityConfig as a slice.
    pub fn raw_slice(&self) -> Vec<u64> {
        self.voters.iter().cloned().collect()
    }

    /// Computes the committed index from those supplied via the
    /// provided AckedIndexer (for the active config).
    ///
    /// The bool flag indicates whether the index is computed by group commit algorithm
    /// successfully.
    ///
    /// Eg. If the matched indexes are [2,2,2,4,5], it will return 2.
    /// If the matched indexes and groups are `[(1, 1), (2, 2), (3, 2)]`, it will return 1.
    pub fn committed_index(&self, use_group_commit: bool, l: &impl AckedIndexer) -> (u64, bool) {
        if self.voters.is_empty() {
            // This plays well with joint quorums which, when one half is the zero
            // MajorityConfig, should behave like the other half.
            return (u64::MAX, true);
        }

        let mut stack_arr: [MaybeUninit<Index>; 7] = unsafe { MaybeUninit::uninit().assume_init() };
        let mut heap_arr;
        let matched = if self.voters.len() <= 7 {
            for (i, v) in self.voters.iter().enumerate() {
                stack_arr[i] = MaybeUninit::new(l.acked_index(*v).unwrap_or_default());
            }
            unsafe {
                slice::from_raw_parts_mut(stack_arr.as_mut_ptr() as *mut _, self.voters.len())
            }
        } else {
            let mut buf = Vec::with_capacity(self.voters.len());
            for v in &self.voters {
                buf.push(l.acked_index(*v).unwrap_or_default());
            }
            heap_arr = Some(buf);
            heap_arr.as_mut().unwrap().as_mut_slice()
        };
        // Reverse sort.
        matched.sort_by(|a, b| b.index.cmp(&a.index));

        let quorum = crate::majority(matched.len());
        let quorum_index = matched[quorum - 1];
        if !use_group_commit {
            return (quorum_index.index, false);
        }
        let (quorum_commit_index, mut checked_group_id) =
            (quorum_index.index, quorum_index.group_id);
        let mut single_group = true;
        for m in matched.iter() {
            if m.group_id == 0 {
                single_group = false;
                continue;
            }
            if checked_group_id == 0 {
                checked_group_id = m.group_id;
                continue;
            }
            if checked_group_id == m.group_id {
                continue;
            }
            return (cmp::min(m.index, quorum_commit_index), true);
        }
        if single_group {
            (quorum_commit_index, false)
        } else {
            (matched.last().unwrap().index, false)
        }
    }

    /// Takes a mapping of voters to yes/no (true/false) votes and returns
    /// a result indicating whether the vote is pending (i.e. neither a quorum of
    /// yes/no has been reached), won (a quorum of yes has been reached), or lost (a
    /// quorum of no has been reached).
    pub fn vote_result(&self, check: impl Fn(u64) -> Option<bool>) -> VoteResult {
        if self.voters.is_empty() {
            // By convention, the elections on an empty config win. This comes in
            // handy with joint quorums because it'll make a half-populated joint
            // quorum behave like a majority quorum.
            return VoteResult::Won;
        }

        let (mut yes, mut missing) = (0, 0);
        for v in &self.voters {
            match check(*v) {
                Some(true) => yes += 1,
                None => missing += 1,
                _ => (),
            }
        }
        let q = crate::majority(self.voters.len());
        if yes >= q {
            VoteResult::Won
        } else if yes + missing >= q {
            VoteResult::Pending
        } else {
            VoteResult::Lost
        }
    }

    /// Describe returns a (multi-line) representation of the commit indexes for the
    /// given lookuper.
    pub fn describe(&self, l: &impl AckedIndexer) -> String {
        let n = self.voters.len();
        if n == 0 {
            return "<empty majority quorum>".to_string();
        }

        struct Tup {
            id: u64,
            idx: Option<Index>,
            // length of bar displayed for this Tup
            bar: usize,
        }

        // Below, populate .bar so that the i-th largest commit index has bar i (we
        // plot this as sort of a progress bar). The actual code is a bit more
        // complicated and also makes sure that equal index => equal bar.

        let mut info = vec![];

        for id in self.raw_slice() {
            let idx = l.acked_index(id);
            info.push(Tup { id, idx, bar: 0 })
        }

        info.sort_by(|a, b| match a.idx.cmp(&b.idx) {
            Ordering::Equal => a.id.cmp(&b.id),
            ordering => ordering,
        });

        for i in 0..n {
            if i > 0 && info[i - 1].idx < info[i].idx {
                info[i].bar = i;
            }
        }

        info.sort_by(|a, b| a.id.cmp(&b.id));

        let mut buf = String::new();
        buf.push_str(&(" ".repeat(n) + "    idx\n"));

        for tup in info {
            let string;
            if let Some(idx) = tup.idx {
                string = "x".repeat(tup.bar)
                    + ">"
                    + " ".repeat(n - tup.bar).as_str()
                    + format!(" {:>10} (id={})\n", format!("{}", idx), tup.id).as_str();
            } else {
                string = String::from("?")
                    + " ".repeat(n).as_str()
                    + format!(" {:>10} (id={})\n", format!("{}", Index::default()), tup.id)
                        .as_str();
            }
            buf.push_str(string.as_str());
        }
        buf
    }
}

impl Deref for Configuration {
    type Target = HashSet<u64>;

    #[inline]
    fn deref(&self) -> &HashSet<u64> {
        &self.voters
    }
}

impl DerefMut for Configuration {
    #[inline]
    fn deref_mut(&mut self) -> &mut HashSet<u64> {
        &mut self.voters
    }
}
