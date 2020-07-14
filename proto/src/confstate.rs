// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::eraftpb::ConfState;

fn eq_without_order(lhs: &[u64], rhs: &[u64]) -> bool {
    for l in lhs {
        if !rhs.contains(l) {
            return false;
        }
    }
    for r in rhs {
        if !lhs.contains(r) {
            return false;
        }
    }
    true
}

// Returns true if the inputs describe the same configuration.
pub fn conf_state_eq(lhs: &ConfState, rhs: &ConfState) -> bool {
    eq_without_order(lhs.get_voters(), rhs.get_voters())
        && eq_without_order(lhs.get_learners(), rhs.get_learners())
        && eq_without_order(lhs.get_voters_outgoing(), rhs.get_voters_outgoing())
        && eq_without_order(lhs.get_learners_next(), rhs.get_learners_next())
        && lhs.auto_leave == rhs.auto_leave
}
