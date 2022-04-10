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
#[must_use]
pub fn conf_state_eq(lhs: &ConfState, rhs: &ConfState) -> bool {
    // The orders are different only when hash algorithm or insert orders are
    // different. In most case, only one hash algorithm is used. Insert orders
    // should be the same due to the raft protocol. So in most case, they can
    // be compared directly.
    if lhs.voters == rhs.voters
        && lhs.learners == rhs.learners
        && lhs.voters_outgoing == rhs.voters_outgoing
        && lhs.learners_next == rhs.learners_next
        && lhs.auto_leave == rhs.auto_leave
    {
        return true;
    }

    eq_without_order(&lhs.voters, &rhs.voters)
        && eq_without_order(&lhs.learners, &rhs.learners)
        && eq_without_order(&lhs.voters_outgoing, &rhs.voters_outgoing)
        && eq_without_order(&lhs.learners_next, &rhs.learners_next)
        && lhs.auto_leave == rhs.auto_leave
}
