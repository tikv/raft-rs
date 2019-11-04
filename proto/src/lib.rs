// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

pub use crate::protos::eraftpb;

#[allow(dead_code)]
#[allow(unknown_lints)]
#[allow(clippy::all)]
#[allow(renamed_and_removed_lints)]
#[allow(bare_trait_objects)]
mod protos {
    include!(concat!(env!("OUT_DIR"), "/protos/mod.rs"));
}

pub mod prelude {
    pub use crate::eraftpb::{
        ConfChange, ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2,
        ConfState, Entry, EntryType, HardState, Message, MessageType, Snapshot, SnapshotMetadata,
    };
}

use self::prelude::*;

impl<Iter1, Iter2> From<(Iter1, Iter2)> for ConfState
where
    Iter1: IntoIterator<Item = u64>,
    Iter2: IntoIterator<Item = u64>,
{
    fn from((voters, learners): (Iter1, Iter2)) -> Self {
        let mut conf_state = ConfState::default();
        conf_state.mut_voters().extend(voters.into_iter());
        conf_state.mut_learners().extend(learners.into_iter());
        conf_state
    }
}

impl From<ConfChange> for ConfChangeV2 {
    fn from(mut cc: ConfChange) -> ConfChangeV2 {
        let mut v2 = ConfChangeV2::default();
        v2.set_transition(ConfChangeTransition::Auto);
        let mut single = ConfChangeSingle::default();
        single.set_change_type(cc.get_change_type());
        single.set_node_id(cc.get_node_id());
        v2.mut_changes().push(single);
        v2.set_context(cc.take_context());
        v2
    }
}

impl ConfState {
    pub fn transition(base: &ConfState, target: &ConfState) -> Self {
        let mut cs = ConfState::default();
        cs.set_voters(target.get_voters().to_vec());
        cs.set_voters_outgoing(base.get_voters().to_vec());
        for id in target.get_learners() {
            if base.get_voters().binary_search(id).is_ok() {
                cs.mut_learners_next().push(*id);
            } else {
                cs.mut_learners().push(*id);
            }
        }
        cs.set_auto_leave(true);
        cs
    }
}

/// Test we need to enter joint status or not after the configuration change
/// is applied. If we need, the second return value indicates we can auto leave
/// joint status or not.
pub fn enter_joint(cc: &ConfChangeV2) -> (bool, bool) {
    let mut use_joint = cc.get_changes().len() > 1;
    use_joint |= cc.get_transition() != ConfChangeTransition::Auto;
    let mut auto_leave = use_joint;
    auto_leave &= cc.get_transition() != ConfChangeTransition::Explicit;
    (use_joint, auto_leave)
}
