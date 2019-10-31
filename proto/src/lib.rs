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

/// Test we need to enter joint status or not after the configuration change
/// is applied. If we need, the second return value indicates we can auto leave
/// joint status or not.
pub fn enter_joint(cc: &ConfChangeV2) -> (bool, bool) {
    if cc.get_changes().len() <= 1 {
        (false, false)
    } else {
        let auto_leave = match cc.get_transition() {
            ConfChangeTransition::Auto | ConfChangeTransition::Implicit => true,
            _ => false,
        };
        (true, auto_leave)
    }
}
