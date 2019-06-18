// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

pub use crate::prost::eraftpb;

mod prost;

pub mod prelude {
    pub use crate::eraftpb::{
        ConfChange, ConfChangeType, ConfState, Entry, EntryType, HardState, Message, MessageType,
        Snapshot, SnapshotMetadata,
    };
}

pub mod util {
    use crate::eraftpb::{ConfChange, ConfChangeType, ConfState};

    // Bring some consistency to things. The protobuf has `nodes` and it's not really a term that's used anymore.
    impl ConfState {
        /// Get the voters. This is identical to `nodes`.
        #[inline]
        pub fn get_voters(&self) -> &[u64] {
            &self.nodes
        }
    }

    impl<Iter1, Iter2> From<(Iter1, Iter2)> for ConfState
    where
        Iter1: IntoIterator<Item = u64>,
        Iter2: IntoIterator<Item = u64>,
    {
        fn from((voters, learners): (Iter1, Iter2)) -> Self {
            let mut conf_state = ConfState::default();
            conf_state.mut_nodes().extend(voters.into_iter());
            conf_state.mut_learners().extend(learners.into_iter());
            conf_state
        }
    }

    impl From<(u64, ConfState)> for ConfChange {
        fn from((start_index, state): (u64, ConfState)) -> Self {
            let mut change = ConfChange::default();
            change.set_change_type(ConfChangeType::BeginMembershipChange);
            change.set_configuration(state);
            change.set_start_index(start_index);
            change
        }
    }
}
