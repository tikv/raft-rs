//! This module contains a collection of various tools to use to manipulate
//! and control messages and data associated with raft.

// Copyright 2017 PingCAP, Inc.
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

use std::u64;

use crate::eraftpb::{ConfChange, ConfChangeType, ConfState, Entry, Message};
use prost::Message as ProstMsg;

/// A number to represent that there is no limit.
pub const NO_LIMIT: u64 = u64::MAX;

/// Truncates the list of entries down to a specific byte-length of
/// all entries together.
///
/// # Examples
///
/// ```
/// use raft::{util::limit_size, prelude::*};
///
/// let template = {
///     let mut entry = Entry::new_();
///     entry.set_data("*".repeat(100).into_bytes());
///     entry
/// };
///
/// // Make a bunch of entries that are ~100 bytes long
/// let mut entries = vec![
///     template.clone(),
///     template.clone(),
///     template.clone(),
///     template.clone(),
///     template.clone(),
/// ];
///
/// assert_eq!(entries.len(), 5);
/// limit_size(&mut entries, Some(220));
/// assert_eq!(entries.len(), 2);
///
/// // `entries` will always have at least 1 Message
/// limit_size(&mut entries, Some(0));
/// assert_eq!(entries.len(), 1);
/// ```
pub fn limit_size<T: ProstMsg + Clone>(entries: &mut Vec<T>, max: Option<u64>) {
    if entries.len() <= 1 {
        return;
    }
    let max = match max {
        None | Some(NO_LIMIT) => return,
        Some(max) => max,
    };

    let mut size = 0;
    let limit = entries
        .iter()
        .take_while(|&e| {
            if size == 0 {
                size += ProstMsg::encoded_len(e) as u64;
                true
            } else {
                size += ProstMsg::encoded_len(e) as u64;
                size <= max
            }
        })
        .count();

    entries.truncate(limit);
}

// Bring some consistency to things. The protobuf has `nodes` and it's not really a term that's used anymore.
impl ConfState {
    /// Get the voters. This is identical to `get_nodes()`.
    #[inline]
    pub fn get_voters(&self) -> &[u64] {
        self.get_nodes()
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
        let mut change = ConfChange::new_();
        change.set_change_type(ConfChangeType::BeginMembershipChange);
        change.set_configuration(state);
        change.set_start_index(start_index);
        change
    }
}

/// Check whether the entry is continuous to the message.
/// i.e msg's next entry index should be equal to the first entries's index
pub fn is_continuous_ents(msg: &Message, ents: &[Entry]) -> bool {
    if !msg.get_entries().is_empty() && !ents.is_empty() {
        let expected_next_idx = msg.get_entries().last().unwrap().get_index() + 1;
        return expected_next_idx == ents.first().unwrap().get_index();
    }
    true
}
