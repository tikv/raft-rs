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

use crate::eraftpb::{ConfChange, ConfChangeType, ConfState};
use protobuf::Message;

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
///     let mut entry = Entry::new();
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
pub fn limit_size<T: Message + Clone>(entries: &mut Vec<T>, max: Option<u64>) {
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
                size += u64::from(Message::compute_size(e));
                true
            } else {
                size += u64::from(Message::compute_size(e));
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

impl From<(u64, ConfState)> for ConfChange {
    fn from((start_index, state): (u64, ConfState)) -> Self {
        let mut change = ConfChange::new();
        change.set_change_type(ConfChangeType::BeginMembershipChange);
        change.set_configuration(state);
        change.set_start_index(start_index);
        change
    }
}
