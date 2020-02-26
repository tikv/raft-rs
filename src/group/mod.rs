// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.
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

//! The module includes the definition of new feature 'raft group' which is used for
//! **Follower Replication**
//!
//! # Follower Replication
//! See https://github.com/tikv/rfcs/pull/33

use std::collections::HashMap;

use crate::progress::progress_set::ProgressSet;
use crate::raft::INVALID_ID;

/// Maintain all the groups info in Follower Replication
///
/// # Notice
///
/// A node only belongs to one group
///
#[derive(Debug, Clone, Default)]
pub struct Groups {
    // node id => (group id, delegate id).
    indexes: HashMap<u64, (u64, u64)>,

    // Use to construct `bcast_targets` for delegates quickly.
    bcast_targets: HashMap<u64, Vec<u64>>,

    // Peers without chosen delegates.
    unresolved: Vec<u64>,

    leader_group_id: u64,
}

impl Groups {
    /// Create new Groups with given configuration.
    pub(crate) fn new(config: Vec<(u64, Vec<u64>)>) -> Self {
        let mut indexes = HashMap::new();
        let mut unresolved = Vec::new();
        for (group_id, members) in config {
            for id in members {
                indexes.insert(id, (group_id, INVALID_ID));
                unresolved.push(id);
            }
        }

        Self {
            indexes,
            unresolved,
            ..Default::default()
        }
    }

    pub(crate) fn set_leader_group_id(&mut self, leader_group: u64) {
        self.leader_group_id = leader_group;
    }

    /// Get group id by member id.
    pub(crate) fn get_group_id(&self, member: u64) -> Option<u64> {
        self.indexes.get(&member).map(|(gid, _)| *gid)
    }

    /// Get a delegate for `to`. The return value could be `to` itself.
    pub fn get_delegate(&self, to: u64) -> u64 {
        match self.indexes.get(&to) {
            Some((_, delegate)) => *delegate,
            None => INVALID_ID,
        }
    }

    // Pick a delegate for the given peer.
    //
    // The delegate must satisfy conditions below:
    // 1. The progress state should be `ProgressState::Replicate`;
    // 2. The progress has biggest `match`;
    // If all the members are requiring snapshots, use given `to`.
    fn pick_delegate(&mut self, to: u64, prs: &ProgressSet) {
        let group_id = match self.indexes.get(&to) {
            Some((_, delegate)) if *delegate != INVALID_ID => return,
            Some((gid, _)) if *gid == self.leader_group_id => return,
            Some((gid, _)) => *gid,
            None => return,
        };

        let (mut chosen, mut matched, mut bcast_targets) = (INVALID_ID, 0, vec![]);
        for id in self.candidate_delegates(group_id) {
            let pr = prs.get(id).unwrap();
            if matched < pr.matched {
                if chosen != INVALID_ID {
                    bcast_targets.push(chosen);
                }
                chosen = id;
                matched = pr.matched;
            } else {
                bcast_targets.push(id);
            }
        }

        // If there is only one member in the group, it remains unresolved.
        if chosen != INVALID_ID && !bcast_targets.is_empty() {
            let (_, d) = self.indexes.get_mut(&chosen).unwrap();
            *d = chosen;
            for id in &bcast_targets {
                let (_, d) = self.indexes.get_mut(id).unwrap();
                *d = chosen;
            }
            self.bcast_targets.insert(chosen, bcast_targets);
        }
    }

    fn candidate_delegates(&self, group_id: u64) -> impl Iterator<Item = u64> + '_ {
        self.indexes.iter().filter_map(move |(peer, (gid, _))| {
            if group_id == *gid {
                return Some(*peer);
            }
            None
        })
    }

    /// Unset the delegate by delegate id. If the peer is not delegate, do nothing.
    pub(crate) fn remove_delegate(&mut self, delegate: u64) {
        if self.bcast_targets.remove(&delegate).is_some() {
            // Remove the delegate from the group system since it's temorary unreachable.
            // And the peer will be re-added after the leader receives a message from it.
            self.indexes.remove(&delegate);
            for (peer, (_, d)) in self.indexes.iter_mut() {
                if *d == delegate {
                    *d = INVALID_ID;
                    self.unresolved.push(*peer);
                }
            }
        }
    }

    pub(crate) fn is_delegated(&self, to: u64) -> bool {
        self.indexes
            .get(&to)
            .map_or(false, |x| x.1 != INVALID_ID && x.1 != to)
    }

    pub(crate) fn get_bcast_targets(&self, delegate: u64) -> Option<&Vec<u64>> {
        debug_assert!(self.unresolved.is_empty());
        self.bcast_targets.get(&delegate)
    }

    /// Update given `peer`'s group ID. Return `true` if any peers are unresolved.
    pub(crate) fn update_group_id(&mut self, peer: u64, group_id: u64) -> bool {
        if group_id == INVALID_ID {
            self.unmark_peer(peer);
        } else if let Some((gid, _)) = self.indexes.get(&peer) {
            if *gid == group_id {
                return false;
            }
            self.unmark_peer(peer);
            self.mark_peer(peer, group_id);
        } else {
            self.mark_peer(peer, group_id);
        }
        !self.unresolved.is_empty()
    }

    fn unmark_peer(&mut self, peer: u64) {
        if let Some((_, del)) = self.indexes.remove(&peer) {
            if peer == del {
                self.remove_delegate(del);
                return;
            }
            let mut targets = self.bcast_targets.remove(&del).unwrap();
            let pos = targets.iter().position(|id| *id == peer).unwrap();
            targets.swap_remove(pos);
            if !targets.is_empty() {
                self.bcast_targets.insert(del, targets);
            }
        }
    }

    fn mark_peer(&mut self, peer: u64, group_id: u64) {
        let (found, delegate) = self
            .indexes
            .iter()
            .find(|(_, (gid, _))| *gid == group_id)
            .map_or((false, INVALID_ID), |(_, (_, d))| (true, *d));

        let _x = self.indexes.insert(peer, (group_id, delegate));
        debug_assert!(_x.is_none(), "peer can't exist before mark");

        if delegate != INVALID_ID {
            self.bcast_targets.get_mut(&delegate).unwrap().push(peer);
        } else if found {
            // We have found a peer in the same group but haven't been delegated, add it to
            // `unresolved`.
            self.unresolved.push(peer);
        }
    }

    // Pick delegates for all peers if need.
    // TODO: change to `pub(crate)` after we find a simple way to test.
    pub fn resolve_delegates(&mut self, prs: &ProgressSet) {
        if !self.unresolved.is_empty() {
            for peer in std::mem::replace(&mut self.unresolved, vec![]) {
                self.pick_delegate(peer, prs);
            }
        }
    }

    // Return the collection of mapping: group id => members
    pub fn dump(&self) -> Vec<(u64, Vec<u64>)> {
        let mut m: HashMap<u64, Vec<u64>> = HashMap::new();
        for (peer, (group, _)) in &self.indexes {
            let v = m.entry(*group).or_default();
            v.push(*peer);
        }
        for v in m.values_mut() {
            v.sort();
        }
        let mut v: Vec<_> = m.into_iter().collect();
        v.sort_by(|a1, a2| a1.0.cmp(&a2.0));
        v
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::progress::Progress;

    fn next_delegate_and_bcast_targets(group: &Groups) -> (u64, Vec<u64>) {
        group
            .bcast_targets
            .iter()
            .next()
            .map(|(k, v)| (*k, v.clone()))
            .unwrap()
    }

    #[test]
    fn test_group() {
        let mut group = Groups::new(vec![(1, vec![1, 2]), (2, vec![3, 4, 5]), (3, vec![6])]);
        assert_eq!(group.unresolved.len(), 6);
        group.set_leader_group_id(1);

        let mut prs = ProgressSet::new(crate::default_logger());
        for id in 1..=6 {
            let mut pr = Progress::new(100, 100);
            pr.matched = 99;
            prs.insert_voter(id, pr).unwrap();
        }

        // After the resolving, only group 2 should be delegated.
        group.resolve_delegates(&prs);
        assert_eq!(group.bcast_targets.len(), 1);
        let (delegate, mut targets) = next_delegate_and_bcast_targets(&group);
        targets.push(delegate);
        targets.sort();
        assert_eq!(targets, [3, 4, 5]);

        // Remove a delegate which doesn't exists.
        group.remove_delegate(6);
        assert!(group.unresolved.is_empty());

        // Remove a peer which is not delegate.
        let remove = match delegate {
            3 => 4,
            4 => 5,
            5 => 3,
            _ => unreachable!(),
        };
        group.remove_delegate(remove);
        assert!(group.unresolved.is_empty());
        let (_, targets) = next_delegate_and_bcast_targets(&group);
        assert_eq!(targets.len(), 2);

        // Remove a delegate.
        group.remove_delegate(delegate);
        assert_eq!(group.unresolved.len(), 2);
        group.resolve_delegates(&prs);
        let (_, targets) = next_delegate_and_bcast_targets(&group);
        assert_eq!(targets.len(), 1);

        // Add the removed peer back, without group id.
        let peer = delegate;
        group.update_group_id(peer, INVALID_ID);
        assert!(group.unresolved.is_empty());

        // Add the removed peer back, with group id.
        assert!(!group.update_group_id(peer, 2));
        let (_, targets) = next_delegate_and_bcast_targets(&group);
        assert_eq!(targets.len(), 2);

        // Get the new delegate.
        let (delegate, _) = next_delegate_and_bcast_targets(&group);
        assert_ne!(peer, delegate);

        // The peer reports to the group again, without group id.
        group.update_group_id(peer, INVALID_ID);
        let (_, targets) = next_delegate_and_bcast_targets(&group);
        assert_eq!(targets.len(), 1);

        // The delegate changes group to 3.
        assert!(group.update_group_id(delegate, 3));
        assert!(group.bcast_targets.is_empty());
        group.resolve_delegates(&prs);
        let (_, targets) = next_delegate_and_bcast_targets(&group);
        assert!(targets.contains(&delegate) || targets.contains(&6));
        assert!(group.get_delegate(6) == 6 || group.get_delegate(6) == delegate);
    }
}
