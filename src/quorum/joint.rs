// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::{AckedIndexer, VoteResult};
use crate::quorum::Index;
use crate::util::Union;
use crate::HashSet;
use crate::MajorityConfig;
use std::cmp;

/// A configuration of two groups of (possibly overlapping) majority configurations.
/// Decisions require the support of both majorities.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Configuration {
    pub(crate) incoming: MajorityConfig,
    pub(crate) outgoing: MajorityConfig,
    ids: HashSet<u64>,
}

impl Configuration {
    /// Creates a new configuration using the given IDs.
    pub fn new(voters: HashSet<u64>) -> Self {
        Self {
            ids: voters.clone(),
            incoming: MajorityConfig::new(voters),
            outgoing: MajorityConfig::default(),
        }
    }

    /// Creates a new configuration using the given IDs.
    pub fn new_joint(incoming: HashSet<u64>, outgoing: HashSet<u64>) -> Self {
        Self {
            ids: incoming.union(&outgoing).into_iter().cloned().collect(),
            incoming: MajorityConfig::new(incoming),
            outgoing: MajorityConfig::new(outgoing),
        }
    }

    /// Creates a new configuration using the given IDs.
    pub fn new_joint_from_configs(incoming: MajorityConfig, outgoing: MajorityConfig) -> Self {
        let ids: HashSet<_> = incoming
            .get_voters()
            .union(&outgoing.get_voters())
            .into_iter()
            .cloned()
            .collect();
        Self {
            ids,
            incoming,
            outgoing,
        }
    }

    /// Creates an empty configuration with given capacity.
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            incoming: MajorityConfig::with_capacity(cap),
            outgoing: MajorityConfig::default(),
            ids: HashSet::default(),
        }
    }

    /// Returns the largest committed index for the given joint quorum. An index is
    /// jointly committed if it is committed in both constituent majorities.
    ///
    /// The bool flag indicates whether the index is computed by group commit algorithm
    /// successfully. It's true only when both majorities use group commit.
    pub fn committed_index(&self, use_group_commit: bool, l: &impl AckedIndexer) -> (Index, bool) {
        let (i_idx, i_use_gc) = self.incoming.committed_index(use_group_commit, l);
        let (o_idx, o_use_gc) = self.outgoing.committed_index(use_group_commit, l);
        (cmp::min(i_idx, o_idx), i_use_gc && o_use_gc)
    }

    /// Takes a mapping of voters to yes/no (true/false) votes and returns a result
    /// indicating whether the vote is pending, lost, or won. A joint quorum requires
    /// both majority quorums to vote in favor.
    pub fn vote_result(&self, check: impl Fn(u64) -> Option<bool>) -> VoteResult {
        let i = self.incoming.vote_result(&check);
        let o = self.outgoing.vote_result(check);
        match (i, o) {
            // It won if won in both.
            (VoteResult::Won, VoteResult::Won) => VoteResult::Won,
            // It lost if lost in either.
            (VoteResult::Lost, _) | (_, VoteResult::Lost) => VoteResult::Lost,
            // It remains pending if pending in both or just won in one side.
            _ => VoteResult::Pending,
        }
    }

    /// Clears all IDs.
    pub fn clear(&mut self) {
        self.incoming.clear();
        self.outgoing.clear();
    }

    /// Returns true if (and only if) there is only one voting member
    /// (i.e. the leader) in the current configuration.
    pub fn is_singleton(&self) -> bool {
        self.outgoing.is_empty() && self.incoming.len() == 1
    }

    /// Returns an iterator over two hash set with cloning.
    pub fn ids(&self) -> Union<'_> {
        Union::new(&self.incoming, &self.outgoing)
    }

    /// Check if an id is a voter.
    #[inline]
    pub fn contains(&self, id: u64) -> bool {
        self.incoming.contains(&id) || self.outgoing.contains(&id)
    }

    /// add doc
    pub fn describe(&self, l: &impl AckedIndexer) -> String {
        MajorityConfig::new(self.ids.clone()).describe(l)
    }
}

// #[cfg(test)]
// mod tests {
//     use crate::quorum::{AckIndexer, Index, VoteResult};
//     use crate::Result;
//     use crate::{HashMap, HashSet, JointConfig};
//     use serde::Deserialize;
//     use std::fs;
//
//     #[derive(Debug, Deserialize)]
//     struct MyValue {
//         command: String,
//         cfg: Vec<u64>,
//         cfgj: Vec<u64>,
//         idx: Vec<Index>,
//         expected_idx: u64,
//         expected_use_group_commit: bool,
//         vote: Vec<String>,
//         expected_vote: String,
//     }
//
//     #[derive(Debug, Deserialize)]
//     struct TestCase {
//         data: Vec<MyValue>,
//     }
//
//     #[test]
//     fn test_quorum_json() -> Result<()> {
//         let mut paths = vec![
//             "src/quorum/testdata/joint_commit.json",
//             "src/quorum/testdata/joint_vote.json",
//             "src/quorum/testdata/joint_group_commit.json",
//         ];
//         for path in paths.drain(..) {
//             let file = fs::read_to_string(path)?;
//             let mut test_case: TestCase = serde_json::from_str(&file).unwrap();
//             for (i, data) in test_case.data.drain(..).enumerate() {
//                 let cfg_set: HashSet<u64> = data.cfg.into_iter().collect();
//                 let cfgj_set: HashSet<u64> = data.cfgj.into_iter().collect();
//                 let mut voters: Vec<u64> = cfg_set.union(&cfgj_set).into_iter().cloned().collect();
//                 voters.sort();
//
//                 match data.command.as_str() {
//                     "commit" => {
//                         assert_eq!(
//                             voters.len(),
//                             data.idx.len(),
//                             "[test_cases #{}] length of Index mismatched.",
//                             i + 1
//                         );
//
//                         let mut l: AckIndexer = AckIndexer::default();
//
//                         for (i, id) in voters.drain(..).enumerate() {
//                             l.insert(id, data.idx[i]);
//                         }
//                         let index1 = JointConfig::new_joint(cfg_set.clone(), cfgj_set.clone())
//                             .committed_index(false, &l)
//                             .0;
//                         let index2 = JointConfig::new_joint(cfgj_set, cfg_set)
//                             .committed_index(false, &l)
//                             .0;
//
//                         assert_eq!(
//                             index1,
//                             index2,
//                             "test_cases #{}: Interchanging the majorities shouldn't make a difference",
//                             i + 1
//                         );
//                         assert_eq!(
//                             index1,
//                             data.expected_idx,
//                             "test_cases #{}: Mismatched Index, expected '{}', found '{}'",
//                             i + 1,
//                             data.expected_idx,
//                             index1
//                         );
//                     }
//                     "vote" => {
//                         assert_eq!(
//                             voters.len(),
//                             data.vote.len(),
//                             "test_cases #{}: length of vote mismatched.",
//                             i + 1
//                         );
//                         let mut l: HashMap<u64, bool> = HashMap::default();
//                         for (i, id) in voters.drain(..).enumerate() {
//                             match data.vote[i].as_str() {
//                                 "y" => l.insert(id, true),
//                                 "n" => l.insert(id, false),
//                                 "?" => None,
//                                 _ => {
//                                     panic!("unknown token, check file {}", path);
//                                 }
//                             };
//                         }
//
//                         let vote_result1 =
//                             JointConfig::new_joint(cfg_set.clone(), cfgj_set.clone())
//                                 .vote_result(|id| l.get(&id).cloned());
//                         let vote_result2 = JointConfig::new_joint(cfgj_set, cfg_set)
//                             .vote_result(|id| l.get(&id).cloned());
//                         assert_eq!(
//                             vote_result1,
//                             vote_result2,
//                             "test_cases #{}: Interchanging the majorities shouldn't make a difference",
//                             i + 1,
//                         );
//                         assert_eq!(
//                             vote_result1.to_string(),
//                             data.expected_vote,
//                             "test_cases #{}: Mismatched VoteResult",
//                             i + 1
//                         );
//                     }
//                     "group_commit" => {
//                         assert_eq!(
//                             voters.len(),
//                             data.idx.len(),
//                             "[test_cases #{}] length of Index mismatched.",
//                             i + 1
//                         );
//
//                         let mut l: AckIndexer = AckIndexer::default();
//
//                         for (i, id) in voters.drain(..).enumerate() {
//                             l.insert(id, data.idx[i]);
//                         }
//                         let (index1, use_group_commit1) =
//                             JointConfig::new_joint(cfg_set.clone(), cfgj_set.clone())
//                                 .committed_index(true, &l);
//                         let (index2, use_group_commit2) =
//                             JointConfig::new_joint(cfgj_set, cfg_set).committed_index(true, &l);
//
//                         assert_eq!((index1, use_group_commit1), (index2, use_group_commit2), "test_case #{}: Interchanging the majorities shouldn't make a difference.", i+1);
//                         assert_eq!(
//                             (index1, use_group_commit1),
//                             (data.expected_idx, data.expected_use_group_commit),
//                             "test_case #{}: Mismatched Result",
//                             i + 1
//                         );
//                     }
//                     _ => {
//                         panic!("unknown command, check file '{}'", path);
//                     }
//                 }
//             }
//         }
//         Ok(())
//     }
//
//     #[test]
//     fn test_joint_commit_single_group() {
//         let mut test_cases = vec![
//             // [1] No difference between a simple majority quorum
//             // and a simple majority quorum joint with an empty majority quorum.
//             (vec![1, 2, 3], vec![0; 0], vec![100, 101, 99], 100),
//             // [2] Joint nonoverlapping singleton quorums.
//             (vec![1], vec![2], vec![0, 0], 0),
//             // [3] Voter 1 has 100 committed, 2 nothing. This means we definitely won't commit
//             // past 100.
//             (vec![1], vec![2], vec![100, 0], 0),
//             // [4] Committed index collapses once both majorities do, to the lower index.
//             (vec![1], vec![2], vec![13, 100], 13),
//             // [5] Joint overlapping (i.e. identical) singleton quorum.
//             (vec![1], vec![1], vec![0], 0),
//             (vec![1], vec![1], vec![100], 100),
//             // [6] Two-node config joint with non-overlapping single node config
//             (vec![1, 3], vec![2], vec![0, 0, 0], 0),
//             (vec![1, 3], vec![2], vec![100, 0, 0], 0),
//             // [7] 1 has 100 committed, 2 has 50 (collapsing half of the joint quorum to 50).
//             (vec![1, 3], vec![2], vec![100, 0, 50], 0),
//             // [8] 2 reports 45, collapsing the other half (to 45).
//             (vec![1, 3], vec![2], vec![100, 45, 50], 45),
//             //
//             // test_cases: 11
//             // [9] Two-node config with overlapping single-node config.
//             (vec![1, 2], vec![2], vec![0, 0], 0),
//             // [10] 1 reports 100.
//             (vec![1, 2], vec![2], vec![100, 0], 0),
//             // [11] 2 reports 100.
//             (vec![1, 2], vec![2], vec![0, 100], 0),
//             (vec![1, 2], vec![2], vec![50, 100], 50),
//             (vec![1, 2], vec![2], vec![100, 50], 50),
//             // [12] Joint non-overlapping two-node configs.
//             (vec![1, 2], vec![3, 4], vec![50, 0, 0, 0], 0),
//             (vec![1, 2], vec![3, 4], vec![50, 0, 49, 0], 0),
//             (vec![1, 2], vec![3, 4], vec![50, 48, 49, 0], 0),
//             (vec![1, 2], vec![3, 4], vec![50, 48, 49, 47], 47),
//             // [13] Joint overlapping two-node configs.
//             (vec![1, 2], vec![2, 3], vec![0, 0, 0], 0),
//             //
//             // test_cases: 21
//             (vec![1, 2], vec![2, 3], vec![100, 0, 0], 0),
//             (vec![1, 2], vec![2, 3], vec![0, 100, 0], 0),
//             (vec![1, 2], vec![2, 3], vec![0, 100, 99], 0),
//             (vec![1, 2], vec![2, 3], vec![101, 100, 99], 99),
//             // [14] Joint identical two-node configs.
//             (vec![1, 2], vec![1, 2], vec![0, 0], 0),
//             (vec![1, 2], vec![1, 2], vec![0, 40], 0),
//             (vec![1, 2], vec![1, 2], vec![41, 40], 40),
//             // [15] Joint disjoint three-node configs.
//             (vec![1, 2, 3], vec![4, 5, 6], vec![0; 6], 0),
//             (vec![1, 2, 3], vec![4, 5, 6], vec![100, 0, 0, 0, 0, 0], 0),
//             (vec![1, 2, 3], vec![4, 5, 6], vec![100, 0, 0, 900, 0, 0], 0),
//             //
//             // test_cases: 31
//             (vec![1, 2, 3], vec![4, 5, 6], vec![100, 99, 0, 0, 0, 0], 0),
//             (vec![1, 2, 3], vec![4, 5, 6], vec![100, 99, 98, 0, 0, 0], 0),
//             // [16] First quorum <= 99, second one <= 97. Both quorums guarantee that 90 is committed
//             (vec![1, 2, 3], vec![4, 5, 6], vec![0, 99, 90, 97, 95, 0], 90),
//             // [17] First quorum collapsed to 92. Second one already had at least 95 committed,
//             // so the result also collapses.
//             (
//                 vec![1, 2, 3],
//                 vec![4, 5, 6],
//                 vec![92, 99, 90, 97, 95, 0],
//                 92,
//             ),
//             // [18] Second quorum collapses, but nothing changes in the output.
//             (
//                 vec![1, 2, 3],
//                 vec![4, 5, 6],
//                 vec![92, 99, 90, 97, 95, 77],
//                 92,
//             ),
//             // [19] Joint overlapping three-node configs.
//             (vec![1, 2, 3], vec![1, 4, 5], vec![0; 5], 0),
//             (vec![1, 2, 3], vec![1, 4, 5], vec![100, 0, 0, 0, 0], 0),
//             (vec![1, 2, 3], vec![1, 4, 5], vec![100, 100, 0, 0, 0], 0),
//             (vec![1, 2, 3], vec![1, 4, 5], vec![100, 101, 100, 0, 0], 0),
//             // [20] Second quorum could commit either 98 or 99, but first quorum is open.
//             (vec![1, 2, 3], vec![1, 4, 5], vec![0, 100, 0, 99, 98], 0),
//             //
//             // test_cases: 41
//             // [21] Additionally, first quorum can commit either 100 or 99
//             (vec![1, 2, 3], vec![1, 4, 5], vec![0, 100, 99, 99, 98], 98),
//             (vec![1, 2, 3], vec![1, 4, 5], vec![1, 100, 99, 99, 98], 98),
//             (vec![1, 2, 3], vec![1, 4, 5], vec![100, 100, 99, 99, 98], 99),
//             // [22] More overlap.
//             (vec![1, 2, 3], vec![2, 3, 4], vec![0; 4], 0),
//             (vec![1, 2, 3], vec![2, 3, 4], vec![0, 100, 99, 0], 99),
//             (vec![1, 2, 3], vec![2, 3, 4], vec![100, 100, 99, 0], 99),
//             (vec![1, 2, 3], vec![2, 3, 4], vec![100, 0, 0, 101], 0),
//             (vec![1, 2, 3], vec![2, 3, 4], vec![100, 99, 0, 101], 99),
//             // [23] Identical
//             (vec![1, 2, 3], vec![1, 2, 3], vec![50, 45, 0], 45),
//         ];
//
//         for (test_case, (cfg, cfgj, idx, expected_index)) in test_cases.drain(..).enumerate() {
//             let cfg_set: HashSet<u64> = cfg.into_iter().collect();
//             let cfgj_set: HashSet<u64> = cfgj.into_iter().collect();
//             let mut voters: Vec<u64> = cfg_set.union(&cfgj_set).into_iter().cloned().collect();
//             voters.sort();
//
//             assert_eq!(
//                 voters.len(),
//                 idx.len(),
//                 "[test_cases #{}] error: mismatched input length for voters, expected '{}', found '{}'",
//                 test_case + 1,
//                 voters.len(),
//                 idx.len(),
//             );
//
//             let mut l: AckIndexer = AckIndexer::default();
//
//             for (i, id) in voters.drain(..).enumerate() {
//                 l.insert(
//                     id,
//                     Index {
//                         index: idx[i],
//                         group_id: 0,
//                     },
//                 );
//             }
//
//             let index1 = JointConfig::new_joint(cfg_set.clone(), cfgj_set.clone())
//                 .committed_index(false, &l)
//                 .0;
//             let index2 = JointConfig::new_joint(cfgj_set, cfg_set)
//                 .committed_index(false, &l)
//                 .0;
//
//             assert_eq!(index1, index2, "[test_cases #{}] Interchanging the majorities shouldn't make a difference. expected '{}', found '{}'", test_case+1, index1, index2);
//
//             assert_eq!(
//                 expected_index,
//                 index1,
//                 "[test_cases #{}] mismatched index, expected '{}', found '{}'",
//                 test_case + 1,
//                 expected_index,
//                 index1,
//             )
//         }
//     }
//
//     #[test]
//     fn test_joint_vote() {
//         let mut test_cases = vec![
//             // votes 0 => vote missing, 1 => vote no, 2 => vote yes
//
//             // [1] Empty joint config wins all votes. This isn't used in production.
//             (vec![], vec![], vec![], VoteResult::Won),
//             // [2] trivial configs
//             (vec![1], vec![], vec![0], VoteResult::Pending),
//             (vec![1], vec![], vec![2], VoteResult::Won),
//             (vec![1], vec![], vec![1], VoteResult::Lost),
//             (vec![1], vec![1], vec![0], VoteResult::Pending),
//             (vec![1], vec![1], vec![1], VoteResult::Lost),
//             (vec![1], vec![1], vec![2], VoteResult::Won),
//             (vec![1], vec![2], vec![0, 0], VoteResult::Pending),
//             (vec![1], vec![2], vec![1, 0], VoteResult::Lost),
//             (vec![1], vec![2], vec![2, 0], VoteResult::Pending),
//             //
//             // test_cases: 11
//             (vec![1], vec![2], vec![1, 1], VoteResult::Lost),
//             (vec![1], vec![2], vec![2, 2], VoteResult::Won),
//             (vec![1], vec![2], vec![1, 2], VoteResult::Lost),
//             (vec![1], vec![2], vec![2, 1], VoteResult::Lost),
//             // [3] two nodes configs
//             (vec![1, 2], vec![3, 4], vec![0; 4], VoteResult::Pending),
//             (
//                 vec![1, 2],
//                 vec![3, 4],
//                 vec![2, 0, 0, 0],
//                 VoteResult::Pending,
//             ),
//             (
//                 vec![1, 2],
//                 vec![3, 4],
//                 vec![2, 2, 0, 0],
//                 VoteResult::Pending,
//             ),
//             (vec![1, 2], vec![3, 4], vec![2, 2, 1, 0], VoteResult::Lost),
//             (vec![1, 2], vec![3, 4], vec![2, 2, 1, 1], VoteResult::Lost),
//             (vec![1, 2], vec![3, 4], vec![2, 2, 2, 1], VoteResult::Lost),
//             //
//             // test_cases: 21
//             (vec![1, 2], vec![3, 4], vec![2, 2, 2, 2], VoteResult::Won),
//             (vec![1, 2], vec![2, 3], vec![0, 0, 0], VoteResult::Pending),
//             (vec![1, 2], vec![2, 3], vec![0, 1, 0], VoteResult::Lost),
//             (vec![1, 2], vec![2, 3], vec![2, 2, 0], VoteResult::Pending),
//             (vec![1, 2], vec![2, 3], vec![2, 2, 1], VoteResult::Lost),
//             (vec![1, 2], vec![2, 3], vec![2, 2, 2], VoteResult::Won),
//             (vec![1, 2], vec![1, 2], vec![0, 0], VoteResult::Pending),
//             (vec![1, 2], vec![1, 2], vec![2, 0], VoteResult::Pending),
//             (vec![1, 2], vec![1, 2], vec![2, 1], VoteResult::Lost),
//             (vec![1, 2], vec![1, 2], vec![1, 0], VoteResult::Lost),
//             //
//             //  test_cases: 31
//             (vec![1, 2], vec![1, 2], vec![1, 1], VoteResult::Lost),
//             // [4] Simple example for overlapping three node configs.
//             (
//                 vec![1, 2, 3],
//                 vec![2, 3, 4],
//                 vec![0; 4],
//                 VoteResult::Pending,
//             ),
//             (
//                 vec![1, 2, 3],
//                 vec![2, 3, 4],
//                 vec![0, 1, 0, 0],
//                 VoteResult::Pending,
//             ),
//             (
//                 vec![1, 2, 3],
//                 vec![2, 3, 4],
//                 vec![0, 1, 1, 0],
//                 VoteResult::Lost,
//             ),
//             (
//                 vec![1, 2, 3],
//                 vec![2, 3, 4],
//                 vec![0, 2, 2, 0],
//                 VoteResult::Won,
//             ),
//             (
//                 vec![1, 2, 3],
//                 vec![2, 3, 4],
//                 vec![2, 2, 0, 0],
//                 VoteResult::Pending,
//             ),
//             (
//                 vec![1, 2, 3],
//                 vec![2, 3, 4],
//                 vec![2, 2, 1, 0],
//                 VoteResult::Pending,
//             ),
//             (
//                 vec![1, 2, 3],
//                 vec![2, 3, 4],
//                 vec![2, 2, 1, 1],
//                 VoteResult::Lost,
//             ),
//             (
//                 vec![1, 2, 3],
//                 vec![2, 3, 4],
//                 vec![2, 2, 1, 2],
//                 VoteResult::Won,
//             ),
//         ];
//         for (test_case, (cfg, cfgj, votes, expected_vote_result)) in
//             test_cases.drain(..).enumerate()
//         {
//             let cfg_set: HashSet<u64> = cfg.into_iter().collect();
//             let cfgj_set: HashSet<u64> = cfgj.into_iter().collect();
//             let mut voters: Vec<u64> = cfg_set.union(&cfgj_set).into_iter().cloned().collect();
//             voters.sort();
//
//             assert_eq!(
//                 voters.len(),
//                 votes.len(),
//                 "[test_cases #{}] error: mismatched input length for voters, expected '{:?}', found '{:?}'",
//                 test_case + 1,
//                 voters.len(),
//                 votes.len(),
//             );
//
//             let mut l: HashMap<u64, bool> = HashMap::default();
//
//             for (i, id) in voters.drain(..).enumerate() {
//                 match votes[i] {
//                     2 => l.insert(id, true),
//                     1 => l.insert(id, false),
//                     _ => None,
//                 };
//             }
//
//             let vote_result1 = JointConfig::new_joint(cfg_set.clone(), cfgj_set.clone())
//                 .vote_result(|id| l.get(&id).cloned());
//             let vote_result2 =
//                 JointConfig::new_joint(cfgj_set, cfg_set).vote_result(|id| l.get(&id).cloned());
//
//             assert_eq!(
//                 vote_result1,
//                 vote_result2,
//                 "[test_cases #{}] Interchanging the majorities shouldn't make a difference. expected '{:?}', found '{:?}'",
//                 test_case+1,
//                 vote_result1,
//                 vote_result2
//             );
//
//             assert_eq!(
//                 expected_vote_result,
//                 vote_result1,
//                 "[test_cases #{}] mismatched VoteResult, expected '{:?}', found '{:?}'",
//                 test_case + 1,
//                 expected_vote_result,
//                 vote_result1,
//             );
//         }
//     }
//     #[test]
//     fn test_joint_commit_multi_group() {
//         let mut test_cases = vec![
//             (vec![], vec![], vec![], vec![], u64::MAX, true),
//             (
//                 vec![1, 2, 3],
//                 vec![],
//                 vec![77, 88, 99],
//                 vec![1, 1, 1],
//                 88,
//                 false,
//             ),
//             (
//                 vec![1, 2, 3],
//                 vec![],
//                 vec![77, 88, 99],
//                 vec![1, 1, 2],
//                 88,
//                 true,
//             ),
//             (
//                 vec![1, 2, 3],
//                 vec![],
//                 vec![77, 88, 99],
//                 vec![2, 1, 1],
//                 77,
//                 true,
//             ),
//             (
//                 vec![1, 2, 3],
//                 vec![],
//                 vec![77, 88, 99],
//                 vec![0, 1, 1],
//                 77,
//                 false,
//             ),
//             (
//                 vec![1, 2, 3],
//                 vec![],
//                 vec![77, 88, 99],
//                 vec![0, 1, 2],
//                 88,
//                 true,
//             ),
//             (
//                 vec![1, 2, 3],
//                 vec![],
//                 vec![77, 88, 99],
//                 vec![1, 2, 0],
//                 77,
//                 true,
//             ),
//             (vec![1], vec![2], vec![7, 8], vec![1, 2], 7, false),
//             (vec![1], vec![2], vec![7, 8], vec![1, 1], 7, false),
//             (vec![1], vec![2], vec![7, 8], vec![1, 0], 7, false),
//             //
//             // test_cases: 11
//             (vec![1, 2], vec![3], vec![7, 8, 9], vec![1, 1, 1], 7, false),
//             (vec![1, 2], vec![3], vec![7, 8, 9], vec![1, 1, 2], 7, false),
//             (vec![1, 2], vec![3], vec![7, 8, 9], vec![1, 2, 1], 7, false),
//             (
//                 vec![1, 2],
//                 vec![3],
//                 vec![77, 88, 9],
//                 vec![1, 1, 1],
//                 9,
//                 false,
//             ),
//             (
//                 vec![1, 2],
//                 vec![3],
//                 vec![77, 88, 9],
//                 vec![0, 1, 1],
//                 9,
//                 false,
//             ),
//             (
//                 vec![1, 2],
//                 vec![3],
//                 vec![77, 88, 79],
//                 vec![0, 1, 1],
//                 77,
//                 false,
//             ),
//             (vec![1, 2], vec![2], vec![4, 5], vec![1, 1], 4, false),
//             (vec![1, 2], vec![2], vec![4, 5], vec![1, 2], 4, false),
//             (vec![1, 2], vec![2], vec![44, 5], vec![1, 2], 5, false),
//             (vec![1, 2], vec![2], vec![4, 5], vec![1, 0], 4, false),
//             //
//             // test_cases: 21
//             (
//                 vec![1, 2],
//                 vec![3, 4],
//                 vec![1, 2, 3, 4],
//                 vec![1, 1, 1, 1],
//                 1,
//                 false,
//             ),
//             (
//                 vec![1, 2],
//                 vec![3, 4],
//                 vec![1, 2, 3, 4],
//                 vec![1, 1, 1, 2],
//                 1,
//                 false,
//             ),
//             (
//                 vec![1, 2],
//                 vec![3, 4],
//                 vec![3, 4, 3, 4],
//                 vec![1, 1, 1, 2],
//                 3,
//                 false,
//             ),
//             (
//                 vec![1, 2],
//                 vec![3, 4],
//                 vec![3, 4, 3, 4],
//                 vec![2, 1, 1, 2],
//                 3,
//                 true,
//             ),
//             (
//                 vec![1, 2],
//                 vec![3, 4],
//                 vec![3, 4, 33, 44],
//                 vec![2, 1, 1, 2],
//                 3,
//                 true,
//             ),
//             (
//                 vec![1, 2],
//                 vec![3, 4],
//                 vec![3, 4, 33, 44],
//                 vec![0, 1, 1, 2],
//                 3,
//                 false,
//             ),
//             (
//                 vec![1, 2],
//                 vec![3, 4],
//                 vec![3, 4, 33, 44],
//                 vec![1, 1, 2, 2],
//                 3,
//                 false,
//             ),
//             (
//                 vec![1, 2, 3],
//                 vec![3, 4, 5],
//                 vec![3, 4, 5, 6, 7],
//                 vec![1, 1, 1, 1, 1],
//                 4,
//                 false,
//             ),
//             (
//                 vec![1, 2, 3],
//                 vec![3, 4, 5],
//                 vec![3, 44, 55, 6, 7],
//                 vec![1, 1, 1, 1, 1],
//                 7,
//                 false,
//             ),
//             (
//                 vec![1, 2, 3],
//                 vec![3, 4, 5],
//                 vec![3, 44, 55, 6, 7],
//                 vec![1, 1, 1, 1, 2],
//                 7,
//                 false,
//             ),
//             //
//             // test_cases: 31
//             (
//                 vec![1, 2, 3],
//                 vec![3, 4, 5],
//                 vec![3, 44, 55, 6, 7],
//                 vec![1, 1, 2, 1, 2],
//                 6,
//                 true,
//             ),
//             (
//                 vec![1, 2, 3],
//                 vec![3, 4, 5],
//                 vec![3, 44, 55, 6, 7],
//                 vec![0, 1, 2, 1, 2],
//                 6,
//                 true,
//             ),
//             (
//                 vec![1, 2, 3],
//                 vec![3, 4, 5],
//                 vec![3, 44, 55, 6, 7],
//                 vec![1, 0, 2, 1, 2],
//                 3,
//                 true,
//             ),
//             (
//                 vec![1, 2, 3],
//                 vec![3, 4, 5],
//                 vec![3, 44, 55, 6, 7],
//                 vec![1, 2, 0, 1, 2],
//                 3,
//                 true,
//             ),
//             (
//                 vec![1, 2, 3],
//                 vec![3, 4, 5],
//                 vec![3, 44, 5, 66, 7],
//                 vec![1, 2, 0, 1, 2],
//                 3,
//                 true,
//             ),
//             (
//                 vec![1, 2, 3],
//                 vec![3, 4, 5],
//                 vec![3, 44, 5, 66, 7],
//                 vec![1, 1, 0, 2, 2],
//                 3,
//                 false,
//             ),
//             (
//                 vec![1, 2, 3],
//                 vec![2, 3, 4],
//                 vec![3, 4, 5, 6],
//                 vec![1, 1, 1, 1],
//                 4,
//                 false,
//             ),
//             (
//                 vec![1, 2, 3],
//                 vec![2, 3, 4],
//                 vec![3, 4, 5, 6],
//                 vec![1, 2, 1, 1],
//                 4,
//                 true,
//             ),
//             (
//                 vec![1, 2, 3],
//                 vec![2, 3, 4],
//                 vec![3, 4, 5, 6],
//                 vec![1, 2, 2, 1],
//                 3,
//                 true,
//             ),
//             (
//                 vec![1, 2, 3],
//                 vec![2, 3, 4],
//                 vec![3, 4, 5, 6],
//                 vec![1, 2, 2, 2],
//                 3,
//                 false,
//             ),
//             //
//             // test_cases: 41
//             (
//                 vec![1, 2, 3],
//                 vec![2, 3, 4],
//                 vec![3, 4, 5, 6],
//                 vec![2, 1, 0, 2],
//                 3,
//                 true,
//             ),
//             (
//                 vec![1, 2, 3],
//                 vec![2, 3, 4],
//                 vec![3, 4, 5, 6],
//                 vec![1, 1, 0, 2],
//                 3,
//                 false,
//             ),
//             (
//                 vec![1, 2, 3],
//                 vec![4, 5, 6],
//                 vec![1, 2, 3, 4, 5, 6],
//                 vec![1, 1, 1, 1, 1, 1],
//                 2,
//                 false,
//             ),
//             (
//                 vec![1, 2, 3],
//                 vec![4, 5, 6],
//                 vec![11, 22, 33, 4, 5, 6],
//                 vec![1, 1, 1, 1, 1, 1],
//                 5,
//                 false,
//             ),
//             (
//                 vec![1, 2, 3],
//                 vec![4, 5, 6],
//                 vec![11, 2, 33, 4, 55, 6],
//                 vec![1, 1, 1, 1, 1, 1],
//                 6,
//                 false,
//             ),
//             (
//                 vec![1, 2, 3],
//                 vec![4, 5, 6],
//                 vec![1, 2, 3, 4, 5, 6],
//                 vec![1, 1, 1, 2, 2, 2],
//                 2,
//                 false,
//             ),
//             (
//                 vec![1, 2, 3],
//                 vec![4, 5, 6],
//                 vec![1, 2, 3, 4, 5, 6],
//                 vec![1, 1, 1, 1, 1, 2],
//                 2,
//                 false,
//             ),
//             (
//                 vec![1, 2, 3],
//                 vec![4, 5, 6],
//                 vec![1, 2, 3, 4, 5, 6],
//                 vec![1, 1, 2, 1, 1, 2],
//                 2,
//                 true,
//             ),
//             (
//                 vec![1, 2, 3],
//                 vec![4, 5, 6],
//                 vec![1, 2, 3, 4, 5, 6],
//                 vec![1, 1, 2, 1, 1, 0],
//                 2,
//                 false,
//             ),
//             (
//                 vec![1, 2, 3],
//                 vec![4, 5, 6],
//                 vec![1, 2, 3, 4, 5, 6],
//                 vec![1, 1, 0, 1, 1, 2],
//                 1,
//                 false,
//             ),
//             //
//             // test_cases: 51
//             (
//                 vec![1, 2, 3],
//                 vec![4, 5, 6],
//                 vec![1, 2, 3, 4, 5, 6],
//                 vec![0, 1, 2, 0, 1, 2],
//                 2,
//                 true,
//             ),
//             (
//                 vec![1, 2, 3],
//                 vec![4, 5, 6],
//                 vec![1, 2, 3, 4, 5, 6],
//                 vec![0, 1, 2, 0, 1, 1],
//                 2,
//                 false,
//             ),
//             (
//                 vec![1, 2, 3],
//                 vec![4, 5, 6],
//                 vec![1, 2, 3, 4, 5, 6],
//                 vec![0, 1, 1, 0, 1, 2],
//                 1,
//                 false,
//             ),
//         ];
//
//         for (i, (cfg, cfgj, idx, group_ids, expected_index, expected_use_group_commit)) in
//             test_cases.drain(..).enumerate()
//         {
//             let cfg_set: HashSet<u64> = cfg.into_iter().collect();
//             let cfgj_set: HashSet<u64> = cfgj.into_iter().collect();
//             let mut voters: Vec<u64> = cfg_set.union(&cfgj_set).into_iter().cloned().collect();
//             voters.sort();
//
//             assert_eq!(
//                 voters.len(),
//                 idx.len(),
//                 "[test_cases #{}] error: mismatched input length for voters, expected '{:?}', found '{:?}'",
//                 i + 1,
//                 voters.len(),
//                 idx.len(),
//             );
//
//             let mut l: AckIndexer = AckIndexer::default();
//
//             for (i, id) in voters.drain(..).enumerate() {
//                 l.insert(
//                     id,
//                     Index {
//                         index: idx[i],
//                         group_id: group_ids[i],
//                     },
//                 );
//             }
//
//             let (index1, use_group_commit1) =
//                 JointConfig::new_joint(cfg_set.clone(), cfgj_set.clone()).committed_index(true, &l);
//             let (index2, use_group_commit2) =
//                 JointConfig::new_joint(cfgj_set, cfg_set).committed_index(true, &l);
//
//             assert_eq!(index1, index2, "[test_cases #{}] mismatched index, interchanging the majorities shouldn't make a difference. expected '{}', found '{}'", i+1, index1, index2);
//             assert_eq!(use_group_commit1, use_group_commit2, "[test_cases #{}] mismatched use_group_commit, interchanging the majorities shouldn't make a difference. expected '{}', found '{}'", i+1, use_group_commit1, use_group_commit2);
//
//             assert_eq!(
//                 expected_index,
//                 index1,
//                 "[test_cases #{}] mismatched index, expected '{}', found '{}'",
//                 i + 1,
//                 expected_index,
//                 index1,
//             );
//
//             assert_eq!(
//                 expected_use_group_commit,
//                 use_group_commit1,
//                 "[test_cases #{}] mismatched use_group_commit, expected '{}', found '{}'",
//                 i + 1,
//                 expected_use_group_commit,
//                 use_group_commit1,
//             );
//         }
//     }
// }
