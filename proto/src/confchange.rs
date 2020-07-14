// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::eraftpb::{
    ConfChange, ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2,
};
use std::borrow::Cow;
use std::fmt::Write;

/// Creates a `ConfChangeSingle`.
pub fn new_conf_change_single(node_id: u64, ty: ConfChangeType) -> ConfChangeSingle {
    let mut single = ConfChangeSingle::default();
    single.node_id = node_id;
    single.set_change_type(ty);
    single
}

/// Parses a Space-delimited sequence of operations into a slice of ConfChangeSingle.
/// The supported operations are:
/// - vn: make n a voter,
/// - ln: make n a learner,
/// - rn: remove n
pub fn parse_conf_change(s: &str) -> Result<Vec<ConfChangeSingle>, String> {
    let s = s.trim();
    if s.is_empty() {
        return Ok(vec![]);
    }
    let mut ccs = vec![];
    let splits = s.split_ascii_whitespace();
    for tok in splits {
        if tok.len() < 2 {
            return Err(format!("unknown token {}", tok));
        }
        let mut cc = ConfChangeSingle::default();
        let mut chars = tok.chars();
        cc.set_change_type(match chars.next().unwrap() {
            'v' => ConfChangeType::AddNode,
            'l' => ConfChangeType::AddLearnerNode,
            'r' => ConfChangeType::RemoveNode,
            _ => return Err(format!("unknown token {}", tok)),
        });
        cc.node_id = match chars.as_str().parse() {
            Ok(id) => id,
            Err(e) => return Err(format!("parse token {} fail: {}", tok, e)),
        };
        ccs.push(cc);
    }
    Ok(ccs)
}

/// The inverse to `parse_conf_change`.
pub fn stringify_conf_change(ccs: &[ConfChangeSingle]) -> String {
    let mut s = String::new();
    for (i, cc) in ccs.iter().enumerate() {
        if i > 0 {
            s.push(' ');
        }
        match cc.get_change_type() {
            ConfChangeType::AddNode => s.push('v'),
            ConfChangeType::AddLearnerNode => s.push('l'),
            ConfChangeType::RemoveNode => s.push('r'),
        }
        write!(&mut s, "{}", cc.node_id).unwrap();
    }
    s
}

/// Abstracts over ConfChangeV2 and (legacy) ConfChange to allow
/// treating them in a unified manner.
pub trait ConfChangeI {
    /// Converts conf change to `ConfChangeV2`.
    fn into_v2(self) -> ConfChangeV2;

    /// Gets conf change as `ConfChangeV2`.
    fn as_v2(&self) -> Cow<ConfChangeV2>;

    /// Converts conf change to `ConfChange`.
    ///
    /// `ConfChangeV2` can't be changed back to `ConfChange`.
    fn as_v1(&self) -> Option<&ConfChange>;
}

impl ConfChangeI for ConfChange {
    #[inline]
    fn into_v2(mut self) -> ConfChangeV2 {
        let mut cc = ConfChangeV2::default();
        let single = new_conf_change_single(self.node_id, self.get_change_type());
        cc.mut_changes().push(single);
        cc.set_context(self.take_context());
        cc
    }

    #[inline]
    fn as_v2(&self) -> Cow<ConfChangeV2> {
        Cow::Owned(self.clone().into_v2())
    }

    #[inline]
    fn as_v1(&self) -> Option<&ConfChange> {
        Some(self)
    }
}

impl ConfChangeI for ConfChangeV2 {
    #[inline]
    fn into_v2(self) -> ConfChangeV2 {
        self
    }

    #[inline]
    fn as_v2(&self) -> Cow<ConfChangeV2> {
        Cow::Borrowed(self)
    }

    #[inline]
    fn as_v1(&self) -> Option<&ConfChange> {
        None
    }
}

impl ConfChangeV2 {
    /// Checks if uses Joint Consensus.
    ///
    /// It will return Some if and only if this config change will use Joint Consensus,
    /// which is the case if it contains more than one change or if the use of Joint
    /// Consensus was requested explicitly. The bool indicates whether the Joint State
    /// will be left automatically.
    pub fn enter_joint(&self) -> Option<bool> {
        // NB: in theory, more config changes could qualify for the "simple"
        // protocol but it depends on the config on top of which the changes apply.
        // For example, adding two learners is not OK if both nodes are part of the
        // base config (i.e. two voters are turned into learners in the process of
        // applying the conf change). In practice, these distinctions should not
        // matter, so we keep it simple and use Joint Consensus liberally.
        if self.get_transition() != ConfChangeTransition::Auto || self.changes.len() > 1 {
            match self.get_transition() {
                ConfChangeTransition::Auto | ConfChangeTransition::Implicit => Some(true),
                ConfChangeTransition::Explicit => Some(false),
            }
        } else {
            None
        }
    }

    /// Checks if the configuration change leaves a joint configuration.
    ///
    /// This is the case if the ConfChangeV2 is zero, with the possible exception of
    /// the Context field.
    pub fn leave_joint(&self) -> bool {
        self.get_transition() == ConfChangeTransition::Auto && self.changes.is_empty()
    }
}
