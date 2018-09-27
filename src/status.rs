// Copyright 2016 PingCAP, Inc.
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

// Copyright 2015 The etcd Authors
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

use eraftpb::HardState;
use fxhash::FxHashMap;

use progress::Progress;
use raft::{Raft, SoftState, StateRole};
use storage::Storage;

/// Represents the current status of the raft
#[derive(Default)]
pub struct Status {
    /// The ID of the current node.
    pub id: u64,
    /// The hardstate of the raft, representing voted state.
    pub hs: HardState,
    /// The softstate of the raft, representing proposed state.
    pub ss: SoftState,
    /// The index of the last entry to have been applied.
    pub applied: u64,
    /// The progress towards catching up and applying logs.
    pub progress: FxHashMap<u64, Progress>,
    /// The progress of learners in catching up and applying logs.
    pub learner_progress: FxHashMap<u64, Progress>,
}

impl Status {
    /// Gets a copy of the current raft status.
    pub fn new<T: Storage>(raft: &Raft<T>) -> Status {
        let mut s = Status {
            id: raft.id,
            ..Default::default()
        };
        s.hs = raft.hard_state();
        s.ss = raft.soft_state();
        s.applied = raft.raft_log.get_applied();
        if s.ss.raft_state == StateRole::Leader {
            s.progress = raft.prs().voters().map(|(&k, v)| (k, v.clone())).collect();
            s.learner_progress = raft
                .prs()
                .learners()
                .map(|(&k, v)| (k, v.clone()))
                .collect();
        }
        s
    }
}
