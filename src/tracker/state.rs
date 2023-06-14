// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

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

use std::fmt;
use std::fmt::{Display, Formatter};

/// The state of the progress.
#[derive(Debug, PartialEq, Eq, Clone, Copy, Default)]
pub enum ProgressState {
    /// Whether it's probing.
    #[default]
    Probe,
    /// Whether it's replicating.
    Replicate,
    /// Whether it's a snapshot.
    Snapshot,
}

impl Display for ProgressState {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ProgressState::Probe => write!(f, "StateProbe"),
            ProgressState::Replicate => write!(f, "StateReplicate"),
            ProgressState::Snapshot => write!(f, "StateSnapshot"),
        }
    }
}
