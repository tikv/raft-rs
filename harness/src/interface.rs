// Copyright 2018 PingCAP, Inc.
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

// Copyright 2015 CoreOS, Inc.
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

use raft::{eraftpb::Message, storage::MemStorage, Progress, ProgressSet, Raft, Result};
use std::ops::{Deref, DerefMut};

/// A simulated Raft façade for testing.
///
/// If the contained value is a `Some` operations happen. If they are a `None` operations are
/// a no-op.
///
// Compare to upstream, we use struct instead of trait here.
// Because to be able to cast Interface later, we have to make
// Raft derive Any, which will require a lot of dependencies to derive Any.
// That's not worthy for just testing purpose.
pub struct Interface {
    /// The raft peer.
    pub raft: Option<Raft<MemStorage>>,
}

impl Interface {
    /// Create a new interface to a new raft.
    pub fn new(r: Raft<MemStorage>) -> Interface {
        Interface { raft: Some(r) }
    }

    /// Step the raft, if it exists.
    pub fn step(&mut self, m: Message) -> Result<()> {
        match self.raft {
            Some(_) => Raft::step(self, m),
            None => Ok(()),
        }
    }

    /// Read messages out of the raft.
    pub fn read_messages(&mut self) -> Vec<Message> {
        match self.raft {
            Some(_) => self.msgs.drain(..).collect(),
            None => vec![],
        }
    }

    /// Initialize a raft with the given ID and peer set.
    pub fn initial(&mut self, id: u64, ids: &[u64]) {
        if self.raft.is_some() {
            self.id = id;
            let prs = self.take_prs();
            self.set_prs(ProgressSet::with_capacity(
                ids.len(),
                prs.learner_ids().len(),
            ));
            for id in ids {
                let progress = Progress::new(0, 256);
                if prs.learner_ids().contains(id) {
                    if let Err(e) = self.mut_prs().insert_learner(*id, progress) {
                        panic!("{}", e);
                    }
                } else if let Err(e) = self.mut_prs().insert_voter(*id, progress) {
                    panic!("{}", e);
                }
            }
            let term = self.term;
            self.reset(term);
        }
    }
}

impl From<Option<Raft<MemStorage>>> for Interface {
    fn from(raft: Option<Raft<MemStorage>>) -> Self {
        Self { raft }
    }
}

impl From<Raft<MemStorage>> for Interface {
    fn from(raft: Raft<MemStorage>) -> Self {
        Self { raft: Some(raft) }
    }
}

impl Deref for Interface {
    type Target = Raft<MemStorage>;
    fn deref(&self) -> &Raft<MemStorage> {
        self.raft.as_ref().unwrap()
    }
}

impl DerefMut for Interface {
    fn deref_mut(&mut self) -> &mut Raft<MemStorage> {
        self.raft.as_mut().unwrap()
    }
}
