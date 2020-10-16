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

pub use super::read_only::{ReadOnlyOption, ReadState};
use super::util::NO_LIMIT;
use super::{
    errors::{Error, Result},
    INVALID_ID,
};

/// Config contains the parameters to start a raft.
#[derive(Clone)]
pub struct Config {
    /// The identity of the local raft. It cannot be 0, and must be unique in the group.
    pub id: u64,

    /// The number of node.tick invocations that must pass between
    /// elections. That is, if a follower does not receive any message from the
    /// leader of current term before ElectionTick has elapsed, it will become
    /// candidate and start an election. election_tick must be greater than
    /// HeartbeatTick. We suggest election_tick = 10 * HeartbeatTick to avoid
    /// unnecessary leader switching
    pub election_tick: usize,

    /// HeartbeatTick is the number of node.tick invocations that must pass between
    /// heartbeats. That is, a leader sends heartbeat messages to maintain its
    /// leadership every heartbeat ticks.
    pub heartbeat_tick: usize,

    /// Applied is the last applied index. It should only be set when restarting
    /// raft. raft will not return entries to the application smaller or equal to Applied.
    /// If Applied is unset when restarting, raft might return previous applied entries.
    /// This is a very application dependent configuration.
    pub applied: u64,

    /// Limit the max size of each append message. Smaller value lowers
    /// the raft recovery cost(initial probing and message lost during normal operation).
    /// On the other side, it might affect the throughput during normal replication.
    /// Note: math.MaxUusize64 for unlimited, 0 for at most one entry per message.
    pub max_size_per_msg: u64,

    /// Limit the max number of in-flight append messages during optimistic
    /// replication phase. The application transportation layer usually has its own sending
    /// buffer over TCP/UDP. Set to avoid overflowing that sending buffer.
    /// TODO: feedback to application to limit the proposal rate?
    pub max_inflight_msgs: usize,

    /// Specify if the leader should check quorum activity. Leader steps down when
    /// quorum is not active for an electionTimeout.
    pub check_quorum: bool,

    /// Enables the Pre-Vote algorithm described in raft thesis section
    /// 9.6. This prevents disruption when a node that has been partitioned away
    /// rejoins the cluster.
    pub pre_vote: bool,

    /// The range of election timeout. In some cases, we hope some nodes has less possibility
    /// to become leader. This configuration ensures that the randomized election_timeout
    /// will always be suit in [min_election_tick, max_election_tick).
    /// If it is 0, then election_tick will be chosen.
    pub min_election_tick: usize,

    /// If it is 0, then 2 * election_tick will be chosen.
    pub max_election_tick: usize,

    /// Choose the linearizability mode or the lease mode to read data. If you don’t care about the read consistency and want a higher read performance, you can use the lease mode.
    ///
    /// Setting this to `LeaseBased` requires `check_quorum = true`.
    pub read_only_option: ReadOnlyOption,

    /// Don't broadcast an empty raft entry to notify follower to commit an entry.
    /// This may make follower wait a longer time to apply an entry. This configuration
    /// May affect proposal forwarding and follower read.
    pub skip_bcast_commit: bool,

    /// Batches every append msg if any append msg already exists
    pub batch_append: bool,

    /// The election priority of this node.
    pub priority: u64,

    /// Specify maximum of uncommited entry size.
    /// When this limit is reached, all proposals to append new log will be dropped
    pub max_uncommitted_size: u64,
}

impl Default for Config {
    fn default() -> Self {
        const HEARTBEAT_TICK: usize = 2;
        Self {
            id: 0,
            election_tick: HEARTBEAT_TICK * 10,
            heartbeat_tick: HEARTBEAT_TICK,
            applied: 0,
            max_size_per_msg: 0,
            max_inflight_msgs: 256,
            check_quorum: false,
            pre_vote: false,
            min_election_tick: 0,
            max_election_tick: 0,
            read_only_option: ReadOnlyOption::Safe,
            skip_bcast_commit: false,
            batch_append: false,
            priority: 0,
            max_uncommitted_size: NO_LIMIT,
        }
    }
}

impl Config {
    /// Creates a new config.
    pub fn new(id: u64) -> Self {
        Self {
            id,
            ..Self::default()
        }
    }

    /// The minimum number of ticks before an election.
    #[inline]
    pub fn min_election_tick(&self) -> usize {
        if self.min_election_tick == 0 {
            self.election_tick
        } else {
            self.min_election_tick
        }
    }

    /// The maximum number of ticks before an election.
    #[inline]
    pub fn max_election_tick(&self) -> usize {
        if self.max_election_tick == 0 {
            2 * self.election_tick
        } else {
            self.max_election_tick
        }
    }

    /// Runs validations against the config.
    pub fn validate(&self) -> Result<()> {
        if self.id == INVALID_ID {
            return Err(Error::ConfigInvalid("invalid node id".to_owned()));
        }

        if self.heartbeat_tick == 0 {
            return Err(Error::ConfigInvalid(
                "heartbeat tick must greater than 0".to_owned(),
            ));
        }

        if self.election_tick <= self.heartbeat_tick {
            return Err(Error::ConfigInvalid(
                "election tick must be greater than heartbeat tick".to_owned(),
            ));
        }

        let min_timeout = self.min_election_tick();
        let max_timeout = self.max_election_tick();
        if min_timeout < self.election_tick {
            return Err(Error::ConfigInvalid(format!(
                "min election tick {} must not be less than election_tick {}",
                min_timeout, self.election_tick
            )));
        }

        if min_timeout >= max_timeout {
            return Err(Error::ConfigInvalid(format!(
                "min election tick {} should be less than max election tick {}",
                min_timeout, max_timeout
            )));
        }

        if self.max_inflight_msgs == 0 {
            return Err(Error::ConfigInvalid(
                "max inflight messages must be greater than 0".to_owned(),
            ));
        }

        if self.read_only_option == ReadOnlyOption::LeaseBased && !self.check_quorum {
            return Err(Error::ConfigInvalid(
                "read_only_option == LeaseBased requires check_quorum == true".into(),
            ));
        }

        if self.max_uncommitted_size < self.max_size_per_msg {
            return Err(Error::ConfigInvalid(
                "max uncommitted size should greater than max_size_per_msg".to_owned(),
            ));
        }

        Ok(())
    }
}
