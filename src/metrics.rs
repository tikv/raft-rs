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
use std::time::Duration;

use prometheus::*;

lazy_static! {
    pub(crate) static ref ELECTION_TIME_HISTOGRAM: Histogram = register_histogram!(
        "raft_election_time_seconds",
        "Bucketed histogram of raft election time",
        exponential_buckets(0.005, 2f64, 10).unwrap()
    ).unwrap();
}

#[inline]
pub(crate) fn duration_to_seconds(d: Duration) -> f64 {
    let nanos = f64::from(d.subsec_nanos()) / 1e9;
    d.as_secs() as f64 + nanos
}
