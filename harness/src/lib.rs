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

/*!

This module contains various testing harness utilities for Raft.

> If you want to build Raft without this, disable the `harness` feature.

*/

#[macro_use]
extern crate slog;

mod interface;
mod network;

pub use self::{interface::Interface, network::Network};
use slog::{Drain, Logger};

/// Build a logger for tests.
///
/// Currently, this is a terminal log. It ensures it is only initialized once to prevent clobbering.
// This is `pub` so that testing and benching functions can use it.
// `#[cfg(test)]` doesn't work for benching.
pub fn testing_logger() -> Logger {
    use std::sync::{Mutex, Once};
    static LOGGER_INITIALIZED: Once = Once::new();
    static mut LOGGER: Option<Logger> = None;

    let logger = unsafe {
        LOGGER_INITIALIZED.call_once(|| {
            let decorator = slog_term::TermDecorator::new().build();
            let drain = slog_term::CompactFormat::new(decorator).build();
            let drain = slog_envlogger::new(drain);
            LOGGER = Some(slog::Logger::root(Mutex::new(drain).fuse(), o!()));
        });
        LOGGER.as_ref().unwrap()
    };
    let case = std::thread::current()
        .name()
        .unwrap()
        .split(":")
        .last()
        .unwrap()
        .to_string();
    logger.new(o!("case" => case))
}
