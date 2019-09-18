#![allow(dead_code)] // Due to criterion we need this to avoid warnings.
#![cfg_attr(feature = "cargo-clippy", allow(clippy::let_and_return))] // Benches often artificially return values. Allow it.

#[macro_use]
extern crate slog;

use criterion::Criterion;
use std::time::Duration;

mod suites;

pub const DEFAULT_RAFT_SETS: [(usize, usize); 4] = [(0, 0), (3, 1), (5, 2), (7, 3)];

/// The default logger we fall back to when passed `None` in external facing constructors.
///
/// Currently, this is a `log` adaptor behind a `Once` to ensure there is no clobbering.
fn default_logger() -> slog::Logger {
    use slog::Drain;
    use std::sync::{Mutex, Once};

    static LOGGER_INITIALIZED: Once = Once::new();
    static mut LOGGER: Option<slog::Logger> = None;

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

fn main() {
    let mut c = Criterion::default()
        // Configure defaults before overriding with args.
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(1))
        .configure_from_args();

    suites::bench_raft(&mut c);
    suites::bench_raw_node(&mut c);
    suites::bench_progress(&mut c);
    suites::bench_progress_set(&mut c);

    c.final_summary();
}
