#![allow(dead_code)] // Due to criterion we need this to avoid warnings.

#[macro_use]
extern crate criterion;
extern crate raft;

mod suites;
use suites::*;

criterion_main!(
    bench_raft,
    bench_raw_node,
    bench_progress,
    bench_progress_set,
);
