// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use criterion::{Bencher, Criterion};
use raft::Progress;

pub fn bench_progress(c: &mut Criterion) {
    bench_progress_default(c);
}

pub fn bench_progress_default(c: &mut Criterion) {
    let bench = |b: &mut Bencher| {
        // No setup.
        b.iter(|| Progress::new(9, 10));
    };

    c.bench_function("Progress::default", bench);
}
