// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use criterion::{Bencher, Criterion};
use raft::Inflights;
use raft::Progress;

pub fn bench_progress(c: &mut Criterion) {
    bench_progress_default(c);
}

pub fn bench_progress_default(c: &mut Criterion) {
    let bench = |b: &mut Bencher| {
        b.iter(|| Progress::new(9, 10));
    };

    c.bench_function("Progress::default", bench);
}

pub fn bench_inflights_256(c: &mut Criterion) {
    bench_inflights(c, 256);
}

pub fn bench_inflights_4096(c: &mut Criterion) {
    bench_inflights(c, 4096);
}

fn bench_inflights(c: &mut Criterion, capacity: usize) {
    let mut inflights = Inflights::new(capacity);
    let bench = |b: &mut Bencher| {
        b.iter(|| {
            (0..capacity).for_each(|i| inflights.add(i as u64 + 1));
            (0..capacity).for_each(|_| inflights.free_first_one());
        })
    };
    c.bench_function(&format!("Inflights({})", capacity), bench);
}
