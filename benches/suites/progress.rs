use criterion::{Bencher, Criterion};
use raft::Progress;

pub fn bench_progress(c: &mut Criterion) {
    bench_progress_default(c);
}

pub fn bench_progress_default(c: &mut Criterion) {
    let bench = |b: &mut Bencher| {
        // No setup.
        b.iter(Progress::default);
    };

    c.bench_function("Progress::default", bench);
}
