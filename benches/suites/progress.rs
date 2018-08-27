use criterion::{Bencher, Criterion};
use raft::Progress;

criterion_group!(bench_progress, bench_progress_default);

pub fn bench_progress_default(c: &mut Criterion) {
    let bench = |b: &mut Bencher| {
        // No setup.
        b.iter(|| Progress::default());
    };

    c.bench_function("Progress::default", bench);
}
