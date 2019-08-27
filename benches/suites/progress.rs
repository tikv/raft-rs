use criterion::{Bencher, Criterion, measurement::Measurement};
use raft::Progress;

pub fn bench_progress<M>(c: &mut Criterion<M>) where M: Measurement + 'static {
    bench_progress_default(c);
}

pub fn bench_progress_default<M>(c: &mut Criterion<M>) where M: Measurement + 'static {
    let bench = |b: &mut Bencher<M>| {
        // No setup.
        b.iter(|| Progress::new(9, 10));
    };

    c.bench_function("Progress::default", bench);
}
