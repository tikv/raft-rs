use criterion::{BatchSize, Bencher, BenchmarkId, Criterion};
use raft::Inflights;

pub fn bench_inflights(c: &mut Criterion) {
    bench_inflights_add(c);
    bench_inflights_free_to(c);
}

pub fn bench_inflights_add(c: &mut Criterion) {
    c.bench_function("Inflights::add", |b: &mut Bencher| {
        b.iter_batched_ref(
            || Inflights::new(256),
            |i| i.add(1),
            BatchSize::PerIteration,
        );
    });
}

pub fn bench_inflights_free_to(c: &mut Criterion) {
    let sizes: Vec<u64> = vec![64, 256, 1024, 4096, 16384, 64 * 1024];
    let mut group = c.benchmark_group("Inflights::free_to");
    for s in sizes {
        let test_idx = vec![
            1,
            (s as f64).log(2f64) as u64,
            s / 4,
            s / 2,
            s / 2 + s / 4,
            s - 1,
        ];
        for i in test_idx {
            group.bench_with_input(
                BenchmarkId::from_parameter(format!("{}@{}", s, i)),
                &(s, i),
                |b: &mut Bencher, (size, free_to)| {
                    let mut inflights = Inflights::new(*size as usize);
                    for i in 0..*size {
                        inflights.add(i);
                    }
                    b.iter_batched_ref(
                        || inflights.clone(),
                        |i| i.free_to(*free_to),
                        BatchSize::PerIteration,
                    );
                },
            );
        }
    }
}
