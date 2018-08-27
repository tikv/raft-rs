use criterion::{Bencher, Criterion};
use raft::ProgressSet;

criterion_group!(
    bench_progress_set,
    bench_progress_set_new,
    bench_progress_set_insert_voter,
    bench_progress_set_insert_learner,
    bench_progress_set_promote_learner,
    bench_progress_set_remove,
    bench_progress_set_iter,
    bench_progress_set_get,
    bench_progress_set_nodes,
);

fn quick_progress_set(voters: usize, learners: usize) -> ProgressSet {
    let mut set = ProgressSet::new(voters, learners);
    (0..voters).for_each(|id| {
        set.insert_voter(id as u64, Default::default()).ok();
    });
    (voters..learners).for_each(|id| {
        set.insert_learner(id as u64, Default::default()).ok();
    });
    set
}

pub fn bench_progress_set_new(c: &mut Criterion) {
    let bench = |voters, learners| {
        move |b: &mut Bencher| {
            // No setup.
            b.iter(|| ProgressSet::new(voters, learners));
        }
    };

    c.bench_function("ProgressSet::new (0, 0)", bench(0, 0));
    c.bench_function("ProgressSet::new (3, 1)", bench(3, 1));
    c.bench_function("ProgressSet::new (5, 2)", bench(5, 2));
    c.bench_function("ProgressSet::new (7, 3)", bench(7, 3));
}

pub fn bench_progress_set_insert_voter(c: &mut Criterion) {
    let bench = |voters, learners| {
        move |b: &mut Bencher| {
            let set = quick_progress_set(voters, learners);
            b.iter(|| {
                let mut set = set.clone();
                set.insert_voter(99, Default::default()).ok()
            });
        }
    };
    c.bench_function("ProgressSet::insert_voter (0, 0)", bench(0, 0));
    c.bench_function("ProgressSet::insert_voter (3, 1)", bench(3, 1));
    c.bench_function("ProgressSet::insert_voter (5, 2)", bench(5, 2));
    c.bench_function("ProgressSet::insert_voter (7, 3)", bench(7, 3));
}

pub fn bench_progress_set_insert_learner(c: &mut Criterion) {
    let bench = |voters, learners| {
        move |b: &mut Bencher| {
            let set = quick_progress_set(voters, learners);
            b.iter(|| {
                let mut set = set.clone();
                set.insert_learner(99, Default::default()).ok()
            });
        }
    };
    c.bench_function("ProgressSet::insert_learner (0, 0)", bench(0, 0));
    c.bench_function("ProgressSet::insert_learner (3, 1)", bench(3, 1));
    c.bench_function("ProgressSet::insert_learner (5, 2)", bench(5, 2));
    c.bench_function("ProgressSet::insert_learner (7, 3)", bench(7, 3));
}

pub fn bench_progress_set_remove(c: &mut Criterion) {
    let bench = |voters, learners| {
        move |b: &mut Bencher| {
            let set = quick_progress_set(voters, learners);
            b.iter(|| {
                let mut set = set.clone();
                set.remove(3)
            });
        }
    };
    c.bench_function("ProgressSet::remove (0, 0)", bench(0, 0));
    c.bench_function("ProgressSet::remove (3, 1)", bench(3, 1));
    c.bench_function("ProgressSet::remove (5, 2)", bench(5, 2));
    c.bench_function("ProgressSet::remove (7, 3)", bench(7, 3));
}

pub fn bench_progress_set_promote_learner(c: &mut Criterion) {
    let bench = |voters, learners| {
        move |b: &mut Bencher| {
            let set = quick_progress_set(voters, learners);
            b.iter(|| {
                let mut set = set.clone();
                set.promote_learner(3)
            });
        }
    };
    c.bench_function("ProgressSet::promote (0, 0)", bench(0, 0));
    c.bench_function("ProgressSet::promote (3, 1)", bench(3, 1));
    c.bench_function("ProgressSet::promote (5, 2)", bench(5, 2));
    c.bench_function("ProgressSet::promote (7, 3)", bench(7, 3));
}

pub fn bench_progress_set_iter(c: &mut Criterion) {
    let bench = |voters, learners| {
        move |b: &mut Bencher| {
            let set = quick_progress_set(voters, learners);
            b.iter(|| {
                let set = set.clone();
                let agg = set.iter().all(|_| true);
                agg
            });
        }
    };
    c.bench_function("ProgressSet::iter (0, 0)", bench(0, 0));
    c.bench_function("ProgressSet::iter (3, 1)", bench(3, 1));
    c.bench_function("ProgressSet::iter (5, 2)", bench(5, 2));
    c.bench_function("ProgressSet::iter (7, 3)", bench(7, 3));
}

pub fn bench_progress_set_nodes(c: &mut Criterion) {
    let bench = |voters, learners| {
        move |b: &mut Bencher| {
            let set = quick_progress_set(voters, learners);
            b.iter(|| {
                let set = set.clone();
                let agg = set.iter().all(|_| true);
                agg
            });
        }
    };
    c.bench_function("ProgressSet::nodes (0, 0)", bench(0, 0));
    c.bench_function("ProgressSet::nodes (3, 1)", bench(3, 1));
    c.bench_function("ProgressSet::nodes (5, 2)", bench(5, 2));
    c.bench_function("ProgressSet::nodes (7, 3)", bench(7, 3));
}

pub fn bench_progress_set_get(c: &mut Criterion) {
    let bench = |voters, learners| {
        move |b: &mut Bencher| {
            let set = quick_progress_set(voters, learners);
            b.iter(|| {
                let set = set.clone();
                {
                    set.get(1);
                }
            });
        }
    };
    c.bench_function("ProgressSet::get (0, 0)", bench(0, 0));
    c.bench_function("ProgressSet::get (3, 1)", bench(3, 1));
    c.bench_function("ProgressSet::get (5, 2)", bench(5, 2));
    c.bench_function("ProgressSet::get (7, 3)", bench(7, 3));
}
