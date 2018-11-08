use criterion::{Bencher, Criterion};
use raft::{Progress, ProgressSet};
use DEFAULT_RAFT_SETS;

pub fn bench_progress_set(c: &mut Criterion) {
    bench_progress_set_new(c);
    bench_progress_set_with_capacity(c);
    bench_progress_set_insert_voter(c);
    bench_progress_set_insert_learner(c);
    bench_progress_set_promote_learner(c);
    bench_progress_set_remove(c);
    bench_progress_set_iter(c);
    bench_progress_set_get(c);
    bench_progress_set_nodes(c);
}

fn quick_progress_set(voters: usize, learners: usize) -> ProgressSet {
    let mut set = ProgressSet::with_capacity(voters, learners);
    (0..voters).for_each(|id| {
        set.insert_voter(id as u64, Progress::new(0, 10)).ok();
    });
    (voters..(learners + voters)).for_each(|id| {
        set.insert_learner(id as u64, Progress::new(0, 10)).ok();
    });
    set
}

pub fn bench_progress_set_new(c: &mut Criterion) {
    let bench = |b: &mut Bencher| {
        // No setup.
        b.iter(|| ProgressSet::new());
    };

    c.bench_function("ProgressSet::new", bench);
}

pub fn bench_progress_set_with_capacity(c: &mut Criterion) {
    let bench = |voters, learners| {
        move |b: &mut Bencher| {
            // No setup.
            b.iter(|| ProgressSet::with_capacity(voters, learners));
        }
    };

    DEFAULT_RAFT_SETS.iter().for_each(|(voters, learners)| {
        c.bench_function(
            &format!("ProgressSet::with_capacity ({}, {})", voters, learners),
            bench(*voters, *learners),
        );
    });
}

pub fn bench_progress_set_insert_voter(c: &mut Criterion) {
    let bench = |voters, learners| {
        move |b: &mut Bencher| {
            let set = quick_progress_set(voters, learners);
            b.iter(|| {
                let mut set = set.clone();
                set.insert_voter(99, Progress::new(0, 10)).ok()
            });
        }
    };

    DEFAULT_RAFT_SETS.iter().for_each(|(voters, learners)| {
        c.bench_function(
            &format!("ProgressSet::insert_voter ({}, {})", voters, learners),
            bench(*voters, *learners),
        );
    });
}

pub fn bench_progress_set_insert_learner(c: &mut Criterion) {
    let bench = |voters, learners| {
        move |b: &mut Bencher| {
            let set = quick_progress_set(voters, learners);
            b.iter(|| {
                let mut set = set.clone();
                set.insert_learner(99, Progress::new(0, 10)).ok()
            });
        }
    };

    DEFAULT_RAFT_SETS.iter().for_each(|(voters, learners)| {
        c.bench_function(
            &format!("ProgressSet::insert_learner ({}, {})", voters, learners),
            bench(*voters, *learners),
        );
    });
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

    DEFAULT_RAFT_SETS.iter().for_each(|(voters, learners)| {
        c.bench_function(
            &format!("ProgressSet::remove ({}, {})", voters, learners),
            bench(*voters, *learners),
        );
    });
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

    DEFAULT_RAFT_SETS.iter().for_each(|(voters, learners)| {
        c.bench_function(
            &format!("ProgressSet::promote ({}, {})", voters, learners),
            bench(*voters, *learners),
        );
    });
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

    DEFAULT_RAFT_SETS.iter().for_each(|(voters, learners)| {
        c.bench_function(
            &format!("ProgressSet::iter ({}, {})", voters, learners),
            bench(*voters, *learners),
        );
    });
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

    DEFAULT_RAFT_SETS.iter().for_each(|(voters, learners)| {
        c.bench_function(
            &format!("ProgressSet::nodes ({}, {})", voters, learners),
            bench(*voters, *learners),
        );
    });
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

    DEFAULT_RAFT_SETS.iter().for_each(|(voters, learners)| {
        c.bench_function(
            &format!("ProgressSet::get ({}, {})", voters, learners),
            bench(*voters, *learners),
        );
    });
}
