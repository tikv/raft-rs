use criterion::{Bencher, Criterion};
use raft::{storage::MemStorage, Config, Raft};

criterion_group!(bench_raft, bench_raft_new, bench_raft_campaign,);

fn quick_raft(voters: usize, learners: usize) -> Raft<MemStorage> {
    let id = 1;
    let storage = MemStorage::default();
    let config = Config::new(id);
    let mut raft = Raft::new(&config, storage);
    (0..voters).for_each(|id| {
        raft.add_node(id as u64);
    });
    (voters..learners).for_each(|id| {
        raft.add_learner(id as u64);
    });
    raft
}

pub fn bench_raft_new(c: &mut Criterion) {
    let bench = |voters, learners| {
        move |b: &mut Bencher| {
            // No setup.
            b.iter(|| quick_raft(voters, learners));
        }
    };

    c.bench_function("Raft::new (0, 0)", bench(0, 0));
    c.bench_function("Raft::new (3, 1)", bench(3, 1));
    c.bench_function("Raft::new (5, 2)", bench(5, 2));
    c.bench_function("Raft::new (7, 3)", bench(7, 3));
}

pub fn bench_raft_campaign(c: &mut Criterion) {
    let bench = |voters, learners, variant| {
        move |b: &mut Bencher| {
            b.iter(|| {
                // TODO: Make raft clone somehow.
                let mut raft = quick_raft(voters, learners);
                raft.campaign(variant)
            })
        }
    };

    // We don't want to make `raft::raft` public at this point.
    let pre_election = b"CampaignPreElection";
    let election = b"CampaignElection";
    let transfer = b"CampaignTransfer";

    c.bench_function(
        "Raft::campaign (3, 1, pre_election)",
        bench(3, 1, &pre_election[..]),
    );
    c.bench_function(
        "Raft::campaign (3, 1, election)",
        bench(3, 1, &election[..]),
    );
    c.bench_function(
        "Raft::campaign (3, 1, transfer)",
        bench(3, 1, &transfer[..]),
    );
    c.bench_function(
        "Raft::campaign (5, 2, pre_election)",
        bench(5, 2, &pre_election[..]),
    );
    c.bench_function(
        "Raft::campaign (5, 2, election)",
        bench(5, 2, &election[..]),
    );
    c.bench_function(
        "Raft::campaign (5, 2, transfer)",
        bench(5, 2, &transfer[..]),
    );
    c.bench_function(
        "Raft::campaign (7, 3, pre_election)",
        bench(7, 3, &pre_election[..]),
    );
    c.bench_function(
        "Raft::campaign (7, 3, election)",
        bench(7, 3, &election[..]),
    );
    c.bench_function(
        "Raft::campaign (7, 3, transfer)",
        bench(7, 3, &transfer[..]),
    );
}
