use criterion::{Bencher, Criterion};
use raft::{storage::MemStorage, Config, Raft};
use DEFAULT_RAFT_SETS;

pub fn bench_raft(c: &mut Criterion) {
    bench_raft_new(c);
    bench_raft_campaign(c);
}

fn quick_raft(voters: usize, learners: usize) -> Raft<MemStorage> {
    let id = 1;
    let storage = MemStorage::default();
    let config = Config::new(id);
    let mut raft = Raft::new(&config, storage).unwrap();
    (0..voters).for_each(|id| {
        raft.add_node(id as u64).unwrap();
    });
    (voters..learners).for_each(|id| {
        raft.add_learner(id as u64).unwrap();
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

    DEFAULT_RAFT_SETS.iter().for_each(|(voters, learners)| {
        c.bench_function(
            &format!("Raft::new ({}, {})", voters, learners),
            bench(*voters, *learners),
        );
    });
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

    DEFAULT_RAFT_SETS
        .iter()
        .skip(1)
        .for_each(|(voters, learners)| {
            // We don't want to make `raft::raft` public at this point.
            let msgs = [
                "CampaignPreElection",
                "CampaignElection",
                "CampaignTransfer",
            ];
            // Skip the first since it's 0,0
            for msg in &msgs {
                c.bench_function(
                    &format!("Raft::campaign ({}, {}, {})", voters, learners, msg),
                    bench(*voters, *learners, msg.as_bytes()),
                );
            }
        });
}
