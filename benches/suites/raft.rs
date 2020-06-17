// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::DEFAULT_RAFT_SETS;
use criterion::Criterion;
use raft::{storage::MemStorage, Config, Raft};

pub fn bench_raft(c: &mut Criterion) {
    bench_raft_new(c);
    bench_raft_campaign(c);
}

fn quick_raft(voters: usize, learners: usize, logger: &slog::Logger) -> Raft<MemStorage> {
    let id = 1;
    let storage = MemStorage::default();
    let config = Config::new(id);
    let mut raft = Raft::new(&config, storage, logger).unwrap();
    (0..voters).for_each(|id| {
        raft.add_node(id as u64).unwrap();
    });
    (voters..learners).for_each(|id| {
        raft.add_learner(id as u64).unwrap();
    });
    raft
}

pub fn bench_raft_new(c: &mut Criterion) {
    DEFAULT_RAFT_SETS.iter().for_each(|(voters, learners)| {
        c.bench_function(&format!("Raft::new ({}, {})", voters, learners), move |b| {
            let logger = raft::default_logger();
            b.iter(|| quick_raft(*voters, *learners, &logger))
        });
    });
}

pub fn bench_raft_campaign(c: &mut Criterion) {
    DEFAULT_RAFT_SETS
        .iter()
        .skip(1)
        .for_each(|(voters, learners)| {
            // We don't want to make `raft::raft` public at this point.
            let msgs = &[
                "CampaignPreElection",
                "CampaignElection",
                "CampaignTransfer",
            ];
            // Skip the first since it's 0,0
            for msg in msgs {
                c.bench_function(
                    &format!("Raft::campaign ({}, {}, {})", voters, learners, msg),
                    move |b| {
                        let logger = raft::default_logger();
                        b.iter(|| {
                            let mut raft = quick_raft(*voters, *learners, &logger);
                            raft.campaign(msg.as_bytes());
                        })
                    },
                );
            }
        });
}
