// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::DEFAULT_RAFT_SETS;
use criterion::Criterion;
use raft::eraftpb::ConfState;
use raft::{storage::MemStorage, Config, Raft};

pub fn bench_raft(c: &mut Criterion) {
    bench_raft_new(c);
    bench_raft_campaign(c);
}

fn new_storage(voters: usize, learners: usize) -> MemStorage {
    let mut cc = ConfState::default();
    for i in 1..=voters {
        cc.voters.push(i as u64);
    }
    for i in 1..=learners {
        cc.learners.push(voters as u64 + i as u64);
    }
    MemStorage::new_with_conf_state(cc)
}

fn quick_raft(storage: MemStorage, logger: &slog::Logger) -> Raft<MemStorage> {
    let id = 1;
    let config = Config::new(id);
    Raft::new(&config, storage, logger).unwrap()
}

pub fn bench_raft_new(c: &mut Criterion) {
    DEFAULT_RAFT_SETS.iter().for_each(|(voters, learners)| {
        c.bench_function(&format!("Raft::new ({}, {})", voters, learners), move |b| {
            let logger = raft::default_logger();
            let storage = new_storage(*voters, *learners);
            b.iter(|| quick_raft(storage.clone(), &logger))
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
                        let storage = new_storage(*voters, *learners);
                        b.iter(|| {
                            let mut raft = quick_raft(storage.clone(), &logger);
                            raft.campaign(msg.as_bytes());
                        })
                    },
                );
            }
        });
}
