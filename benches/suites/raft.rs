use crate::DEFAULT_RAFT_SETS;
use criterion::Criterion;
use raft::eraftpb::ConfState;
use raft::{storage::MemStorage, Config, Raft};

pub fn bench_raft(c: &mut Criterion) {
    bench_raft_new(c);
    bench_raft_campaign(c);
}

fn quick_raft(voters: usize, learners: usize, logger: &slog::Logger) -> Raft<MemStorage> {
    let id = 1;
    let config = Config::new(id);

    let (voters, learners) = (voters as u64, learners as u64);
    let voter_ids: Vec<u64> = (1..=voters).collect();
    let learner_ids: Vec<u64> = ((1 + voters)..=(voters + learners)).collect();
    let conf_state = ConfState::from((voter_ids, learner_ids));
    let storage = MemStorage::new_with_conf_state(conf_state);

    Raft::new(&config, storage, logger).unwrap()
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
