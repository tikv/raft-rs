// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use criterion::{BatchSize, Bencher, BenchmarkId, Criterion, Throughput};
use raft::eraftpb::{ConfState, Entry, Message, Snapshot, SnapshotMetadata};
use raft::{storage::MemStorage, Config, RawNode};
use std::time::Duration;

pub fn bench_raw_node(c: &mut Criterion) {
    bench_raw_node_new(c);
    bench_raw_node_leader_propose(c);
    bench_raw_node_new_ready(c);
}

fn quick_raw_node(logger: &slog::Logger) -> RawNode<MemStorage> {
    let id = 1;
    let conf_state = ConfState::from((vec![1], vec![]));
    let storage = MemStorage::new_with_conf_state(conf_state);
    let config = Config::new(id);
    RawNode::new(&config, storage, logger).unwrap()
}

pub fn bench_raw_node_new(c: &mut Criterion) {
    let bench = |b: &mut Bencher| {
        let logger = raft::default_logger();
        b.iter(|| quick_raw_node(&logger));
    };

    c.bench_function("RawNode::new", bench);
}

pub fn bench_raw_node_leader_propose(c: &mut Criterion) {
    static KB: usize = 1024;
    let mut test_sets = vec![
        0,
        32,
        128,
        512,
        KB,
        4 * KB,
        16 * KB,
        128 * KB,
        512 * KB,
        KB * KB,
    ];
    let mut group = c.benchmark_group("RawNode::leader_propose");
    for size in test_sets.drain(..) {
        // Calculate measurement time in seconds according to the input size.
        // The approximate time might not be the best but should work fine.
        let mtime = if size < KB {
            1
        } else if size < 128 * KB {
            3
        } else {
            7
        };
        group
            .measurement_time(Duration::from_secs(mtime))
            .throughput(Throughput::Bytes(size as u64))
            .bench_with_input(
                BenchmarkId::from_parameter(size),
                &size,
                |b: &mut Bencher, size| {
                    let logger = raft::default_logger();
                    let mut node = quick_raw_node(&logger);
                    node.raft.become_candidate();
                    node.raft.become_leader();
                    b.iter_batched(
                        || (vec![0; 8], vec![0; *size]),
                        |(context, value)| node.propose(context, value).expect(""),
                        BatchSize::SmallInput,
                    );
                },
            );
    }
}

pub fn bench_raw_node_new_ready(c: &mut Criterion) {
    let logger = raft::default_logger();
    let mut group = c.benchmark_group("RawNode::ready");
    group
        // TODO: The proper measurement time could be affected by the system and machine.
        .measurement_time(Duration::from_secs(20))
        .bench_function("Default", |b: &mut Bencher| {
            b.iter_batched(
                || test_ready_raft_node(&logger),
                |mut node| {
                    let _ = node.ready();
                },
                // NOTICE: SmallInput accumulates (iters + 10 - 1) / 10 samples per batch
                BatchSize::SmallInput,
            );
        });
}

// Create a raft node calling `ready()` with things below:
//  - 100 new entries with 32KB data each
//  - 100 committed entries with 32KB data each
//  - 100 raft messages
//  - A snapshot with 8MB data
// TODO: Maybe gathering all the things we need into a struct(e.g. something like `ReadyBenchOption`) and use it
//       to customize the output.
fn test_ready_raft_node(logger: &slog::Logger) -> RawNode<MemStorage> {
    let mut node = quick_raw_node(logger);
    node.raft.become_candidate();
    node.raft.become_leader();
    node.raft.raft_log.stable_to(1, 1);
    node.raft.commit_apply(1);
    let mut entries = vec![];
    for i in 1..101 {
        let mut e = Entry::default();
        e.data = vec![0; 32 * 1024];
        e.context = vec![];
        e.index = i;
        e.term = 1;
        entries.push(e);
    }
    let mut unstable_entries = entries.clone();
    node.raft.raft_log.store.wl().append(&entries).expect("");
    node.raft.raft_log.unstable.offset = 102;
    // This increases 'committed_index' to `last_index` because there is only one node in quorum.
    let _ = node.raft.append_entry(&mut unstable_entries);

    let mut snap = Snapshot::default();
    snap.set_data(vec![0; 8 * 1024 * 1024]);
    // We don't care about the contents in snapshot here since it won't be applied.
    snap.set_metadata(SnapshotMetadata::default());
    for _ in 0..100 {
        node.raft.msgs.push(Message::default());
    }
    // Force reverting committed index to provide us some entries to be stored from next `Ready`
    node.raft.raft_log.committed = 101;
    node
}
