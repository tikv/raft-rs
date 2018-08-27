use criterion::{Bencher, Criterion};
use raft::{storage::MemStorage, Config, RawNode};

criterion_group!(bench_raw_node, bench_raw_node_new,);

fn quick_raw_node() -> RawNode<MemStorage> {
    let id = 1;
    let peers = vec![];
    let storage = MemStorage::default();
    let config = Config::new(id);
    let node = RawNode::new(&config, storage, peers).unwrap();
    node
}

pub fn bench_raw_node_new(c: &mut Criterion) {
    let bench = |b: &mut Bencher| {
        // No setup.
        b.iter(|| quick_raw_node());
    };

    c.bench_function("RawNode::new", bench);
}
