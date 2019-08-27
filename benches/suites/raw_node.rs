use criterion::{Bencher, Criterion, measurement::Measurement};
use raft::{storage::MemStorage, Config, RawNode};

pub fn bench_raw_node<M>(c: &mut Criterion<M>) where M: Measurement + 'static {
    bench_raw_node_new(c);
}

fn quick_raw_node() -> RawNode<MemStorage> {
    let id = 1;
    let storage = MemStorage::default();
    let config = Config::new(id);
    RawNode::new(&config, storage).unwrap()
}

pub fn bench_raw_node_new<M>(c: &mut Criterion<M>) where M: Measurement + 'static {
    let bench = |b: &mut Bencher<M>| {
        // No setup.
        b.iter(quick_raw_node);
    };

    c.bench_function("RawNode::new", bench);
}
