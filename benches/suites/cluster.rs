use criterion::Criterion;
use criterion::{BatchSize, Bencher, BenchmarkId, Throughput};
use raft::prelude::*;
use raft::storage::MemStorage;
use raft::{StateRole, INVALID_ID};
use slog::Logger;
use std::cell::Cell;
use std::collections::{HashMap, VecDeque};
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::{Duration, Instant};

pub fn bench_cluster(c: &mut Criterion) {
    static KB: usize = 1024;
    // TODO: Entry with empty data will be omitted when being applied so that a client will
    // never be notified. Maybe using a unique ID for each entry 
    let mut test_sets = vec![1, 32, 128, 512, KB, 4 * KB, 32 * KB];
    let mut group = c.benchmark_group("Raft::cluster");
    for size in test_sets.drain(..) {
        group
            .measurement_time(Duration::from_secs(10))
            .sample_size(10)
            .throughput(Throughput::Bytes(size as u64))
            .bench_with_input(
                BenchmarkId::from_parameter(size),
                &size,
                |b: &mut Bencher, size| {
                    let logger = raft::default_logger();
                    let mut cluster = Cluster::new(3);
                    cluster.start(&logger);
                    thread::sleep(Duration::from_secs(1));
                    b.iter_batched(
                        || bench_proposal_msg(cluster.leader, cluster.leader, *size),
                        |m| cluster.propose(m),
                        BatchSize::SmallInput,
                    )
                },
            );
    }
}

fn bench_proposal_msg(from: u64, to: u64,  data_size: usize) -> Message {
    let mut m = Message::default();
    m.set_msg_type(MessageType::MsgPropose);
    m.from = from;
    m.to = to;
    let mut e = Entry::default();
    e.data = vec![0; data_size];
    e.context = vec![0; 8];
    m.set_entries(vec![e].into());
    m
}

struct Cluster {
    leader: u64, // always 1 if node_count >0
    node_count: u64,
    network: Network,
    notify_center: Arc<Mutex<VecDeque<Sender<()>>>>,
}

impl Cluster {
    fn new(node_count: u64) -> Self {
        Cluster {
            leader: INVALID_ID,
            node_count: node_count,
            network: Network::default(),
            notify_center: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    fn start(&mut self, logger: &Logger) {
        let node_count = if self.node_count == 0 {
            1
        } else {
            self.node_count
        };
        self.node_count = node_count;
        // Make sure leader id is valid
        let leader = if self.leader == INVALID_ID || self.leader > node_count {
            1
        } else {
            self.leader
        };
        self.leader = leader;
        let voters = (1..=node_count).fold(vec![], |mut sum, id| {
            sum.push(id);
            sum
        });
        let mut recvs = (1..=node_count).fold(vec![], |mut sum, id| {
            let recv = self.network.register(id);
            sum.push(recv);
            sum
        });
        for (id, receiver) in (1..=node_count).zip(recvs.drain(..)) {
            let mut config = Config {
                id,
                election_tick: 10,
                heartbeat_tick: 1,
                ..Default::default()
            };
            let conf_state = ConfState::from((voters.clone(), vec![]));
            let store = MemStorage::new_with_conf_state(conf_state);
            if id == leader {
                config.election_tick = 3;
            }
            let raw_node =
                RawNode::new(&config, store, logger).expect("Creating raw node must be successful");
            let notify_center = self.notify_center.clone();
            let n = self.network.clone();
            thread::spawn(move || {
                let mut node = Node {
                    raft: raw_node,
                    network: n,
                    my_mailbox: receiver,
                    notify_center,
                };
                let timer = Cell::new(Instant::now());
                loop {
                    node.handle_raft(&timer);
                }
            });
        }
    }

    // Propose a Message and return a signal receiver to indicate the
    // proposal is committed.
    fn propose(&self, proposal: Message) {
        let (tx, rx) = mpsc::channel();
        {
            let mut queue = self.notify_center.lock().unwrap();
            queue.push_back(tx);
        }
        self.network.send(proposal);
        rx.recv().expect("Proposal must be committed successfully")
    }
}

struct Node {
    raft: RawNode<MemStorage>,
    my_mailbox: Receiver<Message>,
    network: Network,
    notify_center: Arc<Mutex<VecDeque<Sender<()>>>>,
}

impl Node {
    fn handle_raft(&mut self, timer: &Cell<Instant>) {
        // Tick every 10ms
        thread::sleep(Duration::from_millis(10));
        loop {
            // Step raft messages.
            match self.my_mailbox.try_recv() {
                Ok(msg) => {
                    self.raft.step(msg).expect("Step message must successful");
                },
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => return,
            }
        }
        if timer.get().elapsed() >= Duration::from_millis(10) {
            // Tick the raft.
            self.raft.tick();
            timer.set(Instant::now());
        }
        self.on_ready();
    }

    fn on_ready(&mut self) {
        if !self.raft.has_ready() {
            return;
        }
        let mut ready = self.raft.ready();
        let store = self.raft.raft.raft_log.store.clone();
        store
            .wl()
            .append(ready.entries())
            .expect("Appending entries must successful");
        if *ready.snapshot() != Snapshot::default() {
            let s = ready.snapshot().clone();
            store
                .wl()
                .apply_snapshot(s)
                .expect("Applying snapshot must successful");
        }
        for msg in ready.messages.drain(..) {
            self.network.send(msg);
        }
        if let Some(committed_entries) = ready.committed_entries.take() {
            for entry in &committed_entries {
                if entry.data.is_empty() || EntryType::EntryConfChange == entry.get_entry_type() {
                    continue;
                }
                // Notify committed proposal to client
                if self.raft.raft.state == StateRole::Leader {
                    let signal = self.notify_center.lock().unwrap().pop_front().unwrap();
                    signal.send(()).unwrap();
                }
            }
            if let Some(last_committed) = committed_entries.last() {
                let mut s = store.wl();
                s.mut_hard_state().commit = last_committed.index;
                s.mut_hard_state().term = last_committed.term;
            }
        }
        self.raft.advance(ready);
    }
}

#[derive(Default, Debug, PartialEq, Eq, Hash)]
struct Connection {
    from: u64,
    to: u64,
}

type DelayMap = HashMap<Connection, (f64, Duration)>;

#[derive(Default, Clone)]
struct Network {
    senders: HashMap<u64, Sender<Message>>,
    delaym: Arc<RwLock<DelayMap>>,
}

impl Network {
    fn register(&mut self, id: u64) -> Receiver<Message> {
        let (tx, rx) = mpsc::channel();
        self.senders.insert(id, tx);
        rx
    }

    fn start(&mut self, mut mailboxes: HashMap<u64, Sender<Message>>) {
        for (id, sender) in mailboxes.drain() {
            let (tx, rx) = mpsc::channel::<Message>();
            let delaym = self.delaym.clone();
            thread::spawn(move || loop {
                match rx.recv() {
                    Ok(msg) => {
                        let dm = delaym.read().unwrap();
                        maybe_delay(&*dm, msg.from, msg.to);
                        sender.send(msg).expect("Fail to send message");
                    }
                    Err(_) => return,
                }
            });
            self.senders.insert(id, tx);
        }
    }

    fn send(&self, m: Message) {
        if let Some(sender) = self.senders.get(&m.to) {
            // ignore error here
            sender.send(m).expect("Fail to send message");
        }
    }

    fn delay(&self, from: u64, to: u64, perc: f64, duration: Duration) {
        if duration.as_nanos() > 0 {
            let mut m = self.delaym.write().unwrap();
            m.insert(Connection { from, to }, (perc, duration));
        }
    }
}

fn maybe_delay(delaym: &DelayMap, from: u64, to: u64) {
    let (perc, duration) = delaym
        .get(&Connection { from, to })
        .cloned()
        .unwrap_or((0f64, Duration::from_micros(0)));
    if perc != 0f64 && rand::random::<f64>() <= perc {
        thread::sleep(duration);
    }
}
