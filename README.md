# Raft

[![Build Status](https://travis-ci.org/pingcap/raft-rs.svg?branch=master)](https://travis-ci.org/pingcap/raft-rs)

## Problem and Importance

When building a distributed system one principal goal is often to build in *fault-tolerance*. That is, if one particular node in a network goes down, or if there is a network partition, the entire cluster does not fall over. The cluster of nodes taking part in a distributed consensus protocol must come to agreement regarding values, and once that decision is reached, that choice is final.

Distributed Consensus Algorithms often take the form of a replicated state machine and log. Each state machine accepts inputs from its log, and represents the value(s) to be replicated, for example, a hash table. They allow a collection of machines to work as a coherent group that can survive the failures of some of its members.

Two well known Distributed Consensus Algorithms are Paxos and Raft. Paxos is used in systems like [Chubby](http://research.google.com/archive/chubby.html) by Google, and Raft is used in things like [`tikv`](https://github.com/pingcap/tikv) or [`etcd`](https://github.com/coreos/etcd/tree/master/raft). Raft is generally seen as a more understandable and simpler to implement than Paxos.

## Links for Further Research

* [The Raft site](https://raftconsensus.github.io/)
* [The Secret Lives of Data - Raft](http://thesecretlivesofdata.com/raft/)
* [Raft Paper](http://ramcloud.stanford.edu/raft.pdf)
* [Raft Dissertation](https://github.com/ongardie/dissertation#readme)
* [Raft Refloated](https://www.cl.cam.ac.uk/~ms705/pub/papers/2015-osr-raft.pdf)

## Using It

Currently `raft-rs` is only available as a Git based dependency. We're working towards making a release available on [crates.io](https://crates.io/) in the future, check out [tracking issue](https://github.com/pingcap/raft-rs/issues/1).

## Developing

`raft` is intended to track the latest `stable`, though you'll need to use `nightly` to simulate a full CI build with `clippy`.

Using `rustup` you can get started this way:

```bash
rustup override set stable
rustup toolchain install nightly
cargo +nightly install rustfmt-nightly
```

In order to have your PR merged running the following must finish without error:

```bash
cargo +nightly test --features dev
```

You may optionally want to install `cargo-check` to allow for automated rebuilding while editing:

```bash
cargo watch -s "cargo check --features dev"
```
