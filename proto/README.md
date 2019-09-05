# Raft Proto

[![Documentation](https://docs.rs/raft-proto/badge.svg)](https://docs.rs/raft-proto/)
[![Crates.io](https://img.shields.io/crates/v/raft-proto.svg)](https://crates.io/crates/raft-proto)

This crate contains the protobuf structs used by raft.

## Regenerate Protobufs

If the protobuf file `eraftpb.proto` changed,
run this command to regenerate `eraftpb.rs`:

```bash
cargo build --features gen
```

or using Cargo watch:

```bash
cargo watch -s "cargo check --features gen"
```

