// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

fn main() {
    jinkela_build::Builder::default().include_dir("proto").compile_proto("proto/eraftpb.proto").build();
}