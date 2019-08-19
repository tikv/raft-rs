// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use protobuf_build::Builder;

fn main() {
    Builder::new().search_dir_for_protos("proto").generate()
}
