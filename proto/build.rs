// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use protobuf_build::Builder;

fn main() {
    let base = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string());
    Builder::new()
        .search_dir_for_protos(&format!("{base}/proto"))
        .includes(&[format!("{base}/include"), format!("{base}/proto")])
        .include_google_protos()
        .generate()
}
