// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use protobuf_build::Builder;

fn main() {
    let base = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string());
    let mut builder = Builder::new();
    builder
        .search_dir_for_protos(&format!("{base}/proto"))
        .includes(&[format!("{base}/include"), format!("{base}/proto")])
        .include_google_protos();
    #[cfg(feature = "prost-codec")]
    builder.wrapper_options(protobuf_build::GenOpt::all() - protobuf_build::GenOpt::MESSAGE);
    builder.generate()
}
