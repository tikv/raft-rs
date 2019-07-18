// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use protobuf_build::*;
use std::env;
use std::fs::read_dir;

fn main() {
    let out_dir = format!("{}/protos", env::var("OUT_DIR").unwrap());
    let file_names: Vec<_> = read_dir("proto")
        .expect("Couldn't read proto directory")
        .filter_map(|e| {
            let e = e.expect("Couldn't list file");
            if e.file_type().expect("File broken").is_dir() {
                None
            } else {
                Some(format!("proto/{}", e.file_name().to_string_lossy()))
            }
        })
        .collect();
    generate_files(
        &["include".to_owned(), "proto".to_owned()],
        &file_names,
        &out_dir,
    );
}
