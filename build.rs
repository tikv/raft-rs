// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use protobuf_build::*;
use std::fs::{read_dir, remove_file, File};
use std::io::Write;

#[allow(unused_must_use)]
fn main() {
    // This build script creates files in the `src` directory. Since that is
    // outside Cargo's OUT_DIR it will cause an error when this crate is used
    // as a dependency. Therefore, the user must opt-in to regenerating the
    // Rust files.
    if !cfg!(feature = "regenerate") {
        println!("cargo:rerun-if-changed=build.rs");
        return;
    }

    check_protoc_version();

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

    for f in &file_names {
        println!("cargo:rerun-if-changed={}", f);
    }

    // Delete the previously-generated wrapper, we're ok if the file does not exist
    remove_file("rsprost/protobuf_compat.rs");

    // Generate Prost files.
    generate_prost_files(&file_names, "src/rsprost");
    let mod_names = module_names_for_dir("src/rsprost");
    generate_wrappers(
        &mod_names
            .iter()
            .map(|m| format!("src/rsprost/{}.rs", m))
            .collect::<Vec<_>>(),
        "src/rsprost",
    );
    generate_prost_rs(&mod_names);

    // Generate rust-protobuf files.
    let file_names: Vec<_> = file_names.iter().map(|s| &**s).collect();
    generate_protobuf_files(file_names, "src/rsprotobuf");

    let mod_names = module_names_for_dir("src/rsprotobuf");

    let out_file_names: Vec<_> = mod_names
        .iter()
        .map(|m| format!("src/rsprotobuf/{}.rs", m))
        .collect();
    let out_file_names: Vec<_> = out_file_names.iter().map(|f| &**f).collect();
    replace_read_unknown_fields(&out_file_names);
    generate_protobuf_rs(&mod_names);
}

fn generate_protobuf_files(file_names: Vec<&str>, out_dir: &str) {
    protoc_rust::run(protoc_rust::Args {
        out_dir,
        input: &file_names,
        includes: &["proto", "include"],
        customize: protoc_rust::Customize {
            ..Default::default()
        },
    })
    .unwrap();

    protoc_grpcio::compile_grpc_protos(file_names, &["proto", "include"], "src/rsprotobuf")
        .unwrap();
}

fn generate_protobuf_rs(mod_names: &[String]) {
    let mut text = "pub use raft::eraftpb;\n\n".to_owned();

    for mod_name in mod_names {
        text.push_str("pub mod ");
        text.push_str(mod_name);
        text.push_str(";\n");
    }

    let mut lib = File::create("src/rsprotobuf.rs").expect("Could not create rsprotobuf.rs");
    lib.write_all(text.as_bytes())
        .expect("Could not write rsprotobuf.rs");
}

fn generate_prost_rs(mod_names: &[String]) {
    let mut text = "#![allow(dead_code)]\n#![allow(missing_docs)]\n\n".to_owned();

    for mod_name in mod_names {
        text.push_str("pub mod ");
        text.push_str(mod_name);
        text.push_str("{\n");
        text.push_str("include!(\"rsprost/");
        text.push_str(mod_name);
        text.push_str(".rs\");");
        text.push_str("include!(\"rsprost/wrapper_");
        text.push_str(mod_name);
        text.push_str(".rs\");");
        text.push_str("}\n\n");
    }
    text.push_str("pub mod protobuf_compat;\n");

    let mut lib = File::create("src/rsprost.rs").expect("Could not create rsprost.rs");
    lib.write_all(text.as_bytes())
        .expect("Could not write rsprost.rs");

    let protobuf_compat_text = "
        pub struct RepeatedField;
        impl RepeatedField {
            #[inline]
            pub fn from_vec<T>(v: Vec<T>) -> Vec<T> {
                v
            }
        }";
    let mut compat_file = File::create("src/rsprost/protobuf_compat.rs")
        .expect("Could not create protobuf_compat.rs");
    compat_file
        .write_all(protobuf_compat_text.as_bytes())
        .expect("Could not write protobuf_compat.rs");
}
