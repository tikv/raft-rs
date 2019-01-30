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

extern crate regex;

use regex::Regex;
use std::env;
use std::fs::read_dir;
use std::fs::File;
use std::io::{Read, Write};
use std::process::Command;

fn main() {
    // This build script creates files in the `src` directory. Since that is
    // outside Cargo's OUT_DIR it will cause an error when this crate is used
    // as a dependency. Therefore, the user must opt-in to regenerating the
    // Rust files.
    if env::var_os("CARGO_FEATURE_REGENERATE").is_none() {
        println!("cargo:rerun-if-changed=build.rs");
        return;
    }

    let buf_lib = BufferLib::from_env_vars();
    if buf_lib == BufferLib::Prost {
        unimplemented!("Prost support is not yet implemented");
    }

    check_protoc_version();

    let file_names: Vec<_> = read_dir("proto")
        .expect("Couldn't read proto directory")
        .map(|e| {
            format!(
                "proto/{}",
                e.expect("Couldn't list file").file_name().to_string_lossy()
            )
        })
        .collect();
    let file_names: Vec<_> = file_names.iter().map(|s| &**s).collect();

    for f in &file_names {
        println!("cargo:rerun-if-changed={}", f);
    }

    if buf_lib == BufferLib::Protobuf {
        generate_protobuf_files(file_names);
        let mod_names: Vec<_> = read_dir("src/rsprotobuf")
            .expect("Couldn't read src directory")
            .filter_map(|e| {
                let file_name = e.expect("Couldn't list file").file_name();
                file_name
                    .to_string_lossy()
                    .split(".rs")
                    .next()
                    .map(|n| n.to_owned())
            })
            .collect();
        replace_read_unknown_fields(&mod_names);
        generate_protobuf_rs(&mod_names);
    }
}

#[derive(Eq, PartialEq)]
enum BufferLib {
    Prost,
    Protobuf,
}

impl BufferLib {
    fn from_env_vars() -> BufferLib {
        match (
            env::var_os("CARGO_FEATURE_LIB_PROST"),
            env::var_os("CARGO_FEATURE_LIB_RUST_PROTOBUF"),
        ) {
            (Some(_), Some(_)) | (None, None) => {
                panic!("You must use exactly one of `lib-rust-protobuf` and `prost-buf` features")
            }
            (Some(_), _) => BufferLib::Prost,
            (_, Some(_)) => BufferLib::Protobuf,
        }
    }
}

fn check_protoc_version() {
    let output = Command::new("bash")
        .arg("common.sh")
        .arg("check_protoc_version")
        .output()
        .expect("Could not execute `check_protoc_version`");
    if !output.status.success() {
        panic!(
            "Invalid version of protoc (required 3.1.x), or protoc not installed\n\nstdout:\n\n{}",
            String::from_utf8_lossy(&output.stdout)
        );
    }
}

fn generate_protobuf_files(file_names: Vec<&str>) {
    protoc_rust::run(protoc_rust::Args {
        out_dir: "src/rsprotobuf",
        input: &file_names,
        includes: &["proto", "include"],
        customize: protoc_rust::Customize {
            ..Default::default()
        },
    })
    .unwrap();

    protoc_grpcio::compile_grpc_protos(file_names, &["proto", "include"], "src/rsprotobuf").unwrap();
}

// Use the old way to read protobuf enums.
// FIXME: Remove this once stepancheg/rust-protobuf#233 is resolved.
fn replace_read_unknown_fields(mod_names: &[String]) {
    let regex =
        Regex::new(r"::protobuf::rt::read_proto3_enum_with_unknown_fields_into\(([^,]+), ([^,]+), &mut ([^,]+), [^\)]+\)\?").unwrap();
    for mod_name in mod_names {
        let file_name = &format!("src/rsprotobuf/{}.rs", mod_name);

        let mut text = String::new();
        {
            let mut f = File::open(file_name).unwrap();
            f.read_to_string(&mut text)
                .expect("Couldn't read source file");
        }

        let text = regex.replace_all(
            &text,
            "if $1 == ::protobuf::wire_format::WireTypeVarint {\
                $3 = $2.read_enum()?;\
             } else {\
                return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));\
             }",
        );
        let mut out = File::create(file_name).unwrap();
        out.write_all(text.as_bytes())
            .expect("Could not write source file");
    }
}

fn generate_protobuf_rs(mod_names: &[String]) {
    let mut text = "".to_owned();

    for mod_name in mod_names {
        text.push_str("pub mod ");
        text.push_str(mod_name);
        text.push_str(";\n");
    }

    let mut lib = File::create("src/rsprotobuf.rs").expect("Could not create rsprotobuf.rs");
    lib.write_all(text.as_bytes())
        .expect("Could not write rsprotobuf.rs");
}
