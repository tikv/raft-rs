// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

extern crate protobuf_build;

use protobuf_build::*;
use std::fs::{read_dir, File};
use std::io::Write;

fn main() {
    // This build script creates files in the `src` directory. Since that is
    // outside Cargo's OUT_DIR it will cause an error when this crate is used
    // as a dependency. Therefore, the user must opt-in to regenerating the
    // Rust files.
    if !cfg!(feature = "gen") {
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

    // Generate Prost output.
    generate_prost_files(&file_names, "src/prost");
    let mod_names = module_names_for_dir("src/prost");
    generate_wrappers(
        &mod_names
            .iter()
            .map(|m| format!("src/prost/{}.rs", m))
            .collect::<Vec<_>>(),
        "src/prost",
        GenOpt::MUT
            | GenOpt::TRIVIAL_GET
            | GenOpt::TRIVIAL_SET
            | GenOpt::HAS
            | GenOpt::TAKE
            | GenOpt::CLEAR
            | GenOpt::MESSAGE,
    );
    generate_prost_rs(&mod_names);

    // Generate rust-protobuf output
    generate_protobuf_files(&file_names, "src/protobuf");
    replace_read_unknown_fields(&file_names);
    generate_lib_file(&mod_names);
}

fn generate_prost_rs(mod_names: &[String]) {
    let mut text = "#![allow(dead_code)]\n\
                    #![allow(missing_docs)]\n\
                    #![allow(clippy::all)]\n\n"
        .to_owned();

    for mod_name in mod_names {
        text.push_str("pub mod ");
        text.push_str(mod_name);
        text.push_str("{\n");
        text.push_str("include!(\"prost/");
        text.push_str(mod_name);
        text.push_str(".rs\");");
        text.push_str("include!(\"prost/wrapper_");
        text.push_str(mod_name);
        text.push_str(".rs\");");
        text.push_str("}\n\n");
    }

    let mut lib = File::create("src/prost.rs").expect("Could not create prost.rs");
    lib.write_all(text.as_bytes())
        .expect("Could not write prost.rs");
}

fn generate_lib_file<T: AsRef<str>>(mod_names: &[T]) {
    let mut text = String::new();

    for mod_name in mod_names {
        text.push_str("#[rustfmt::skip]\n#[allow(bare_trait_objects)]\n#[allow(renamed_and_removed_lints)]\npub mod ");
        text.push_str(mod_name.as_ref());
        text.push_str(";\n");
    }

    let mut lib = File::create("src/protobuf.rs").expect("Could not create protobuf.rs");
    lib.write_all(text.as_bytes())
        .expect("Could not write protobuf.rs");
}
