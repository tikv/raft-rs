#![allow(dead_code)]

pub mod eraftpb {
    include!("rsprost/eraftpb.rs");
    include!("rsprost/wrapper_eraftpb.rs");
}

pub mod protobuf_compat;
