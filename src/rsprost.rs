#![allow(dead_code)]
#![allow(missing_docs)]

pub mod eraftpb {
    include!("rsprost/eraftpb.rs");include!("rsprost/wrapper_eraftpb.rs");
}

pub mod protobuf_compat;
