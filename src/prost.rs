#![allow(dead_code)]
#![allow(missing_docs)]

pub mod eraftpb {
    include!("prost/eraftpb.rs");
    include!("prost/wrapper_eraftpb.rs");
}

pub mod protobuf_compat;
