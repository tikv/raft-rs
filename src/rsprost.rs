#![allow(dead_code)]
#![allow(missing_docs)]

/// Protobuf structs.
pub mod eraftpb {
    include!("rsprost/eraftpb.rs");
    include!("rsprost/wrapper_eraftpb.rs");
}

/// `RepeatedField` wrapper.
pub mod protobuf_compat;
