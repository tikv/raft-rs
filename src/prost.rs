#![allow(dead_code)]
#![allow(missing_docs)]
#![allow(clippy::all)]

pub mod eraftpb {
    include!("prost/eraftpb.rs");
    include!("prost/wrapper_eraftpb.rs");
}
