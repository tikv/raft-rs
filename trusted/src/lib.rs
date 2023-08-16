#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(not(feature = "std"), feature(error_in_core))]

extern crate alloc;
extern crate core;
extern crate prost;
extern crate raft;
extern crate slog;

pub mod endpoint {
    #![allow(non_snake_case)]
    include!(concat!(env!("OUT_DIR"), "/endpoint.rs"));
}

pub mod driver;
pub mod model;
pub mod platform;

#[cfg(not(feature = "std"))]
use core::error::Error as StdError;
#[cfg(feature = "std")]
use std::error::Error as StdError;
