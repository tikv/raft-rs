//! # datadriven
//!
//! **datadriven** is a tool for testing.

#![deny(missing_docs)]

#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
#[macro_use]
extern crate anyhow;

#[cfg(test)]
#[macro_use]
extern crate pretty_assertions;

mod datadriven;
mod line_scanner;
mod line_sparser;
mod test_data;
mod test_data_reader;
