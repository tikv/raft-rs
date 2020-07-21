#![deny(missing_docs)]

#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
#[macro_use]
extern crate nom;

#[cfg(test)]
#[macro_use]
extern crate pretty_assertions;

mod datadriven;
mod line_scanner;
mod line_sparser;
mod test_data;
mod test_data_reader;

/// The error types used through out this crate.
pub mod errors {
    pub(crate) use anyhow::{bail, ensure, Context};
    pub use anyhow::{Error, Result};
}
