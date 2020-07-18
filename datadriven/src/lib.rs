mod datadriven;
mod line_sparser;
mod test_data;
mod test_data_reader;

/// The error types used through out this crate.
pub mod errors {
    pub use anyhow::{Error, Result};
}
