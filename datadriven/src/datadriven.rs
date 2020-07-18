use crate::errors::Result;
use crate::test_data::TestData;
use std::fs::OpenOptions;
use std::io;
use std::path::Path;

pub fn run_test<P: AsRef<Path>>(path: P, f: impl Fn(&TestData) -> Option<String>) -> Result<()> {
    let file = OpenOptions::new().read(true).open(&path)?;
    Ok(())
}

fn run_directive(f: impl Fn(&TestData) -> Option<String>) -> Result<()> {
    Ok(())
}
