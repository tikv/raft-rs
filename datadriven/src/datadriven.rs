use std::path::Path;
use std::fs::OpenOptions;
use std::io;
use crate::errors::Result;

pub fn run_test<P: AsRef<Path>>(path: P, f: impl Fn(TestData) -> Option<String>) -> Result<()> {
    let file = OpenOptions::new().read(true).open(&path)?;

    Ok(())
}


// CmdArg contains information about an argument on the directive line. An
// argument is specified in one of the following forms:
//  - argument
//  - argument=value
//  - argument=(values, ...)
struct CmdArg {
    key: String,
    values: Vec<String>,
}

// TestData contains information about one data-driven test case that was parsed from the test file.
pub struct TestData {
    // Pos is a file:line prefix for the input test file, suitable for
    // inclusion in logs and error messages.
    pos: String,

    // Cmd is the first string on the directive line (up to the first whitespace).
    cmd: String,

    // CmdArgs contains the k/v arguments to the command.
    cmd_args: Vec<CmdArg>,

    // Input is the text between the first directive line and the ---- separator.
    input: String,

    // Expected is the value below the ---- separator. In most cases,
    // tests need not check this, and instead return their own actual
    // output.
    // This field is provided so that a test can perform an early return
    // with "return d.Expected" to signal that nothing has changed.
    expected: String,
}