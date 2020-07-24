use std::fmt;

// CmdArg contains information about an argument on the directive line. An
// argument is specified in one of the following forms:
//  - argument
//  - argument=value
//  - argument=(values, ...)
#[derive(Clone)]
pub struct CmdArg {
    pub key: String,
    pub values: Vec<String>,
}

impl fmt::Display for CmdArg {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}={:?}", self.key, self.values)
    }
}

impl fmt::Debug for CmdArg {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

/// TestData contains information about one data-driven test case that was parsed from the test file.
#[derive(Default, Clone)]
pub struct TestData {
    /// Pos is a file:line prefix for the input test file, suitable for
    /// inclusion in logs and error messages.
    pub pos: String,

    /// Cmd is the first string on the directive line (up to the first whitespace).
    pub cmd: String,

    /// CmdArgs contains the k/v arguments to the command.
    pub cmd_args: Vec<CmdArg>,

    /// Expected is the value below the ---- separator. In most cases,
    /// tests need not check this, and instead return their own actual
    /// output.
    /// This field is provided so that a test can perform an early return
    /// with "return d.expected" to signal that nothing has changed.
    pub expected: String,
}
