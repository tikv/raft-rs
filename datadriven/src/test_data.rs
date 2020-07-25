use std::fmt;

/// CmdArg contains information about an argument on the directive line. An
/// argument is specified in one of the following forms:
/// - argument
/// key: argument, values: []
/// - argument=
/// key: argument, values: []
/// - argument=a
/// key: argument, values: ["a"]
/// - argument=a,b,c
/// key: argument, values: ["a","b","c"]
/// - argument=(a,b,c)
/// key: argument, values: ["a","b","c"]
/// - a,b,c
/// key: a,b,c, values: []
///
#[derive(Clone)]
pub struct CmdArg {
    /// key of CmdArg
    pub key: String,
    /// values of CmdArg
    pub values: Vec<String>,
}

impl fmt::Display for CmdArg {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.values.len() {
            0 => write!(f, "{}", self.key),
            1 => write!(f, "{}={:?}", self.key, self.values[0]),
            _ => write!(f, "{}={:?}", self.key, self.values.join(",")),
        }
    }
}

impl fmt::Debug for CmdArg {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

/// TestData contains information about one data-driven test case that was parsed from the test file.
#[derive(Clone, Default)]
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

impl TestData {
    /// Return `true` if the `cmd_args` contains a value for the specified key.
    pub fn contains_key(&self, k: &str) -> bool {
        for cmd_arg in self.cmd_args.iter() {
            if cmd_arg.key == k {
                return true;
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use crate::{default_logger, CmdArg, TestData};

    #[test]
    fn test_contains_key() {
        let logger = default_logger();
        let cmd_arg = CmdArg {
            key: "key".to_string(),
            values: vec!["123".to_string(), "92".to_string(), "92".to_string()],
        };
        let mut d = TestData::default();
        d.cmd_args.push(cmd_arg);
        let cmd_arg = CmdArg {
            key: "key2".to_string(),
            values: vec!["some string".to_string()],
        };
        d.cmd_args.push(cmd_arg);
        assert_eq!(d.contains_key("key2"), true);
        assert_eq!(d.contains_key("key1"), false);
    }
}
