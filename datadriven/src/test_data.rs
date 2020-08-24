use std::fmt;

/// CmdArg contains information about an argument on the directive line. An
/// argument is specified in one of the following forms:
///
/// - key         (no value)
/// - key=        (empty value)
/// - key=()      (empty value)
/// - key=a       (single value)
/// - key=a,b,c   (single value)
/// - key=(a,b,c) (multiple value)
#[derive(Clone)]
pub struct CmdArg {
    /// Key of CmdArg
    pub key: String,
    /// Values of CmdArg
    pub vals: Vec<String>,
}

impl fmt::Display for CmdArg {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.vals.len() {
            0 => write!(f, "{}", self.key),
            1 => write!(f, "{}={}", self.key, self.vals[0]),
            _ => write!(f, "{}=({})", self.key, self.vals.join(",")),
        }
    }
}

impl fmt::Debug for CmdArg {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

/// TestData contains information about datadriven testcase that was parsed from the test file.
/// Data format is text file(txt).
#[derive(Clone, Default)]
pub struct TestData {
    /// Pos is a file:line prefix for the input test file, suitable for
    /// inclusion in logs and error messages.
    pub pos: String,

    /// Cmd is the first string on the directive line (up to the first whitespace).
    pub cmd: String,

    /// CmdArgs contains the k/v arguments to the command.
    pub cmd_args: Vec<CmdArg>,

    /// Input is the text between the first directive line and the ---- separator.
    pub input: String,

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
    use crate::{CmdArg, TestData};

    #[test]
    fn test_contains_key() {
        let cmd_arg = CmdArg {
            key: "key".to_string(),
            vals: vec!["123".to_string(), "92".to_string(), "92".to_string()],
        };
        let mut d = TestData::default();
        d.cmd_args.push(cmd_arg);
        let cmd_arg = CmdArg {
            key: "key2".to_string(),
            vals: vec!["some string".to_string()],
        };
        d.cmd_args.push(cmd_arg);
        assert_eq!(d.contains_key("key2"), true);
        assert_eq!(d.contains_key("key1"), false);
    }
}
