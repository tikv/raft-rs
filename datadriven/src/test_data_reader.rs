use crate::line_sparser::parse_line;
use crate::test_data::TestData;
use anyhow::Result;
use std::iter::Enumerate;
use std::path::{Path, PathBuf};
use std::str::Lines;

pub struct TestDataReader<'a> {
    source_name: PathBuf,
    pub data: TestData,
    scanner: Enumerate<Lines<'a>>,
    pub logger: slog::Logger,
}

impl<'a> TestDataReader<'a> {
    pub fn new<P>(source_name: P, content: &'a str, logger: &slog::Logger) -> Self
    where
        P: AsRef<Path>,
    {
        Self {
            source_name: source_name.as_ref().to_path_buf(),
            scanner: content.lines().enumerate(),
            data: TestData::default(),
            logger: logger.clone(),
        }
    }

    pub fn next(&mut self) -> Result<bool> {
        loop {
            let line = self.scanner.next();
            if line.is_none() {
                return Ok(false);
            }

            let mut pos = line.unwrap().0;
            let mut line = line.unwrap().1.to_string();

            // Only (1) comment (2) empty line
            // are accepted before argument.
            if line.starts_with('#') || line.is_empty() {
                // Skip comment lines.
                continue;
            }

            // Support wrapping directive lines using \, for example:
            // build-scalar \
            // vars(int)
            while line.ends_with('\\') {
                line.pop();

                let l = self
                    .scanner
                    .next()
                    .expect("expect argument ends without '\\'")
                    .1
                    .trim();

                // skip blank line, if not we might get some whitespace at the end,
                // without trimming it, parsing line will fails
                if !l.is_empty() {
                    // Add a whitespace for separating arguments
                    line.push(' ');
                    line.push_str(l);
                }

                // We need the last line number of argument
                pos += 1;
            }

            debug!(self.logger, "argument_after_cleanup: {}", line);

            // Start reading argument
            // Init data
            self.data = TestData::default();

            // Save `line` information for error/debug message usage
            self.data.pos = format!("{} : L{}", self.source_name.as_path().display(), pos + 1);

            let (cmd, cmd_args) = parse_line(&line, &self.logger)?;

            if cmd.is_empty() {
                bail!("cmd must not be empty");
            }

            debug!(self.logger, "cmd: {}, cmd_args: {:?}", cmd, cmd_args,);

            self.data.cmd = cmd;
            self.data.cmd_args = cmd_args;

            let mut buf = String::new();
            let mut separator = false;

            loop {
                let line = self.scanner.next();
                if line.is_none() {
                    break;
                }

                let line = line.unwrap().1;

                debug!(self.logger, "input line: {:?}", line);
                if line == "----" {
                    separator = true;
                    break;
                } else {
                    let line = line.to_string() + "\n";
                    buf.push_str(&line);
                }
            }

            // trim() including '\n', only whitespace is expected to be trimmed
            self.data.input = buf
                .trim_start_matches(' ')
                .trim_end_matches(' ')
                .to_string();

            debug!(self.logger, "input before separator: {:?}", self.data.input);

            if separator {
                self.read_expected();
            }

            return Ok(true);
        }
    }

    // check whether there is two separator or one
    fn read_expected(&mut self) {
        // after reading first separator
        // we expected two input
        // (1) second separator
        // (2) non empty output

        // We need to add an blank line for each line in order to distinguish the exact output,
        // otherwise we may consider an unexpected output combination to be correct

        if let Some((_, line)) = self.scanner.next() {
            if line == "----" {
                loop {
                    let mut line = self.scanner.next().unwrap().1.trim().to_string();
                    if line == "----" {
                        let mut line2 = self.scanner.next().unwrap().1.trim().to_string();
                        if line2 == "----" {
                            let line3 = self.scanner.next().unwrap().1.trim();
                            assert!(
                                line3.is_empty(),
                                "expected an blank line after second separator, found '{}'",
                                line3
                            );
                            break;
                        }
                        line2 += "\n";
                        self.data.expected.push_str(&line2);
                    }
                    line += "\n";
                    self.data.expected.push_str(&line);
                }
            } else {
                // Read the expected value after separator
                let mut l = line.trim().to_string();
                while !l.is_empty() {
                    l += "\n";
                    self.data.expected.push_str(&l);
                    l = self
                        .scanner
                        .next()
                        .expect("expect to get empty line as the terminator.")
                        .1
                        .trim()
                        .to_string();
                }
            }
        }
    }
}
