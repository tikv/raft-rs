use crate::line_sparser::parse_line;
use crate::test_data::TestData;
use anyhow::Result;
use std::path::{Path, PathBuf};
// use for writeln! macro
use std::fmt::Write;
use std::iter::Enumerate;
use std::str::Lines;

pub struct TestDataReader<'a> {
    source_name: PathBuf,
    pub data: TestData,
    scanner: Enumerate<Lines<'a>>,
    logger: slog::Logger,
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

            self.data = TestData::default();
            self.data.pos = format!(
                "{} : L{}",
                self.source_name.as_path().display(),
                line.unwrap().0
            );

            let mut line = line.unwrap().1.to_string();

            // before argument only (1) comment (2) empty line are accepted
            if line.starts_with('#') || line.is_empty() {
                // Skip comment lines.
                continue;
            }

            // Support wrapping directive lines using \, for example:
            // build-scalar \
            // vars(int)
            while line.ends_with('\\') {
                line = line.trim_end_matches('\\').to_string();

                let l = self
                    .scanner
                    .next()
                    .expect("expected argument that is not end with '\\'")
                    .1
                    .trim();
                line.push_str(l);
            }

            line = line.trim().to_string();

            debug!(self.logger, "argument_after_cleanup: {}", line);

            let (cmd, cmd_args) = parse_line(line.as_str(), &self.logger)?;

            if cmd.is_empty() {
                bail!("cmd must not be empty");
            }

            debug!(self.logger, "cmd: {}, cmd_args: {:?}", cmd, cmd_args,);

            self.data.cmd = cmd;
            self.data.cmd_args = cmd_args;

            loop {
                let line = self.scanner.next();
                if line.is_none() {
                    bail!("testdata is not complete, we expected '----' after command");
                }

                let line = line.unwrap().1;

                if line == "----" {
                    self.read_expected();
                    return Ok(true);
                } else if !line.is_empty() {
                    bail!("expected '----', found '{}'", line);
                }
            }
        }
    }

    // check whether there is two separator or one
    fn read_expected(&mut self) {
        // after reading first separator
        // we expected two input
        // (1) second separator
        // (2) non empty output

        // We need to add an end line for each line in order to distinguish the exact output,
        // otherwise we may consider an unexpected output combination to be correct

        if let Some((_, line)) = self.scanner.next() {
            if line == "----" {
                loop {
                    let mut line = self.scanner.next().unwrap().1.trim().to_string();
                    if line == "----" {
                        let mut line2 = self.scanner.next().unwrap().1.trim().to_string();
                        if line2 == "----" {
                            let line3 = self.scanner.next().unwrap().1.trim().to_string();
                            assert!(
                                line3.is_empty(),
                                "we expected an empty line after second separator, found '{}'",
                                line3
                            );
                            break;
                        }
                        if !line2.is_empty() {
                            writeln!(&mut line2).unwrap();
                            self.data.expected.push_str(line2.as_str());
                        }
                    }
                    if !line.is_empty() {
                        writeln!(&mut line).unwrap();
                        self.data.expected.push_str(line.as_str());
                    }
                }
            } else {
                let mut l = line.trim().to_string();
                if !l.is_empty() {
                    writeln!(&mut l).unwrap();
                    self.data.expected.push_str(l.as_str());
                }
                loop {
                    let mut line = self.scanner.next().unwrap().1.trim().to_string();
                    if line.is_empty() {
                        break;
                    } else {
                        writeln!(&mut line).unwrap();
                        self.data.expected.push_str(line.as_str());
                    }
                }
            }
        }
    }
}
