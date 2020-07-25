use crate::line_scanner::LineScanner;
use crate::line_sparser::parse_line;
use crate::test_data::TestData;
use anyhow::Result;
use std::path::{Path, PathBuf};
// use for writeln! macro
use std::fmt::Write;

pub struct TestDataReader<'a> {
    source_name: PathBuf,
    data: TestData,
    scanner: LineScanner<'a>,
}

impl<'a> TestDataReader<'a> {
    pub fn new<P>(source_name: P, content: &'a str) -> Self
    where
        P: AsRef<Path>,
    {
        Self {
            source_name: source_name.as_ref().to_path_buf(),
            scanner: LineScanner::new(content),
            data: TestData::default(),
        }
    }

    pub fn next(&mut self) -> Result<bool> {
        loop {
            let line = self.scanner.scan();
            if line.is_none() {
                return Ok(false);
            }
            let mut line = String::from(line.unwrap().trim());

            self.data = TestData::default();
            self.data.pos = format!(
                "{} : L{}",
                self.source_name.as_path().display(),
                self.scanner.line
            );

            // before argument only (1) comment (2) empty line are accepted
            if line.starts_with('#') {
                // Skip comment lines.
                continue;
            } else if line.is_empty() {
                continue;
            }

            // Support wrapping directive lines using \, for example:
            //   build-scalar \
            //   vars(int)
            while line.ends_with('\\') {
                line = line.trim_end_matches('\\').to_string();

                let l = self.scanner.scan().expect("expected argument that is not end with '\\'");
                line.push_str(l);
            }

            line = line.trim().to_string();

            debug!("line after cleanup: {:?}", line);

            let (cmd, cmd_args) = parse_line(line.as_str())?;

            if cmd.is_empty() {
                bail!("cmd must not be empty");
            }

            self.data.cmd = cmd;
            self.data.cmd_args = cmd_args;

            loop {
                let line = self.scanner.scan();
                if line.is_none() {
                    bail!("testdata is not complete, we expected '----' after command");
                }
                let line = line.unwrap();
                if line == "----" {
                    self.read_expected();
                    return Ok(true);
                } else if !line.is_empty() {
                    bail!("expected '----' only");
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

        if let Some(line) = self.scanner.scan() {
            if line == "----" {
                loop {
                    let mut line = self.scanner.scan().unwrap().trim().to_string();
                    if line == "----" {
                        let mut line2 = self.scanner.scan().unwrap().trim().to_string();
                        if line2 == "----" {
                            let line3 = self.scanner.scan().unwrap();
                            assert!(line3.is_empty(), "we expected an empty line after second separator, found '{}'", line3);
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
                    let mut line = self.scanner.scan().unwrap().trim().to_string();
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

    pub fn get_data(&self) -> TestData {
        self.data.clone()
    }
}
