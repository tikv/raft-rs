use crate::line_scanner::LineScanner;
use crate::line_sparser::parse_line;
use crate::test_data::TestData;
use std::path::{Path, PathBuf};

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

    pub fn next(&mut self) -> bool {
        loop {
            let line = self.scanner.scan();
            if line.is_none() {
                break false;
            }
            let mut line = String::from(line.unwrap().trim());

            self.data = TestData::default();
            self.data.pos = format!(
                "{} : L{}",
                self.source_name.as_path().display(),
                self.scanner.line
            );

            if line.starts_with('#') {
                // Skip comment lines.
                continue;
            }

            // Support wrapping directive lines using \, for example:
            //   build-scalar \
            //   vars(int)
            while line.ends_with('\\') {
                line = line.trim_end_matches('\\').to_string();
                if let Some(l) = self.scanner.scan() {
                    line.push_str(l);
                } else {
                    break;
                }
            }

            line = line.trim().to_string();

            debug!("line after cleanup: {:?}", line);

            let (cmd, cmd_args) = parse_line(line.as_str()).unwrap();
            if cmd == "" {
                // Nothing to do here.
                continue;
            }

            self.data.cmd = cmd.clone();
            self.data.cmd_args = cmd_args;

            if cmd == "subtest" {
                // Subtest directives do not have an input and expected output.
                break true;
            }

            let mut separator = false;
            let mut buf = String::new();

            loop {
                let line = self.scanner.scan();
                if line.is_none() {
                    break;
                }
                let line = line.unwrap();
                if line == "----" {
                    separator = true;
                    break;
                }
                buf.push_str(line);
            }

            // TODO(accelsao): remove useless data.input
            self.data.input = buf.trim().to_string();

            if separator {
                self.read_expected()
            }

            break true;
        }
    }

    // check whether there is two separator or one
    fn read_expected(&mut self) {
        if let Some(line) = self.scanner.scan() {
            if line == "----" {
                loop {
                    let line = self
                        .scanner
                        .scan()
                        .expect("this should not fails")
                        .trim()
                        .to_string();
                    if line == "----" {
                        let line2 = self
                            .scanner
                            .scan()
                            .expect("this should not fails")
                            .trim()
                            .to_string();
                        if line2 == "----" {
                            let line3 = self.scanner.scan().expect("this should not fails");
                            assert!(line3.is_empty());
                            break;
                        }
                        if !line2.is_empty() {
                            self.data.expected.push_str((line2 + "\n").as_str())
                        }
                    }
                    if !line.is_empty() {
                        self.data.expected.push_str((line + "\n").as_str())
                    }
                }
            } else {
                let l = line.trim().to_string();
                if !l.is_empty() {
                    self.data.expected.push_str((l + "\n").as_str())
                }
                loop {
                    let line = self
                        .scanner
                        .scan()
                        .expect("this should not fails")
                        .trim()
                        .to_string();
                    if line.is_empty() {
                        break;
                    }
                    if !line.is_empty() {
                        self.data.expected.push_str((line + "\n").as_str())
                    }
                }
            }
        }
    }

    pub fn get_data(&self) -> TestData {
        self.data.clone()
    }
}
