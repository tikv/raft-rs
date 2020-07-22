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

            debug!("line_options: {:?}", line);

            if line.is_none() {
                break false;
            }
            let mut line = String::from(line.unwrap().trim());

            self.data = TestData::default();
            self.data.pos = format!(
                "{}:{}",
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
                if let Some(l) = self.scanner.scan() {
                    line.push_str(l);
                } else {
                    break;
                }
            }

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

#[cfg(test)]
mod tests {
    use crate::test_data_reader::TestDataReader;
    use anyhow::Result;
    use std::fs;

    #[test]
    fn test_data_reader() -> Result<()> {
        let source_name = "src/testdata/data.txt";
        let file = fs::read_to_string(source_name)?;
        let mut r = TestDataReader::new(source_name, file.as_str());
        while r.next() {
            // println!("cmd: {}, cmd_args: {:?}", r.data.cmd, r.data.cmd_args);
        }
        Ok(())
    }

    #[test]
    fn test_data() -> Result<()> {
        let source_name = "src/testdata/data.txt";
        let file = fs::read_to_string(source_name)?;
        let mut content = file.lines();
        loop {
            let p = content.next();
            if p.is_some() {
                // println!("{:?}", p);
            } else {
                // println!("NONNN");
                break;
            }
        }
        Ok(())
    }
}
