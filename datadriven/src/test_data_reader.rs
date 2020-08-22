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
    pub rewrite_buffer: Option<String>,
}

impl<'a> TestDataReader<'a> {
    pub fn new<P>(source_name: P, content: &'a str, rewrite: bool, logger: &slog::Logger) -> Self
    where
        P: AsRef<Path>,
    {
        Self {
            source_name: source_name.as_ref().to_path_buf(),
            scanner: content.lines().enumerate(),
            data: TestData::default(),
            logger: logger.clone(),
            rewrite_buffer: match rewrite {
                true => Some(String::new()),
                false => None,
            },
        }
    }

    pub fn next(&mut self) -> Result<bool> {
        loop {
            let line = self.scanner.next();
            if line.is_none() {
                return Ok(false);
            }

            let mut pos = line.unwrap().0;
            let line = line.unwrap().1;
            self.emit(line);

            let mut line = line.trim().to_string();

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

                let mut next_line = self
                    .scanner
                    .next()
                    .expect("expect argument ends without '\\'")
                    .1;

                self.emit(next_line);

                next_line = next_line.trim();

                // skip blank line, if not we might get some whitespace at the end,
                // without trimming it, parsing line will fails
                if !next_line.is_empty() {
                    // Add a whitespace for separating arguments
                    line.push(' ');
                    line.push_str(next_line);
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
                    self.emit(line);
                    let line = line.to_string() + "\n";
                    buf.push_str(&line);
                }
            }

            self.data.input = buf.trim().to_string();

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

        let mut allow_blank_lines = false;

        let mut line = "";
        if let Some((_, l)) = self.scanner.next() {
            if l == "----" {
                allow_blank_lines = true;
            }
            line = l;
        } else {
            // In rewrite mode, no lines after first separator
        }

        if allow_blank_lines {
            loop {
                let mut line = self.scanner.next().unwrap().1.to_string();
                if line == "----" {
                    let mut line2 = self.scanner.next().unwrap().1.to_string();
                    if line2 == "----" {
                        // Read the following blank line (if we don't do this, we will emit
                        // an extra blank line when rewriting).
                        if let Some((_, line3)) = self.scanner.next() {
                            // we should assert line3 is empty
                            assert!(
                                line3.is_empty(),
                                "non-blank line after end of double ---- separator section"
                            );
                        } else {
                            // if None, then it if fine.
                        }

                        break;
                    }
                    line += "\n";
                    self.data.expected.push_str(&line);
                    line2 += "\n";
                    self.data.expected.push_str(&line2);
                    continue;
                }
                line += "\n";
                self.data.expected.push_str(&line);
            }
        } else {
            // Terminate on first blank line.
            loop {
                if line.trim().is_empty() {
                    break;
                }
                let l = line.to_string() + "\n";
                self.data.expected.push_str(&l);

                let l = self.scanner.next();
                if l.is_none() {
                    break;
                }
                line = l.unwrap().1;
            }
        }
    }

    pub fn emit(&mut self, str: &str) {
        self.rewrite_buffer.as_mut().map(|rb| {
            let str = str.to_string() + "\n";
            rb.push_str(&str);
            rb
        });
    }
}
