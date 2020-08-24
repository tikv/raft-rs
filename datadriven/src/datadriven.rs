#[cfg(test)]
mod datadriven_test;

use crate::get_dirs_or_file;
use crate::test_data::TestData;
use crate::test_data_reader::TestDataReader;
use anyhow::Result;
use regex::Regex;
use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

/// The main function to run tests
///
/// You need to pass the path of `testdata` where store the test cases, and your function
/// to output expected result.
///
/// It will be compared with crate `difference`
///
/// It invokes a data-driven test. The test cases are contained in a
/// separate test file and are dynamically loaded, parsed, and executed by this
/// testing framework. By convention, test files are typically located in a
/// sub-directory called `testdata`. Each test file has the following format:
///
/// ```txt
/// <command> [arg | arg=val | arg=(val1, val2, ...)]... \
/// <more args> \
/// <more args>
/// ----
/// <expected results>
/// <blank line>
/// ````
///
/// The command input can contain blank lines. However, by default, the expected
/// results cannot contain blank lines. This alternate syntax allows the use of
/// blank lines:
/// ```txt
/// <command> [arg | arg=val | arg=(val1, val2, ...)]... \
/// <more args> \
/// <more args>
/// ----
/// ----
/// <expected results>
/// <optional blank line>
/// <more expected results>
/// ----
/// ----
/// <blank line>
/// ````
///
/// Data store in `TestData`
///
/// `<command>` => `String`: `cmd`
///
/// `<args>` => `Vec<CmdArg>`: `cmd_args`
///
///  see more detail for [CmdArg](struct.CmdArg.html)
///
/// `F` is customize function: `FnOnce(&TestData) -> String`
///
/// you will get input as type `TestData` and your expected output as type `String`
///
/// Note: you need `end line` after each line if you have multiple lines.
/// This is used to separate them, instead of mixing the characters altogether.
///
/// ```rust
/// use datadriven::{TestData, CmdArg};
///
/// fn func(d: &TestData) -> String {
///     let args : Vec<CmdArg> = d.cmd_args.clone();
///     let cmd : String = d.cmd.clone();
///
///     // DO SOMETHINGS
///     // NOTE: You need to add end line, if needed
///
///     String::from("YOUR EXPECTED OUTPUT")
/// }
/// ```
///
/// Rewrite Mode:
///
/// When the testdata is changed, you can run the rewrite mode directly to get the new testdata.
/// If there are no blank line in expected value, it will be overwritten with one separator,
/// instead of double separator, vice versa.
///
pub fn run_test<F>(path: &str, f: F, rewrite: bool, logger: &slog::Logger) -> Result<()>
where
    F: FnOnce(&TestData) -> String + Copy,
{
    let files = get_dirs_or_file(path)?;

    for path in &files {
        let content = fs::read_to_string(path)?;
        if let Some(rewrite_data) = run_test_internal(path, &content, f, rewrite, logger)? {
            let mut file = OpenOptions::new().write(true).truncate(true).open(path)?;
            file.write_all(rewrite_data.as_bytes())?;
            file.sync_data()?;
        } else {
            // test mode, skip rewriting data
        }
    }

    Ok(())
}

fn run_test_internal<F, P>(
    source_name: P,
    content: &str,
    f: F,
    rewrite: bool,
    logger: &slog::Logger,
) -> Result<Option<String>>
where
    F: FnOnce(&TestData) -> String + Copy,
    P: AsRef<Path>,
{
    let mut r = TestDataReader::new(source_name, content, rewrite, logger);

    while r.next()? {
        run_directive(&mut r, f)?;
    }

    // remove redundant '\n'
    let data = r.rewrite_buffer.map(|mut rb| {
        if rb.ends_with("\n\n") {
            rb.pop();
        }
        rb
    });
    debug!(logger, "rewrite_buffer: {:?}", data);
    Ok(data)
}

// run_directive runs just one directive in the input.
//
fn run_directive<F>(r: &mut TestDataReader, f: F) -> Result<()>
where
    F: FnOnce(&TestData) -> String,
{
    let mut actual = f(&r.data);

    if !actual.is_empty() && !actual.ends_with('\n') {
        actual += "\n";
    }

    // test mode
    if r.rewrite_buffer == None {
        assert_diff!(&actual, &r.data.expected, "\n", 0);
    } else {
        r.emit("----");
        if has_blank_line(&actual) {
            r.emit("----");

            r.rewrite_buffer.as_mut().map(|rb| {
                rb.push_str(&actual);
                rb
            });

            r.emit("----");
            r.emit("----");
            r.emit("");
        } else {
            // Here actual already ends in \n so emit adds a blank line.
            r.emit(&actual);
        }
    }

    Ok(())
}

lazy_static! {
    // Multi-line mode means ^ and $ no longer match just at the
    // beginning/end of the input, but at the beginning/end of lines
    // from regex.doc
    static ref BLANK_LINE_RE: Regex = Regex::new(r"(?m)^[\t ]*\n").unwrap();
}

fn has_blank_line(str: &str) -> bool {
    BLANK_LINE_RE.captures(str).is_some()
}
