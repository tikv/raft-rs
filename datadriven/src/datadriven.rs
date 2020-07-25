use crate::test_data::TestData;
use crate::test_data_reader::TestDataReader;
use anyhow::Result;
use std::fs;
use std::path::{Path, PathBuf};

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
pub fn run_test<F>(path: &str, f: F, logger: &slog::Logger) -> Result<()>
where
    F: FnOnce(&TestData) -> String + Copy,
{
    let entries: Vec<PathBuf> = fs::read_dir(path)?
        .map(|res| res.map(|e| e.path()))
        .collect::<Result<Vec<_>, std::io::Error>>()?;

    for path in entries.iter() {
        let file = fs::read_to_string(path)?;
        run_test_internal(path, file.as_str(), f, logger)?;
    }

    Ok(())
}

fn run_test_internal<F, P>(source_name: P, content: &str, f: F, logger: &slog::Logger) -> Result<()>
where
    F: FnOnce(&TestData) -> String + Copy,
    P: AsRef<Path>,
{
    let mut r = TestDataReader::new(source_name, content);
    while r.next(logger)? {
        run_directive(&r, f)?;
    }
    Ok(())
}

// run_directive runs just one directive in the input.
//
// The stopNow and subTestSkipped booleans are modified by-reference
// instead of returned because the testing module implements t.Skip
// and t.Fatal using panics, and we're not guaranteed to get back to
// the caller via a return in those cases.
fn run_directive<F>(r: &TestDataReader, f: F) -> Result<()>
where
    F: FnOnce(&TestData) -> String,
{
    let d = r.data.clone();
    let actual = f(&d);
    assert_diff!(&actual, &d.expected, "\n", 0);
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::datadriven::run_test;
    use crate::test_data::TestData;
    use anyhow::Result;
    use slog::Drain;

    fn default_logger() -> slog::Logger {
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).build().fuse();
        slog::Logger::root(drain, o!())
    }

    fn fibonacci(n: u32) -> u32 {
        match n {
            0 => 1,
            1 => 1,
            _ => fibonacci(n - 1) + fibonacci(n - 2),
        }
    }

    fn factorial(n: u32) -> u32 {
        match n {
            0 => 1,
            1 => 1,
            _ => factorial(n - 1) * n,
        }
    }

    fn fibonacci_or_factorial_or_sum(d: &TestData) -> String {
        let mut expected = String::new();

        match d.cmd.as_str() {
            "fibonacci" => {
                for arg in d.cmd_args.iter() {
                    assert_eq!(
                        arg.values.len(),
                        1,
                        r#"expected value len is 1, check "{}""#,
                        d.pos
                    );
                    let v = fibonacci(arg.values[0].parse().unwrap());
                    let line = arg.key.clone() + "=" + v.to_string().as_str() + "\n";
                    expected.push_str(&line);
                }
            }
            "factorial" => {
                for arg in d.cmd_args.iter() {
                    assert_eq!(
                        arg.values.len(),
                        1,
                        r#"expected value len is 1, check "{}""#,
                        d.pos
                    );
                    let v = factorial(arg.values[0].parse().unwrap());
                    let line = arg.key.clone() + "=" + v.to_string().as_str() + "\n";
                    expected.push_str(&line);
                }
            }
            "sum" => {
                for arg in d.cmd_args.iter() {
                    if arg.values.is_empty() {
                        let ks: Vec<u32> = arg
                            .key
                            .clone()
                            .split_terminator(',')
                            .map(|v| v.parse::<u32>().unwrap())
                            .collect();
                        let sum: u32 = ks.iter().sum();
                        let line = sum.to_string() + "\n";
                        expected.push_str(&line);
                    } else {
                        let vs: Vec<u32> = arg
                            .values
                            .clone()
                            .into_iter()
                            .map(|v| v.parse::<u32>().unwrap())
                            .collect();
                        let sum: u32 = vs.iter().sum();
                        let line = arg.key.clone() + "=" + sum.to_string().as_str() + "\n";
                        expected.push_str(&line);
                    }
                }
            }
            "max" => {
                for arg in d.cmd_args.iter() {
                    if arg.values.is_empty() {
                        let ks: Vec<u32> = arg
                            .key
                            .split_terminator(',')
                            .map(|v| v.parse::<u32>().unwrap())
                            .collect();
                        let mx = ks.iter().max().unwrap();
                        let res = mx.to_string() + "\n";
                        expected.push_str(&res);
                    } else {
                        let vs: Vec<u32> = arg
                            .values
                            .clone()
                            .into_iter()
                            .map(|v| v.parse::<u32>().unwrap())
                            .collect();
                        let mx = vs.iter().max().unwrap();
                        let res = arg.key.clone() + "=" + mx.to_string().as_str() + "\n";
                        expected.push_str(&res);
                    }
                }
            }
            _ => panic!("unknown command"),
        }
        expected
    }

    fn get_log_message(d: &TestData) -> String {
        match d.cmd.as_str() {
            "debug" => format!("DEBUG {:?}\n", d.cmd_args[0]),
            "info" => format!("Please use debug instead.\n"),
            "warn" => format!("Please use debug instead.\nPlease use debug instead.\n"),
            _ => String::new(),
        }
    }

    #[test]
    fn test_datadriven() -> Result<()> {
        let logger = default_logger();
        run_test(
            "src/testdata/datadriven",
            fibonacci_or_factorial_or_sum,
            &logger,
        )?;
        Ok(())
    }

    #[test]
    fn test_unknwon_data() -> Result<()> {
        let logger = default_logger();
        let e = run_test(
            "src/testdata/unknown_data",
            fibonacci_or_factorial_or_sum,
            &logger,
        );
        assert!(e.is_err());
        Ok(())
    }

    #[test]
    fn test_log_message() {
        let logger = default_logger();
        let e = run_test("src/testdata/log_message", get_log_message, &logger);
        println!("e={:?}", e)
    }
}
