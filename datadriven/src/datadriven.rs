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
/// It will be compare with crate `difference`
///
/// It invokes a data-driven test. The test cases are contained in a
/// separate test file and are dynamically loaded, parsed, and executed by this
/// testing framework. By convention, test files are typically located in a
/// sub-directory called "testdata". Each test file has the following format:
///
/// ```txt
/// <command> [arg | arg=val | arg=(val1, val2, ...)]...
/// <input to the command>
/// ----
/// <expected results>
/// ````
///
/// The command input can contain blank lines. However, by default, the expected
/// results cannot contain blank lines. This alternate syntax allows the use of
/// blank lines:
/// ```txt
/// <command> [arg | arg=val | arg=(val1, val2, ...)]...
/// <input to the command>
/// ----
/// ----
/// <expected results>
///
/// <more expected results>
/// ----
/// ----
/// ````
/// Note: blank line is required after the second *`----`*.
pub fn run_test<F>(path: &str, f: F) -> Result<()>
where
    F: FnOnce(&TestData) -> String + Copy,
{
    let entries: Vec<PathBuf> = fs::read_dir(path)?
        .map(|res| res.map(|e| e.path()))
        .collect::<Result<Vec<_>, std::io::Error>>()?;

    for path in entries.iter() {
        let file = fs::read_to_string(path)?;
        run_test_internal(path, file.as_str(), f)?;
    }

    Ok(())
}

fn run_test_internal<F, P>(source_name: P, content: &str, f: F) -> Result<()>
where
    F: FnOnce(&TestData) -> String + Copy,
    P: AsRef<Path>,
{
    let mut r = TestDataReader::new(source_name, content);
    while r.next()? {
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
    let d = r.get_data();
    let actual = f(&d);
    debug!("actual: {:?}", actual);
    debug!("expected: {:?}", d.expected);

    assert_diff!(&actual, &d.expected, "\n", 0);

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::datadriven::run_test;
    use crate::test_data::TestData;
    use anyhow::Result;

    fn init() -> Result<()> {
        // TODO(accelsao): is there any way to init once instead of inserting to every test?
        let _ = env_logger::builder().is_test(true).try_init();
        Ok(())
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
        debug!("cmd: {:?}, cmd_args: {:?}", d.cmd, d.cmd_args);
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
                // let mut val = 0;
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

    #[test]
    fn test_datadriven() -> Result<()> {
        init()?;
        run_test("src/testdata/datadriven", fibonacci_or_factorial_or_sum)?;
        Ok(())
    }

    #[test]
    // #[should_panic]
    fn test_unknwon_data() -> Result<()> {
        init()?;
        let e = run_test("src/testdata/unknown_data", fibonacci_or_factorial_or_sum);
        assert!(e.is_err());
        Ok(())
    }
}
