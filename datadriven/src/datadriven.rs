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
pub fn run_test<F>(path: &str, f: F, logger: &slog::Logger) -> Result<()>
where
    F: FnOnce(&TestData) -> String + Copy,
{
    match fs::read_dir(path) {
        Ok(read_dir) => {
            let entries: Vec<PathBuf> = read_dir
                .map(|res| res.map(|e| e.path()))
                .collect::<Result<Vec<_>, std::io::Error>>()?;

            for path in entries.iter() {
                let file = fs::read_to_string(path)?;
                run_test_internal(path, &file, f, logger)?;
            }
        }
        _ => {
            let file = fs::read_to_string(path)?;
            run_test_internal(path, &file, f, logger)?;
        }
    }

    Ok(())
}

fn run_test_internal<F, P>(source_name: P, content: &str, f: F, logger: &slog::Logger) -> Result<()>
where
    F: FnOnce(&TestData) -> String + Copy,
    P: AsRef<Path>,
{
    let mut r = TestDataReader::new(source_name, content, logger);
    while r.next()? {
        run_directive(&r, f)?;
    }
    Ok(())
}

// run_directive runs just one directive in the input.
//
fn run_directive<F>(r: &TestDataReader, f: F) -> Result<()>
where
    F: FnOnce(&TestData) -> String,
{
    let actual = f(&r.data);

    assert_diff!(&actual, &r.data.expected, "\n", 0);

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::datadriven::run_test;
    use crate::default_logger;
    use crate::test_data::TestData;
    use anyhow::Result;
    use std::cmp;

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
                        arg.vals.len(),
                        1,
                        r#"expected value len is 1, check "{}""#,
                        d.pos
                    );
                    // value must exists
                    assert!(!arg.vals[0].is_empty());

                    let v = fibonacci(arg.vals[0].parse().unwrap());
                    let line = arg.key.clone() + "=" + v.to_string().as_str() + "\n";
                    expected.push_str(&line);
                }
            }
            "factorial" => {
                for arg in d.cmd_args.iter() {
                    assert_eq!(
                        arg.vals.len(),
                        1,
                        r#"expected value len is 1, check "{}""#,
                        d.pos
                    );
                    // value must exists
                    assert!(!arg.vals[0].is_empty());
                    let v = factorial(arg.vals[0].parse().unwrap());
                    let line = arg.key.clone() + "=" + v.to_string().as_str() + "\n";
                    expected.push_str(&line);
                }
            }
            "sum" => {
                for arg in &d.cmd_args {
                    if arg.vals.is_empty() {
                        // if no value, assume is 0
                        let res = arg.key.clone() + "=0\n";
                        expected.push_str(&res);
                    } else {
                        let mut sum = 0;
                        for val in &arg.vals {
                            if val.is_empty() {
                                continue;
                            }
                            let vs = val.split(',').collect::<Vec<&str>>();
                            let vs = vs
                                .into_iter()
                                .map(|v| {
                                    v.parse::<u32>().unwrap_or_else(|_| {
                                        panic!(
                                            "value: {:?} can't parse, check {}",
                                            arg.vals.clone(),
                                            d.pos
                                        )
                                    })
                                })
                                .collect::<Vec<u32>>();

                            let vs = vs.into_iter().sum::<u32>();
                            sum += vs;
                        }
                        let line = arg.key.clone() + "=" + sum.to_string().as_str() + "\n";
                        expected.push_str(&line);
                    }
                }
            }
            "max" => {
                for arg in d.cmd_args.iter() {
                    if arg.vals.is_empty() {
                        // if no value, assume is 0
                        let res = arg.key.clone() + "=0\n";
                        expected.push_str(&res);
                    } else {
                        let mut max = 0;
                        for val in &arg.vals {
                            if val.is_empty() {
                                continue;
                            }
                            let vs = val.split(',').collect::<Vec<&str>>();
                            let vs = vs
                                .into_iter()
                                .map(|v| {
                                    v.parse::<u32>().unwrap_or_else(|_| {
                                        panic!(
                                            "value: {:?} can't parse, check {}",
                                            arg.vals.clone(),
                                            d.pos
                                        )
                                    })
                                })
                                .collect::<Vec<u32>>();
                            let vs = vs
                                .into_iter()
                                .max()
                                .expect("Vec is empty, this should not happen.");
                            max = cmp::max(max, vs);
                        }
                        let line = arg.key.clone() + "=" + max.to_string().as_str() + "\n";
                        expected.push_str(&line);
                    }
                }
            }
            "do_nothing" => {
                // this is for testing
            }
            "repeat_me" => {
                for arg in &d.cmd_args {
                    if arg.vals.is_empty() {
                        let res = arg.key.clone() + "=\n";
                        expected.push_str(&res);
                    } else {
                        let mut res = arg.key.clone() + "=";
                        for v in &arg.vals {
                            res += v;
                        }
                        res += "\n";
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
        let logger = default_logger();
        run_test(
            "src/testdata/datadriven",
            fibonacci_or_factorial_or_sum,
            &logger,
        )?;
        Ok(())
    }

    #[test]
    fn test_unknown_data() -> Result<()> {
        let logger = default_logger();
        let e = run_test(
            "src/testdata/unknown_data_1.txt",
            fibonacci_or_factorial_or_sum,
            &logger,
        );
        assert!(e.is_err());
        let e = run_test(
            "src/testdata/unknown_data_2.txt",
            fibonacci_or_factorial_or_sum,
            &logger,
        );
        assert!(e.is_err());
        Ok(())
    }
}
