use crate::datadriven::{has_blank_line, run_test, run_test_from_string, run_test_internal};
use crate::line_sparser::parse_line;
use crate::test_data::TestData;
use crate::{default_logger, get_dirs_or_file};
use anyhow::Result;
use std::cmp;
use std::fs::{read_to_string, OpenOptions};
use std::io::Write;

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
fn test_new_line_between_directives() -> Result<()> {
    let logger = default_logger();

    let str = r#"
# Some testing of sensitivity to newlines
foo
----
unknown command

bar
----
unknown command





bar
----
unknown command
"#;

    debug!(logger, "str: {:?}", str);

    fn test(d: &TestData) -> String {
        if d.input != "sentnce" {
            "unknown command".to_string()
        } else {
            "".to_string()
        }
    }
    run_test_from_string(str, test, &logger)
}

#[test]
fn test_parse_line() -> Result<()> {
    let logger = default_logger();

    let str = r#"
parse
xx +++
----
here: cannot parse directive at column 4: xx +++

parse
xx a=b a=c
----
"xx" [a=b, a=c]

parse
xx a=b b=c c=(1,2,3)
----
"xx" [a=b, b=c, c=(1, 2, 3)]
"#;

    debug!(logger, "str: {:?}", str);

    fn test(d: &TestData) -> String {
        let logger = default_logger();
        match parse_line(&d.input, &logger) {
            Ok((cmd, args)) => format!("{:?} {:?}", cmd, args),
            Err(e) => format!("here: {}", e),
        }
    }
    run_test_from_string(str, test, &logger)
}

#[test]
fn test_multi_line() -> Result<()> {
    let logger = default_logger();
    let rewrite = false;

    fn test(d: &TestData) -> String {
        match d.cmd.as_str() {
            "small" => r#"just
two lines of output
"#
            .to_string(),
            "large" => r#"more
than
five
lines
of

output"#
                .to_string(),
            _ => panic!("unknown directive: {}", d.cmd),
        }
    }

    run_test("src/testdata/multiline", test, rewrite, &logger)
}

// Different from cockroachdb/datadriven
// 1. create filename-before, read as string,
// 2. pass to `run_test_from_string` with rewrite mode to filename-after,
// 3. test filename-after
#[test]
fn test_rewrite() -> Result<()> {
    // set true if you want to rewrite 'rewrite test'
    let rewrite_testfiles = false;
    // assure we set the correct value while testing
    assert_eq!(rewrite_testfiles, false);

    let logger = default_logger();

    let path = "src/testdata/rewrite";

    let files = get_dirs_or_file(path).unwrap();

    let files = files
        .into_iter()
        .filter(|f| f.to_str().unwrap().ends_with("-before"))
        .map(|f| f.to_str().unwrap().trim_end_matches("-before").to_string())
        .collect::<Vec<String>>();

    fn handle(d: &TestData) -> String {
        match d.cmd.as_str() {
            "noop" => d.input.clone(),
            "duplicate" => d.input.clone() + "\n" + d.input.as_str(),
            "duplicate-with-blank" => d.input.clone() + "\n\n" + d.input.as_str(),
            "no-output" => "".to_string(),
            _ => panic!("unknown directive: {}", d.cmd),
        }
    }

    debug!(logger, "files: {:?}", files);

    for file in &files {
        let content = read_to_string(file.to_owned() + "-before")?;

        if let Some(rewrite_data) =
            run_test_internal(path, &content, handle, /*rewrite*/ true, &logger)?
        {
            if rewrite_testfiles {
                let mut after_path = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .open(file.to_owned() + "-after")?;
                after_path.write_all(rewrite_data.as_bytes())?;
            } else {
                // replace '\r' for passing CI on windows
                let content = read_to_string(file.to_owned() + "-after")?.replace('\r', "");
                assert_diff!(&rewrite_data, &content, "\n", 0);
            }
        }
    }

    Ok(())
}

#[test]
fn test_datadriven() -> Result<()> {
    let logger = default_logger();
    let rewrite = false;

    run_test(
        "src/testdata/datadriven",
        fibonacci_or_factorial_or_sum,
        rewrite,
        &logger,
    )?;
    Ok(())
}

#[test]
fn test_unknown_data() -> Result<()> {
    let logger = default_logger();
    let rewrite = false;

    let e = run_test(
        "src/testdata/unknown_data_1.txt",
        fibonacci_or_factorial_or_sum,
        rewrite,
        &logger,
    );
    assert!(e.is_err());
    let e = run_test(
        "src/testdata/unknown_data_2.txt",
        fibonacci_or_factorial_or_sum,
        rewrite,
        &logger,
    );
    assert!(e.is_err());
    Ok(())
}

#[test]
fn test_blank_line() {
    let str = "123\n  123\n";
    assert_eq!(has_blank_line(str), false);
    let str = "123\n  \n123\n";
    assert_eq!(has_blank_line(str), true);
    let str = "123\n\n123\n";
    assert_eq!(has_blank_line(str), true);
    let str = "\n";
    assert_eq!(has_blank_line(str), true);
    let str = "";
    assert_eq!(has_blank_line(str), false);
}
