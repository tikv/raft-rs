use crate::test_data::TestData;
use crate::test_data_reader::TestDataReader;
use anyhow::Result;
use std::fs;
use std::path::Path;

pub fn run_test<F>(path: &str, f: F) -> Result<()>
where
    F: FnOnce(&TestData) -> String + Copy,
{
    let file = fs::read_to_string(path)?;
    debug!("file: {}", file);
    run_test_internal(path, file.as_str(), f);
    Ok(())
}

fn run_test_internal<F>(source_name: &str, content: &str, f: F)
where
    F: FnOnce(&TestData) -> String + Copy,
{
    let mut r = TestDataReader::new(source_name, content);
    while r.next() {
        run_directive_or_sub_test(&r, "", f);
    }
}

// run_directive_or_sub_test runs either a "subtest" directive or an
// actual test directive. The "mandatory_sub_test_prefix" argument indicates
// a mandatory prefix required from all sub-test names at this point.
fn run_directive_or_sub_test<F>(
    r: &TestDataReader,
    mandatory_sub_test_prefix: &str,
    f: F,
) -> Result<()>
where
    F: FnOnce(&TestData) -> String,
{
    let (subtest_name, has_subtest) =
        is_sub_test_start(r, mandatory_sub_test_prefix).expect("should not fails");
    debug!("has_subtest: {}", has_subtest);
    if has_subtest {
        run_sub_test(subtest_name.as_str(), r, f);
    } else {
        run_directive(r, f);
    }
    Ok(())
}

// run_sub_test runs a subtest up to and including the final `subtest
// end`. The opening `subtest` directive has been consumed already.
// The first parameter `sub_test_name` is the full path to the subtest,
// including the parent subtest names as prefix. This is used to
// validate the nesting and thus prevent mistakes.
fn run_sub_test<F>(sub_test_name: &str, r: &TestDataReader, f: F) -> Result<()>
where
    F: FnOnce(&TestData) -> String,
{
    // Remember the current reader position in case we need to spell out
    // an error message below.
    let subtest_start_pos = r.get_data().pos;

    // seen_sub_test_end is used below to verify that a "subtest end" directive
    // has been detected (as opposed to EOF).
    let seen_sub_test_end = false;

    // seen_skip is used below to verify that "Skip" has not been used
    // inside a subtest. See below for details.
    let seen_skip = false;

    // The name passed to t.Run is the last component in the subtest
    // name, because all components before that are already prefixed by
    // t.Run from the names of the parent sub-tests.
    let testing_subname = sub_test_name;
    debug!("testing_subname: {}", testing_subname);
    Ok(())
    // testingSubTestName := subTestName[strings.LastIndex(subTestName, "/")+1:]
}

fn is_sub_test_start(
    r: &TestDataReader,
    mandatory_sub_test_prefix: &str,
) -> Result<(String, bool)> {
    let data = r.get_data();
    if data.cmd != "subtest" {
        return Ok((String::new(), false));
    }
    if data.cmd_args.len() != 1 {
        panic!("invalid syntax for subtest")
    }
    let subtest_name = data.cmd_args[0].key();
    if subtest_name == "end" {
        panic!("subtest end without corresponding start")
    }
    if !subtest_name.starts_with(mandatory_sub_test_prefix) {
        panic!(
            "name of nested subtest must begin with {:?}",
            mandatory_sub_test_prefix
        )
    }
    Ok((subtest_name, true))
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
    debug!("actual: {}", actual);
    debug!("expected: {:?}", d.expected);
    // if actual != "" && !actual.ends_with('\n') {
    //     actual.push('\n');
    // }

    if actual != d.expected {
        panic!("difference output")
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::datadriven::run_test;
    use crate::test_data::TestData;
    use crate::test_data_reader::TestDataReader;
    use anyhow::Result;
    use std::fs;

    fn init() {
        env_logger::try_init();
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
                    let k = arg.key();
                    let v = arg.values();
                    assert_eq!(v.len(), 1);
                    let v = fibonacci(v[0].parse().unwrap());
                    expected.push_str((k + "=" + v.to_string().as_str()).as_str());
                }
            }
            "factorial" => {
                for arg in d.cmd_args.iter() {
                    let k = arg.key();
                    let v = arg.values();
                    assert_eq!(v.len(), 1);
                    let v = factorial(v[0].parse().unwrap());
                    expected.push_str((k + "=" + v.to_string().as_str()).as_str());
                }
            }
            "sum" => {
                let mut val = 0;
                for arg in d.cmd_args.iter() {
                    let ks: Vec<String> = arg
                        .key()
                        .split_terminator(',')
                        .map(|s| s.to_string())
                        .collect();
                    let v: u32 = ks.iter().map(|k| k.parse::<u32>().unwrap()).sum();
                    val += v;
                }
                expected.push_str(val.to_string().as_str());
            }
            _ => panic!("unknown command"),
        }

        debug!("e: {:?}", expected);
        expected
    }

    #[test]
    fn test_data() -> Result<()> {
        init();
        let source_name = "src/testdata/data.txt";
        run_test(source_name, fibonacci_or_factorial_or_sum);
        Ok(())
    }
}
