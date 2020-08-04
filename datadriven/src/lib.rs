/*!
# datadriven
**datadriven** is a tool for testing. Ported from [cockroachdb/datadriven](https://github.com/cockroachdb/datadriven)

To execute data-driven tests, pass the path of the test file as well as a
function which can interpret and execute whatever commands are present in
the test file. The framework invokes the function, passing it information
about the test case in a TestData struct.

The function must return the actual results of the case, which
run_test() compares with the expected results. If the two are not
equal, the test is marked to fail.

[run_test()](fn.run_test.html) will run the test on a file or given folder

Recommend usage:

for test function `test_func_001`, place the testdata in `src/testdata/test_func_001`, `run_test(src/testdata/test_func_001, func_001)`

for test function `test_func_002`, place the testdata in `src/testdata/test_func_002`, `run_test(src/testdata/test_func_002, func_002)`

or just run a file `run_test(src/testdata/data.txt, func_002)`

and so on.

The path tree looks like the following:
```text
.
├── Cargo.toml
└── src
    ├── datadriven.rs
    ├── lib.rs
    └── testdata
        ├── data.txt
        ├── test_func_001
        │   ├── data_001.txt
        │   └── data_002.txt
        └── test_func_002
            ├── data_001.txt
            └── data_002.txt
```

The comparision is done by [difference](https://docs.rs/difference/2.0.0/difference/)

The difference between [cockroachdb/datadriven](https://github.com/cockroachdb/datadriven)
1. no rewrite
2. no subtest

*/

#![deny(missing_docs)]

#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate anyhow;
#[macro_use(assert_diff)]
extern crate difference;
#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;

mod datadriven;
mod line_sparser;
mod test_data;
mod test_data_reader;

pub use self::datadriven::run_test;
pub use self::test_data::CmdArg;
pub use self::test_data::TestData;
use slog::Drain;

#[allow(dead_code)]
fn default_logger() -> slog::Logger {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    slog::Logger::root(drain, o!())
}
