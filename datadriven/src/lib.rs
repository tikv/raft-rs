/*! # datadriven
**datadriven** is a tool for testing. ported from [cockroachdb/datadriven](https://github.com/cockroachdb/datadriven)

To execute data-driven tests, pass the path of the test file as well as a
function which can interpret and execute whatever commands are present in
the test file. The framework invokes the function, passing it information
about the test case in a TestData struct.

The function must return the actual results of the case, which
run_test() compares with the expected results. If the two are not
equal, the test is marked to fail.

`run_test()` will test all the file in `testdata` folder.
If you put it under `src/`, the path to need to pass is `src/testdata`

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

fn default_logger() -> slog::Logger {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    slog::Logger::root(drain, o!())
}
