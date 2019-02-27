// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

#![cfg_attr(not(feature = "cargo-clippy"), allow(unknown_lints))]
#![cfg_attr(feature = "failpoint", allow(dead_code, unused_imports))]

#[macro_use]
extern crate log;

#[cfg(feature = "failpoint")]
#[macro_use]
extern crate lazy_static;
#[cfg(feature = "failpoint")]
extern crate fail;

/// Get the count of macro's arguments.
///
/// # Examples
///
/// ```
/// # #[macro_use] extern crate tikv;
/// # fn main() {
/// assert_eq!(count_args!(), 0);
/// assert_eq!(count_args!(1), 1);
/// assert_eq!(count_args!(1, 2), 2);
/// assert_eq!(count_args!(1, 2, 3), 3);
/// # }
/// ```
#[macro_export]
macro_rules! count_args {
    () => { 0 };
    ($head:expr $(, $tail:expr)*) => { 1 + count_args!($($tail),*) };
}

/// Initial a `HashMap` with specify key-value pairs.
///
/// # Examples
///
/// ```
/// # #[macro_use] extern crate tikv;
/// # fn main() {
/// // empty map
/// let m: tikv::util::collections::HashMap<u8, u8> = map!();
/// assert!(m.is_empty());
///
/// // one initial kv pairs.
/// let m = map!("key" => "value");
/// assert_eq!(m.len(), 1);
/// assert_eq!(m["key"], "value");
///
/// // initialize with multiple kv pairs.
/// let m = map!("key1" => "value1", "key2" => "value2");
/// assert_eq!(m.len(), 2);
/// assert_eq!(m["key1"], "value1");
/// assert_eq!(m["key2"], "value2");
/// # }
/// ```
#[macro_export]
macro_rules! map {
    () => {
        {
            use std::collections::HashMap;
            HashMap::new()
        }
    };
    ( $( $k:expr => $v:expr ),+ ) => {
        {
            use std::collections::HashMap;
            let mut temp_map = HashMap::with_capacity(count_args!($(($k, $v)),+));
            $(
                temp_map.insert($k, $v);
            )+
            temp_map
        }
    };
}

#[cfg(feature = "failpoint")]
mod failpoint_cases;
#[cfg(not(feature = "failpoint"))]
mod integration_cases;
mod test_util;
