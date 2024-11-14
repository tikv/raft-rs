// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#![cfg_attr(feature = "failpoints", allow(dead_code, unused_imports))]
// We use `default` method a lot to be support prost and rust-protobuf at the
// same time. And reassignment can be optimized by compiler.
#![allow(clippy::field_reassign_with_default)]

/// Get the count of macro's arguments.
///
/// # Examples
///
/// ```
/// # use tikv::count_args;
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
/// # use tikv::map;
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

#[macro_export]
macro_rules! assert_iter_eq {
    (o $lhs:expr, $rhs:expr) => {{
        assert_iter_eq!(internal $lhs.iter(), $rhs.iter().cloned());
    }};
    ($lhs:expr, $rhs:expr) => {{
        assert_iter_eq!(internal $lhs.iter().cloned(), $rhs.iter().cloned());
    }};
    (internal $lhs:expr, $rhs:expr) => {{
        let mut lhs: Vec<_> = $lhs.collect();
        let mut rhs: Vec<_> = $rhs.collect();
        lhs.sort_unstable();
        rhs.sort_unstable();
        assert_eq!(lhs, rhs);
    }};
}

#[cfg(feature = "failpoints")]
mod failpoints_cases;
#[cfg(not(feature = "failpoints"))]
mod integration_cases;
mod test_util;
