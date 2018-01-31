//Copyright 2016 PingCAP, Inc.
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

/*!
 * `raft` has adopted a layered `Error` & `ErrorKind` pattern based on
 * [`Failure`](https://github.com/withoutboats/failure).
 *
 * You may be interested in reading the following documents:
 *
 *  + [Using `Fail`](https://boats.gitlab.io/failure/fail.html)
 *  + [Using the `Error` and `ErrorKind` pattern](https://boats.gitlab.io/failure/error-errorkind.html)
 *
 * Working with this error pattern leverages the `From` trait heavily.
 *
 * ```rust
 * use raft::errors::*;
 *
 * let failed: RaftError = RaftErrorKind::Store.into();
 * assert_eq!(failed.kind(), RaftErrorKind::Store);
 * ```
 *
 * Raft tries to follow a layered approach to errors.
 * You can drill down to find the root cause:
 *
 * ```rust
 * use raft::errors::*;
 *
 * let failed: StorageError = StorageErrorKind::Unavailable
 *     .into();
 * // Then, as we raise it up the call stack:
 * let failed: RaftError = failed.context(RaftErrorKind::Store)
 *     .into();
 * // Finally, handling it we can just get the `Store`:
 * assert_eq!(failed.kind(), RaftErrorKind::Store);
 * // Or we can dig deeper:
 * let cause = failed.root_cause();
 * let failed = cause.downcast_ref::<StorageError>().unwrap();
 * assert_eq!(failed.kind(), StorageErrorKind::Unavailable);
 * ```
 *
 * There is some rationale to this method. It means we are able to optimize error memory footprint
 * quite well.
 *
 * ```rust
 * # use raft::errors::*;
 * # use std::mem::size_of;
 * # use raft::StateRole;
 * assert!(size_of::<Result<(), Error<RaftErrorKind>>>() <= size_of::<Result<(), std::io::Error>>());
 * ```
 */

pub use failure::{Fail, Backtrace, Context};
use std::fmt;

/// An `Error` containing a `RaftErrorKind`.
pub type RaftError = Error<RaftErrorKind>;

/// An `Error` containing a `StorageErrorKind`.
pub type StorageError = Error<StorageErrorKind>;

/// Our generic `Error` type. See `RaftError` and `StorageError`
///
/// Error holds some `*ErrorKind` along with a context.
/// The kind from the `*ErrorKind` type can be discovered.
#[derive(Debug)]
pub struct Error<T>
where T: Fail + fmt::Debug + Clone + Eq + PartialEq + Copy + Sync {
    inner: Box<Context<T>>,
}

impl<T> Fail for Error<T>
where T: Fail + fmt::Debug + Clone + Eq + PartialEq + Copy + Sync {
    fn cause(&self) -> Option<&Fail> {
        self.inner.cause()
    }

    fn backtrace(&self) -> Option<&Backtrace> {
        self.inner.backtrace()
    }
}

impl<T> fmt::Display for Error<T>
where T: Fail + fmt::Debug + Clone + Eq + PartialEq + Copy + Sync {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(&self.inner, f)
    }
}

impl<T> Error<T>
where T: Fail + fmt::Debug + Clone + Eq + PartialEq + Copy + Sync {
    pub fn kind(&self) -> T {
        *self.inner.get_context()
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Debug, Fail)]
pub enum RaftErrorKind {
    #[fail(display = "raft: cannot step raft local message")]
    StepLocalMsg,
    #[fail(display = "raft: cannot step as peer not found")]
    StepPeerNotFound,
    #[fail(display = "config")]
    Config,
    #[fail(display = "store")]
    Store,
    #[fail(display = "codec")]
    Codec,
}

#[derive(Copy, Clone, Eq, PartialEq, Debug, Fail)]
pub enum StorageErrorKind {
    #[fail(display = "storage: log compacted")]
    Compacted,
    #[fail(display = "storage: log unavailable")]
    Unavailable,
    #[fail(display = "storage: snapshot out of date")]
    SnapshotOutOfDate,
    #[fail(display = "storage: snapshot is temporarily unavailable")]
    SnapshotTemporarilyUnavailable,
}

impl From<RaftErrorKind> for RaftError {
    fn from(kind: RaftErrorKind) -> Self {
        Error { inner: Box::new(Context::new(kind)) }
    }
}

impl From<Context<RaftErrorKind>> for RaftError  {
    fn from(inner: Context<RaftErrorKind>) -> Self {
        Error { inner: Box::new(inner) }
    }
}

impl From<StorageErrorKind> for StorageError {
    fn from(kind: StorageErrorKind) -> Self {
        Error { inner: Box::new(Context::new(kind)) }
    }
}

impl From<StorageErrorKind> for RaftError {
    fn from(kind: StorageErrorKind) -> Self {
        Error { inner: Box::new(kind.context(RaftErrorKind::Store)) }
    }
}

impl From<Context<StorageErrorKind>> for StorageError  {
    fn from(inner: Context<StorageErrorKind>) -> Self {
        Error { inner: Box::new(inner) }
    }
}

impl From<StorageError> for RaftError {
    fn from(error: StorageError) -> Self {
        Error { inner: Box::new(error.kind().context(RaftErrorKind::Store)) }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_equal() {
        assert_eq!(
            RaftErrorKind::StepPeerNotFound,
            RaftErrorKind::StepPeerNotFound
        );
        assert_eq!(
            RaftErrorKind::Store,
            RaftErrorKind::Store
        );
        assert_eq!(
            RaftErrorKind::StepLocalMsg,
            RaftErrorKind::StepLocalMsg
        );
        assert_eq!(
            RaftErrorKind::Config,
            RaftErrorKind::Config
        );
        assert_ne!(
            RaftErrorKind::StepPeerNotFound,
            RaftErrorKind::Store
        );
        assert_ne!(
            RaftErrorKind::Codec,
            RaftErrorKind::StepLocalMsg
        );
    }

    #[test]
    fn test_storage_error_equal() {
        assert_eq!(
            StorageErrorKind::Compacted,
            StorageErrorKind::Compacted
        );
        assert_eq!(
            StorageErrorKind::Unavailable,
            StorageErrorKind::Unavailable
        );
        assert_eq!(
            StorageErrorKind::SnapshotOutOfDate,
            StorageErrorKind::SnapshotOutOfDate
        );
        assert_eq!(
            StorageErrorKind::SnapshotTemporarilyUnavailable,
            StorageErrorKind::SnapshotTemporarilyUnavailable
        );
        assert_ne!(
            StorageErrorKind::Compacted,
            StorageErrorKind::Unavailable
        );
        assert_ne!(
            StorageErrorKind::SnapshotTemporarilyUnavailable,
            StorageErrorKind::Unavailable
        );
    }
}
