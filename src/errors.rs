// Copyright 2016 PingCAP, Inc.
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

use failure::{Fail, Backtrace, Context};
use std::fmt;

pub type RaftError = Error<RaftErrorKind>;
pub type StorageError = Error<StorageErrorKind>;

#[derive(Debug)]
pub struct Error<T>
where T: Fail + fmt::Debug + Clone + Eq + PartialEq + Copy + Sync {
    inner: Context<T>,
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

impl From<RaftErrorKind> for RaftError {
    fn from(kind: RaftErrorKind) -> Self {
        Error { inner: Context::new(kind) }
    }
}

impl From<Context<RaftErrorKind>> for RaftError  {
    fn from(inner: Context<RaftErrorKind>) -> Self {
        Error { inner: inner }
    }
}

impl From<StorageErrorKind> for StorageError {
    fn from(kind: StorageErrorKind) -> Self {
        Error { inner: Context::new(kind) }
    }
}

impl From<StorageErrorKind> for RaftError {
    fn from(kind: StorageErrorKind) -> Self {
        Error { inner: kind.context(RaftErrorKind::Store) }
    }
}

impl From<Context<StorageErrorKind>> for StorageError  {
    fn from(inner: Context<StorageErrorKind>) -> Self {
        Error { inner: inner }
    }
}

impl From<StorageError> for RaftError {
    fn from(error: StorageError) -> Self {
        Error { inner: error.kind().context(RaftErrorKind::Store) }
    }
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
