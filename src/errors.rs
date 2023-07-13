// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::StdError;
use alloc::boxed::Box;
use alloc::string::String;
use core::fmt;

/// The base error type for raft
#[derive(Debug)]
pub enum Error {
    /// An IO error occurred
    #[cfg(feature = "std")]
    Io(std::io::Error),
    /// A storage error occurred.
    Store(StorageError),
    /// Raft cannot step the local message.
    StepLocalMsg,
    /// The raft peer is not found and thus cannot step.
    StepPeerNotFound,
    /// The proposal of changes was dropped.
    ProposalDropped,
    /// The configuration is invalid.
    ConfigInvalid(String),
    /// A protobuf message codec failed in some manner.
    CodecError(protobuf::ProtobufError),
    /// The node exists, but should not.
    Exists {
        /// The node id.
        id: u64,
        /// The node set.
        set: &'static str,
    },
    /// The node does not exist, but should.
    NotExists {
        /// The node id.
        id: u64,
        /// The node set.
        set: &'static str,
    },
    /// ConfChange proposal is invalid.
    ConfChangeError(String),
    /// The request snapshot is dropped.
    RequestSnapshotDropped,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            #[cfg(feature = "std")]
            Error::Io(ref e) => write!(f, "{}", e),
            Error::Store(ref e) => write!(f, "{}", e),
            Error::StepLocalMsg => write!(f, "raft: cannot step raft local message"),
            Error::StepPeerNotFound => write!(f, "raft: cannot step as peer not found"),
            Error::ProposalDropped => write!(f, "raft: proposal dropped"),
            Error::ConfigInvalid(ref m) => write!(f, "{}", m),
            Error::CodecError(ref e) => write!(f, "protobuf codec error {0:?}", e),
            Error::Exists { id, set } => {
                write!(f, "The node {id} already exists in the {set} set.")
            }
            Error::NotExists { id, set } => {
                write!(f, "The node {id} is not in the {set} set.")
            }
            Error::ConfChangeError(ref m) => write!(f, "{}", m),
            Error::RequestSnapshotDropped => write!(f, "raft: request snapshot dropped"),
        }
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match *self {
            #[cfg(feature = "std")]
            Error::Io(ref e) => Some(e),
            Error::Store(ref e) => Some(e),
            Error::CodecError(ref e) => Some(e),
            _ => None,
        }
    }
}

#[cfg(feature = "std")]
impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error {
        Error::Io(err)
    }
}

impl From<StorageError> for Error {
    fn from(err: StorageError) -> Error {
        Error::Store(err)
    }
}

impl From<protobuf::ProtobufError> for Error {
    fn from(err: protobuf::ProtobufError) -> Error {
        Error::CodecError(err)
    }
}

impl PartialEq for Error {
    #[cfg_attr(feature = "cargo-clippy", allow(clippy::match_same_arms))]
    fn eq(&self, other: &Error) -> bool {
        match (self, other) {
            (Error::StepPeerNotFound, Error::StepPeerNotFound) => true,
            (Error::ProposalDropped, Error::ProposalDropped) => true,
            (Error::Store(ref e1), Error::Store(ref e2)) => e1 == e2,
            #[cfg(feature = "std")]
            (Error::Io(ref e1), Error::Io(ref e2)) => e1.kind() == e2.kind(),
            (Error::StepLocalMsg, Error::StepLocalMsg) => true,
            (Error::ConfigInvalid(ref e1), Error::ConfigInvalid(ref e2)) => e1 == e2,
            (Error::RequestSnapshotDropped, Error::RequestSnapshotDropped) => true,
            (Error::ConfChangeError(e1), Error::ConfChangeError(e2)) => e1 == e2,
            _ => false,
        }
    }
}

/// An error with the storage.
#[derive(Debug)]
pub enum StorageError {
    /// The storage was compacted and not accessible
    Compacted,
    /// The log is not available.
    Unavailable,
    /// The log is being fetched.
    LogTemporarilyUnavailable,
    /// The snapshot is out of date.
    SnapshotOutOfDate,
    /// The snapshot is being created.
    SnapshotTemporarilyUnavailable,
    /// Some other error occurred.
    Other(Box<dyn StdError + Sync + Send>),
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            StorageError::Compacted => write!(f, "log compacted"),
            StorageError::Unavailable => write!(f, "log unavailable"),
            StorageError::LogTemporarilyUnavailable => write!(f, "log is temporarily unavailable"),
            StorageError::SnapshotOutOfDate => write!(f, "snapshot out of date"),
            StorageError::SnapshotTemporarilyUnavailable => {
                write!(f, "snapshot is temporarily unavailable")
            }
            StorageError::Other(ref e) => write!(f, "unknown error {}", e),
        }
    }
}

impl StdError for StorageError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        None
    }
}

impl PartialEq for StorageError {
    #[cfg_attr(feature = "cargo-clippy", allow(clippy::match_same_arms))]
    fn eq(&self, other: &StorageError) -> bool {
        matches!(
            (self, other),
            (StorageError::Compacted, StorageError::Compacted)
                | (StorageError::Unavailable, StorageError::Unavailable)
                | (
                    StorageError::LogTemporarilyUnavailable,
                    StorageError::LogTemporarilyUnavailable
                )
                | (
                    StorageError::SnapshotOutOfDate,
                    StorageError::SnapshotOutOfDate
                )
                | (
                    StorageError::SnapshotTemporarilyUnavailable,
                    StorageError::SnapshotTemporarilyUnavailable,
                )
        )
    }
}

/// A result type that wraps up the raft errors.
pub type Result<T> = core::result::Result<T, Error>;

#[allow(clippy::eq_op)]
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_equal() {
        assert_eq!(Error::StepPeerNotFound, Error::StepPeerNotFound);
        assert_eq!(
            Error::Store(StorageError::Compacted),
            Error::Store(StorageError::Compacted)
        );
        assert_eq!(Error::StepLocalMsg, Error::StepLocalMsg);
        assert_eq!(
            Error::ConfigInvalid(String::from("config error")),
            Error::ConfigInvalid(String::from("config error"))
        );
        assert_ne!(
            Error::ConfigInvalid(String::from("config error")),
            Error::ConfigInvalid(String::from("other error"))
        );
        assert_ne!(
            Error::StepPeerNotFound,
            Error::Store(StorageError::Compacted)
        );
    }

    #[cfg(feature = "std")]
    #[test]
    fn test_error_equal_std() {
        use std::io;

        assert_eq!(
            Error::Io(io::Error::new(io::ErrorKind::UnexpectedEof, "oh no!")),
            Error::Io(io::Error::new(io::ErrorKind::UnexpectedEof, "oh yes!"))
        );
        assert_ne!(
            Error::Io(io::Error::new(io::ErrorKind::NotFound, "error")),
            Error::Io(io::Error::new(io::ErrorKind::BrokenPipe, "error"))
        );
        assert_eq!(
            Error::from(io::Error::new(io::ErrorKind::Other, "oh no!")),
            Error::from(io::Error::new(io::ErrorKind::Other, "oh yes!"))
        );
    }

    #[test]
    fn test_storage_error_equal() {
        assert_eq!(StorageError::Compacted, StorageError::Compacted);
        assert_eq!(StorageError::Unavailable, StorageError::Unavailable);
        assert_eq!(
            StorageError::SnapshotOutOfDate,
            StorageError::SnapshotOutOfDate
        );
        assert_eq!(
            StorageError::SnapshotTemporarilyUnavailable,
            StorageError::SnapshotTemporarilyUnavailable
        );
        assert_ne!(StorageError::Compacted, StorageError::Unavailable);
        assert_ne!(
            StorageError::Other(Box::new(StorageError::Unavailable)),
            StorageError::Unavailable
        );
    }
}
