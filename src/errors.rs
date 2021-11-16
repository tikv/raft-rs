// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.
use thiserror::Error;

/// The base error type for raft
#[derive(Debug, Error)]
pub enum Error {
    /// An IO error occurred
    #[error("{0}")]
    Io(#[from] std::io::Error),
    /// A storage error occurred.
    #[error("{0}")]
    Store(#[from] StorageError),
    /// Raft cannot step the local message.
    #[error("raft: cannot step raft local message")]
    StepLocalMsg,
    /// The raft peer is not found and thus cannot step.
    #[error("raft: cannot step as peer not found")]
    StepPeerNotFound,
    /// The proposal of changes was dropped.
    #[error("raft: proposal dropped")]
    ProposalDropped,
    /// The configuration is invalid.
    #[error("{0}")]
    ConfigInvalid(String),
    /// A protobuf message codec failed in some manner.
    #[error("protobuf codec error {0:?}")]
    CodecError(#[from] protobuf::ProtobufError),
    /// The node exists, but should not.
    #[error("The node {id} already exists in the {set} set.")]
    Exists {
        /// The node id.
        id: u64,
        /// The node set.
        set: &'static str,
    },
    /// The node does not exist, but should.
    #[error("The node {id} is not in the {set} set.")]
    NotExists {
        /// The node id.
        id: u64,
        /// The node set.
        set: &'static str,
    },
    /// ConfChange proposal is invalid.
    #[error("{0}")]
    ConfChangeError(String),
    /// The request snapshot is dropped.
    #[error("raft: request snapshot dropped")]
    RequestSnapshotDropped,
}

impl PartialEq for Error {
    #[cfg_attr(feature = "cargo-clippy", allow(clippy::match_same_arms))]
    fn eq(&self, other: &Error) -> bool {
        match (self, other) {
            (Error::StepPeerNotFound, Error::StepPeerNotFound) => true,
            (Error::ProposalDropped, Error::ProposalDropped) => true,
            (Error::Store(ref e1), Error::Store(ref e2)) => e1 == e2,
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
#[derive(Debug, Error)]
pub enum StorageError {
    /// The storage was compacted and not accessible
    #[error("log compacted")]
    Compacted,
    /// The log is not available.
    #[error("log unavailable")]
    Unavailable,
    /// The log is being fetched.
    #[error("log is temporarily unavailable")]
    LogTemporarilyUnavailable,
    /// The snapshot is out of date.
    #[error("snapshot out of date")]
    SnapshotOutOfDate,
    /// The snapshot is being created.
    #[error("snapshot is temporarily unavailable")]
    SnapshotTemporarilyUnavailable,
    /// Some other error occurred.
    #[error("unknown error {0}")]
    Other(#[from] Box<dyn std::error::Error + Sync + Send>),
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
pub type Result<T> = std::result::Result<T, Error>;

#[allow(clippy::eq_op)]
#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    #[test]
    fn test_error_equal() {
        assert_eq!(Error::StepPeerNotFound, Error::StepPeerNotFound);
        assert_eq!(
            Error::Store(StorageError::Compacted),
            Error::Store(StorageError::Compacted)
        );
        assert_eq!(
            Error::Io(io::Error::new(io::ErrorKind::UnexpectedEof, "oh no!")),
            Error::Io(io::Error::new(io::ErrorKind::UnexpectedEof, "oh yes!"))
        );
        assert_ne!(
            Error::Io(io::Error::new(io::ErrorKind::NotFound, "error")),
            Error::Io(io::Error::new(io::ErrorKind::BrokenPipe, "error"))
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
        assert_eq!(
            Error::from(io::Error::new(io::ErrorKind::Other, "oh no!")),
            Error::from(io::Error::new(io::ErrorKind::Other, "oh yes!"))
        );
        assert_ne!(
            Error::StepPeerNotFound,
            Error::Store(StorageError::Compacted)
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
