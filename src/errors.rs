// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

/// A result type that wraps up the raft errors.
pub type Result<T> = std::result::Result<T, Error>;

/// The base error type for raft
#[derive(Debug)]
pub enum Error {
    /// An IO error occurred
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
    Exists(u64, &'static str),
    /// The node does not exist, but should.
    NotExists(u64, &'static str),
    /// The request snapshot is dropped.
    RequestSnapshotDropped,
}

impl PartialEq for Error {
    #[cfg_attr(feature = "cargo-clippy", allow(clippy::match_same_arms))]
    fn eq(&self, other: &Error) -> bool {
        match (self, other) {
            (&Error::StepPeerNotFound, &Error::StepPeerNotFound) => true,
            (&Error::ProposalDropped, &Error::ProposalDropped) => true,
            (&Error::Store(ref e1), &Error::Store(ref e2)) => e1 == e2,
            (&Error::Io(ref e1), &Error::Io(ref e2)) => e1.kind() == e2.kind(),
            (&Error::StepLocalMsg, &Error::StepLocalMsg) => true,
            (&Error::ConfigInvalid(ref e1), &Error::ConfigInvalid(ref e2)) => e1 == e2,
            (&Error::RequestSnapshotDropped, &Error::RequestSnapshotDropped) => true,
            _ => false,
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(err) => Some(err),
            Error::Store(err) => Some(err),
            Error::CodecError(err) => Some(err),
            _ => None,
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Io(err) => write!(f, "{}", err),
            Error::Store(err) => write!(f, "{}", err),
            Error::StepLocalMsg => write!(f, "raft: cannot step raft local message"),
            Error::StepPeerNotFound => write!(f, "raft: cannot step as peer not found"),
            Error::ProposalDropped => write!(f, "raft: proposal dropped"),
            Error::ConfigInvalid(err) => write!(f, "{}", err),
            Error::CodecError(err) => write!(f, "protobuf codec error {}", err),
            Error::Exists(id, set) => {
                write!(f, "The node {} already exists in the {} set.", id, set)
            }
            Error::NotExists(id, set) => write!(f, "The node {} is not in the {} set.", id, set),
            Error::RequestSnapshotDropped => write!(f, "raft: request snapshot dropped"),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::Io(err)
    }
}

impl From<StorageError> for Error {
    fn from(err: StorageError) -> Self {
        Error::Store(err)
    }
}

impl From<protobuf::ProtobufError> for Error {
    fn from(err: protobuf::ProtobufError) -> Self {
        Error::CodecError(err)
    }
}

/// An error with the storage.
#[derive(Debug)]
pub enum StorageError {
    /// The storage was compacted and not accessible
    Compacted,
    /// The log is not available.
    Unavailable,
    /// The snapshot is out of date.
    SnapshotOutOfDate,
    /// The snapshot is being created.
    SnapshotTemporarilyUnavailable,
    /// Some other error occurred.
    Other(Box<dyn std::error::Error + Sync + Send>),
}

impl PartialEq for StorageError {
    #[cfg_attr(feature = "cargo-clippy", allow(clippy::match_same_arms))]
    fn eq(&self, other: &StorageError) -> bool {
        match (self, other) {
            (&StorageError::Compacted, &StorageError::Compacted) => true,
            (&StorageError::Unavailable, &StorageError::Unavailable) => true,
            (&StorageError::SnapshotOutOfDate, &StorageError::SnapshotOutOfDate) => true,
            (
                &StorageError::SnapshotTemporarilyUnavailable,
                &StorageError::SnapshotTemporarilyUnavailable,
            ) => true,
            _ => false,
        }
    }
}

impl std::error::Error for StorageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            StorageError::Other(err) => Some(err.as_ref()),
            _ => None,
        }
    }
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::Compacted => write!(f, "log compacted"),
            StorageError::Unavailable => write!(f, "log unavailable"),
            StorageError::SnapshotOutOfDate => write!(f, "snapshot out of date"),
            StorageError::SnapshotTemporarilyUnavailable => {
                write!(f, "snapshot is temporarily unavailable")
            }
            StorageError::Other(err) => write!(f, "unknown error {}", err),
        }
    }
}

impl From<Box<dyn std::error::Error + Sync + Send>> for StorageError {
    fn from(err: Box<dyn std::error::Error + Sync + Send>) -> Self {
        StorageError::Other(err)
    }
}

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
