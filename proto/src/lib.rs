// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

// We use `default` method a lot to be support prost and rust-protobuf at the
// same time. And reassignment can be optimized by compiler.
#![allow(clippy::field_reassign_with_default)]
#![allow(mismatched_lifetime_syntaxes)]
#![allow(unused_parens)]

mod confchange;
mod confstate;

pub use crate::confchange::{
    new_conf_change_single, parse_conf_change, stringify_conf_change, ConfChangeI,
};
pub use crate::confstate::conf_state_eq;
pub use crate::protos::eraftpb;

/// compatibility utils between `protobuf` and `prost`
///
/// To write code that is compatible with both the `protobuf` and `prost` codecs the following
/// import is often enough:
/// ```
/// use raft_proto::protocompat::*;
/// ```
pub mod protocompat {
    /// A reexport of `[::protobuf]` or `[::prost]` if the `prost-codec` feature is
    /// enabled.
    pub use crate::protoimpl::pbcrate;
    /// A reexport of `[::protobuf::ProtobufError]` or the equivalent type in `[prost]` if the
    /// `prost-codec` feature is enabled.
    pub use crate::protoimpl::PbError;
    /// A reexport of `[::protobuf::Message]` or `[::prost::Message]` if the `prost-codec` feature is
    /// enabled.
    pub use crate::protoimpl::PbMessage;

    /// A compatibility layer to bring some protobuf specific methods to the prost message trait
    ///
    /// See the documentation of the method of the same name in `[::protobuf::Message]`
    pub use crate::protoimpl::PbMessageExt;

    /// A reexport of `[::protobuf::ProtobufEnum]` if the `protobuf-codec` feature is
    /// enabled.
    ///
    /// It is an empty trait when using `prost-codec`.
    pub use crate::protoimpl::ProtobufEnum;
}

#[cfg(all(feature = "prost-codec", feature = "protobuf-codec"))]
compile_error!("`prost-codec` and `protobuf-codec` are mutually exclusive. Enable exactly one.");

#[cfg(not(any(feature = "prost-codec", feature = "protobuf-codec")))]
compile_error!("Enable one codec feature: `prost-codec` or `protobuf-codec`.");

#[cfg(feature = "prost-codec")]
mod protoimpl {
    pub use ::prost as pbcrate;
    pub use ::prost::DecodeError as PbError;
    pub use ::prost::Message as PbMessage;
    pub trait PbMessageExt: PbMessage {
        fn compute_size(&self) -> u32;
        fn merge_from_bytes(&mut self, bytes: &[u8]) -> Result<(), PbError>;
        fn write_to_bytes(&self) -> Result<Vec<u8>, PbError>;
    }
    impl<T: PbMessage> PbMessageExt for T {
        #[inline]
        fn compute_size(&self) -> u32 {
            self.encoded_len() as u32
        }
        #[inline]
        fn merge_from_bytes(&mut self, bytes: &[u8]) -> Result<(), PbError> {
            self.merge(bytes.as_ref())
        }
        #[inline]
        fn write_to_bytes(&self) -> Result<Vec<u8>, PbError> {
            Ok(self.encode_to_vec())
        }
    }
    pub trait ProtobufEnum {}
}
#[cfg(feature = "protobuf-codec")]
mod protoimpl {
    pub use ::protobuf as pbcrate;
    pub use ::protobuf::Message as PbMessage;
    pub use ::protobuf::ProtobufEnum;
    pub use ::protobuf::ProtobufError as PbError;
    pub trait PbMessageExt: PbMessage {}
    impl<T: PbMessage> super::protocompat::PbMessageExt for T {}
}

#[allow(dead_code)]
#[allow(unknown_lints)]
#[allow(clippy::all)]
#[allow(renamed_and_removed_lints)]
#[allow(bare_trait_objects)]
mod protos {
    include!(concat!(env!("OUT_DIR"), "/protos/mod.rs"));

    use self::eraftpb::Snapshot;

    impl Snapshot {
        /// For a given snapshot, determine if it's empty or not.
        pub fn is_empty(&self) -> bool {
            self.get_metadata().index == 0
        }
    }
}

pub mod prelude {
    pub use crate::eraftpb::{
        ConfChange, ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2,
        ConfState, Entry, EntryType, HardState, Message, MessageType, Snapshot, SnapshotMetadata,
    };
}

pub mod util {
    use crate::eraftpb::ConfState;

    impl<Iter1, Iter2> From<(Iter1, Iter2)> for ConfState
    where
        Iter1: IntoIterator<Item = u64>,
        Iter2: IntoIterator<Item = u64>,
    {
        fn from((voters, learners): (Iter1, Iter2)) -> Self {
            let mut conf_state = ConfState::default();
            conf_state.mut_voters().extend(voters);
            conf_state.mut_learners().extend(learners);
            conf_state
        }
    }
}
