//! This module expresses a simple abstraction layer between the trusted host and
//! trusted application it runs. The main goal is to abstract away details of the
//! trusted hosting and support mutiple host implementations. The host implementations
//! differ in their capabilities with some imposing significant restrictions but
//! offering better security and privacy guarantees. Hence the abstraction layer
//! focuses on the most restricted host and assumes restricted kernel.
//!
//! Restricted kernel limits the execution model to processing signals (or rather
//! messages) received from the untrusted launcher over a communication channel. Given
//! this limitation the abstraction layer assumes that the trusted application performs
//! processing in response to the signal from the untrusted launcher which can be
//! either a message or a clock tick. In other words the abstraction layer defines
//! a poll based execution model driven by a signal generating loop in untrusted
//! launcher.
//!
//! [Application] trait must be implemented by a concrete trusted application to
//! receive signals from the trusted host originating from untrusted launcher.
//!
//! [Host] trait must be implemented by a concrete trusted host to expose its capabilities
//! to the trusted application.

use alloc::boxed::Box;
use alloc::vec::Vec;
use anyhow::Result;

type Message = Vec<u8>;

/// Represents a set of claims for the trusted host and application, provides capability
/// sign and verify application data.
pub trait Attestation {
    /// TODO: Expose application identity, provenance and configuration information such
    /// that applications can assert the properties of the peer.

    /// Serializes attestation such that it can be transferred over the wire to a peer
    /// and attested by the peer.
    ///
    /// # Returns
    ///
    /// An error if failed to serialize into a byte string, a success otherwise.
    ///
    /// # Note
    ///
    /// The serialized representation must contain enough information to
    /// verify the attestation itself (e.g. that it has been produced by a proper root
    /// of trust and that it represents a trusted chain of components) and to verify
    /// signed application data (e.g. trusted applications sharing data with each other).
    ///
    /// For example, before two trusted application peers can start interacting they must
    /// establish trust. The trusted application initiating the interaction will first
    /// obtain its own attestation, sign the data it wants to share with the peer,
    /// share serialized attestation, the data and its signature with the peer. The peer
    /// deserializes and verifies the attestation (see `Host::verify_peer_attestation`),
    /// uses the attestation to verify the data signatures, performs similar handshake
    /// with the peer.
    fn serialize(&self) -> Result<Vec<u8>>;

    /// Signs a byte string using private signing key.
    ///
    /// # Arguments
    ///
    /// * `data` - The byte string to sign.
    ///
    /// # Returns
    ///
    /// An error if the attestation doesn't possess the signing capability (e.g. this
    /// instance has been transferred over the wire and meant to be used for verification)
    /// or if the signing failed, a success otherwise.
    fn sign(&self, data: &[u8]) -> Result<Vec<u8>>;

    /// Verifies signature of a byte string using public signing key.
    ///
    /// # Arguments
    ///
    /// * `data` - The byte string to verify signature for.
    /// * `signature` - The signature of the byte string.
    ///
    /// # Returns
    ///
    /// An error if the signature cannot be verifed or is invalid, a success otherwise.
    fn verify(&self, data: &[u8], signature: &[u8]) -> Result<()>;

    /// Gets serialized public key used for signing by the trusted application.
    /// The signing key is generated at the start of the trusted application and
    /// remains unchanged through the lifetime.
    ///
    /// # Note
    ///
    /// Public signing key can be used to derive a trusted application identity.
    /// For example, a trusted application that represents a node in Raft cluster
    /// running inside of a group trusted hosts, it is important that Raft node
    /// identity cannot be forged, does not require coordination to pick one and
    /// has low collision chance. Hash of the public signing key is an identity
    /// mechanism that is compliant with these requirements.
    fn public_signing_key(&self) -> Vec<u8>;
}

/// Represents a trusted host, abstracting away the details of how trusted
/// application is hosted (e.g. trusted host can be restricted kernel bare
/// metal based encrypted virtual machine or a linux kernel docker based
/// encrypted virtual machine).
pub trait Host {
    /// Gets attestation that represens a set of claims about the trusted application
    /// and its host. The attestation is generated after the trusted application and
    /// host are fully initialiezed, the attestation doesn't change throughout the
    /// lifetime.
    fn get_self_attestation(&self) -> Box<dyn Attestation>;

    /// Gets serialized configuration that stays immutable through the lifetime of
    /// the trusted application.
    fn get_self_config(&self) -> Vec<u8>;

    /// Sends messages through the communication channel that connects the trusted
    /// application to the untrusted launcher.
    ///
    /// # Argumnets
    ///
    /// * `messages` - A set of messsages to send through the channel.
    ///
    /// # Returns
    ///
    /// Error if the communication channel is irrepairably broken, a success otherise.
    fn send_messages(&mut self, messages: &[Message]) -> Result<()>;

    /// Attempts to deserialize peer attestation and perform initial verification
    /// of the attestation. The application specific verification (e.g. ensuring
    /// that attestation points to a particular binary and configuration) is the
    /// responsibility of the trusted application.
    ///
    /// # Arguments
    ///
    /// * `peer_attestation` - A serialized attestation of a peer trusted application.
    ///
    /// # Returns
    ///
    /// A deserialized and initially verified peer attestation if success, an error
    /// otherwise. An error may be caused by malformed serialized representation or
    /// by failing verification.
    fn verify_peer_attestation(&self, peer_attestation: &[u8]) -> Result<Box<dyn Attestation>>;
}

/// Represents a trusted application running inside a trusted host. The trusted
/// application is considered passive and performs execution in response to
/// receiving messages through the communication channel that connects the trusted
/// application to the untrusted launcher. In the absence of messages to be processed
/// the trusted application will receive periodically empty set of messages to allow
/// trusted application to make progress based on time change.
pub trait Application {
    /// Receives messages to process by the trusted application. Conceptually represents
    /// a computation slice. A set of messages may be empty.
    ///
    /// # Arguments
    ///
    /// * `host` - Trusted host that is responsible the trusted application. Provides
    /// access to message sending and attestation capabilities.
    /// * `instant` - A measurement of a monotonically nondecreasing clock provided by
    /// the untrusted launcher to the trusted host. The resolution of the instant is
    /// mesured in milliseconds. Instants are opaque that can only be compared to one
    /// another. In other words the absolute value must not be interpretted as wall
    /// clock time or time since the trusted application start.
    /// * `messages` - A potentially empty set of messages received from the untrusted
    /// launcher for the trusted application to process.
    ///
    /// # Returns
    ///
    /// Error if the trusted application encountered an unrecoverable error and must
    /// be terminated, a success otherwise. The application level recoverable errors
    /// are represented and communicated using messages.
    fn receive_messages(
        &mut self,
        host: &mut impl Host,
        instant: u64,
        messages: &[Message],
    ) -> Result<()>;
}
