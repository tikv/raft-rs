// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

#![cfg(feature = "prost-codec")]

use prost::Message as _;
use raft_proto::prelude::*;
use raft_proto::protocompat::PbMessageExt;

#[test]
fn entry_wire_format_and_accessors() {
    let mut entry = Entry::default();
    entry.set_entry_type(EntryType::EntryConfChangeV2);
    entry.set_term(2);
    entry.set_index(3);
    entry.set_data(b"data".to_vec());
    entry.set_context(b"ctx".to_vec());
    let bytes = b"\x08\x02\x10\x02\x18\x03\x22\x04data\x32\x03ctx";
    assert_eq!(entry.encode_to_vec(), bytes);
    assert_eq!(entry.write_to_bytes().unwrap(), bytes);
    assert_eq!(entry.compute_size() as usize, bytes.len());
    assert_eq!(Entry::decode(bytes.as_slice()).unwrap(), entry);
    let mut decoded = Entry::default();
    decoded.merge_from_bytes(bytes).unwrap();
    assert_eq!(decoded.get_entry_type(), EntryType::EntryConfChangeV2);
    assert_eq!(decoded.take_data(), b"data");
    assert!(Entry::decode([0x80].as_slice()).is_err());
}

#[test]
fn nested_messages_and_configuration_changes() {
    let mut message = Message::default();
    message.set_msg_type(MessageType::MsgSnapshot);
    message.mut_snapshot().mut_metadata().set_index(42);
    message
        .mut_snapshot()
        .mut_metadata()
        .mut_conf_state()
        .mut_voters()
        .extend([1, 2, 3]);
    message.mut_entries().push(Entry::default());
    let decoded = Message::decode(message.encode_to_vec().as_slice()).unwrap();
    assert_eq!(decoded, message);
    assert_eq!(decoded.get_msg_type(), MessageType::MsgSnapshot);
    assert_eq!(decoded.get_snapshot().get_metadata().get_index(), 42);

    let mut change = ConfChangeSingle::default();
    change.set_change_type(ConfChangeType::AddLearnerNode);
    change.set_node_id(4);
    let mut changes = ConfChangeV2::default();
    changes.set_transition(ConfChangeTransition::Explicit);
    changes.mut_changes().push(change);
    let decoded = ConfChangeV2::decode(changes.encode_to_vec().as_slice()).unwrap();
    assert_eq!(decoded, changes);
    assert_eq!(decoded.get_transition(), ConfChangeTransition::Explicit);
    assert_eq!(
        decoded.get_changes()[0].get_change_type(),
        ConfChangeType::AddLearnerNode
    );
}
