// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

// Copyright 2016 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::VecDeque;

use slog::Logger;

use crate::eraftpb::Message;
use crate::util::entry_approximate_size;
use crate::{HashMap, HashSet};

/// Determines the relative safety of and consistency of read only requests.
#[derive(Debug, PartialEq, Eq, Clone, Copy, Default)]
pub enum ReadOnlyOption {
    /// Safe guarantees the linearizability of the read only request by
    /// communicating with the quorum. It is the default and suggested option.
    #[default]
    Safe,
    /// LeaseBased ensures linearizability of the read only request by
    /// relying on the leader lease. It can be affected by clock drift.
    /// If the clock drift is unbounded, leader might keep the lease longer than it
    /// should (clock can move backward/pause without any bound). ReadIndex is not safe
    /// in that case.
    LeaseBased,
}

/// ReadState provides state for read only query.
///
/// It's caller's responsibility to send MsgReadIndex first before getting
/// this state from ready. It's also caller's duty to differentiate if this
/// state is what it requests through request_ctx, e.g. given a unique id as
/// request_ctx.
#[derive(Default, Debug, PartialEq, Eq, Clone)]
pub struct ReadState {
    /// The index of the read state.
    pub index: u64,
    /// A datagram consisting of context about the request.
    pub request_ctx: Vec<u8>,
}

#[derive(Default, Debug, Clone)]
pub struct ReadIndexStatus {
    pub req: Message,
    pub index: u64,
    pub acks: HashSet<u64>,
}

/// `MsgReadIndex` messages that reached a leader before it committed an entry
/// in its term. A read index can't be served before that commit, so they are
/// kept here and handled as soon as it happens.
///
/// The queue is bounded in size (see `Config::max_pending_read_index_size`)
/// and holds a request (a sender and its context) at most once, so a
/// requester that retries does not grow it. A message that does not fit is
/// not kept; its sender gets no response and has to retry, as for any other
/// lost message.
#[derive(Default, Debug, Clone)]
pub struct PendingReadIndexMessages {
    msgs: Vec<Message>,
    keys: HashSet<(u64, Vec<u8>)>,
    size: u64,
    max_size: u64,
}

impl PendingReadIndexMessages {
    /// Creates a queue that keeps at most `max_size` bytes of messages.
    pub fn new(max_size: u64) -> Self {
        PendingReadIndexMessages {
            max_size,
            ..Default::default()
        }
    }

    /// Keeps `m`, a `MsgReadIndex` with its context in the first entry.
    /// Returns false if it was not added: the same sender already has a
    /// request with this context in the queue, or the queue is full.
    pub fn push(&mut self, m: Message) -> bool {
        let size = Self::approximate_size(&m);
        if self.size.saturating_add(size) > self.max_size {
            return false;
        }
        let key = (m.from, m.entries[0].data.to_vec());
        if !self.keys.insert(key) {
            return false;
        }
        self.size += size;
        self.msgs.push(m);
        true
    }

    /// Returns the kept messages in arrival order and leaves the queue empty,
    /// with no memory retained.
    pub fn take(&mut self) -> Vec<Message> {
        std::mem::replace(self, Self::new(self.max_size)).msgs
    }

    /// Discards the kept messages and releases their memory.
    pub fn clear(&mut self) {
        *self = Self::new(self.max_size);
    }

    /// The number of kept messages.
    #[inline]
    pub fn len(&self) -> usize {
        self.msgs.len()
    }

    /// Whether no message is kept.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.msgs.is_empty()
    }

    /// The memory a kept message accounts for: the message itself plus the
    /// payload it owns.
    fn approximate_size(m: &Message) -> u64 {
        let entries: usize = m.entries.iter().map(entry_approximate_size).sum();
        (std::mem::size_of::<Message>() + entries + m.context.len()) as u64
    }
}

#[derive(Default, Debug, Clone)]
pub struct ReadOnly {
    pub option: ReadOnlyOption,
    pub pending_read_index: HashMap<Vec<u8>, ReadIndexStatus>,
    pub read_index_queue: VecDeque<Vec<u8>>,
}

impl ReadOnly {
    pub fn new(option: ReadOnlyOption) -> ReadOnly {
        ReadOnly {
            option,
            pending_read_index: HashMap::default(),
            read_index_queue: VecDeque::new(),
        }
    }

    /// Adds a read only request into readonly struct.
    ///
    /// `index` is the commit index of the raft state machine when it received
    /// the read only request.
    ///
    /// `m` is the original read only request message from the local or remote node.
    pub fn add_request(&mut self, index: u64, req: Message, self_id: u64) {
        // TODO: deduplicate by `(requester_peer_id, context)` instead of by
        // context alone. Today a request from a different peer that reuses a
        // pending context is dropped here and that peer gets no response. The
        // change also has to keep heartbeat ack correlation working: acks are
        // matched by context, and an ack's `from` is the responding voter, not
        // the requester.
        let ctx = {
            let key: &[u8] = req.entries[0].data.as_ref();
            if self.pending_read_index.contains_key(key) {
                return;
            }
            key.to_vec()
        };
        let mut acks = HashSet::<u64>::default();
        acks.insert(self_id);
        let status = ReadIndexStatus { req, index, acks };
        self.pending_read_index.insert(ctx.clone(), status);
        self.read_index_queue.push_back(ctx);
    }

    /// Notifies the ReadOnly struct that the raft state machine received
    /// an acknowledgment of the heartbeat that attached with the read only request
    /// context.
    pub fn recv_ack(&mut self, id: u64, ctx: &[u8]) -> Option<&HashSet<u64>> {
        self.pending_read_index.get_mut(ctx).map(|rs| {
            rs.acks.insert(id);
            &rs.acks
        })
    }

    /// Advances the read only request queue kept by the ReadOnly struct.
    /// It dequeues the requests until it finds the read only request that has
    /// the same context as the given `ctx`.
    pub fn advance(&mut self, ctx: &[u8], logger: &Logger) -> Vec<ReadIndexStatus> {
        let mut rss = vec![];
        if let Some(i) = self.read_index_queue.iter().position(|x| {
            if !self.pending_read_index.contains_key(x) {
                fatal!(logger, "cannot find correspond read state from pending map");
            }
            *x == ctx
        }) {
            for _ in 0..=i {
                let rs = self.read_index_queue.pop_front().unwrap();
                let status = self.pending_read_index.remove(&rs).unwrap();
                rss.push(status);
            }
        }
        rss
    }

    /// Returns the context of the last pending read only request in ReadOnly struct.
    pub fn last_pending_request_ctx(&self) -> Option<Vec<u8>> {
        self.read_index_queue.back().cloned()
    }

    /// Returns the context a heartbeat should carry to confirm the pending
    /// read only requests: the one of the last pending request that has a
    /// non-empty context. A quorum acknowledging it confirms that request and
    /// every request queued before it. A request with an empty context can't
    /// be used, because an acknowledgement of an empty context is ignored.
    pub fn last_confirmable_request_ctx(&self) -> Option<Vec<u8>> {
        self.read_index_queue
            .iter()
            .rev()
            .find(|ctx| !ctx.is_empty())
            .cloned()
    }

    #[inline]
    pub fn pending_read_count(&self) -> usize {
        self.read_index_queue.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::eraftpb::{Entry, MessageType};

    fn read_index_msg(from: u64, ctx: &[u8]) -> Message {
        let mut e = Entry::default();
        e.data = ctx.to_vec().into();
        let mut m = Message::default();
        m.set_msg_type(MessageType::MsgReadIndex);
        m.from = from;
        m.entries = vec![e].into();
        m
    }

    #[test]
    fn test_pending_read_index_messages_dedup() {
        let mut q = PendingReadIndexMessages::new(u64::MAX);
        assert!(q.push(read_index_msg(2, b"ctx")));
        // A retry of the same request is not kept twice.
        assert!(!q.push(read_index_msg(2, b"ctx")));
        // The same context from another sender is another request.
        assert!(q.push(read_index_msg(3, b"ctx")));
        assert!(q.push(read_index_msg(2, b"ctx2")));
        assert_eq!(q.len(), 3);

        let msgs = q.take();
        assert_eq!(
            msgs.iter().map(|m| m.from).collect::<Vec<_>>(),
            vec![2, 3, 2]
        );
        assert!(q.is_empty());
        // The request can be queued again once it has been taken.
        assert!(q.push(read_index_msg(2, b"ctx")));
        q.clear();
        assert!(q.is_empty());
        assert!(q.push(read_index_msg(2, b"ctx")));
    }

    #[test]
    fn test_pending_read_index_messages_bounded() {
        let one = PendingReadIndexMessages::approximate_size(&read_index_msg(2, b"ctx0"));
        // Room for exactly three messages of this size.
        let mut q = PendingReadIndexMessages::new(3 * one);
        assert!(q.push(read_index_msg(2, b"ctx0")));
        assert!(q.push(read_index_msg(2, b"ctx1")));
        assert!(q.push(read_index_msg(2, b"ctx2")));
        assert!(!q.push(read_index_msg(2, b"ctx3")));
        assert_eq!(q.len(), 3);

        // The limit survives `take` and `clear`, and a rejected message is
        // not remembered: it fits once there is room.
        assert_eq!(q.take().len(), 3);
        assert!(q.push(read_index_msg(2, b"ctx3")));
        q.clear();
        for ctx in [b"ctx0", b"ctx1", b"ctx2"] {
            assert!(q.push(read_index_msg(2, ctx)));
        }
        assert!(!q.push(read_index_msg(2, b"ctx3")));

        // A larger payload counts for more.
        let mut q = PendingReadIndexMessages::new(3 * one);
        assert!(q.push(read_index_msg(2, &vec![0u8; 2 * one as usize])));
        assert!(!q.push(read_index_msg(2, b"ctx0")));

        // A limit of zero keeps nothing.
        let mut q = PendingReadIndexMessages::new(0);
        assert!(!q.push(read_index_msg(2, b"ctx0")));
        assert!(q.is_empty());
    }
}
