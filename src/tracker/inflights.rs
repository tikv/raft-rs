// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

// Copyright 2015 The etcd Authors
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
use std::mem;

/// A buffer of inflight messages.
#[derive(Debug, PartialEq)]
pub struct Inflights {
    // Capacity of the buffer.
    capacity: usize,

    // Number of inflights in the buffer.
    count: usize,

    // Start offset in the first chunk.
    offset: usize,

    // Ring buffer.
    buffer: VecDeque<Box<[u64; Self::BUF_CHUNK_LEN]>>,
}

impl Inflights {
    const BUF_CHUNK_LEN: usize = 16;

    /// Creates a new buffer for inflight messages.
    pub fn new(capacity: usize) -> Inflights {
        Inflights {
            capacity,
            count: 0,
            offset: 0,
            buffer: VecDeque::with_capacity(0),
        }
    }

    /// Returns true if the inflights is full.
    #[inline]
    pub fn full(&self) -> bool {
        self.count == self.capacity
    }

    /// Returns the current inflights count.
    pub fn count(&self) -> usize {
        self.count
    }

    /// Adds an inflight into inflights
    pub fn add(&mut self, inflight: u64) {
        if self.full() {
            panic!("cannot add into a full inflights")
        }

        let end_offset = (self.count + self.offset) % Self::BUF_CHUNK_LEN;
        if self.buffer.is_empty() || end_offset == 0 {
            self.buffer.push_back(Box::new([0; Self::BUF_CHUNK_LEN]));
        }

        let chunk = self.buffer.back_mut().unwrap();
        chunk[end_offset] = inflight;
        self.count += 1;
    }

    /// Frees the inflights smaller or equal to the given `to` flight.
    pub fn free_to(&mut self, to: u64) {
        if self.count == 0 || to < self.buffer[0][self.offset] {
            // out of the left side of the window
            return;
        }
        let end_offset = (self.count + self.offset - 1) % Self::BUF_CHUNK_LEN;
        if to >= self.buffer.back().unwrap()[end_offset] {
            self.reset();
            return;
        }

        let (mut free_to, one_chunk) = (None, self.buffer.len() == 1);
        'LOOP: for (i, chunk) in self.buffer.iter().enumerate() {
            let range = match i {
                0 if one_chunk => self.offset..end_offset + 1,
                0 if !one_chunk => self.offset..Self::BUF_CHUNK_LEN,
                v if v == self.buffer.len() - 1 => 0..end_offset + 1,
                _ => 0..Self::BUF_CHUNK_LEN,
            };
            for j in range {
                if to < chunk[j] {
                    free_to = Some((i, j));
                    break 'LOOP;
                }
            }
        }

        if let Some((i, j)) = free_to {
            (0..i).for_each(|_| drop(self.buffer.pop_front()));
            if i > 0 {
                self.count += self.offset;
                self.count -= Self::BUF_CHUNK_LEN * i;
                self.offset = j;
                self.count -= self.offset;
            } else {
                self.count -= j - self.offset;
                self.offset = j;
            }
        }

        let shrink_capacity = self.buffer.capacity() / 2;
        if self.buffer.len() <= shrink_capacity {
            // TODO: use `shrink_to` after this feature is stable.
            let new_bufer = VecDeque::with_capacity(shrink_capacity);
            let old_buffer = mem::replace(&mut self.buffer, new_bufer);
            self.buffer.extend(old_buffer.into_iter());
        }
    }

    /// Frees the first buffer entry.
    ///
    /// # Panics
    ///
    /// Panics if the `Inflights` is empty.
    #[inline]
    pub fn free_first_one(&mut self) {
        let to = self.buffer[0][self.offset];
        self.free_to(to);
    }

    /// Frees all inflights.
    #[inline]
    pub fn reset(&mut self) {
        self.count = 0;
        self.offset = 0;
        self.buffer = VecDeque::with_capacity(0);
    }
}

/************************
#[cfg(test)]
mod tests {
    use super::Inflights;

    #[test]
    fn test_inflight_add() {
        let mut inflight = Inflights::new(10);
        for i in 0..5 {
            inflight.add(i);
        }

        let wantin = Inflights {
            start: 0,
            count: 5,
            buffer: vec![0, 1, 2, 3, 4],
        };

        assert_eq!(inflight, wantin);

        for i in 5..10 {
            inflight.add(i);
        }

        let wantin2 = Inflights {
            start: 0,
            count: 10,
            buffer: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
        };

        assert_eq!(inflight, wantin2);

        let mut inflight2 = Inflights::new(10);
        inflight2.start = 5;
        inflight2.buffer.extend_from_slice(&[0, 0, 0, 0, 0]);

        for i in 0..5 {
            inflight2.add(i);
        }

        let wantin21 = Inflights {
            start: 5,
            count: 5,
            buffer: vec![0, 0, 0, 0, 0, 0, 1, 2, 3, 4],
        };

        assert_eq!(inflight2, wantin21);

        for i in 5..10 {
            inflight2.add(i);
        }

        let wantin22 = Inflights {
            start: 5,
            count: 10,
            buffer: vec![5, 6, 7, 8, 9, 0, 1, 2, 3, 4],
        };

        assert_eq!(inflight2, wantin22);
    }

    #[test]
    fn test_inflight_free_to() {
        let mut inflight = Inflights::new(10);
        for i in 0..10 {
            inflight.add(i);
        }

        inflight.free_to(4);

        let wantin = Inflights {
            start: 5,
            count: 5,
            buffer: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
        };

        assert_eq!(inflight, wantin);

        inflight.free_to(8);

        let wantin2 = Inflights {
            start: 9,
            count: 1,
            buffer: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
        };

        assert_eq!(inflight, wantin2);

        for i in 10..15 {
            inflight.add(i);
        }

        inflight.free_to(12);

        let wantin3 = Inflights {
            start: 3,
            count: 2,
            buffer: vec![10, 11, 12, 13, 14, 5, 6, 7, 8, 9],
        };

        assert_eq!(inflight, wantin3);

        inflight.free_to(14);

        let wantin4 = Inflights {
            start: 5,
            count: 0,
            buffer: vec![10, 11, 12, 13, 14, 5, 6, 7, 8, 9],
        };

        assert_eq!(inflight, wantin4);
    }

    #[test]
    fn test_inflight_free_first_one() {
        let mut inflight = Inflights::new(10);
        for i in 0..10 {
            inflight.add(i);
        }

        inflight.free_first_one();

        let wantin = Inflights {
            start: 1,
            count: 9,
            buffer: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
        };

        assert_eq!(inflight, wantin);
    }
}
************************/
