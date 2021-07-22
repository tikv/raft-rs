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

/// A buffer of inflight messages.
#[derive(Debug, PartialEq, Clone)]
pub struct Inflights {
    // the starting index in the buffer
    start: usize,
    // number of inflights in the buffer
    count: usize,

    // ring buffer
    buffer: Vec<u64>,

    // capacity
    cap: usize,
}

impl Inflights {
    /// Creates a new buffer for inflight messages.
    pub fn new(cap: usize) -> Inflights {
        Inflights {
            buffer: Vec::with_capacity(cap),
            start: 0,
            count: 0,
            cap,
        }
    }

    /// Returns true if the inflights is full.
    #[inline]
    pub fn full(&self) -> bool {
        self.count == self.cap
    }

    /// Adds an inflight into inflights
    pub fn add(&mut self, inflight: u64) {
        if self.full() {
            panic!("cannot add into a full inflights")
        }

        if self.buffer.capacity() == 0 {
            debug_assert_eq!(self.count, 0);
            debug_assert_eq!(self.start, 0);
            self.buffer = Vec::with_capacity(self.cap);
        }

        let mut next = self.start + self.count;
        if next >= self.cap {
            next -= self.cap;
        }
        assert!(next <= self.buffer.len());
        if next == self.buffer.len() {
            self.buffer.push(inflight);
        } else {
            self.buffer[next] = inflight;
        }
        self.count += 1;
    }

    /// Frees the inflights smaller or equal to the given `to` flight.
    pub fn free_to(&mut self, to: u64) {
        if self.count == 0 || to < self.buffer[self.start] {
            // out of the left side of the window
            return;
        }

        let mut i = 0usize;
        let mut idx = self.start;
        while i < self.count {
            if to < self.buffer[idx] {
                // found the first large inflight
                break;
            }

            // increase index and maybe rotate
            idx += 1;
            if idx >= self.cap {
                idx -= self.cap;
            }

            i += 1;
        }

        // free i inflights and set new start index
        self.count -= i;
        self.start = idx;
    }

    /// Frees the first buffer entry.
    #[inline]
    pub fn free_first_one(&mut self) {
        let start = self.buffer[self.start];
        self.free_to(start);
    }

    /// Frees all inflights.
    #[inline]
    pub fn reset(&mut self) {
        self.count = 0;
        self.start = 0;
        self.buffer = vec![];
    }

    // Number of inflight messages. It's for tests.
    #[doc(hidden)]
    #[inline]
    pub fn count(&self) -> usize {
        self.count
    }

    // Capacity of inflight buffer.
    #[doc(hidden)]
    #[inline]
    pub fn cap(&self) -> usize {
        self.buffer.capacity()
    }

    // Whether buffer is allocated or not. It's for tests.
    #[doc(hidden)]
    #[inline]
    pub fn buffer_is_allocated(&self) -> bool {
        self.cap() > 0
    }

    /// Free unused memory
    #[inline]
    pub(crate) fn maybe_free_buffer(&mut self) {
        if self.count == 0 {
            self.start = 0;
            self.buffer = vec![];
            debug_assert_eq!(self.buffer.capacity(), 0);
        }
    }
}

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
            cap: 10,
        };

        assert_eq!(inflight, wantin);

        for i in 5..10 {
            inflight.add(i);
        }

        let wantin2 = Inflights {
            start: 0,
            count: 10,
            buffer: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            cap: 10,
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
            cap: 10,
        };

        assert_eq!(inflight2, wantin21);

        for i in 5..10 {
            inflight2.add(i);
        }

        let wantin22 = Inflights {
            start: 5,
            count: 10,
            buffer: vec![5, 6, 7, 8, 9, 0, 1, 2, 3, 4],
            cap: 10,
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
            cap: 10,
        };

        assert_eq!(inflight, wantin);

        inflight.free_to(8);

        let wantin2 = Inflights {
            start: 9,
            count: 1,
            buffer: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            cap: 10,
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
            cap: 10,
        };

        assert_eq!(inflight, wantin3);

        inflight.free_to(14);

        let wantin4 = Inflights {
            start: 5,
            count: 0,
            buffer: vec![10, 11, 12, 13, 14, 5, 6, 7, 8, 9],
            cap: 10,
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
            cap: 10,
        };

        assert_eq!(inflight, wantin);
    }
}
