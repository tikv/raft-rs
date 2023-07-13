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

use alloc::vec;
use alloc::vec::Vec;

use core::cmp::Ordering;

/// A buffer of inflight messages.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Inflights {
    // the starting index in the buffer
    start: usize,
    // number of inflights in the buffer
    count: usize,

    // ring buffer
    buffer: Vec<u64>,

    // capacity
    cap: usize,

    // To support dynamically change inflight size.
    incoming_cap: Option<usize>,
}

impl Inflights {
    /// Creates a new buffer for inflight messages.
    pub fn new(cap: usize) -> Inflights {
        Inflights {
            buffer: Vec::with_capacity(cap),
            start: 0,
            count: 0,
            cap,
            incoming_cap: None,
        }
    }

    /// Adjust inflight buffer capacity. Set it to `0` will disable the progress.
    // Calling it between `self.full()` and `self.add()` can cause a panic.
    pub fn set_cap(&mut self, incoming_cap: usize) {
        match self.cap.cmp(&incoming_cap) {
            Ordering::Equal => self.incoming_cap = None,
            Ordering::Less => {
                if self.start + self.count <= self.cap {
                    if self.buffer.capacity() > 0 {
                        self.buffer.reserve(incoming_cap - self.buffer.len());
                    }
                } else {
                    debug_assert_eq!(self.cap, self.buffer.len());
                    let mut buffer = Vec::with_capacity(incoming_cap);
                    buffer.extend_from_slice(&self.buffer[self.start..]);
                    buffer.extend_from_slice(&self.buffer[0..self.count - (self.cap - self.start)]);
                    self.buffer = buffer;
                    self.start = 0;
                }
                self.cap = incoming_cap;
                self.incoming_cap = None;
            }
            Ordering::Greater => {
                if self.count == 0 {
                    self.cap = incoming_cap;
                    self.incoming_cap = None;
                    self.start = 0;
                    if self.buffer.capacity() > 0 {
                        self.buffer = Vec::with_capacity(incoming_cap);
                    }
                } else {
                    self.incoming_cap = Some(incoming_cap);
                }
            }
        }
    }

    /// Returns true if the inflights is full.
    #[inline]
    pub fn full(&self) -> bool {
        self.count == self.cap || self.incoming_cap.map_or(false, |cap| self.count >= cap)
    }

    /// Adds an inflight into inflights
    pub fn add(&mut self, inflight: u64) {
        if self.full() {
            panic!("cannot add into a full inflights")
        }

        if self.buffer.capacity() == 0 {
            debug_assert_eq!(self.count, 0);
            debug_assert_eq!(self.start, 0);
            debug_assert!(self.incoming_cap.is_none());
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

        if self.count == 0 {
            if let Some(incoming_cap) = self.incoming_cap.take() {
                self.start = 0;
                self.cap = incoming_cap;
                self.buffer = Vec::with_capacity(self.cap);
            }
        }
    }

    /// Frees the first buffer entry.
    #[inline]
    pub fn free_first_one(&mut self) {
        if self.count > 0 {
            let start = self.buffer[self.start];
            self.free_to(start);
        }
    }

    /// Frees all inflights.
    #[inline]
    pub fn reset(&mut self) {
        self.count = 0;
        self.start = 0;
        self.buffer = vec![];
        self.cap = self.incoming_cap.take().unwrap_or(self.cap);
    }

    // Number of inflight messages. It's for tests.
    #[doc(hidden)]
    #[inline]
    pub fn count(&self) -> usize {
        self.count
    }

    // Capacity of the internal buffer.
    #[doc(hidden)]
    #[inline]
    pub fn buffer_capacity(&self) -> usize {
        self.buffer.capacity()
    }

    // Whether buffer is allocated or not. It's for tests.
    #[doc(hidden)]
    #[inline]
    pub fn buffer_is_allocated(&self) -> bool {
        self.buffer_capacity() > 0
    }

    /// Free unused memory
    #[inline]
    pub fn maybe_free_buffer(&mut self) {
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
    use alloc::vec;
    use alloc::vec::Vec;

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
            incoming_cap: None,
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
            incoming_cap: None,
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
            incoming_cap: None,
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
            incoming_cap: None,
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
            incoming_cap: None,
        };

        assert_eq!(inflight, wantin);

        inflight.free_to(8);

        let wantin2 = Inflights {
            start: 9,
            count: 1,
            buffer: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            cap: 10,
            incoming_cap: None,
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
            incoming_cap: None,
        };

        assert_eq!(inflight, wantin3);

        inflight.free_to(14);

        let wantin4 = Inflights {
            start: 5,
            count: 0,
            buffer: vec![10, 11, 12, 13, 14, 5, 6, 7, 8, 9],
            cap: 10,
            incoming_cap: None,
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
            incoming_cap: None,
        };

        assert_eq!(inflight, wantin);
    }

    #[test]
    fn test_inflights_set_cap() {
        // Prepare 3 `Inflights` with 16 items, but start at 16, 112 and 120.
        let mut inflights = Vec::with_capacity(3);
        for &start in &[16, 112, 120] {
            let mut inflight = Inflights::new(128);
            (0..start).for_each(|i| inflight.add(i));
            inflight.free_to(start - 1);
            (0..16).for_each(|i| inflight.add(i));
            assert_eq!(inflight.count(), 16);
            assert_eq!(inflight.start, start as usize);
            inflights.push(inflight);
        }

        // Adjust cap to a larger value.
        for (i, inflight) in inflights.iter_mut().enumerate() {
            inflight.set_cap(1024);
            assert_eq!(inflight.cap, 1024);
            assert_eq!(inflight.incoming_cap, None);
            assert_eq!(inflight.buffer_capacity(), 1024);
            if i < 2 {
                // The internal buffer is extended directly.
                assert_ne!(inflight.start, 0);
            } else {
                // The internal buffer is re-allocated instead of extended.
                assert_eq!(inflight.start, 0);
            }
        }

        // Prepare 3 `Inflights` with given `start`, `count` and `buffer_cap`.
        let mut inflights = Vec::with_capacity(3);
        for &(start, count, buffer_cap) in &[(1, 0, 0), (1, 0, 128), (1, 8, 128)] {
            let mut inflight = Inflights::new(128);
            inflight.start = start;
            inflight.buffer = vec![0; buffer_cap];
            (0..count).for_each(|i| inflight.add(i));
            inflights.push(inflight);
        }

        // Adjust cap to a less value.
        for (i, inflight) in inflights.iter_mut().enumerate() {
            inflight.set_cap(64);
            if i == 0 || i == 1 {
                assert_eq!(inflight.cap, 64);
                assert_eq!(inflight.incoming_cap, None);
                assert_eq!(inflight.start, 0);
                if i == 0 {
                    assert_eq!(inflight.buffer.capacity(), 0)
                } else {
                    assert_eq!(inflight.buffer.capacity(), 64)
                }
            } else {
                assert_eq!(inflight.cap, 128);
                assert_eq!(inflight.incoming_cap, Some(64));
                assert_eq!(inflight.start, 1);
                assert_eq!(inflight.buffer.capacity(), 128)
            }
        }

        // `incoming_cap` can be cleared if the buffer is freed totally.
        let mut inflight = inflights[2].clone();
        inflight.free_to(7);
        assert_eq!(inflight.cap, 64);
        assert_eq!(inflight.incoming_cap, None);
        assert_eq!(inflight.start, 0);

        // `incoming_cap` can be cleared when `cap` is enlarged.
        for &new_cap in &[128, 1024] {
            let mut inflight = inflights[2].clone();
            inflight.set_cap(new_cap);
            assert_eq!(inflight.cap, new_cap);
            assert_eq!(inflight.incoming_cap, None);
        }
    }
}
