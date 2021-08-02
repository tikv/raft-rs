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

use std::cmp::Ordering;

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
                self.buffer.reserve(incoming_cap - self.count);
                self.cap = incoming_cap;
                self.incoming_cap = None;
            }
            Ordering::Greater => {
                if self.count <= incoming_cap && self.start + self.count < incoming_cap {
                    self.cap = incoming_cap;
                    let (cur_cap, cur_len) = (self.buffer.capacity(), self.buffer.len());
                    if cur_cap > incoming_cap && cur_len <= incoming_cap {
                        // TODO: Simplify it after `shrink_to` is stable.
                        unsafe {
                            self.buffer.set_len(incoming_cap);
                            self.buffer.shrink_to_fit();
                            self.buffer.set_len(cur_len);
                        }
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
        let start = self.buffer[self.start];
        self.free_to(start);
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
        let mut inflight = Inflights::new(128);

        (0..16).for_each(|i| inflight.add(i));
        assert_eq!(inflight.count(), 16);

        // Adjust cap to a larger value.
        inflight.set_cap(1024);
        assert_eq!(inflight.cap, 1024);
        assert_eq!(inflight.incoming_cap, None);
        assert_eq!(inflight.buffer_capacity(), 1024);

        // Adjust cap to a less value than the current one.
        inflight.set_cap(8);
        assert_eq!(inflight.cap, 1024);
        assert_eq!(inflight.incoming_cap, Some(8));
        assert!(inflight.full());

        // Free somethings. It should still be full.
        inflight.free_to(7);
        assert!(inflight.full());

        // Free more one slot, then it won't be full. However buffer capacity can't
        // shrink in the current implementation.
        inflight.free_first_one();
        assert!(!inflight.full());
        assert_eq!(inflight.buffer_capacity(), 1024);

        // The internal buffer can be shrinked after it is freed totally.
        inflight.free_to(15);
        assert!(inflight.start < inflight.buffer_capacity());
        assert_eq!(inflight.buffer_capacity(), 8);

        // 1024 -> 8 -> 1024. `incoming_cap` should be cleared after the second `set_cap`.
        inflight.set_cap(1024);
        (0..16).for_each(|i| inflight.add(i));
        inflight.set_cap(8);
        assert_eq!(inflight.cap, 1024);
        assert_eq!(inflight.incoming_cap, Some(8));
        inflight.set_cap(1024);
        assert_eq!(inflight.incoming_cap, None);

        // 1024 -> 512. The internal buffer should be shrinked.
        inflight.set_cap(512);
        assert_eq!(inflight.cap, 512);
        assert_eq!(inflight.incoming_cap, None);
        assert_eq!(inflight.buffer_capacity(), 512);

        // 1024 -> 512. The buffer shouldn't be shrinked as the tail part is in used.
        let mut inflight = Inflights::new(1024);
        inflight.buffer = vec![0; 1024];
        inflight.start = 800;
        (0..16).for_each(|i| inflight.add(i));
        inflight.set_cap(512);
        assert_eq!(inflight.incoming_cap, Some(512));
        assert_eq!(inflight.buffer_capacity(), 1024);
    }
}
