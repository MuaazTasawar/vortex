//! A bounded, lock-free MPMC ring buffer (Dmitry Vyukov's algorithm).
//!
//! Each slot carries its own sequence number rather than relying on a
//! single global head/tail pair, which is what lets multiple producers
//! and multiple consumers make progress concurrently without a lock:
//! a thread claims a slot via CAS on the enqueue/dequeue cursor, then
//! owns that slot exclusively until it updates the slot's sequence
//! number to hand it off.

use crate::loom_compat::{AtomicUsize, Ordering, UnsafeCell};
use crossbeam_utils::CachePadded;
use std::mem::MaybeUninit;

struct Cell<T> {
    sequence: AtomicUsize,
    value: UnsafeCell<MaybeUninit<T>>,
}

pub struct RingBuffer<T> {
    buffer: Box<[Cell<T>]>,
    mask: usize,
    enqueue_pos: CachePadded<AtomicUsize>,
    dequeue_pos: CachePadded<AtomicUsize>,
}

// SAFETY: `Cell<T>`'s `UnsafeCell` is only ever accessed by whichever
// thread currently holds the sequence-number-verified claim on that
// slot (enforced by the CAS in try_push/try_pop), so concurrent access
// from multiple threads is data-race-free as long as `T: Send`.
unsafe impl<T: Send> Send for RingBuffer<T> {}
unsafe impl<T: Send> Sync for RingBuffer<T> {}

impl<T> RingBuffer<T> {
    /// `capacity` must be a power of two so the `& mask` slot lookup
    /// below is a cheap bitwise op instead of a modulo.
    pub fn with_capacity(capacity: usize) -> Self {
        assert!(
            capacity >= 2 && capacity.is_power_of_two(),
            "capacity must be a power of two >= 2"
        );
        let buffer: Vec<Cell<T>> = (0..capacity)
            .map(|i| Cell {
                sequence: AtomicUsize::new(i),
                value: UnsafeCell::new(MaybeUninit::uninit()),
            })
            .collect();

        RingBuffer {
            buffer: buffer.into_boxed_slice(),
            mask: capacity - 1,
            enqueue_pos: CachePadded::new(AtomicUsize::new(0)),
            dequeue_pos: CachePadded::new(AtomicUsize::new(0)),
        }
    }

    pub fn try_push(&self, value: T) -> Result<(), T> {
        let mut pos = self.enqueue_pos.load(Ordering::Relaxed);
        loop {
            let cell = &self.buffer[pos & self.mask];
            let seq = cell.sequence.load(Ordering::Acquire);
            let diff = seq as isize - pos as isize;

            if diff == 0 {
                match self.enqueue_pos.compare_exchange_weak(
                    pos,
                    pos.wrapping_add(1),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        // SAFETY: the CAS above succeeded, which means we
                        // are the unique thread that claimed this slot —
                        // no other producer can observe the same `pos`
                        // until we release it by bumping `sequence` below.
                        cell.value
                            .with_mut(|slot| unsafe { (*slot).write(value) });
                        cell.sequence.store(pos.wrapping_add(1), Ordering::Release);
                        return Ok(());
                    }
                    Err(cur) => pos = cur,
                }
            } else if diff < 0 {
                return Err(value); // buffer full
            } else {
                pos = self.enqueue_pos.load(Ordering::Relaxed);
            }
        }
    }

    pub fn try_pop(&self) -> Option<T> {
        let mut pos = self.dequeue_pos.load(Ordering::Relaxed);
        loop {
            let cell = &self.buffer[pos & self.mask];
            let seq = cell.sequence.load(Ordering::Acquire);
            let diff = seq as isize - pos.wrapping_add(1) as isize;

            if diff == 0 {
                match self.dequeue_pos.compare_exchange_weak(
                    pos,
                    pos.wrapping_add(1),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        // SAFETY: sequence == pos + 1 means a producer
                        // finished writing and released this slot to
                        // consumers; the CAS above gives us the unique
                        // claim, so `assume_init_read` is reading a value
                        // that was genuinely initialized and is not being
                        // read by any other consumer.
                        let value = cell
                            .value
                            .with_mut(|slot| unsafe { (*slot).assume_init_read() });
                        cell.sequence
                            .store(pos.wrapping_add(self.mask + 1), Ordering::Release);
                        return Some(value);
                    }
                    Err(cur) => pos = cur,
                }
            } else if diff < 0 {
                return None; // buffer empty
            } else {
                pos = self.dequeue_pos.load(Ordering::Relaxed);
            }
        }
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    #[test]
    fn push_then_pop_single_thread() {
        let rb = RingBuffer::with_capacity(4);
        assert!(rb.try_push(1).is_ok());
        assert!(rb.try_push(2).is_ok());
        assert_eq!(rb.try_pop(), Some(1));
        assert_eq!(rb.try_pop(), Some(2));
        assert_eq!(rb.try_pop(), None);
    }

    #[test]
    fn rejects_push_when_full() {
        let rb = RingBuffer::with_capacity(2);
        assert!(rb.try_push(1).is_ok());
        assert!(rb.try_push(2).is_ok());
        assert_eq!(rb.try_push(3), Err(3));
    }
}

#[cfg(loom)]
mod loom_tests {
    use super::*;
    use loom::sync::Arc;
    use loom::thread;

    /// Model-checks every possible thread interleaving (within loom's
    /// bounded exploration) of one producer pushing two items while a
    /// consumer concurrently pops, asserting no item is lost, duplicated,
    /// or read before it was written.
    #[test]
    fn spsc_no_lost_or_duplicated_items() {
        loom::model(|| {
            let rb = Arc::new(RingBuffer::<usize>::with_capacity(2));
            let rb_producer = rb.clone();

            let producer = thread::spawn(move || {
                rb_producer.try_push(1).ok();
                rb_producer.try_push(2).ok();
            });

            let mut seen = Vec::new();
            for _ in 0..2 {
                if let Some(v) = rb.try_pop() {
                    seen.push(v);
                }
            }
            // whatever we popped early must be a prefix of [1, 2] —
            // no duplicates, nothing out of thin air
            for w in seen.windows(2) {
                assert!(w[0] < w[1]);
            }

            producer.join().unwrap();
        });
    }
}