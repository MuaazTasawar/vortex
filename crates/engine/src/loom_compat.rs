//! Swaps between real `std` atomics/cells and `loom`'s instrumented
//! equivalents depending on whether `--cfg loom` is set. Library code
//! (ring_buffer.rs) never touches `std::sync` or `std::cell` directly —
//! it goes through here, so the exact same source is what gets model-
//! checked and what ships to production.

#[cfg(loom)]
pub(crate) use loom::cell::UnsafeCell;
#[cfg(loom)]
pub(crate) use loom::sync::atomic::{AtomicUsize, Ordering};

#[cfg(not(loom))]
pub(crate) use std::sync::atomic::{AtomicUsize, Ordering};

#[cfg(not(loom))]
pub(crate) struct UnsafeCell<T>(std::cell::UnsafeCell<T>);

#[cfg(not(loom))]
impl<T> UnsafeCell<T> {
    pub(crate) fn new(v: T) -> Self {
        UnsafeCell(std::cell::UnsafeCell::new(v))
    }

    pub(crate) fn with_mut<R>(&self, f: impl FnOnce(*mut T) -> R) -> R {
        f(self.0.get())
    }
}