//! Unpadded control variant: identical to [`crate::padded`] except that
//! `head` and `tail` are plain adjacent fields, so they (almost certainly)
//! share a cache line and every index store invalidates the other side's hot
//! line. This variant exists to isolate the cost of that false sharing in the
//! benchmark; use the padded variant for real work.

use std::mem::MaybeUninit;

use crate::sync::{Arc, AtomicUsize, Ordering, UnsafeCell};

struct Ring<T> {
    /// Next slot to write. Written only by the producer.
    head: AtomicUsize,
    /// Next slot to read. Written only by the consumer.
    tail: AtomicUsize,
    buffer: Box<[UnsafeCell<MaybeUninit<T>>]>,
    cap: usize,
}

// A `T` is handed from producer to consumer exactly once, so `T: Send` is
// sufficient. The handles enforce single-producer/single-consumer.
unsafe impl<T: Send> Send for Ring<T> {}
unsafe impl<T: Send> Sync for Ring<T> {}

/// Creates a bounded SPSC channel with `capacity` slots, all usable.
///
/// Indices grow without bound and are masked on access, so full/empty are
/// distinguished by `head - tail` and no slot is sacrificed.
///
/// # Panics
///
/// Panics if `capacity` is zero or not a power of two.
pub fn channel<T>(capacity: usize) -> (Producer<T>, Consumer<T>) {
    assert!(
        capacity.is_power_of_two(),
        "capacity must be a nonzero power of two"
    );

    let ring = Arc::new(Ring {
        head: AtomicUsize::new(0),
        tail: AtomicUsize::new(0),
        buffer: (0..capacity)
            .map(|_| UnsafeCell::new(MaybeUninit::uninit()))
            .collect(),
        cap: capacity,
    });

    (
        Producer {
            ring: Arc::clone(&ring),
        },
        Consumer { ring },
    )
}

/// The write end. Not clonable; `push` takes `&mut self`, so there is exactly
/// one producer.
pub struct Producer<T> {
    ring: Arc<Ring<T>>,
}

impl<T> Producer<T> {
    /// Attempts to push, returning `Err(value)` if the buffer is full.
    pub fn push(&mut self, value: T) -> Result<(), T> {
        let ring = &*self.ring;
        // Relaxed: only this thread writes `head`.
        let head = ring.head.load(Ordering::Relaxed);
        // Acquire pairs with the consumer's Release store of `tail`: once we
        // observe a slot as consumed, the consumer's read of it has completed
        // and the slot may be overwritten.
        let tail = ring.tail.load(Ordering::Acquire);

        if head.wrapping_sub(tail) == ring.cap {
            return Err(value);
        }

        let slot = &ring.buffer[head & (ring.cap - 1)];
        slot.with_mut(|p| unsafe { (*p).write(value) });

        // Release publishes the slot write above to the consumer's Acquire
        // load of `head`.
        ring.head.store(head.wrapping_add(1), Ordering::Release);
        Ok(())
    }
}

/// The read end. Not clonable; `pop` takes `&mut self`, so there is exactly
/// one consumer.
pub struct Consumer<T> {
    ring: Arc<Ring<T>>,
}

impl<T> Consumer<T> {
    /// Attempts to pop, returning `None` if the buffer is empty.
    pub fn pop(&mut self) -> Option<T> {
        let ring = &*self.ring;
        // Relaxed: only this thread writes `tail`.
        let tail = ring.tail.load(Ordering::Relaxed);
        // Acquire pairs with the producer's Release store of `head`, making
        // the slot write visible before we read it.
        let head = ring.head.load(Ordering::Acquire);

        if head == tail {
            return None;
        }

        let slot = &ring.buffer[tail & (ring.cap - 1)];
        let value = slot.with(|p| unsafe { (*p).assume_init_read() });

        // Release hands the slot back to the producer's Acquire load of
        // `tail`.
        ring.tail.store(tail.wrapping_add(1), Ordering::Release);
        Some(value)
    }
}

impl<T> Drop for Ring<T> {
    fn drop(&mut self) {
        // Runs when the last handle drops; access is exclusive by then.
        let head = self.head.load(Ordering::Relaxed);
        let mut tail = self.tail.load(Ordering::Relaxed);
        while tail != head {
            self.buffer[tail & (self.cap - 1)].with_mut(|p| unsafe { (*p).assume_init_drop() });
            tail = tail.wrapping_add(1);
        }
    }
}
