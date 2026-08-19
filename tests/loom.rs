//! Model checks: `RUSTFLAGS="--cfg loom" cargo test --test loom --release`
//!
//! Loom explores the interleavings of producer and consumer under the C11
//! memory model, checking the acquire/release pairing on `head`/`tail` and
//! every `UnsafeCell` access for data races. Capacity 2 with 3 items forces
//! the full, empty, and wraparound paths through the model.

#![cfg(loom)]

use loom::thread;

const ITEMS: usize = 3;
const CAPACITY: usize = 2;

#[test]
fn padded_push_pop_interleavings() {
    loom::model(|| {
        let (mut tx, mut rx) = spsc::padded::channel::<usize>(CAPACITY);

        let producer = thread::spawn(move || {
            for i in 0..ITEMS {
                while tx.push(i).is_err() {
                    thread::yield_now();
                }
            }
        });

        for expected in 0..ITEMS {
            let value = loop {
                if let Some(v) = rx.pop() {
                    break v;
                }
                thread::yield_now();
            };
            assert_eq!(value, expected);
        }

        producer.join().unwrap();
    });
}

#[test]
fn unpadded_push_pop_interleavings() {
    loom::model(|| {
        let (mut tx, mut rx) = spsc::unpadded::channel::<usize>(CAPACITY);

        let producer = thread::spawn(move || {
            for i in 0..ITEMS {
                while tx.push(i).is_err() {
                    thread::yield_now();
                }
            }
        });

        for expected in 0..ITEMS {
            let value = loop {
                if let Some(v) = rx.pop() {
                    break v;
                }
                thread::yield_now();
            };
            assert_eq!(value, expected);
        }

        producer.join().unwrap();
    });
}

#[test]
fn drop_reclaims_unconsumed_items() {
    loom::model(|| {
        let (mut tx, rx) = spsc::padded::channel::<Box<usize>>(CAPACITY);

        let producer = thread::spawn(move || {
            let _ = tx.push(Box::new(1));
            let _ = tx.push(Box::new(2));
        });

        // Dropping the consumer while pushes race must still free every
        // published item (loom's leak checker verifies).
        drop(rx);
        producer.join().unwrap();
    });
}
