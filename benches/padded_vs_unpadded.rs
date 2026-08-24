//! Padded vs unpadded under a symmetric producer/consumer ping workload.
//!
//! Barrier-synchronized `iter_custom` timing: both threads park on a start
//! barrier, the coordinator stamps the clock, and a stop barrier closes the
//! measurement — thread spawn and join never land inside the timed region.
//! Reported time is per transferred item.

use std::hint::spin_loop;
use std::sync::Barrier;
use std::thread;
use std::time::{Duration, Instant};

use core_affinity::CoreId;
use criterion::{criterion_group, criterion_main, Criterion};
use spsc::{padded, unpadded};

const CAPACITY: usize = 1024;

/// Runs `producer` and `consumer` to completion on their own threads and
/// returns the wall time between the start and stop barriers.
fn timed_run(producer: impl FnOnce() + Send, consumer: impl FnOnce() + Send) -> Duration {
    let start = Barrier::new(3);
    let stop = Barrier::new(3);
    let (start, stop) = (&start, &stop);

    let mut elapsed = Duration::ZERO;
    thread::scope(|s| {
        s.spawn(move || {
            core_affinity::set_for_current(CoreId { id: 2 });
            start.wait();
            producer();
            stop.wait();
        });
        s.spawn(move || {
            core_affinity::set_for_current(CoreId { id: 3 });
            start.wait();
            consumer();
            stop.wait();
        });

        start.wait();
        let t0 = Instant::now();
        stop.wait();
        elapsed = t0.elapsed();
    });
    elapsed
}

fn bench_ring_buffer(c: &mut Criterion) {
    let mut group = c.benchmark_group("spsc_ring_buffer");
    group.warm_up_time(Duration::from_secs(3));
    group.sample_size(100);

    group.bench_function("unpadded", |b| {
        b.iter_custom(|iters| {
            let (mut tx, mut rx) = unpadded::channel::<usize>(CAPACITY);
            timed_run(
                move || {
                    for i in 0..iters {
                        // Full buffer applies backpressure; spin until a slot frees.
                        while tx.push(i as usize).is_err() {
                            spin_loop();
                        }
                    }
                },
                move || {
                    for _ in 0..iters {
                        while rx.pop().is_none() {
                            spin_loop();
                        }
                    }
                },
            )
        });
    });

    group.bench_function("padded", |b| {
        b.iter_custom(|iters| {
            let (mut tx, mut rx) = padded::channel::<usize>(CAPACITY);
            timed_run(
                move || {
                    for i in 0..iters {
                        while tx.push(i as usize).is_err() {
                            spin_loop();
                        }
                    }
                },
                move || {
                    for _ in 0..iters {
                        while rx.pop().is_none() {
                            spin_loop();
                        }
                    }
                },
            )
        });
    });

    group.finish();
}

criterion_group!(benches, bench_ring_buffer);
criterion_main!(benches);
