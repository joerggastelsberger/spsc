# spsc

[![CI](https://github.com/joerggastelsberger/spsc/actions/workflows/ci.yml/badge.svg)](https://github.com/joerggastelsberger/spsc/actions/workflows/ci.yml)

Bounded lock-free SPSC ring buffer. Padding the two indices onto separate cache lines
gives a 1.6x to 1.8x reduction in time per transferred item on an L1-resident workload,
and makes latency substantially more predictable. Nothing else differs between the two
variants.

## Design

- **Split `Producer`/`Consumer` handles, not a shared queue type.** `channel(capacity)`
  returns two non-clonable handles whose methods take `&mut self`, so the
  single-producer/single-consumer contract is enforced by the type system. A shared
  `&self` push/pop API would make a data race reachable from safe code.
- **`UnsafeCell<MaybeUninit<T>>` slots.** `MaybeUninit` avoids writing a default value
  into every slot at construction; `UnsafeCell` is what makes interior writes through a
  shared ring sound to express at all.
- **Unbounded indices, masked on access.** `head` and `tail` grow monotonically
  (wrapping); full is `head - tail == capacity`, empty is `head == tail`. All `capacity`
  slots are usable, no sacrificial empty slot, and the power-of-two requirement turns
  modulo into a mask.
- **Acquire/release pairing, nothing stronger.** The producer's `Release` store of `head`
  publishes the slot write to the consumer's `Acquire` load of `head`; the consumer's
  `Release` store of `tail` hands the slot back to the producer's `Acquire` load of
  `tail`. Each side loads its own index `Relaxed`, since it is the only writer.
- **`#[repr(align(128))]` on each index in the padded variant.** 128 rather than 64:
  x86_64's adjacent-line prefetcher pulls in the neighbouring line, making 128 bytes the
  effective destructive-interference range even though the L1 line is 64 bytes on this
  machine. Crossbeam's `CachePadded` reasons the same way. The unpadded variant is
  byte-for-byte the same code with plain adjacent fields, kept as the control.

## Results

Most recent session, five independent invocations:

| Variant  | Median   | Range            | Spread |
| -------- | -------- | ---------------- | ------ |
| unpadded | 17.25 ns | 16.09 – 21.11 ns | 29.1%  |
| padded   | 9.38 ns  | 9.18 – 10.80 ns  | 17.2%  |

**1.84x, a 46% reduction.** Confidence intervals do not overlap between variants on any
run.

### Variance is the second result

Each variant produced one outlier across the five runs, and padded's full-range spread
above is driven entirely by a single 10.80 ns run. Discarding one outlier from each, the
remaining four spread very differently:

| Variant  | Four-run spread |
| -------- | --------------- |
| padded   | 2.2%            |
| unpadded | 14.8%           |

That asymmetry is not measurement noise. External interference would affect both variants
roughly equally, and it does not. Whether the two cores fall into a pathological
ping-pong rhythm on the contended line varies between runs, so **jitter is intrinsic to
the unpadded case**. Padding buys predictability as well as throughput, and for a
latency-sensitive consumer the tail matters at least as much as the mean.

### Session-to-session drift

Across two independently configured sessions on the same machine, both with the
environment verified as described below:

| Session | padded median | unpadded median | ratio |
| ------- | ------------- | --------------- | ----- |
| 1       | 9.83 ns       | 16.14 ns        | 1.64x |
| 2       | 9.38 ns       | 17.25 ns        | 1.84x |

Medians move about 5% (padded) and 7% (unpadded) between sessions even with the
environment nominally identical. **The ratio is the more robust quantity; treat the
absolute nanosecond figures as approximate.** Quoting a single number to three
significant figures would overstate what this measurement supports.

A third session was discarded entirely: turbo and the powersave governor had silently
reverted across a reboot, so those runs measured a machine that was not configured as
intended.

### Measurement conditions

```
Machine     Lenovo ThinkCentre M910q, Intel i5-6500T @ 2.50 GHz
            4 physical cores, 1 thread per core (no SMT on this part)
            L1d 128 KiB (4 instances), L2 1 MiB (4), L3 6 MiB, 64-byte line
OS          Ubuntu 26.04 LTS, kernel 7.0.0-30-generic
Toolchain   rustc 1.98.0, cargo 1.98.0
Clocks      Turbo disabled (intel_pstate/no_turbo=1), performance governor
            Verified at 2494 MHz under load via cpupower monitor -m Mperf
Isolation   isolcpus=2,3 nohz_full=2,3 rcu_nocbs=2,3
            Producer pinned to core 2, consumer to core 3 via core_affinity
            Cores 2 and 3 at 99.8% C0 during runs; cores 0 and 1 ~98% idle
Memory      ASLR disabled (kernel.randomize_va_space=0)
Thermals    42 degC peak under sustained load against an 84 degC throttle point
Harness     Criterion, 200 samples, 10 s measurement, 3 s warm-up
            Barrier-synchronized iter_custom; thread spawn/join outside the timed region
Buffer      Capacity 1024 (8 KB, L1-resident), spinning producer and consumer
```

`bench-environment.txt` in the repo root is the raw capture.

### Getting the environment quiet

Three problems produced plausible-looking but invalid numbers before this run, none of
them visible in the benchmark output itself:

- A malformed `GRUB_CMDLINE_LINUX_DEFAULT` line meant `isolcpus` was silently swallowed
  as part of another parameter, while `nohz_full` and `rcu_nocbs` did apply. The machine
  ran half-configured.
- The governor, turbo and ASLR settings do not persist across a reboot. A full set of
  runs was collected with turbo enabled and the powersave governor before this was
  noticed.
- `/proc/cpuinfo` and `lscpu -e` report cores 2 and 3 at 800 MHz even during a run. They
  do not sample MPERF/APERF on `intel_pstate` and return a stale idle value.
  `cpupower monitor -m Mperf` reads the counters directly and showed the true 2494 MHz.

Core isolation was the single largest improvement. It lowered both variants in absolute
terms, since the benchmark cores were no longer sharing with kernel work, and it reduced
run-to-run spread. Raising Criterion's sample count and measurement window from the
100/5 s defaults to 200/10 s tightened the padded variant substantially and unpadded
barely at all, which was the first hint that the remaining jitter was intrinsic to
contention rather than environmental.

## Evidence

- The two variants differ only in index alignment. Same algorithm, same orderings, same
  buffer, so the delta isolates the padding.
- A layout test (`padded::tests::head_and_tail_on_separate_cache_lines`) generates the
  layout claim with `mem::offset_of!` rather than asserting it by hand: `head` and `tail`
  are each 128-byte aligned and at least 128 bytes apart.
- **Pending:** `perf c2c` HITM counts per cache line and byte offset. This is the direct
  hardware evidence that the delta is false sharing specifically rather than some other
  consequence of the layout change, and it will be added as a before/after table.

## Correctness

- **loom** model-checks producer/consumer interleavings under the C11 memory model at
  capacity 2 with 3 items, forcing the full, empty, and wraparound paths, and checks
  every `UnsafeCell` access for races plus leak-freedom when handles drop mid-stream.
- **miri** runs the full test suite, including the cross-thread FIFO tests, clean.
- CI runs test, clippy, fmt, loom, and miri on every push.

## Run it

```sh
cargo test                                                # unit + integration
RUSTFLAGS="--cfg loom" cargo test --test loom --release   # model checking
cargo +nightly miri test                                  # UB detection
cargo bench --bench padded_vs_unpadded                    # criterion comparison
```

Reproducing the numbers depends more on the environment than the command. With
`isolcpus=2,3 nohz_full=2,3 rcu_nocbs=2,3` on the kernel command line:

```sh
sudo cpupower frequency-set -g performance
echo 1 | sudo tee /sys/devices/system/cpu/intel_pstate/no_turbo
sudo sysctl -w kernel.randomize_va_space=0
taskset -c 2,3 cargo bench --bench padded_vs_unpadded

# verify actual clocks during the run, from a second shell
sudo cpupower monitor -m Mperf
```

```rust
let (mut tx, mut rx) = spsc::padded::channel::<u64>(1024);
tx.push(7).unwrap();           // Err(value) when full — backpressure, no blocking
assert_eq!(rx.pop(), Some(7)); // None when empty
```

## Limitations / next

- **`perf c2c` evidence not yet collected.** The hardware-counter proof that this is
  false sharing is the obvious gap and the next thing to land.
- **Same-core control not yet run.** Pinning both threads to one core should make the
  padding difference vanish, since there is a single L1 and no coherence traffic. Showing
  the effect appear and disappear on demand is stronger than any ratio.
- **Workload dependence measured only on macOS.** With an 8 MB buffer the two variants
  converged there, as data-cell traffic swamps the index lines. That needs re-measuring
  on this machine before it is quoted.
- **No opposing-index caching.** Each `push` loads `tail` and each `pop` loads `head`
  every call. Caching the last-seen opposing index (as in rigtorp's SPSCQueue) would cut
  coherence traffic further and is the natural next measurement.
- **Fixed power-of-two capacity**, chosen at construction.
- **Spin-only API.** `push`/`pop` return immediately; there is no blocking or waiting
  variant, callers bring their own backoff.
