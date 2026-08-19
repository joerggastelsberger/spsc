//! Bounded lock-free single-producer single-consumer ring buffer.
//!
//! Two deliberately near-identical variants:
//!
//! - [`padded`]: producer and consumer indices aligned to separate cache
//!   lines. Use this one.
//! - [`unpadded`]: indices adjacent, sharing a line. Exists as the control in
//!   the false-sharing benchmark.
//!
//! Both expose `channel(capacity) -> (Producer, Consumer)`; the handles are
//! not clonable and their methods take `&mut self`, so the SPSC contract is
//! enforced by the type system rather than by documentation.

mod sync;

pub mod padded;
pub mod unpadded;
