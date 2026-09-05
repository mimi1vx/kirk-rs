//! Session runner: `Session::run` over suites with restore, filtering,
//! iterate, randomize, runtime, fault injection, and dry-run support.
//!
//! Ports `kirk/libkirk/session.py`, generic over the scheduler's
//! [`Sut`](kirk_scheduler::Sut) and [`Framework`](kirk_scheduler::Framework)
//! traits.

pub mod session;

#[cfg(test)]
mod tests;

pub use session::{RunOptions, Session, SessionConfig, SessionFramework, SessionSut};
