//! Job schedulers ported from upstream `kirk/libkirk/scheduler.py`.
//!
//! [`TestScheduler`] runs [`Test`](kirk_core::data::Test)
//! definitions and [`SuiteScheduler`] runs
//! [`Suite`](kirk_core::data::Suite) definitions, rebooting the SUT on kernel
//! failures. Both depend only on the minimal [`Sut`] and
//! [`Framework`] traits defined here, never on the
//! `kirk-sut` / `kirk-ltp` crates.

pub mod scheduler;
pub mod suite_sched;
pub mod test_sched;

pub use suite_sched::SuiteScheduler;
pub use test_sched::{Framework, StdoutBuffer, Sut, TestScheduler};
