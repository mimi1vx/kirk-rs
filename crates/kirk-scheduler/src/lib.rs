//! Job schedulers ported from upstream `kirk/libkirk/scheduler.py`.
//!
//! [`TestScheduler`](test_sched::TestScheduler) runs [`Test`](kirk_core::data::Test)
//! definitions and [`SuiteScheduler`](suite_sched::SuiteScheduler) runs
//! [`Suite`](kirk_core::data::Suite) definitions, rebooting the SUT on kernel
//! failures. Both depend only on the minimal [`Sut`](test_sched::Sut) and
//! [`Framework`](test_sched::Framework) traits defined here, never on the
//! `kirk-sut` / `kirk-ltp` crates.

pub mod scheduler;
pub mod suite_sched;
pub mod test_sched;

pub use scheduler::Scheduler;
pub use suite_sched::SuiteScheduler;
pub use test_sched::{Framework, StdoutBuffer, Sut, TestScheduler, TestStatus};
