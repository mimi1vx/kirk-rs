//! Common scheduler interface mirroring upstream `Scheduler`.

use async_trait::async_trait;
use kirk_core::KirkError;

/// Schedule jobs to run on target.
#[async_trait]
pub trait Scheduler {
    /// Job definition accepted by [`Scheduler::schedule`].
    type Job;
    /// Result record produced per completed job.
    type Output;

    /// Current results, reset before every [`Scheduler::schedule`] call.
    async fn results(&self) -> Vec<Self::Output>;

    /// Whether the scheduler has been stopped.
    fn stopped(&self) -> bool;

    /// Stop all running jobs.
    async fn stop(&self);

    /// Schedule and execute a list of jobs.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Scheduler`] when `jobs` is empty, or the kernel
    /// error that interrupted execution when no stop was requested.
    async fn schedule(&self, jobs: &[Self::Job]) -> Result<(), KirkError>;
}
