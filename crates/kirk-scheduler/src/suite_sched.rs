//! Suite scheduler ported from `SuiteScheduler` in `kirk/libkirk/scheduler.py`.
//!
//! Suites run sequentially; each suite's tests go through the inner
//! [`TestScheduler`]. Kernel failures reboot
//! the SUT via `SuiteScheduler::restart_sut`, and a suite timeout marks the
//! leftover tests `CONF`/skipped with return code 32.
//!
//! Deliberate differences from upstream:
//!
//! * The `reboot_event` rendezvous is dropped: suites already run one at a
//!   time, so the `reboot_lock.locked()` branch is unreachable and every
//!   kernel error reboots directly.
//! * A non-positive `suite_timeout` disables the suite timeout, matching the
//!   `TestScheduler` timeout rule.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use kirk_core::KirkError;
use kirk_core::data::Suite;
use kirk_core::results::{ResultStatus, SuiteResults, TestResults};
use tokio::sync::Mutex;
use tokio::time::timeout;

use crate::scheduler::Scheduler;
use crate::test_sched::{Framework, Sut, TestScheduler};

/// Suite-level state accumulated across (re)scheduled rounds.
struct SuiteLoop {
    exec_times: Vec<f64>,
    tests: Vec<TestResults>,
    timed_out: bool,
}

/// The Scheduler class implementation for suites execution.
pub struct SuiteScheduler<S, F> {
    inner: TestScheduler<S, F>,
    results: Mutex<Vec<SuiteResults>>,
    stop_flag: AtomicBool,
    stopped: AtomicBool,
    schedule_lock: Mutex<()>,
    reboot_lock: Mutex<()>,
    suite_timeout: f64,
}

impl<S, F> SuiteScheduler<S, F>
where
    S: Sut + 'static,
    F: Framework + 'static,
{
    /// Create a scheduler. Non-positive or non-finite timeouts disable the
    /// corresponding timeout; `max_workers < 1` clamps to `1`.
    #[must_use]
    pub fn new(
        sut: S,
        framework: F,
        suite_timeout: f64,
        exec_timeout: f64,
        max_workers: usize,
    ) -> Self {
        let suite_timeout = if suite_timeout.is_finite() && suite_timeout > 0.0 {
            suite_timeout
        } else {
            0.0
        };
        Self {
            inner: TestScheduler::new(sut, framework, exec_timeout, max_workers),
            results: Mutex::new(Vec::new()),
            stop_flag: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
            schedule_lock: Mutex::new(()),
            reboot_lock: Mutex::new(()),
            suite_timeout,
        }
    }

    /// Suite timeout in seconds (`0.0` means disabled).
    #[must_use]
    pub fn suite_timeout(&self) -> f64 {
        self.suite_timeout
    }

    /// Borrow the inner test scheduler.
    #[must_use]
    pub fn test_scheduler(&self) -> &TestScheduler<S, F> {
        &self.inner
    }

    /// Current suite results.
    pub async fn results(&self) -> Vec<SuiteResults> {
        self.results.lock().await.clone()
    }

    /// Whether the scheduler has been stopped.
    #[must_use]
    pub fn stopped(&self) -> bool {
        self.stopped.load(Ordering::SeqCst)
    }

    /// Stop suites execution.
    pub async fn stop(&self) {
        self.stop_flag.store(true, Ordering::SeqCst);
        self.inner.stop().await;
        // Released immediately: no guard is held across an await.
        drop(self.schedule_lock.lock().await);
        self.stop_flag.store(false, Ordering::SeqCst);
        self.stopped.store(true, Ordering::SeqCst);
    }

    /// Schedule and execute a list of suites, one after another.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Scheduler`] when `suites` is empty, or propagates
    /// unexpected (non-kernel, non-timeout) failures.
    #[allow(
        clippy::await_holding_lock,
        reason = "sequencing lock mirroring upstream _schedule_lock; stop() only takes it with \
            nothing else held and in-flight suites never wait on it, so the acquisition order \
            cannot deadlock"
    )]
    pub async fn schedule(&self, suites: &[Suite]) -> Result<(), KirkError> {
        if suites.is_empty() {
            return Err(KirkError::Scheduler("jobs list is empty".to_owned()));
        }

        let _sequence = self.schedule_lock.lock().await;
        self.results.lock().await.clear();

        for suite in suites {
            self.run_suite(suite).await?;
        }
        Ok(())
    }

    /// Restart the SUT after stopping the tests scheduling.
    #[allow(
        clippy::await_holding_lock,
        reason = "reboot sequencing lock mirroring upstream _reboot_lock; suites already run \
            sequentially so no other holder can block progress"
    )]
    async fn restart_sut(&self) -> Result<(), KirkError> {
        let _rebooting = self.reboot_lock.lock().await;
        drop(
            self.inner
                .events()
                .fire("sut_restart", Some(self.inner.sut().name()))
                .await,
        );
        self.inner.stop().await;
        self.inner.sut().restart().await
    }

    async fn run_suite(&self, suite: &Suite) -> Result<(), KirkError> {
        drop(
            self.inner
                .events()
                .fire("suite_started", Some(suite.name().to_owned()))
                .await,
        );
        // Propagates with no suite recorded, mirroring upstream (outside try).
        let info = self.inner.sut().get_info().await?;

        let mut state = SuiteLoop {
            exec_times: Vec::new(),
            tests: Vec::new(),
            timed_out: false,
        };
        let outcome = self.drive_suite(suite, &mut state).await;

        // Suite results are recorded on every path out of the loop,
        // mirroring the upstream finally block.
        let completed = apply_info(suite.clone(), std::mem::take(&mut state.tests), &info);
        drop(
            self.inner
                .events()
                .fire("suite_completed", Some(suite.name().to_owned()))
                .await,
        );
        self.results.lock().await.push(completed);

        outcome
    }

    async fn drive_suite(&self, suite: &Suite, state: &mut SuiteLoop) -> Result<(), KirkError> {
        let mut tests_left: Vec<kirk_core::data::Test> = suite.tests().to_vec();

        while !self.stop_flag.load(Ordering::SeqCst) && !tests_left.is_empty() {
            let start = Instant::now();
            let scheduled = self.inner.schedule(&tests_left);
            // On expiry the inner schedule is dropped (its JoinSet tasks are
            // aborted); already-completed results stay in its buffer.
            let round = if self.suite_timeout > 0.0 && self.suite_timeout.is_finite() {
                match timeout(Duration::from_secs_f64(self.suite_timeout), scheduled).await {
                    Err(_elapsed) => {
                        drop(
                            self.inner
                                .events()
                                .fire("suite_timeout", Some(suite.name().to_owned()))
                                .await,
                        );
                        state.timed_out = true;
                        Ok(())
                    }
                    Ok(round) => round,
                }
            } else {
                scheduled.await
            };

            match round {
                Ok(()) => {}
                Err(
                    KirkError::KernelPanic(_)
                    | KirkError::KernelTainted(_)
                    | KirkError::KernelTimeout(_),
                ) => {
                    self.restart_sut().await?;
                }
                Err(error) => return Err(error),
            }

            state.exec_times.push(start.elapsed().as_secs_f64());
            state.tests.extend(self.inner.results().await);

            let done: HashSet<&str> = state
                .tests
                .iter()
                .map(|results| results.test().name())
                .collect();
            tests_left = suite
                .tests()
                .iter()
                .filter(|test| !done.contains(test.name()))
                .cloned()
                .collect();

            if state.timed_out {
                for test in &tests_left {
                    state.tests.push(
                        TestResults::new(test.clone())
                            .with_failed(0)
                            .with_passed(0)
                            .with_broken(0)
                            .with_skipped(1)
                            .with_warnings(0)
                            .with_exec_time(0.0)
                            .with_retcode(32)
                            .with_stdout("")
                            .with_status(ResultStatus::CONF),
                    );
                }
                tests_left.clear();
                break;
            }
        }
        Ok(())
    }
}

fn apply_info(
    suite: Suite,
    tests: Vec<TestResults>,
    info: &HashMap<String, String>,
) -> SuiteResults {
    let mut completed = SuiteResults::new(suite).with_tests(tests);
    if let Some(value) = info.get("distro") {
        completed = completed.with_distro(value);
    }
    if let Some(value) = info.get("distro_ver") {
        completed = completed.with_distro_ver(value);
    }
    if let Some(value) = info.get("kernel") {
        completed = completed.with_kernel(value);
    }
    if let Some(value) = info.get("cmdline") {
        completed = completed.with_cmdline(value);
    }
    if let Some(value) = info.get("arch") {
        completed = completed.with_arch(value);
    }
    if let Some(value) = info.get("cpu") {
        completed = completed.with_cpu(value);
    }
    if let Some(value) = info.get("swap") {
        completed = completed.with_swap(value);
    }
    if let Some(value) = info.get("ram") {
        completed = completed.with_ram(value);
    }
    completed
}

#[async_trait::async_trait]
impl<S, F> Scheduler for SuiteScheduler<S, F>
where
    S: Sut + 'static,
    F: Framework + 'static,
{
    type Job = Suite;
    type Output = SuiteResults;

    async fn results(&self) -> Vec<Self::Output> {
        SuiteScheduler::results(self).await
    }

    fn stopped(&self) -> bool {
        SuiteScheduler::stopped(self)
    }

    async fn stop(&self) {
        SuiteScheduler::stop(self).await;
    }

    async fn schedule(&self, jobs: &[Self::Job]) -> Result<(), KirkError> {
        SuiteScheduler::schedule(self, jobs).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constructor_clamps_suite_timeout() {
        struct DummySut;
        struct DummyFramework;

        #[async_trait::async_trait]
        impl Sut for DummySut {
            async fn get_tainted_info(&self) -> Result<(i64, Vec<String>), KirkError> {
                Ok((0, Vec::new()))
            }

            async fn run_command(
                &self,
                _command: &str,
                _cwd: Option<&str>,
                _env: &HashMap<String, String>,
                _capture: &crate::test_sched::StdoutBuffer,
            ) -> Result<Option<kirk_com::CmdResult>, KirkError> {
                Ok(None)
            }

            async fn ping(&self) -> Result<f64, KirkError> {
                Ok(0.0)
            }

            async fn stop(&self) -> Result<(), KirkError> {
                Ok(())
            }

            async fn restart(&self) -> Result<(), KirkError> {
                Ok(())
            }

            async fn get_info(&self) -> Result<HashMap<String, String>, KirkError> {
                Ok(HashMap::new())
            }

            fn name(&self) -> String {
                String::from("dummy")
            }
        }

        #[async_trait::async_trait]
        impl Framework for DummyFramework {
            async fn read_result(
                &self,
                test: &kirk_core::data::Test,
                _stdout: &str,
                _retcode: i32,
                _exec_time: f64,
            ) -> Result<TestResults, KirkError> {
                Ok(TestResults::new(test.clone()))
            }
        }

        let scheduler = SuiteScheduler::new(DummySut, DummyFramework, -1.0, -2.0, 0);
        assert!((scheduler.suite_timeout() - 0.0).abs() < f64::EPSILON);
        assert!((scheduler.test_scheduler().test_timeout() - 0.0).abs() < f64::EPSILON);
        assert_eq!(scheduler.test_scheduler().max_workers(), 1);
    }
}
