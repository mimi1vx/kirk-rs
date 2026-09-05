//! Test scheduler ported from `TestScheduler` in `kirk/libkirk/scheduler.py`.
//!
//! Parallel tests run as owned [`JoinSet`] tasks gated
//! by a per-schedule [`Semaphore`]; every task is
//! joined before `schedule` returns, so no task outlives the call. The
//! semaphore is recreated per phase (parallel vs sequential) like upstream,
//! so `stop` waiting on one permit really waits for the running test.
//!
//! Deliberate differences from upstream:
//!
//! * `timeout <= 0.0` (or non-finite) disables the execution timeout instead
//!   of expiring immediately; upstream's default `0.0` only works because the
//!   real tests always pass an explicit timeout.
//! * A `ping` failure reporting [`KirkError::KernelTimeout`] is diagnosed as
//!   a kernel timeout (result recorded, then raised) instead of propagating
//!   with no result; otherwise a hanging SUT would reboot-loop forever at the
//!   suite level without ever completing a test.
//! * Concurrent `schedule` calls serialize on a sequencing mutex held across
//!   the body (same role as upstream `_schedule_lock`); data mutexes are only
//!   ever held for short synchronous sections.
//! * The `asyncio.gather` first-exception race is replaced by a deterministic
//!   drain: all tasks complete, results are all recorded, then the first
//!   kernel error is raised.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use kirk_com::CmdResult;
use kirk_core::KirkError;
use kirk_core::data::Test;
use kirk_core::results::TestResults;
use kirk_events::EventRegistry;
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinSet;
use tokio::time::timeout;

use crate::scheduler::Scheduler;

/// Delay before probing the SUT after a test timeout (mirrors the 10s `ping` arm).
const PING_TIMEOUT: Duration = Duration::from_secs(10);

/// Synthesized return code for kernel failures (mirrors `-signal.SIGKILL` skip logic).
const KILLED_RETURNCODE: i32 = -9;

/// Status codes returned by test execution in the scheduler.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TestStatus {
    /// Test completed without kernel interference.
    Ok,
    /// Test exceeded its execution timeout but the SUT still replies.
    TestTimeout,
    /// Kernel panic detected during the test.
    KernelPanic,
    /// Kernel taint flags changed during the test.
    KernelTainted,
    /// SUT stopped replying after a test timeout.
    KernelTimeout,
}

/// Async stdout capture, mirroring upstream `RedirectTestStdout`.
///
/// Passed into [`Sut::run_command`] so partial output survives the timeout
/// arm that drops the command future.
#[derive(Debug, Clone, Default)]
pub struct StdoutBuffer {
    inner: Arc<Mutex<String>>,
}

impl StdoutBuffer {
    /// Append `data` to the capture.
    pub async fn push(&self, data: &str) {
        self.inner.lock().await.push_str(data);
    }

    /// Current captured contents.
    #[must_use]
    pub async fn snapshot(&self) -> String {
        self.inner.lock().await.clone()
    }
}

/// Minimal SUT surface the scheduler needs.
///
/// `kirk-sut` implements its own richer types concurrently; this trait stays
/// local so neither crate depends on the other.
#[async_trait]
pub trait Sut: Send + Sync {
    /// Current kernel taint code plus messages.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError`] when the SUT cannot be queried.
    async fn get_tainted_info(&self) -> Result<(i64, Vec<String>), KirkError>;

    /// Run `command`, streaming stdout into `capture`.
    ///
    /// Implementations must be cancellation-safe: the future may be dropped
    /// by the execution-timeout arm. Partial stdout at drop time stays
    /// available through `capture`.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::KernelPanic`] carrying partial stdout on kernel
    /// panic, or [`KirkError`] on communication failures.
    async fn run_command(
        &self,
        command: &str,
        cwd: Option<&str>,
        env: &HashMap<String, String>,
        capture: &StdoutBuffer,
    ) -> Result<Option<CmdResult>, KirkError>;

    /// Ping the target, returning round-trip time in seconds.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError`] when the target does not reply.
    async fn ping(&self) -> Result<f64, KirkError>;

    /// Stop the SUT.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError`] when the SUT cannot be stopped.
    async fn stop(&self) -> Result<(), KirkError>;

    /// Restart the SUT.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError`] when the SUT cannot be restarted.
    async fn restart(&self) -> Result<(), KirkError>;

    /// Static SUT information (distro, kernel, arch, ...).
    ///
    /// # Errors
    ///
    /// Returns [`KirkError`] when the information cannot be fetched.
    async fn get_info(&self) -> Result<HashMap<String, String>, KirkError>;

    /// SUT name, used in the `sut_restart` event.
    #[must_use]
    fn name(&self) -> String;
}

/// Minimal framework surface the scheduler needs.
///
/// `kirk-ltp` implements its own richer types concurrently; this trait stays
/// local so neither crate depends on the other.
#[async_trait]
pub trait Framework: Send + Sync {
    /// Build test results from runner output and the test definition.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError`] when results cannot be parsed.
    async fn read_result(
        &self,
        test: &Test,
        stdout: &str,
        retcode: i32,
        exec_time: f64,
    ) -> Result<TestResults, KirkError>;
}

/// Schedule and run tests, tracking kernel status and test timeouts.
#[derive(Clone)]
pub struct TestScheduler<S, F> {
    shared: Arc<Shared<S, F>>,
    events: EventRegistry,
    test_timeout: f64,
    max_workers: usize,
}

struct Shared<S, F> {
    sut: S,
    framework: F,
    results: Mutex<Vec<TestResults>>,
    stop_cnt: AtomicUsize,
    stopped: AtomicBool,
    running_sem: Mutex<Arc<Semaphore>>,
    schedule_lock: Mutex<()>,
}

/// Outcome of one command execution: either a buildable row or a fatal error.
struct ExecOutcome {
    row: Option<CmdResult>,
    status: TestStatus,
    detail: String,
    fatal: Option<KirkError>,
}

fn fatal_outcome(error: KirkError) -> ExecOutcome {
    ExecOutcome {
        row: None,
        status: TestStatus::Ok,
        detail: String::new(),
        fatal: Some(error),
    }
}

/// Best-effort event delivery: upstream fires without awaiting handlers, so a
/// failing registry must not fail test execution.
async fn fire(events: &EventRegistry, name: &str, message: Option<String>) {
    drop(events.fire(name, message).await);
}

impl<S, F> TestScheduler<S, F>
where
    S: Sut + 'static,
    F: Framework + 'static,
{
    /// Create a scheduler. Non-positive or non-finite `test_timeout` disables
    /// the execution timeout; `max_workers < 1` clamps to `1`.
    #[must_use]
    pub fn new(sut: S, framework: F, test_timeout: f64, max_workers: usize) -> Self {
        let test_timeout = if test_timeout.is_finite() && test_timeout > 0.0 {
            test_timeout
        } else {
            0.0
        };
        Self {
            shared: Arc::new(Shared {
                sut,
                framework,
                results: Mutex::new(Vec::new()),
                stop_cnt: AtomicUsize::new(0),
                stopped: AtomicBool::new(false),
                running_sem: Mutex::new(Arc::new(Semaphore::new(1))),
                schedule_lock: Mutex::new(()),
            }),
            events: EventRegistry::new(),
            test_timeout,
            max_workers: max_workers.max(1),
        }
    }

    /// Use `events` instead of the default empty registry.
    #[must_use]
    pub fn with_events(mut self, events: EventRegistry) -> Self {
        self.events = events;
        self
    }

    /// Execution timeout in seconds (`0.0` means disabled).
    #[must_use]
    pub fn test_timeout(&self) -> f64 {
        self.test_timeout
    }

    /// Maximum number of parallel workers.
    #[must_use]
    pub fn max_workers(&self) -> usize {
        self.max_workers
    }

    /// Borrow the SUT handle.
    #[must_use]
    pub fn sut(&self) -> &S {
        &self.shared.sut
    }

    /// Borrow the framework handle.
    #[must_use]
    pub fn framework(&self) -> &F {
        &self.shared.framework
    }

    /// Borrow the event registry.
    #[must_use]
    pub fn events(&self) -> &EventRegistry {
        &self.events
    }

    /// Current results, in completion order.
    pub async fn results(&self) -> Vec<TestResults> {
        self.shared.results.lock().await.clone()
    }

    /// Whether the scheduler has been stopped.
    #[must_use]
    pub fn stopped(&self) -> bool {
        self.shared.stopped.load(Ordering::SeqCst)
    }

    /// Stop tests execution. The first call lets the running test finish; a
    /// second concurrent call forces the SUT down so it finishes immediately.
    pub async fn stop(&self) {
        let count = self.shared.stop_cnt.fetch_add(1, Ordering::SeqCst) + 1;
        if count > 1 {
            drop(self.shared.sut.stop().await);
        }

        // Wait for a running slot, then for the schedule to finish. Each
        // acquisition is released before the next await: no guard is held
        // across an await.
        let sem = self.shared.running_sem.lock().await.clone();
        drop(sem.acquire_owned().await);
        drop(self.shared.schedule_lock.lock().await);

        self.shared.stop_cnt.store(0, Ordering::SeqCst);
        self.shared.stopped.store(true, Ordering::SeqCst);
    }

    /// Schedule and execute tests: parallelizable ones concurrently on up to
    /// `max_workers` workers, the rest sequentially (`max_workers == 1` runs
    /// everything sequentially).
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Scheduler`] when `tests` is empty, or the first
    /// kernel error when no stop was requested.
    #[allow(
        clippy::await_holding_lock,
        reason = "sequencing lock mirroring upstream _schedule_lock; stop() only takes it with \
            nothing else held and in-flight tests never wait on it, so the acquisition order \
            cannot deadlock"
    )]
    pub async fn schedule(&self, tests: &[Test]) -> Result<(), KirkError> {
        if tests.is_empty() {
            return Err(KirkError::Scheduler("jobs list is empty".to_owned()));
        }

        let _sequence = self.shared.schedule_lock.lock().await;
        self.shared.results.lock().await.clear();

        let error = self.drive(tests).await.err();
        // Running tasks already drained (JoinSet joined / loop ended); swallow
        // errors when a stop was requested, mirroring upstream.
        if let Some(error) = error
            && self.shared.stop_cnt.load(Ordering::SeqCst) == 0
        {
            return Err(error);
        }
        Ok(())
    }

    async fn drive(&self, tests: &[Test]) -> Result<(), KirkError> {
        if self.max_workers > 1 {
            let parallel: Vec<Test> = tests
                .iter()
                .filter(|test| test.parallelizable())
                .cloned()
                .collect();
            self.run_parallel(&parallel).await?;
            let sequential: Vec<Test> = tests
                .iter()
                .filter(|test| !test.parallelizable())
                .cloned()
                .collect();
            self.run_sequential(&sequential).await
        } else {
            self.run_sequential(tests).await
        }
    }

    async fn run_parallel(&self, tests: &[Test]) -> Result<(), KirkError> {
        if tests.is_empty() {
            return Ok(());
        }
        let sem = Arc::new(Semaphore::new(self.max_workers));
        *self.shared.running_sem.lock().await = Arc::clone(&sem);

        let mut set: JoinSet<Option<KirkError>> = JoinSet::new();
        for test in tests {
            let shared = Arc::clone(&self.shared);
            let events = self.events.clone();
            let sem = Arc::clone(&sem);
            let owned = test.clone();
            let test_timeout = self.test_timeout;
            set.spawn(async move { run_one(&shared, &events, &sem, test_timeout, owned).await });
        }
        // Deterministic drain: join every task, then report the first error.
        let mut first: Option<KirkError> = None;
        while let Some(outcome) = set.join_next().await {
            match outcome {
                Ok(error) => {
                    if first.is_none() {
                        first = error;
                    }
                }
                Err(join) => {
                    if first.is_none() {
                        first = Some(KirkError::Scheduler(format!("test task failed: {join}")));
                    }
                }
            }
        }
        if let Some(error) = first {
            return Err(error);
        }
        Ok(())
    }

    async fn run_sequential(&self, tests: &[Test]) -> Result<(), KirkError> {
        if tests.is_empty() {
            return Ok(());
        }
        let sem = Arc::new(Semaphore::new(1));
        *self.shared.running_sem.lock().await = Arc::clone(&sem);

        for test in tests {
            if let Some(error) = run_one(
                &self.shared,
                &self.events,
                &sem,
                self.test_timeout,
                test.clone(),
            )
            .await
            {
                return Err(error);
            }
        }
        Ok(())
    }
}

/// Run one test: returns the kernel error to propagate, if any. Completed
/// results are pushed into the shared buffer before returning, so they
/// survive cancellation of the surrounding `schedule` call.
async fn run_one<S, F>(
    shared: &Arc<Shared<S, F>>,
    events: &EventRegistry,
    sem: &Arc<Semaphore>,
    test_timeout: f64,
    test: Test,
) -> Option<KirkError>
where
    S: Sut,
    F: Framework,
{
    if shared.stop_cnt.load(Ordering::SeqCst) > 0 {
        return None;
    }
    let permit = match Arc::clone(sem).acquire_owned().await {
        Ok(permit) => permit,
        Err(error) => {
            return Some(KirkError::Scheduler(format!(
                "worker semaphore closed: {error}"
            )));
        }
    };
    if shared.stop_cnt.load(Ordering::SeqCst) > 0 {
        drop(permit);
        return None;
    }

    fire(events, "test_started", Some(test.name().to_owned())).await;
    write_kmsg(shared, &test, None).await;

    let capture = StdoutBuffer::default();
    let outcome = exec_test(shared, events, test_timeout, &test, &capture).await;

    let Some(row) = outcome.row else {
        drop(permit);
        return outcome.fatal;
    };
    // Tests killed by kirk during a forced stop are dropped silently.
    if row.returncode == KILLED_RETURNCODE && shared.stop_cnt.load(Ordering::SeqCst) > 1 {
        drop(permit);
        return None;
    }
    let parsed = match shared
        .framework
        .read_result(&test, &row.stdout, row.returncode, row.exec_time)
        .await
    {
        Ok(parsed) => parsed,
        Err(error) => {
            drop(permit);
            return Some(error);
        }
    };

    fire(events, "test_completed", Some(test.name().to_owned())).await;
    write_kmsg(shared, &test, Some(&parsed)).await;
    shared.results.lock().await.push(parsed);
    drop(permit);

    // Kernel errors raise after results are collected, mirroring upstream.
    match outcome.status {
        TestStatus::KernelTainted => {
            fire(events, "kernel_tainted", Some(outcome.detail.clone())).await;
            Some(KirkError::KernelTainted(outcome.detail))
        }
        TestStatus::KernelPanic => {
            fire(events, "kernel_panic", None).await;
            Some(KirkError::KernelPanic(outcome.detail))
        }
        TestStatus::KernelTimeout => {
            fire(events, "sut_not_responding", None).await;
            Some(KirkError::KernelTimeout("SUT is not responding".to_owned()))
        }
        TestStatus::Ok | TestStatus::TestTimeout => None,
    }
}

/// Execute the command with pre/post taint checks and the timeout + ping arm.
async fn exec_test<S, F>(
    shared: &Arc<Shared<S, F>>,
    events: &EventRegistry,
    test_timeout: f64,
    test: &Test,
    capture: &StdoutBuffer,
) -> ExecOutcome
where
    S: Sut,
    F: Framework,
{
    let start = Instant::now();

    let tainted_before = match shared.sut.get_tainted_info().await {
        Ok((code, _)) => code,
        Err(KirkError::KernelPanic(partial)) => {
            return ExecOutcome {
                row: None,
                status: TestStatus::KernelPanic,
                detail: partial,
                fatal: None,
            };
        }
        Err(error) => return fatal_outcome(error),
    };

    let command = test.full_command();
    let run = shared
        .sut
        .run_command(&command, test.cwd(), test.env(), capture);
    // A non-positive timeout disables the wrapper; the future is awaited
    // directly. Dropping `run` on expiry is cancel-safe for channels that do
    // not hold unrecoverable state across awaits (documented on Sut).
    let outcome = if test_timeout > 0.0 && test_timeout.is_finite() {
        timeout(Duration::from_secs_f64(test_timeout), run).await
    } else {
        Ok(run.await)
    };

    let mut status = TestStatus::Ok;
    let mut row: Option<CmdResult> = None;
    let mut detail = String::new();

    match outcome {
        Ok(Ok(data)) => {
            row = data;
        }
        Ok(Err(KirkError::KernelPanic(partial))) => {
            status = TestStatus::KernelPanic;
            detail = partial;
        }
        Ok(Err(error)) => return fatal_outcome(error),
        Err(_elapsed) => {
            status = TestStatus::TestTimeout;
            match timeout(PING_TIMEOUT, shared.sut.ping()).await {
                Ok(Ok(_)) => {}
                Ok(Err(KirkError::KernelTimeout(_))) => {
                    status = TestStatus::KernelTimeout;
                }
                Ok(Err(error)) => return fatal_outcome(error),
                Err(_elapsed) => status = TestStatus::KernelTimeout,
            }
        }
    }

    if status == TestStatus::Ok && row.is_none() {
        return fatal_outcome(KirkError::Scheduler("Test command return None".to_owned()));
    }

    if status == TestStatus::Ok {
        match shared.sut.get_tainted_info().await {
            Ok((code, messages)) => {
                if code != tainted_before {
                    status = TestStatus::KernelTainted;
                    detail = messages.join(", ");
                    fire(events, "kernel_tainted", Some(detail.clone())).await;
                }
            }
            Err(KirkError::KernelPanic(partial)) => {
                status = TestStatus::KernelPanic;
                detail = partial;
            }
            Err(error) => return fatal_outcome(error),
        }
    }

    let exec_time = start.elapsed().as_secs_f64();
    let row = match status {
        TestStatus::Ok | TestStatus::KernelTainted => row,
        TestStatus::TestTimeout | TestStatus::KernelPanic | TestStatus::KernelTimeout => {
            let stdout = if detail.is_empty() {
                capture.snapshot().await
            } else {
                detail.clone()
            };
            Some(CmdResult {
                command,
                returncode: -1,
                stdout,
                exec_time,
            })
        }
    };

    ExecOutcome {
        row,
        status,
        detail,
        fatal: None,
    }
}

/// Best-effort `/dev/kmsg` annotation, skipped for non-root users.
async fn write_kmsg<S, F>(shared: &Arc<Shared<S, F>>, test: &Test, parsed: Option<&TestResults>)
where
    S: Sut,
    F: Framework,
{
    let probe = StdoutBuffer::default();
    let root = match shared
        .sut
        .run_command("id -u", None, &HashMap::new(), &probe)
        .await
    {
        Ok(Some(data)) => data.stdout == "0\n",
        Ok(None) | Err(_) => false,
    };
    if !root {
        return;
    }
    let pid = std::process::id();
    let cmd = match parsed {
        Some(parsed) => format!(
            "echo -n \"kirk[{pid}]: {}: end (returncode: {})\" > /dev/kmsg",
            test.name(),
            parsed.return_code()
        ),
        None => format!(
            "echo -n \"kirk[{pid}]: {}: start (command: {})\" > /dev/kmsg",
            test.name(),
            test.full_command()
        ),
    };
    drop(
        shared
            .sut
            .run_command(&cmd, None, &HashMap::new(), &probe)
            .await,
    );
}

#[async_trait]
impl<S, F> Scheduler for TestScheduler<S, F>
where
    S: Sut + 'static,
    F: Framework + 'static,
{
    type Job = Test;
    type Output = TestResults;

    async fn results(&self) -> Vec<Self::Output> {
        TestScheduler::results(self).await
    }

    fn stopped(&self) -> bool {
        TestScheduler::stopped(self)
    }

    async fn stop(&self) {
        TestScheduler::stop(self).await;
    }

    async fn schedule(&self, jobs: &[Self::Job]) -> Result<(), KirkError> {
        TestScheduler::schedule(self, jobs).await
    }
}

#[cfg(test)]
mod tests {
    use kirk_core::results::ResultStatus;

    use super::*;

    struct DummySut;
    struct DummyFramework;

    #[async_trait]
    impl Sut for DummySut {
        async fn get_tainted_info(&self) -> Result<(i64, Vec<String>), KirkError> {
            Ok((0, Vec::new()))
        }

        async fn run_command(
            &self,
            command: &str,
            _cwd: Option<&str>,
            _env: &HashMap<String, String>,
            _capture: &StdoutBuffer,
        ) -> Result<Option<CmdResult>, KirkError> {
            Ok(Some(CmdResult {
                command: command.to_owned(),
                returncode: 0,
                stdout: String::new(),
                exec_time: 0.0,
            }))
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

    #[async_trait]
    impl Framework for DummyFramework {
        async fn read_result(
            &self,
            test: &Test,
            _stdout: &str,
            _retcode: i32,
            _exec_time: f64,
        ) -> Result<TestResults, KirkError> {
            Ok(TestResults::new(test.clone()))
        }
    }

    #[test]
    fn constructor_clamps_timeout_and_workers() {
        let scheduler = TestScheduler::new(DummySut, DummyFramework, -5.0, 0);
        assert!((scheduler.test_timeout() - 0.0).abs() < f64::EPSILON);
        assert_eq!(scheduler.max_workers(), 1);
    }

    #[test]
    fn constructor_keeps_valid_values() {
        let scheduler = TestScheduler::new(DummySut, DummyFramework, 30.0, 4);
        assert!((scheduler.test_timeout() - 30.0).abs() < f64::EPSILON);
        assert_eq!(scheduler.max_workers(), 4);
    }

    #[test]
    fn constructor_rejects_non_finite_timeout() {
        let scheduler = TestScheduler::new(DummySut, DummyFramework, f64::NAN, 2);
        assert!((scheduler.test_timeout() - 0.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn schedule_rejects_empty_jobs() {
        let scheduler = TestScheduler::new(DummySut, DummyFramework, 0.0, 1);
        let error = scheduler.schedule(&[]).await.unwrap_err();
        assert!(matches!(error, KirkError::Scheduler(_)));
    }

    #[tokio::test]
    async fn status_variants_are_distinct() {
        for pair in [
            (TestStatus::Ok, TestStatus::TestTimeout),
            (TestStatus::KernelPanic, TestStatus::KernelTainted),
            (TestStatus::KernelTimeout, TestStatus::Ok),
        ] {
            assert_ne!(pair.0, pair.1);
        }
    }

    #[test]
    fn result_status_codes_match_upstream() {
        assert_eq!(ResultStatus::PASS, 0);
        assert_eq!(ResultStatus::CONF, 32);
    }
}
