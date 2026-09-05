//! In-process fakes mirroring `test_scheduler.py` mocks.
//!
//! `FakeSut` simulates a shell target (`echo`/`sleep`, `id -u` reporting a
//! non-root user, kernel-panic output raising [`KirkError::KernelPanic`]);
//! `FakeFramework` applies LTP-legacy-inspired counters (the real parser
//! lives in `kirk-ltp`, written concurrently). Clones share state so tests
//! can observe restarts and concurrency after the scheduler owns its handle.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use kirk_com::CmdResult;
use kirk_core::KirkError;
use kirk_core::data::Test;
use kirk_core::results::{ResultStatus, TestResults};
use kirk_scheduler::{Framework, StdoutBuffer, Sut};

/// Shell operators are out of scope for the fake; commands are `echo ...`,
/// `sleep <secs>`, `id -u`, or contain `Kernel panic`. Kept small (but still
/// ~1500x the 0.02s test timeout) so a missed timeout arm hangs CI for
/// seconds, not an hour.
const HANG_SECS: u64 = 30;

#[derive(Debug, Default)]
struct FakeState {
    concurrent: AtomicUsize,
    max_concurrent: AtomicUsize,
    restarts: AtomicUsize,
    stops: AtomicUsize,
    taint_calls: AtomicUsize,
}

/// Fake SUT handle with shared observable state.
#[derive(Debug, Clone, Default)]
pub struct FakeSut {
    state: Arc<FakeState>,
    taint_mode: Arc<AtomicBool>,
    hang: Arc<AtomicBool>,
}

impl FakeSut {
    /// Normally behaving SUT.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Every call returns a fresh code (like the upstream test popping
    /// `TAINTED_MSG`), so each post-execution check observes a change.
    #[must_use]
    pub fn tainting() -> Self {
        Self {
            taint_mode: Arc::new(AtomicBool::new(true)),
            ..Self::default()
        }
    }

    /// `run_command` hangs (until the scheduler timeout) and `ping` reports
    /// a kernel timeout.
    #[must_use]
    pub fn hanging() -> Self {
        Self {
            hang: Arc::new(AtomicBool::new(true)),
            ..Self::default()
        }
    }

    /// Number of `restart` calls observed.
    #[must_use]
    pub fn restarts(&self) -> usize {
        self.state.restarts.load(Ordering::SeqCst)
    }

    /// Number of `stop` calls observed.
    #[must_use]
    pub fn stops(&self) -> usize {
        self.state.stops.load(Ordering::SeqCst)
    }

    /// Peak concurrent `run_command` executions observed.
    #[must_use]
    pub fn max_concurrent(&self) -> usize {
        self.state.max_concurrent.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl Sut for FakeSut {
    async fn get_tainted_info(&self) -> Result<(i64, Vec<String>), KirkError> {
        if !self.taint_mode.load(Ordering::SeqCst) {
            return Ok((0, vec![String::new()]));
        }
        let code = self.state.taint_calls.fetch_add(1, Ordering::SeqCst);
        if code == 0 {
            Ok((0, vec![String::new()]))
        } else {
            let code = i64::try_from(code).unwrap_or(i64::MAX);
            Ok((code, vec![String::from("proprietary module was loaded")]))
        }
    }

    async fn run_command(
        &self,
        command: &str,
        _cwd: Option<&str>,
        _env: &HashMap<String, String>,
        capture: &StdoutBuffer,
    ) -> Result<Option<CmdResult>, KirkError> {
        if command == "id -u" {
            return Ok(Some(CmdResult {
                command: command.to_owned(),
                returncode: 0,
                stdout: String::from("1\n"),
                exec_time: 0.0,
            }));
        }
        if self.hang.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_secs(HANG_SECS)).await;
            return Ok(Some(CmdResult {
                command: command.to_owned(),
                returncode: 0,
                stdout: String::new(),
                exec_time: f64::from(u32::try_from(HANG_SECS).unwrap_or(u32::MAX)),
            }));
        }
        if command.contains("Kernel panic") {
            capture.push("Kernel panic\n").await;
            return Err(KirkError::KernelPanic(String::from("Kernel panic\n")));
        }

        let current = self.state.concurrent.fetch_add(1, Ordering::SeqCst) + 1;
        self.state
            .max_concurrent
            .fetch_max(current, Ordering::SeqCst);
        let outcome = if let Some(rest) = command.strip_prefix("sleep ") {
            let secs: f64 = rest
                .split_whitespace()
                .next()
                .unwrap_or("0")
                .parse()
                .unwrap_or(0.0);
            let secs = secs.max(0.0);
            let start = Instant::now();
            tokio::time::sleep(Duration::from_secs_f64(secs)).await;
            Ok(Some(CmdResult {
                command: command.to_owned(),
                returncode: 0,
                stdout: String::new(),
                exec_time: start.elapsed().as_secs_f64(),
            }))
        } else {
            let start = Instant::now();
            let stdout = command
                .strip_prefix("echo ")
                .unwrap_or("")
                .split_whitespace()
                .filter(|word| *word != "-n")
                .collect::<Vec<_>>()
                .join(" ");
            // Keep a measurable (but tiny) execution time like a real spawn.
            tokio::task::yield_now().await;
            Ok(Some(CmdResult {
                command: command.to_owned(),
                returncode: 0,
                stdout,
                exec_time: start.elapsed().as_secs_f64().max(f64::EPSILON),
            }))
        };
        self.state.concurrent.fetch_sub(1, Ordering::SeqCst);
        outcome
    }

    async fn ping(&self) -> Result<f64, KirkError> {
        if self.hang.load(Ordering::SeqCst) {
            return Err(KirkError::KernelTimeout(String::from(
                "SUT is not responding",
            )));
        }
        Ok(0.001)
    }

    async fn stop(&self) -> Result<(), KirkError> {
        self.state.stops.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn restart(&self) -> Result<(), KirkError> {
        self.state.restarts.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn get_info(&self) -> Result<HashMap<String, String>, KirkError> {
        Ok(HashMap::from([
            (String::from("distro"), String::from("openSUSE")),
            (String::from("distro_ver"), String::from("15.3")),
            (String::from("kernel"), String::from("5.10")),
            (String::from("cmdline"), String::from("ima_policy=tcb")),
            (String::from("arch"), String::from("x86_64")),
            (String::from("cpu"), String::from("x86_64")),
            (String::from("swap"), String::from("0")),
            (String::from("ram"), String::from("1M")),
        ]))
    }

    fn name(&self) -> String {
        String::from("fake-sut")
    }
}

/// Fake framework with LTP-legacy-inspired counters.
#[derive(Debug, Clone, Default)]
pub struct FakeFramework;

impl FakeFramework {
    /// Create a fake framework.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Framework for FakeFramework {
    async fn read_result(
        &self,
        test: &Test,
        stdout: &str,
        retcode: i32,
        exec_time: f64,
    ) -> Result<TestResults, KirkError> {
        let passed = u32::try_from(stdout.matches("TPASS").count()).unwrap_or(0);
        let failed = u32::try_from(stdout.matches("TFAIL").count()).unwrap_or(0);
        let skipped = u32::try_from(stdout.matches("TSKIP").count()).unwrap_or(0);
        let broken = u32::try_from(stdout.matches("TBROK").count()).unwrap_or(0);
        let warnings = u32::try_from(stdout.matches("TWARN").count()).unwrap_or(0);

        let mut results = TestResults::new(test.clone())
            .with_stdout(stdout)
            .with_retcode(retcode)
            .with_exec_time(exec_time);
        if passed + failed + skipped + broken + warnings > 0 {
            let status = if failed > 0 || broken > 0 {
                ResultStatus::FAIL
            } else {
                ResultStatus::PASS
            };
            results = results
                .with_passed(passed)
                .with_failed(failed)
                .with_skipped(skipped)
                .with_broken(broken)
                .with_warnings(warnings)
                .with_status(status);
        } else {
            // Legacy test: derive counters from the return code alone.
            match retcode {
                0 => {
                    results = results.with_passed(1).with_status(ResultStatus::PASS);
                }
                4 => {
                    results = results.with_warnings(1).with_status(ResultStatus::WARN);
                }
                32 => {
                    results = results.with_skipped(1).with_status(ResultStatus::CONF);
                }
                -1 => {
                    results = results.with_broken(1).with_status(ResultStatus::BROK);
                }
                _ => {
                    results = results.with_failed(1).with_status(ResultStatus::FAIL);
                }
            }
        }
        Ok(results)
    }
}

/// `echo -n ciao` test, parallelizable.
///
/// # Panics
///
/// Panics when the hardcoded name or command is rejected (never happens).
#[must_use]
pub fn echo_test(index: usize) -> Test {
    Test::new(&format!("test{index}"), "echo")
        .expect("test name and command are non-empty")
        .with_args(vec![String::from("-n"), String::from("ciao")])
        .with_parallelizable(true)
}

/// `sleep <secs>` test, parallelizable.
///
/// # Panics
///
/// Panics when the hardcoded name or command is rejected (never happens).
#[must_use]
pub fn sleep_test(index: usize, secs: &str) -> Test {
    Test::new(&format!("test{index}"), "sleep")
        .expect("test name and command are non-empty")
        .with_args(vec![String::from(secs)])
        .with_parallelizable(true)
}
