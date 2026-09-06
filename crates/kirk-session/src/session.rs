//! Session runner ported from `kirk/libkirk/session.py`.
//!
//! [`Session`] drives a [`SuiteScheduler`]
//! over selected suites: executed-file restore/skip, regex test filtering,
//! iterate renaming, randomize/runtime/fault-injection/dry-run options, and
//! `results.json` plus report export.
//!
//! The session is generic over the scheduler's own [`Sut`] and [`Framework`]
//! traits (extended by the minimal [`SessionSut`] and [`SessionFramework`]
//! traits for suite discovery, SUT lifecycle, and fault injection), so the
//! real `kirk-sut`/`kirk-ltp` types can be wired in without new glue.
//!
//! # Security
//!
//! Restore, report, and monitor-adjacent paths are traversal-checked
//! (canonicalize + containment). Secrets are never logged: errors carry
//! short static descriptions, never file contents or environment values.
//!
//! [`Sut`]: kirk_scheduler::Sut
//! [`Framework`]: kirk_scheduler::Framework

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use kirk_com::CmdResult;
use kirk_core::KirkError;
use kirk_core::data::{Suite, Test};
use kirk_core::results::SuiteResults;
use kirk_events::EventRegistry;
use kirk_scheduler::{Framework, SuiteScheduler, Sut};
use tokio::sync::Mutex;
use tokio::time::timeout;

use kirk_support::{AsyncFile, JSONExporter, TempDir};

/// Extra SUT surface the session needs beyond [`Sut`].
#[async_trait]
pub trait SessionSut: Sut {
    /// Start communicating with the SUT.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError`] when the SUT cannot start.
    async fn session_start(&self) -> Result<(), KirkError>;

    /// Stop the SUT.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError`] when the SUT cannot stop.
    async fn session_stop(&self) -> Result<(), KirkError>;

    /// Whether the SUT is running.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError`] when the state cannot be read.
    async fn session_is_running(&self) -> Result<bool, KirkError>;

    /// Whether the SUT channel supports parallel execution.
    fn session_parallel_execution(&self) -> bool;

    /// Whether the SUT session runs as root.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError`] when the user id cannot be determined.
    async fn session_logged_as_root(&self) -> Result<bool, KirkError>;

    /// Whether kernel fault injection knobs exist.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError`] when the check fails.
    async fn session_fault_enabled(&self) -> Result<bool, KirkError>;

    /// Configure kernel fault injection (`prob == 0` resets to defaults).
    ///
    /// # Errors
    ///
    /// Returns [`KirkError`] when the knobs reject their values.
    async fn session_setup_fault(&self, prob: u32, interval: u32) -> Result<(), KirkError>;

    /// Run one command, returning its result.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError`] on communication failures.
    async fn session_run_command(
        &self,
        full_command: &str,
        cwd: Option<&str>,
        env: &HashMap<String, String>,
    ) -> Result<Option<CmdResult>, KirkError>;
}

/// Extra framework surface the session needs beyond [`Framework`].
#[async_trait]
pub trait SessionFramework: Framework {
    /// Find the suite `name` inside the SUT.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError`] when the suite cannot be found.
    async fn session_find_suite(&self, name: &str) -> Result<Suite, KirkError>;

    /// Build an executable [`Test`] for a single `command`.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError`] when the command is invalid.
    async fn session_find_command(&self, command: &str) -> Result<Test, KirkError>;
}

/// Options for [`Session::run`], mirroring the Python keyword arguments.
#[derive(Debug, Clone, Default)]
pub struct RunOptions {
    /// Single command to run before suites.
    pub command: Option<String>,
    /// Suites to execute.
    pub suites: Vec<String>,
    /// Regex keeping only matching tests.
    pub pattern: Option<String>,
    /// Regex skipping matching tests.
    pub skip_tests: Option<String>,
    /// JSON report path.
    pub report_path: Option<String>,
    /// Previous session folder to restore (skips already executed tests).
    pub restore_path: Option<String>,
    /// Execute all suites this many times (`<= 1` means once).
    pub suite_iterate: usize,
    /// Shuffle tests before scheduling.
    pub randomize: bool,
    /// Run for this many seconds (`<= 0` means once).
    pub runtime: f64,
    /// Fault injection probability (`0` disables).
    pub fault_prob: u32,
    /// Fault injection interval.
    pub fault_interval: u32,
    /// List selected tests without executing them.
    pub dry_run: bool,
}

/// Construction parameters for [`Session`].
#[derive(Debug, Clone, Copy)]
pub struct SessionConfig {
    /// Per-test execution timeout in seconds.
    pub exec_timeout: f64,
    /// Per-suite timeout in seconds.
    pub suite_timeout: f64,
    /// Scheduler worker count.
    pub workers: usize,
    /// Force every test to run in parallel.
    pub force_parallel: bool,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            exec_timeout: 3600.0,
            suite_timeout: 3600.0,
            workers: 1,
            force_parallel: false,
        }
    }
}

/// The session runner.
pub struct Session<S, F> {
    scheduler: SuiteScheduler<S, F>,
    events: EventRegistry,
    tmpdir: TempDir,
    exec_timeout: f64,
    force_parallel: bool,
    stop_flag: AtomicBool,
    run_lock: Mutex<()>,
    exec_lock: Mutex<()>,
    results: Mutex<Vec<SuiteResults>>,
}

impl<S, F> Session<S, F>
where
    S: SessionSut + 'static,
    F: SessionFramework + 'static,
{
    /// Create a session. Non-positive or non-finite timeouts disable the
    /// corresponding timeout; `workers < 1` clamps to `1` (via the
    /// scheduler).
    pub fn new(
        tmpdir: TempDir,
        sut: S,
        framework: F,
        events: EventRegistry,
        config: SessionConfig,
    ) -> Self {
        Self {
            scheduler: SuiteScheduler::new(
                sut,
                framework,
                config.suite_timeout,
                config.exec_timeout,
                config.workers,
            ),
            events,
            tmpdir,
            exec_timeout: config.exec_timeout,
            force_parallel: config.force_parallel,
            stop_flag: AtomicBool::new(false),
            run_lock: Mutex::new(()),
            exec_lock: Mutex::new(()),
            results: Mutex::new(Vec::new()),
        }
    }

    /// Temporary directory of the session.
    #[must_use]
    pub fn tmpdir(&self) -> &TempDir {
        &self.tmpdir
    }

    /// Filter suite tests by `regex`: keep matching tests, or keep
    /// non-matching ones when `when_matching` is set (skip filter).
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Session`] when `regex` does not compile.
    pub(crate) fn filter_tests(
        suites: &mut [Suite],
        regex: Option<&str>,
        when_matching: bool,
    ) -> Result<(), KirkError> {
        let Some(regex) = regex.filter(|r| !r.is_empty()) else {
            return Ok(());
        };
        let matcher = regex::Regex::new(regex)
            .map_err(|err| KirkError::Session(format!("invalid test filter: {err}")))?;
        for suite in &mut *suites {
            let kept: Vec<Test> = suite
                .tests()
                .iter()
                .filter(|test| matcher.is_match(test.name()) != when_matching)
                .cloned()
                .collect();
            let name = suite.name().to_owned();
            *suite = Suite::new(&name, kept);
        }
        Ok(())
    }

    /// Rename suites for repeated execution: `suite[i]` per iteration.
    /// `iterate <= 1` returns the input unchanged.
    #[must_use]
    pub(crate) fn apply_iterate(suites: Vec<Suite>, iterate: usize) -> Vec<Suite> {
        if iterate <= 1 {
            return suites;
        }
        let mut out = Vec::with_capacity(suites.len() * iterate);
        for suite in &suites {
            for i in 0..iterate {
                out.push(Suite::new(
                    &format!("{}[{i}]", suite.name()),
                    suite.tests().to_vec(),
                ));
            }
        }
        out
    }

    /// Read the `executed` file of a previous session, traversal-checked.
    async fn read_restored_session(
        &self,
        path: Option<&str>,
    ) -> Result<HashMap<String, Vec<String>>, KirkError> {
        let mut data: HashMap<String, Vec<String>> = HashMap::new();
        let Some(path) = path.filter(|p| !p.is_empty()) else {
            return Ok(data);
        };
        let owned = path.to_owned();
        let canonical =
            tokio::task::spawn_blocking(move || std::path::PathBuf::from(&owned).canonicalize())
                .await
                .map_err(|err| KirkError::Session(format!("can't resolve restore path: {err}")))?;
        let Ok(canonical) = canonical else {
            return Ok(data);
        };
        let executed = canonical.join("executed");
        if !tokio::fs::try_exists(&executed).await.unwrap_or(false) {
            return Ok(data);
        }
        let mut file = AsyncFile::new(&executed.to_string_lossy(), "r");
        file.open()
            .await
            .map_err(|err| KirkError::Session(err.to_string()))?;
        loop {
            let line = file
                .next_line()
                .await
                .map_err(|err| KirkError::Session(err.to_string()))?;
            let Some(line) = line else { break };
            let Some((suite, test)) = line.split_once("::") else {
                continue;
            };
            if suite.is_empty() || test.trim_end().is_empty() {
                continue;
            }
            data.entry(suite.to_owned())
                .or_default()
                .push(test.trim_end().to_owned());
        }
        file.close().await;
        Ok(data)
    }

    /// Best-effort event delivery: a failing registry never fails the run.
    async fn fire(&self, name: &str, message: Option<String>) {
        drop(self.events.fire(name, message).await);
    }

    fn sut(&self) -> &S {
        self.scheduler.test_scheduler().sut()
    }

    fn framework(&self) -> &F {
        self.scheduler.test_scheduler().framework()
    }

    async fn start_sut(&self) -> Result<(), KirkError> {
        let sut = self.sut();
        self.fire("sut_start", Some(sut.name())).await;
        sut.session_start().await
    }

    async fn stop_sut(&self) -> Result<(), KirkError> {
        let sut = self.sut();
        if !sut.session_is_running().await? {
            return Ok(());
        }
        self.fire("sut_stop", Some(sut.name())).await;
        sut.session_stop().await
    }

    async fn get_suites_objects(&self, names: &[String]) -> Result<Vec<Suite>, KirkError> {
        if names.is_empty() {
            return Err(KirkError::Framework(format!(
                "can't find suites: {names:?}"
            )));
        }
        let mut suites = Vec::with_capacity(names.len());
        for name in names {
            let suite = self.framework().session_find_suite(name).await?;
            suites.push(suite);
        }
        Ok(suites)
    }

    async fn restore_tests(
        &self,
        suites: &mut [Suite],
        restore_path: Option<&str>,
    ) -> Result<(), KirkError> {
        let Some(path) = restore_path.filter(|p| !p.is_empty()) else {
            return Ok(());
        };
        let restored = self.read_restored_session(Some(path)).await?;
        if restored.is_empty() {
            return Ok(());
        }
        self.fire("session_restore", Some(path.to_owned())).await;
        for suite in &mut *suites {
            let Some(done) = restored.get(suite.name()) else {
                continue;
            };
            let kept: Vec<Test> = suite
                .tests()
                .iter()
                .filter(|test| !done.iter().any(|name| name == test.name()))
                .cloned()
                .collect();
            let name = suite.name().to_owned();
            *suite = Suite::new(&name, kept);
        }
        Ok(())
    }

    async fn read_suites(&self, opts: &RunOptions) -> Result<Vec<Suite>, KirkError> {
        let mut suites = self.get_suites_objects(&opts.suites).await?;
        self.restore_tests(&mut suites, opts.restore_path.as_deref())
            .await?;
        Self::filter_tests(&mut suites, opts.pattern.as_deref(), false)?;
        Self::filter_tests(&mut suites, opts.skip_tests.as_deref(), true)?;

        if suites.iter().map(|s| s.tests().len()).sum::<usize>() == 0 {
            return Err(KirkError::Session(String::from("no tests selected")));
        }
        if self.force_parallel {
            for suite in &mut *suites {
                let mut tests = suite.tests().to_vec();
                for test in &mut tests {
                    test.force_parallel();
                }
                let name = suite.name().to_owned();
                *suite = Suite::new(&name, tests);
            }
        }
        Ok(suites)
    }

    async fn exec_command(&self, command: &str) -> Result<(), KirkError> {
        let _guard = self.exec_lock.lock().await;
        if self.stop_flag.load(Ordering::SeqCst) {
            return Ok(());
        }
        self.fire("run_cmd_start", Some(command.to_owned())).await;
        let test = self.framework().session_find_command(command).await?;
        let full = test.full_command();
        let (cwd, env) = (test.cwd().map(str::to_owned), test.env().clone());
        let run = self.sut().session_run_command(&full, cwd.as_deref(), &env);
        let row = if self.exec_timeout.is_finite() && self.exec_timeout > 0.0 {
            match timeout(Duration::from_secs_f64(self.exec_timeout), run).await {
                Err(_) => {
                    return Err(KirkError::Session(format!("command timeout: {command:?}")));
                }
                Ok(row) => row?,
            }
        } else {
            run.await?
        };
        let Some(row) = row else {
            return Err(KirkError::Session(format!(
                "can't execute command '{full}'"
            )));
        };
        self.fire(
            "run_cmd_stop",
            Some(format!("{}\n{}\n{}", command, row.stdout, row.returncode)),
        )
        .await;
        Ok(())
    }

    async fn apply_fault_injection(&self, fault_prob: u32, fault_interval: u32) {
        let sut = self.sut();
        let warn = match sut.session_logged_as_root().await {
            Ok(false) if fault_prob != 0 => {
                Some(String::from("run as root to use kernel fault injection"))
            }
            Err(_) | Ok(false) => None,
            Ok(true) => match sut.session_fault_enabled().await {
                Ok(true) => {
                    if sut
                        .session_setup_fault(fault_prob, fault_interval)
                        .await
                        .is_err()
                    {
                        Some(String::from("can't setup kernel fault injection"))
                    } else {
                        None
                    }
                }
                Err(_) | Ok(false) if fault_prob != 0 => Some(String::from(
                    "fault injection is not enabled. running tests normally",
                )),
                Err(_) | Ok(false) => None,
            },
        };
        if let Some(msg) = warn {
            self.fire("session_warning", Some(msg)).await;
        }
    }

    async fn append_executed(&self, results: &[SuiteResults]) {
        if self.tmpdir.abspath().as_os_str().is_empty() || results.is_empty() {
            return;
        }
        let path = self.tmpdir.abspath().join("executed");
        let mut file = AsyncFile::new(&path.to_string_lossy(), "a");
        if file.open().await.is_err() {
            return;
        }
        for suite_results in results {
            for test_results in suite_results.tests_results() {
                let line = format!(
                    "{}::{}\n",
                    suite_results.suite().name(),
                    test_results.test().name()
                );
                if file.write(&line).await.is_err() {
                    break;
                }
            }
        }
        file.close().await;
    }

    async fn schedule_once(&self, suites: &[Suite]) -> Result<(), KirkError> {
        let outcome = self.scheduler.schedule(suites).await;
        let fresh = self.scheduler.results().await;
        self.results.lock().await.extend(fresh.clone());
        self.append_executed(&fresh).await;
        outcome
    }

    async fn schedule_infinite(&self, suites: &[Suite]) -> Result<(), KirkError> {
        let mut count = 0usize;
        while !self.stop_flag.load(Ordering::SeqCst) {
            let mut round = Vec::with_capacity(suites.len());
            for suite in suites {
                round.push(Suite::new(
                    &format!("{}[{count}]", suite.name()),
                    suite.tests().to_vec(),
                ));
            }
            self.schedule_once(&round).await?;
            if self.scheduler.stopped() {
                break;
            }
            count += 1;
        }
        Ok(())
    }

    async fn run_scheduler(&self, suites: &[Suite], runtime: f64) -> Result<(), KirkError> {
        if runtime.is_finite() && runtime > 0.0 {
            let _ = timeout(
                Duration::from_secs_f64(runtime),
                self.schedule_infinite(suites),
            )
            .await;
            self.scheduler.stop().await;
            return Ok(());
        }
        self.schedule_once(suites).await
    }

    /// Stop the scheduler and the SUT (best-effort teardown).
    async fn inner_stop(&self) {
        self.scheduler.stop().await;
        let _ = self.stop_sut().await;
    }

    /// Stop the current session.
    pub async fn stop(&self) {
        let already_stopped = self.stop_flag.swap(true, Ordering::SeqCst);
        self.inner_stop().await;
        // Wait for an in-flight run, mirroring the `_run_lock` rendezvous.
        let _run = self.run_lock.lock().await;
        let _exec = self.exec_lock.lock().await;
        if !already_stopped {
            self.fire("session_stopped", None).await;
        }
        self.stop_flag.store(false, Ordering::SeqCst);
    }

    async fn run_inner(&self, opts: &RunOptions) -> Result<(), KirkError> {
        self.fire(
            "session_started",
            Some(format!(
                "{}\n{}",
                opts.suites.len(),
                self.tmpdir.abspath().display()
            )),
        )
        .await;
        if !self.sut().session_parallel_execution() {
            self.fire(
                "session_warning",
                Some(String::from("SUT doesn't support parallel execution")),
            )
            .await;
        }
        if opts.dry_run
            && let Some(command) = &opts.command
        {
            self.fire("session_dry_run_command", Some(command.clone()))
                .await;
        }

        if !opts.suites.is_empty() || !opts.dry_run {
            self.start_sut().await?;
        }

        if !opts.dry_run {
            if let Some(command) = &opts.command {
                self.exec_command(command).await?;
            }
            if opts.fault_prob != 0 {
                self.apply_fault_injection(opts.fault_prob, opts.fault_interval)
                    .await;
            }
        }

        if !opts.suites.is_empty() {
            let mut suites = self.read_suites(opts).await?;
            suites = Self::apply_iterate(suites, opts.suite_iterate);
            if opts.randomize {
                use rand::seq::SliceRandom;
                let mut rng = rand::rng();
                for suite in &mut *suites {
                    let mut tests = suite.tests().to_vec();
                    tests.shuffle(&mut rng);
                    let name = suite.name().to_owned();
                    *suite = Suite::new(&name, tests);
                }
            }
            if opts.dry_run {
                let names: Vec<String> = suites
                    .iter()
                    .flat_map(|s| s.tests().iter().map(|t| t.name().to_owned()))
                    .collect();
                self.fire("session_dry_run", Some(names.join("\n"))).await;
            } else {
                self.run_scheduler(&suites, opts.runtime).await?;
            }
        }
        Ok(())
    }

    /// Run a session, exporting `results.json` plus `report_path` when tests
    /// ran. Partial results collected before a failure are still exported,
    /// then the failure is re-raised.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError`] when suite discovery, execution, or the report
    /// export fails.
    pub async fn run(&self, opts: &RunOptions) -> Result<(), KirkError> {
        let _run = self.run_lock.lock().await;
        let mut outcome = self.run_inner(opts).await;
        if let Err(err) = &outcome
            && !self.stop_flag.load(Ordering::SeqCst)
        {
            self.fire("session_error", Some(short_error(err))).await;
        }

        if opts.fault_prob != 0 && !opts.dry_run {
            self.apply_fault_injection(0, 1).await;
        }

        let results = std::mem::take(&mut *self.results.lock().await);
        if !results.is_empty() {
            let exporter = JSONExporter::new();
            let mut targets = Vec::new();
            if !self.tmpdir.abspath().as_os_str().is_empty() {
                targets.push(
                    self.tmpdir
                        .abspath()
                        .join("results.json")
                        .to_string_lossy()
                        .into_owned(),
                );
            }
            if let Some(report) = opts.report_path.as_deref().filter(|p| !p.is_empty()) {
                targets.push(report.to_owned());
            }
            for target in &targets {
                if let Err(err) = exporter.save_file(&results, target).await {
                    self.fire("session_error", Some(short_error(&err))).await;
                    outcome = Err(err);
                    break;
                }
            }
        }

        self.inner_stop().await;
        self.fire("session_completed", Some(results.len().to_string()))
            .await;
        outcome
    }
}

/// Short, secret-free error description for event channels.
fn short_error(err: &KirkError) -> String {
    match err {
        KirkError::Session(_)
        | KirkError::Scheduler(_)
        | KirkError::Framework(_)
        | KirkError::Communication(_)
        | KirkError::Sut(_) => err.to_string(),
        _ => String::from("session failed"),
    }
}
