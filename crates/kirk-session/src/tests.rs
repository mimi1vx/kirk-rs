//! Fake SUT/framework port of `test_session.py`, plus `Session` tests.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use kirk_com::CmdResult;
use kirk_core::KirkError;
use kirk_core::data::{Suite, Test};
use kirk_core::results::TestResults;
use kirk_events::EventRegistry;
use kirk_scheduler::test_sched::StdoutBuffer;
use tokio::sync::Mutex;

use crate::session::{RunOptions, Session, SessionConfig, SessionFramework, SessionSut};
use kirk_support::TempDir;

/// Fake SUT: fast echo commands, slow `sleep` commands, controllable root.
struct FakeSut {
    running: Mutex<bool>,
    root: bool,
    fault: Mutex<(u32, u32)>,
    taint_calls: AtomicUsize,
    restart_fails: bool,
}

impl FakeSut {
    fn new() -> Self {
        Self {
            running: Mutex::new(false),
            root: false,
            fault: Mutex::new((0, 1)),
            taint_calls: AtomicUsize::new(usize::MAX),
            restart_fails: false,
        }
    }

    fn tainting_with_failed_restart() -> Self {
        Self {
            taint_calls: AtomicUsize::new(0),
            restart_fails: true,
            ..Self::new()
        }
    }

    async fn exec(&self, command: &str) -> Option<CmdResult> {
        if command.contains("sleep") {
            tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        } else {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        Some(CmdResult {
            command: command.to_owned(),
            returncode: 0,
            stdout: String::from("ciao"),
            exec_time: 0.01,
        })
    }
}

#[async_trait]
impl kirk_scheduler::Sut for FakeSut {
    async fn get_tainted_info(&self) -> Result<(i64, Vec<String>), KirkError> {
        let calls = self.taint_calls.fetch_add(1, Ordering::SeqCst);
        if calls == 0 {
            Ok((0, Vec::new()))
        } else {
            Ok((1, vec![String::from("proprietary module was loaded")]))
        }
    }

    async fn run_command(
        &self,
        command: &str,
        _cwd: Option<&str>,
        _env: &HashMap<String, String>,
        capture: &StdoutBuffer,
    ) -> Result<Option<CmdResult>, KirkError> {
        let row = self.exec(command).await;
        if let Some(row) = &row {
            capture.push(&row.stdout).await;
        }
        Ok(row)
    }

    async fn ping(&self) -> Result<f64, KirkError> {
        Ok(0.0)
    }

    async fn stop(&self) -> Result<(), KirkError> {
        Ok(())
    }

    async fn restart(&self) -> Result<(), KirkError> {
        if self.restart_fails {
            Err(KirkError::Communication(String::from("restart failed")))
        } else {
            Ok(())
        }
    }

    async fn get_info(&self) -> Result<HashMap<String, String>, KirkError> {
        Ok(HashMap::from([
            (String::from("distro"), String::from("openSUSE")),
            (String::from("distro_ver"), String::from("15.3")),
            (String::from("kernel"), String::from("5.17")),
            (String::from("cmdline"), String::from("quiet")),
            (String::from("arch"), String::from("x86_64")),
            (String::from("cpu"), String::from("x86_64")),
            (String::from("swap"), String::from("10 kB")),
            (String::from("ram"), String::from("1000 kB")),
        ]))
    }

    fn name(&self) -> String {
        String::from("fake")
    }
}

#[async_trait]
impl SessionSut for FakeSut {
    async fn session_start(&self) -> Result<(), KirkError> {
        *self.running.lock().await = true;
        Ok(())
    }

    async fn session_stop(&self) -> Result<(), KirkError> {
        *self.running.lock().await = false;
        Ok(())
    }

    async fn session_is_running(&self) -> Result<bool, KirkError> {
        Ok(*self.running.lock().await)
    }

    fn session_parallel_execution(&self) -> bool {
        true
    }

    async fn session_logged_as_root(&self) -> Result<bool, KirkError> {
        Ok(self.root)
    }

    async fn session_fault_enabled(&self) -> Result<bool, KirkError> {
        Ok(true)
    }

    async fn session_setup_fault(&self, prob: u32, interval: u32) -> Result<(), KirkError> {
        *self.fault.lock().await = (prob, interval);
        Ok(())
    }

    async fn session_run_command(
        &self,
        full_command: &str,
        _cwd: Option<&str>,
        _env: &HashMap<String, String>,
    ) -> Result<Option<CmdResult>, KirkError> {
        Ok(self.exec(full_command).await)
    }
}

/// Fake framework mirroring `DummyFramework`, with a `fail_after` hook that
/// fails `read_result` to simulate a mid-suite abort.
struct FakeFramework {
    calls: AtomicUsize,
    fail_after: AtomicUsize,
}

impl FakeFramework {
    fn new() -> Self {
        Self {
            calls: AtomicUsize::new(0),
            fail_after: AtomicUsize::new(usize::MAX),
        }
    }
}

fn suite01() -> Suite {
    Suite::new(
        "suite01",
        vec![
            Test::new("test01", "echo").unwrap(),
            Test::new("test02", "echo").unwrap(),
        ],
    )
}

fn suite02() -> Suite {
    Suite::new(
        "suite02",
        vec![
            Test::new("test01", "echo").unwrap(),
            Test::new("test02", "echo")
                .unwrap()
                .with_parallelizable(true),
        ],
    )
}

fn sleep_suite() -> Suite {
    Suite::new(
        "sleep",
        vec![
            Test::new("test01", "sleep").unwrap(),
            Test::new("test02", "sleep").unwrap(),
        ],
    )
}

#[async_trait]
impl kirk_scheduler::Framework for FakeFramework {
    async fn read_result(
        &self,
        test: &Test,
        stdout: &str,
        retcode: i32,
        exec_time: f64,
    ) -> Result<TestResults, KirkError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        if call > self.fail_after.load(Ordering::SeqCst) {
            return Err(KirkError::Communication(String::from(
                "SUT connection dropped mid-suite",
            )));
        }
        let (passed, failed) = if retcode == 0 { (1, 0) } else { (0, 1) };
        Ok(TestResults::new(test.clone())
            .with_passed(passed)
            .with_failed(failed)
            .with_exec_time(exec_time)
            .with_retcode(retcode)
            .with_stdout(stdout))
    }
}

#[async_trait]
impl SessionFramework for FakeFramework {
    async fn session_find_suite(&self, name: &str) -> Result<Suite, KirkError> {
        match name {
            "suite01" => Ok(suite01()),
            "suite02" => Ok(suite02()),
            "sleep" => Ok(sleep_suite()),
            "environ" => Ok(Suite::new(
                "environ",
                vec![Test::new("test01", "echo").unwrap()],
            )),
            _ => Err(KirkError::Framework(format!("can't find suite {name}"))),
        }
    }

    async fn session_find_command(&self, command: &str) -> Result<Test, KirkError> {
        Test::new(command, command).map_err(|err| KirkError::Framework(err.to_string()))
    }
}

fn sandbox(name: &str) -> String {
    let root = std::env::temp_dir().join(format!("kirk-session-{name}"));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    root.to_string_lossy().into_owned()
}

fn session(root: &str) -> Session<FakeSut, FakeFramework> {
    Session::new(
        TempDir::new(Some(root), 5).unwrap(),
        FakeSut::new(),
        FakeFramework::new(),
        EventRegistry::new(),
        SessionConfig {
            exec_timeout: 30.0,
            suite_timeout: 30.0,
            workers: 2,
            force_parallel: false,
        },
    )
}

async fn report_len(path: &str) -> usize {
    let text = tokio::fs::read_to_string(path).await.unwrap();
    let data: serde_json::Value = serde_json::from_str(&text).unwrap();
    data["results"].as_array().unwrap().len()
}

#[tokio::test]
async fn run_executes_suites() {
    let root = sandbox("run");
    let session = session(&root);
    session
        .run(&RunOptions {
            suites: vec![String::from("suite01"), String::from("suite02")],
            ..Default::default()
        })
        .await
        .unwrap();
    let results = session.tmpdir().abspath().join("results.json");
    assert_eq!(report_len(&results.to_string_lossy()).await, 4);
    let executed = tokio::fs::read_to_string(session.tmpdir().abspath().join("executed"))
        .await
        .unwrap();
    assert!(executed.contains("suite01::test01\n"));
    assert!(executed.contains("suite02::test02\n"));
}

#[tokio::test]
async fn scheduler_events_reach_session_registry() {
    // Proves the session's shared registry, not an isolated one inside the
    // scheduler, is what actually dispatches scheduler-owned events.
    let root = sandbox("events");
    let events = EventRegistry::new();

    let counters: HashMap<&str, Arc<AtomicUsize>> = [
        "suite_started",
        "test_started",
        "test_completed",
        "suite_completed",
    ]
    .into_iter()
    .map(|name| (name, Arc::new(AtomicUsize::new(0))))
    .collect();
    for (name, counter) in &counters {
        let counter = Arc::clone(counter);
        events
            .register(
                name,
                Arc::new(move |_args: kirk_events::EventArgs| {
                    let counter = Arc::clone(&counter);
                    Box::pin(async move {
                        counter.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    })
                }),
                false,
            )
            .await
            .unwrap();
    }

    let session = Session::new(
        TempDir::new(Some(&root), 5).unwrap(),
        FakeSut::new(),
        FakeFramework::new(),
        events.clone(),
        SessionConfig {
            exec_timeout: 30.0,
            suite_timeout: 30.0,
            workers: 1,
            force_parallel: false,
        },
    );

    let events_bg = events.clone();
    let events_task = tokio::spawn(async move { events_bg.start().await });

    session
        .run(&RunOptions {
            suites: vec![String::from("suite01")],
            pattern: Some(String::from("test01")),
            ..Default::default()
        })
        .await
        .unwrap();

    events.stop();
    events_task.await.unwrap();

    for (name, counter) in &counters {
        assert_eq!(
            counter.load(Ordering::SeqCst),
            1,
            "{name} must fire exactly once"
        );
    }
}

#[tokio::test]
async fn run_with_pattern() {
    let root = sandbox("pattern");
    let session = session(&root);
    let report = format!("{root}/report.json");
    session
        .run(&RunOptions {
            suites: vec![String::from("suite01"), String::from("suite02")],
            pattern: Some(String::from("test01|test02")),
            report_path: Some(report.clone()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(report_len(&report).await, 4);
}

#[tokio::test]
async fn run_with_report() {
    let root = sandbox("report");
    let session = session(&root);
    let report = format!("{root}/report.json");
    session
        .run(&RunOptions {
            suites: vec![String::from("suite01"), String::from("suite02")],
            report_path: Some(report.clone()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(report_len(&report).await, 4);
}

#[tokio::test]
async fn run_report_abort_persists_partial_results() {
    let root = sandbox("abort");
    let tmp = TempDir::new(Some(&root), 5).unwrap();
    let framework = FakeFramework::new();
    framework.fail_after.store(2, Ordering::SeqCst);
    let session = Session::new(
        tmp,
        FakeSut::new(),
        framework,
        EventRegistry::new(),
        SessionConfig {
            exec_timeout: 30.0,
            suite_timeout: 30.0,
            workers: 2,
            force_parallel: false,
        },
    );
    let report = format!("{root}/report.json");
    let err = session
        .run(&RunOptions {
            suites: vec![String::from("suite01"), String::from("suite02")],
            report_path: Some(report.clone()),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert!(err.to_string().contains("dropped"));
    assert_eq!(report_len(&report).await, 2);
}

#[tokio::test]
async fn run_report_persists_results_before_failed_restart() {
    let root = sandbox("restart-abort");
    let session = Session::new(
        TempDir::new(Some(&root), 5).unwrap(),
        FakeSut::tainting_with_failed_restart(),
        FakeFramework::new(),
        EventRegistry::new(),
        SessionConfig {
            exec_timeout: 30.0,
            suite_timeout: 30.0,
            workers: 1,
            force_parallel: false,
        },
    );
    let report = format!("{root}/report.json");

    let error = session
        .run(&RunOptions {
            suites: vec![String::from("suite01")],
            report_path: Some(report.clone()),
            ..Default::default()
        })
        .await
        .expect_err("restart failure must reach the session caller");

    assert!(matches!(error, KirkError::Communication(message) if message == "restart failed"));
    let text = tokio::fs::read_to_string(report).await.unwrap();
    let data: serde_json::Value = serde_json::from_str(&text).unwrap();
    let results = data["results"].as_array().unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["test_fqn"], "test01");
    assert_eq!(results[0]["test"]["passed"], 1);
    assert_eq!(data["stats"]["passed"], 1);
}

#[tokio::test]
async fn run_dry_run_writes_nothing() {
    let root = sandbox("dry");
    let session = session(&root);
    let report = format!("{root}/report.json");
    session
        .run(&RunOptions {
            suites: vec![String::from("suite01"), String::from("suite02")],
            report_path: Some(report.clone()),
            dry_run: true,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(!std::path::Path::new(&report).exists());
}

#[tokio::test]
async fn run_stop_during_sleep() {
    let root = sandbox("stop");
    let session = Arc::new(session(&root));
    let worker = session.clone();
    let run = tokio::spawn(async move {
        worker
            .run(&RunOptions {
                suites: vec![String::from("sleep")],
                ..Default::default()
            })
            .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    session.stop().await;
    session.stop().await;
    let _ = run.await.unwrap();
}

#[tokio::test]
async fn run_single_command() {
    let root = sandbox("cmd");
    let session = session(&root);
    session
        .run(&RunOptions {
            command: Some(String::from("test")),
            ..Default::default()
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn run_skip_tests() {
    let root = sandbox("skip");
    let session = session(&root);
    let report = format!("{root}/report.json");
    session
        .run(&RunOptions {
            suites: vec![String::from("suite01"), String::from("suite02")],
            skip_tests: Some(String::from("test02")),
            report_path: Some(report.clone()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(report_len(&report).await, 2);
}

#[tokio::test]
async fn run_suite_iterate() {
    for (iterate, expect) in [(0, 4), (1, 4), (3, 12)] {
        let root = sandbox(&format!("iterate-{iterate}"));
        let session = session(&root);
        let report = format!("{root}/report.json");
        session
            .run(&RunOptions {
                suites: vec![String::from("suite01"), String::from("suite02")],
                suite_iterate: iterate,
                report_path: Some(report.clone()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(report_len(&report).await, expect);
    }
}

#[tokio::test]
async fn run_randomize_keeps_count() {
    let root = sandbox("random");
    let session = session(&root);
    let report = format!("{root}/report.json");
    session
        .run(&RunOptions {
            suites: vec![String::from("suite01"); 10],
            randomize: true,
            report_path: Some(report.clone()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(report_len(&report).await, 20);
}

#[tokio::test]
async fn restore_skips_executed_tests() {
    let root = sandbox("restore");
    let first = session(&root);
    first
        .run(&RunOptions {
            suites: vec![String::from("suite01")],
            ..Default::default()
        })
        .await
        .unwrap();
    let restore = first.tmpdir().abspath().to_string_lossy().into_owned();

    let root2 = sandbox("restore-second");
    let second = session(&root2);
    let err = second
        .run(&RunOptions {
            suites: vec![String::from("suite01")],
            restore_path: Some(restore),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert!(err.to_string().contains("no tests selected"));
}

#[test]
fn filter_tests_keeps_matching() {
    let mut suites = vec![suite01(), suite02()];
    Session::<FakeSut, FakeFramework>::filter_tests(&mut suites, Some("test01"), false).unwrap();
    assert!(suites.iter().all(|s| s.tests().len() == 1));
    assert!(suites.iter().all(|s| s.tests()[0].name() == "test01"));
}

#[test]
fn filter_tests_skips_matching() {
    let mut suites = vec![suite01(), suite02()];
    Session::<FakeSut, FakeFramework>::filter_tests(&mut suites, Some("test02"), true).unwrap();
    assert!(suites.iter().all(|s| s.tests().len() == 1));
}

#[test]
fn filter_tests_rejects_bad_regex() {
    let mut suites = vec![suite01()];
    assert!(
        Session::<FakeSut, FakeFramework>::filter_tests(&mut suites, Some("(unclosed"), false)
            .is_err()
    );
}

#[test]
fn apply_iterate_renames() {
    let once = Session::<FakeSut, FakeFramework>::apply_iterate(vec![suite01()], 1);
    assert_eq!(once.len(), 1);
    assert_eq!(once[0].name(), "suite01");
    let thrice = Session::<FakeSut, FakeFramework>::apply_iterate(vec![suite01()], 3);
    assert_eq!(thrice.len(), 3);
    assert_eq!(thrice[0].name(), "suite01[0]");
    assert_eq!(thrice[2].name(), "suite01[2]");
    assert!(thrice.iter().all(|s| s.tests().len() == 2));
}
