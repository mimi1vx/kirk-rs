//! Session monitor ported from `kirk/libkirk/monitor.py`.
//!
//! [`JSONFileMonitor`] rewrites one single-line `{"type", "message"}`
//! document on every event, so readers always see the latest state.
//! Concurrent writers serialize on an internal mutex, mirroring the upstream
//! `asyncio.Lock`.
//!
//! # Security
//!
//! The monitor path is confined to its canonical parent directory (which
//! must already exist). No secrets are logged: payloads only carry the
//! fixed fields below, never credentials.

use std::path::PathBuf;
use std::sync::Arc;

use kirk_core::KirkError;
use kirk_core::data::{Suite, Test};
use kirk_core::results::{SuiteResults, TestResults};
use kirk_events::{EventArgs, EventRegistry, HandlerResult};
use serde_json::{Map, Value};
use tokio::sync::Mutex;

use crate::io::AsyncFile;

/// Event types handled by [`JSONFileMonitor`], in upstream order.
const EVENT_TYPES: &[&str] = &[
    "session_restore",
    "session_started",
    "session_stopped",
    "sut_stdout",
    "sut_start",
    "sut_stop",
    "sut_restart",
    "sut_not_responding",
    "run_cmd_start",
    "run_cmd_stop",
    "test_stdout",
    "test_started",
    "test_completed",
    "test_timed_out",
    "suite_started",
    "suite_completed",
    "suite_timeout",
    "session_warning",
    "session_error",
    "kernel_panic",
    "kernel_tainted",
];

struct Inner {
    path: PathBuf,
    lock: Mutex<()>,
}

/// Monitor writing executor status into a JSON file.
#[derive(Clone)]
pub struct JSONFileMonitor {
    inner: Arc<Inner>,
}

#[allow(
    clippy::missing_errors_doc,
    reason = "all 21 recorders share the one fallible write documented on `record`"
)]
impl JSONFileMonitor {
    /// Create a monitor for `path`. The parent directory must exist.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Session`] when `path` is empty, escapes its
    /// parent directory, or the parent directory is missing.
    pub fn new(path: &str) -> Result<Self, KirkError> {
        if path.is_empty() {
            return Err(KirkError::Session(String::from("monitor path is empty")));
        }
        let raw = PathBuf::from(path);
        let name = raw
            .file_name()
            .and_then(|name| name.to_str())
            .filter(|name| !name.is_empty() && *name != "." && *name != "..")
            .ok_or_else(|| KirkError::Session(format!("invalid monitor path: '{path}'")))?;
        let parent = raw.parent().filter(|p| !p.as_os_str().is_empty());
        let canonical = if let Some(parent) = parent {
            parent
                .canonicalize()
                .map_err(|_| KirkError::Session(format!("monitor folder is missing: '{path}'")))?
        } else {
            std::env::current_dir()
                .map_err(|err| KirkError::Session(format!("can't resolve monitor path: {err}")))?
        };
        Ok(Self {
            inner: Arc::new(Inner {
                path: canonical.join(name),
                lock: Mutex::new(()),
            }),
        })
    }

    /// Write one `{type, message}` single-line document, overwriting the file.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Session`] when the file cannot be written.
    async fn record(&self, msg_type: &str, message: Value) -> Result<(), KirkError> {
        let mut data = Map::new();
        data.insert(String::from("type"), Value::String(msg_type.to_owned()));
        data.insert(String::from("message"), message);
        let text = serde_json::to_string(&Value::Object(data))
            .map_err(|err| KirkError::Session(format!("can't encode monitor data: {err}")))?;

        let _guard = self.inner.lock.lock().await;
        let mut file = AsyncFile::new(&self.inner.path.to_string_lossy(), "w");
        file.open()
            .await
            .map_err(|err| KirkError::Session(err.to_string()))?;
        file.write(&text)
            .await
            .map_err(|err| KirkError::Session(err.to_string()))?;
        file.close().await;
        Ok(())
    }

    /// Subscribe to all `EVENT_TYPES` on `registry`.
    ///
    /// String payloads are recorded as `{"message": payload}` (`{}` when
    /// absent); payloads that already parse as JSON objects are embedded
    /// as-is.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError`] when a registration fails.
    pub async fn attach(&self, registry: &EventRegistry) -> Result<(), KirkError> {
        for name in EVENT_TYPES {
            let monitor = self.clone();
            let msg_type = (*name).to_owned();
            let handler: kirk_events::Handler = Arc::new(move |args: EventArgs| {
                let monitor = monitor.clone();
                let msg_type = msg_type.clone();
                Box::pin(async move {
                    monitor
                        .record(&msg_type, payload(&args))
                        .await
                        .map_err(|err| {
                            // Never leak path or payload detail into error channels.
                            let _ = err;
                            String::from("monitor write failed")
                        })
                }) as kirk_events::BoxFuture<HandlerResult>
            });
            registry.register(name, handler, false).await?;
        }
        Ok(())
    }

    /// Unsubscribe from all `EVENT_TYPES` on `registry`.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError`] when unregistration fails.
    pub async fn detach(&self, registry: &EventRegistry) -> Result<(), KirkError> {
        for name in EVENT_TYPES {
            registry.unregister(name, None).await?;
        }
        Ok(())
    }

    /// Record a `session_restore` event.
    pub async fn session_restore(&self, restore: &str) -> Result<(), KirkError> {
        self.record("session_restore", serde_json::json!({"restore": restore}))
            .await
    }

    /// Record a `session_started` event.
    pub async fn session_started(&self, tmpdir: &str) -> Result<(), KirkError> {
        self.record("session_started", serde_json::json!({"tmpdir": tmpdir}))
            .await
    }

    /// Record a `session_stopped` event.
    pub async fn session_stopped(&self) -> Result<(), KirkError> {
        self.record("session_stopped", serde_json::json!({})).await
    }

    /// Record a `sut_stdout` event.
    pub async fn sut_stdout(&self, sut: &str, data: &str) -> Result<(), KirkError> {
        self.record("sut_stdout", serde_json::json!({"sut": sut, "data": data}))
            .await
    }

    /// Record a `sut_start` event.
    pub async fn sut_start(&self, sut: &str) -> Result<(), KirkError> {
        self.record("sut_start", serde_json::json!({"sut": sut}))
            .await
    }

    /// Record a `sut_stop` event.
    pub async fn sut_stop(&self, sut: &str) -> Result<(), KirkError> {
        self.record("sut_stop", serde_json::json!({"sut": sut}))
            .await
    }

    /// Record a `sut_restart` event.
    pub async fn sut_restart(&self, sut: &str) -> Result<(), KirkError> {
        self.record("sut_restart", serde_json::json!({"sut": sut}))
            .await
    }

    /// Record a `sut_not_responding` event.
    pub async fn sut_not_responding(&self) -> Result<(), KirkError> {
        self.record("sut_not_responding", serde_json::json!({}))
            .await
    }

    /// Record a `run_cmd_start` event.
    pub async fn run_cmd_start(&self, cmd: &str) -> Result<(), KirkError> {
        self.record("run_cmd_start", serde_json::json!({"cmd": cmd}))
            .await
    }

    /// Record a `run_cmd_stop` event.
    pub async fn run_cmd_stop(
        &self,
        command: &str,
        stdout: &str,
        returncode: i32,
    ) -> Result<(), KirkError> {
        self.record(
            "run_cmd_stop",
            serde_json::json!({
                "command": command,
                "stdout": stdout,
                "returncode": returncode,
            }),
        )
        .await
    }

    /// Record a `test_stdout` event.
    pub async fn test_stdout(&self, test: &Test, data: &str) -> Result<(), KirkError> {
        self.record(
            "test_stdout",
            serde_json::json!({"test": test_dict(test), "data": data}),
        )
        .await
    }

    /// Record a `test_started` event.
    pub async fn test_started(&self, test: &Test) -> Result<(), KirkError> {
        self.record("test_started", serde_json::json!({"test": test_dict(test)}))
            .await
    }

    /// Record a `test_completed` event.
    pub async fn test_completed(&self, results: &TestResults) -> Result<(), KirkError> {
        self.record(
            "test_completed",
            serde_json::json!({
                "test": test_dict(results.test()),
                "stdout": results.stdout(),
                "status": results.status(),
                "exec_time": results.exec_time(),
                "passed": results.passed(),
                "failed": results.failed(),
                "broken": results.broken(),
                "skipped": results.skipped(),
                "warnings": results.warnings(),
            }),
        )
        .await
    }

    /// Record a `test_timed_out` event.
    pub async fn test_timed_out(&self, test: &Test, timeout: f64) -> Result<(), KirkError> {
        self.record(
            "test_timed_out",
            serde_json::json!({"test": test_dict(test), "timeout": timeout}),
        )
        .await
    }

    /// Record a `suite_started` event.
    pub async fn suite_started(&self, suite: &Suite) -> Result<(), KirkError> {
        self.record("suite_started", suite_dict(suite)).await
    }

    /// Record a `suite_completed` event.
    pub async fn suite_completed(
        &self,
        results: &SuiteResults,
        exec_time: f64,
    ) -> Result<(), KirkError> {
        self.record(
            "suite_completed",
            serde_json::json!({
                "suite": suite_dict(results.suite()),
                "exec_time": exec_time,
                "total_run": results.suite().tests().len(),
                "passed": results.passed(),
                "failed": results.failed(),
                "skipped": results.skipped(),
                "broken": results.broken(),
                "warnings": results.warnings(),
                "kernel_version": results.kernel().unwrap_or_default(),
                "cmdline": results.cmdline().unwrap_or_default(),
                "cpu": results.cpu().unwrap_or_default(),
                "arch": results.arch().unwrap_or_default(),
                "ram": results.ram().unwrap_or_default(),
                "swap": results.swap().unwrap_or_default(),
                "distro": results.distro().unwrap_or_default(),
                "distro_version": results.distro_ver().unwrap_or_default(),
            }),
        )
        .await
    }

    /// Record a `suite_timeout` event.
    pub async fn suite_timeout(&self, suite: &Suite, timeout: f64) -> Result<(), KirkError> {
        self.record(
            "suite_timeout",
            serde_json::json!({"suite": suite_dict(suite), "timeout": timeout}),
        )
        .await
    }

    /// Record a `session_warning` event.
    pub async fn session_warning(&self, msg: &str) -> Result<(), KirkError> {
        self.record("session_warning", serde_json::json!({"message": msg}))
            .await
    }

    /// Record a `session_error` event.
    pub async fn session_error(&self, error: &str) -> Result<(), KirkError> {
        self.record("session_error", serde_json::json!({"error": error}))
            .await
    }

    /// Record a `kernel_panic` event.
    pub async fn kernel_panic(&self) -> Result<(), KirkError> {
        self.record("kernel_panic", serde_json::json!({})).await
    }

    /// Record a `kernel_tainted` event.
    pub async fn kernel_tainted(&self, message: &str) -> Result<(), KirkError> {
        self.record("kernel_tainted", serde_json::json!({"message": message}))
            .await
    }
}

/// Fixed test payload, mirroring `_test_to_dict`.
fn test_dict(test: &Test) -> Value {
    let cwd: Option<&str> = test.cwd();
    serde_json::json!({
        "name": test.name(),
        "command": test.command(),
        "arguments": test.arguments(),
        "parallelizable": test.parallelizable(),
        "cwd": cwd,
        "env": test.env(),
    })
}

/// Fixed suite payload, mirroring `_suite_to_dict`.
fn suite_dict(suite: &Suite) -> Value {
    serde_json::json!({
        "name": suite.name(),
        "tests": suite.tests().iter().map(test_dict).collect::<Vec<_>>(),
    })
}

/// Convert an event payload into its `message` document.
fn payload(args: &EventArgs) -> Value {
    match &args.message {
        None => serde_json::json!({}),
        Some(text) => match serde_json::from_str::<Value>(text) {
            Ok(Value::Object(_)) => serde_json::from_str(text).unwrap_or(Value::Null),
            _ => serde_json::json!({"message": text}),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn monitor(name: &str) -> (JSONFileMonitor, PathBuf) {
        let dir = std::env::temp_dir().join(format!("kirk-monitor-{name}"));
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let path = dir.join("monitor.json");
        let _ = tokio::fs::remove_file(&path).await;
        let monitor = JSONFileMonitor::new(&path.to_string_lossy()).unwrap();
        (monitor, path)
    }

    async fn read_line(path: &PathBuf) -> Value {
        let text = tokio::fs::read_to_string(path).await.unwrap();
        serde_json::from_str(text.lines().next().unwrap()).unwrap()
    }

    #[tokio::test]
    async fn single_write_overwrites() {
        let (monitor, path) = monitor("single").await;
        for _ in 0..10 {
            monitor.session_stopped().await.unwrap();
            let data = read_line(&path).await;
            assert_eq!(
                data,
                serde_json::json!({"type": "session_stopped", "message": {}})
            );
        }
    }

    #[tokio::test]
    async fn later_events_override() {
        let (monitor, path) = monitor("override").await;
        monitor.session_started("/tmp/dir").await.unwrap();
        monitor.kernel_panic().await.unwrap();
        monitor.session_stopped().await.unwrap();
        let data = read_line(&path).await;
        assert_eq!(
            data,
            serde_json::json!({"type": "session_stopped", "message": {}})
        );
    }

    fn sample_test() -> Test {
        Test::new("mytest", "echo")
            .unwrap()
            .with_args(vec![String::from("-n"), String::from("hello")])
    }

    fn sample_suite() -> Suite {
        Suite::new("mysuite", vec![Test::new("t1", "echo").unwrap()])
    }

    fn expected_test() -> Value {
        serde_json::json!({
            "name": "mytest",
            "command": "echo",
            "arguments": ["-n", "hello"],
            "parallelizable": false,
            "cwd": Value::Null,
            "env": {},
        })
    }

    #[tokio::test]
    async fn sut_event_shapes() {
        let (monitor, path) = monitor("shapes-sut").await;
        monitor.session_restore("/tmp/restore").await.unwrap();
        assert_eq!(
            read_line(&path).await,
            serde_json::json!({"type": "session_restore", "message": {"restore": "/tmp/restore"}})
        );
        monitor.sut_stdout("mysut", "hello").await.unwrap();
        assert_eq!(
            read_line(&path).await,
            serde_json::json!({"type": "sut_stdout", "message": {"sut": "mysut", "data": "hello"}})
        );
        monitor.sut_start("mysut").await.unwrap();
        assert_eq!(
            read_line(&path).await,
            serde_json::json!({"type": "sut_start", "message": {"sut": "mysut"}})
        );
        monitor.sut_stop("mysut").await.unwrap();
        assert_eq!(
            read_line(&path).await,
            serde_json::json!({"type": "sut_stop", "message": {"sut": "mysut"}})
        );
        monitor.sut_restart("mysut").await.unwrap();
        assert_eq!(
            read_line(&path).await,
            serde_json::json!({"type": "sut_restart", "message": {"sut": "mysut"}})
        );
        monitor.sut_not_responding().await.unwrap();
        assert_eq!(
            read_line(&path).await,
            serde_json::json!({"type": "sut_not_responding", "message": {}})
        );
        monitor.run_cmd_start("ls -la").await.unwrap();
        assert_eq!(
            read_line(&path).await,
            serde_json::json!({"type": "run_cmd_start", "message": {"cmd": "ls -la"}})
        );
        monitor.run_cmd_stop("ls -la", "output", 0).await.unwrap();
        assert_eq!(
            read_line(&path).await,
            serde_json::json!({
                "type": "run_cmd_stop",
                "message": {"command": "ls -la", "stdout": "output", "returncode": 0},
            })
        );
    }

    #[tokio::test]
    async fn test_event_shapes() {
        let (monitor, path) = monitor("shapes-test").await;
        let test = sample_test();
        monitor.test_started(&test).await.unwrap();
        assert_eq!(
            read_line(&path).await,
            serde_json::json!({"type": "test_started", "message": {"test": expected_test()}})
        );
        monitor.test_stdout(&test, "output").await.unwrap();
        let data = read_line(&path).await;
        assert_eq!(data["type"], serde_json::json!("test_stdout"));
        assert_eq!(data["message"]["data"], serde_json::json!("output"));
        assert_eq!(data["message"]["test"], expected_test());
        monitor.test_timed_out(&test, 30.0).await.unwrap();
        let data = read_line(&path).await;
        assert_eq!(data["type"], serde_json::json!("test_timed_out"));
        assert_eq!(data["message"]["timeout"], serde_json::json!(30.0));
        let completed = TestResults::new(test).with_stdout("output");
        monitor.test_completed(&completed).await.unwrap();
        let data = read_line(&path).await;
        assert_eq!(data["type"], serde_json::json!("test_completed"));
        assert_eq!(data["message"]["stdout"], serde_json::json!("output"));
        assert_eq!(data["message"]["status"], serde_json::json!(0));
    }

    #[tokio::test]
    async fn suite_session_event_shapes() {
        let (monitor, path) = monitor("shapes-suite").await;
        let suite = sample_suite();
        monitor.suite_started(&suite).await.unwrap();
        assert_eq!(
            read_line(&path).await,
            serde_json::json!({
                "type": "suite_started",
                "message": {
                    "name": "mysuite",
                    "tests": [{
                        "name": "t1",
                        "command": "echo",
                        "arguments": [],
                        "parallelizable": false,
                        "cwd": Value::Null,
                        "env": {},
                    }],
                },
            })
        );
        monitor.suite_timeout(&suite, 60.0).await.unwrap();
        let data = read_line(&path).await;
        assert_eq!(data["type"], serde_json::json!("suite_timeout"));
        assert_eq!(data["message"]["timeout"], serde_json::json!(60.0));
        let results = SuiteResults::new(suite);
        monitor.suite_completed(&results, 1.5).await.unwrap();
        let data = read_line(&path).await;
        assert_eq!(data["type"], serde_json::json!("suite_completed"));
        assert_eq!(data["message"]["exec_time"], serde_json::json!(1.5));
        monitor.session_warning("beware").await.unwrap();
        assert_eq!(
            read_line(&path).await,
            serde_json::json!({"type": "session_warning", "message": {"message": "beware"}})
        );
        monitor.session_error("oops").await.unwrap();
        assert_eq!(
            read_line(&path).await,
            serde_json::json!({"type": "session_error", "message": {"error": "oops"}})
        );
        monitor.kernel_tainted("proprietary module").await.unwrap();
        assert_eq!(
            read_line(&path).await,
            serde_json::json!({"type": "kernel_tainted", "message": {"message": "proprietary module"}})
        );
    }

    #[tokio::test]
    async fn attach_receives_registry_events() {
        let (monitor, path) = monitor("attach").await;
        let registry = EventRegistry::new();
        monitor.attach(&registry).await.unwrap();
        let worker = registry.clone();
        let handle = tokio::spawn(async move { worker.start().await });
        registry.fire("kernel_panic", None).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        registry.stop();
        handle.await.unwrap();
        let data = read_line(&path).await;
        assert_eq!(
            data,
            serde_json::json!({"type": "kernel_panic", "message": {}})
        );
        assert_eq!(EVENT_TYPES.len(), 21);
        registry.reset().await;
    }
}
