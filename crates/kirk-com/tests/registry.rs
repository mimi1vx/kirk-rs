//! Registry and `ensure_communicate` parity with `test_com.py`.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use kirk_com::{CmdResult, ComChannel, IOBuffer, Registry};
use kirk_core::KirkError;
use kirk_plugin::Plugin;

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_dir(name: &str) -> PathBuf {
    let id = TEMP_COUNTER.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("kirk-com-{name}-{}-{id}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

struct Printer;

#[async_trait]
impl IOBuffer for Printer {
    async fn write(&self, _data: &str) -> Result<(), KirkError> {
        Ok(())
    }
}

struct FakeChannel {
    name: String,
    parallel: bool,
    communicate_failures: usize,
    communicate_calls: usize,
    stop_calls: usize,
    active: bool,
}

impl FakeChannel {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            parallel: false,
            communicate_failures: 0,
            communicate_calls: 0,
            stop_calls: 0,
            active: false,
        }
    }

    fn flaky(name: &str, failures: usize) -> Self {
        Self {
            communicate_failures: failures,
            ..Self::new(name)
        }
    }
}

#[async_trait]
impl Plugin for FakeChannel {
    fn name(&self) -> &str {
        &self.name
    }

    fn config_help(&self) -> HashMap<String, String> {
        HashMap::new()
    }

    fn setup(&mut self, _cfg: &HashMap<String, String>) -> Result<(), KirkError> {
        Ok(())
    }

    fn clone_box(&self, name: &str) -> Box<dyn Plugin> {
        Box::new(Self {
            name: name.to_string(),
            parallel: self.parallel,
            communicate_failures: 0,
            communicate_calls: 0,
            stop_calls: 0,
            active: false,
        })
    }
}

#[async_trait]
impl ComChannel for FakeChannel {
    fn parallel_execution(&self) -> bool {
        self.parallel
    }

    async fn active(&self) -> bool {
        self.active
    }

    async fn communicate(&mut self, _iobuffer: Option<Arc<dyn IOBuffer>>) -> Result<(), KirkError> {
        self.communicate_calls += 1;
        if self.communicate_calls <= self.communicate_failures {
            return Err(KirkError::Communication("connect failed".to_string()));
        }
        if self.active {
            return Err(KirkError::Communication("already active".to_string()));
        }
        self.active = true;
        Ok(())
    }

    async fn stop(&mut self, _iobuffer: Option<Arc<dyn IOBuffer>>) -> Result<(), KirkError> {
        self.stop_calls += 1;
        self.active = false;
        Ok(())
    }

    async fn ping(&mut self) -> Result<f64, KirkError> {
        if self.active {
            Ok(0.001)
        } else {
            Err(KirkError::Communication("not active".to_string()))
        }
    }

    async fn run_command(
        &mut self,
        command: &str,
        _cwd: Option<&str>,
        _env: Option<&HashMap<String, String>>,
        _iobuffer: Option<Arc<dyn IOBuffer>>,
    ) -> Result<Option<CmdResult>, KirkError> {
        Ok(Some(CmdResult {
            command: command.to_string(),
            returncode: 0,
            stdout: String::new(),
            exec_time: 0.001,
        }))
    }

    async fn fetch_file(&mut self, target_path: &str) -> Result<Vec<u8>, KirkError> {
        if target_path.is_empty() {
            return Err(KirkError::Communication("empty path".to_string()));
        }
        Ok(vec![])
    }

    fn clone_channel_box(&self, new_name: &str) -> Box<dyn ComChannel> {
        Box::new(Self {
            name: new_name.to_string(),
            parallel: self.parallel,
            communicate_failures: 0,
            communicate_calls: 0,
            stop_calls: 0,
            active: false,
        })
    }
}

#[test]
fn parallel_flag_defaults_off() {
    assert!(!FakeChannel::new("chan").parallel_execution());
}

#[tokio::test]
async fn ensure_communicate_retries_then_succeeds() {
    let mut channel = FakeChannel::flaky("chan", 2);
    let buf: Arc<dyn IOBuffer> = Arc::new(Printer);

    channel
        .ensure_communicate(Some(buf), 10)
        .await
        .expect("flaky channel connects");

    assert_eq!(channel.communicate_calls, 3);
    assert_eq!(channel.stop_calls, 2);
    assert!(channel.active().await);
}

#[tokio::test]
async fn ensure_communicate_reraises_with_single_retry() {
    let mut channel = FakeChannel::flaky("chan", 1);

    let err = channel
        .ensure_communicate(None, 1)
        .await
        .expect_err("must re-raise");
    assert!(matches!(err, KirkError::Communication(_)));
    assert_eq!(channel.communicate_calls, 1);
    assert_eq!(channel.stop_calls, 0);
}

#[tokio::test]
async fn ensure_communicate_exhausts_retries() {
    let mut channel = FakeChannel::flaky("chan", 10);

    let err = channel
        .ensure_communicate(None, 3)
        .await
        .expect_err("must fail after retries");
    assert!(matches!(err, KirkError::Communication(_)));
    assert_eq!(channel.communicate_calls, 3);
    assert_eq!(channel.stop_calls, 2);
}

#[test]
fn discover_empty_dir_loads_nothing() {
    let dir = temp_dir("empty");
    let mut registry = Registry::new();

    registry.discover(&dir, false).expect("empty dir is fine");

    assert!(registry.get_channels().is_empty());
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn discover_missing_dir_errors() {
    let mut registry = Registry::new();
    let missing = temp_dir("missing-parent").join("no-such-dir");

    let err = registry
        .discover(&missing, false)
        .expect_err("missing dir must error");
    assert!(matches!(err, KirkError::Plugin(_)));
}

#[test]
fn discover_skips_non_plugin_files() {
    let dir = temp_dir("nonplugin");
    std::fs::write(dir.join("notes.txt"), "not a plugin").expect("write txt");
    // Garbage bytes with a dylib extension: loader fails, registry skips.
    std::fs::write(dir.join("bogus.so"), b"definitely not an ELF image").expect("write bogus so");

    let mut registry = Registry::new();
    registry.discover(&dir, false).expect("graceful skip");

    assert!(registry.get_channels().is_empty());
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn register_sorts_after_discover_and_clones() {
    let dir = temp_dir("sort");
    let mut registry = Registry::new();
    let mut second = FakeChannel::new("b-chan");
    second.parallel = true;
    registry.register(Box::new(second));
    registry.register(Box::new(FakeChannel::new("a-chan")));

    // Discover over the empty dir re-sorts without adding anything.
    registry.discover(&dir, true).expect("discover sorts");
    assert_eq!(registry.names(), vec!["a-chan", "b-chan"]);

    registry
        .clone_channel("a-chan", "newchan")
        .expect("clone known channel");
    assert!(registry.names().contains(&"newchan"));

    let err = registry
        .clone_channel("missing", "other")
        .expect_err("unknown channel must error");
    assert!(matches!(err, KirkError::Plugin(_)));

    std::fs::remove_dir_all(&dir).ok();
}
