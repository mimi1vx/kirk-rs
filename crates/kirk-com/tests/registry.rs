//! Registry and `ensure_communicate` parity with `test_com.py`.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use kirk_com::{CmdResult, ComChannel, IOBuffer, Registry};
use kirk_core::KirkError;
use kirk_plugin::Plugin;

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
fn register_and_clones() {
    let mut registry = Registry::new();
    let mut second = FakeChannel::new("b-chan");
    second.parallel = true;
    registry.register(Box::new(second));
    registry.register(Box::new(FakeChannel::new("a-chan")));

    assert_eq!(registry.names(), vec!["b-chan", "a-chan"]);

    registry
        .clone_channel("a-chan", "newchan")
        .expect("clone known channel");
    assert!(registry.names().contains(&"newchan"));

    let err = registry
        .clone_channel("missing", "other")
        .expect_err("unknown channel must error");
    assert!(matches!(err, KirkError::Plugin(_)));
}
