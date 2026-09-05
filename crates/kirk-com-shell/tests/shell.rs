//! Ports of `kirk/libkirk/tests/test_shell.py` + generic `test_com.py` cases.
//!
//! Adaptations for argv-exec (no shell): `cwd` uses `pwd` instead of
//! `echo -n $PWD`, `env` uses `printenv` instead of `echo -n $HELLO`, and
//! file fixtures are created with [`std::fs`] instead of `>` redirects.
//! Handles are `Clone`d for concurrent `stop`-during-`run` cases, mirroring
//! upstream coroutines sharing one channel object.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use kirk_com::{ComChannel, IOBuffer};
use kirk_com_shell::ShellChannel;
use kirk_core::KirkError;
use kirk_plugin::Plugin;
use tokio::sync::Mutex;

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_dir(name: &str) -> std::path::PathBuf {
    let id = TEMP_COUNTER.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("kirk-shell-{name}-{}-{id}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

struct Recorder {
    text: Mutex<String>,
}

impl Recorder {
    fn new() -> Self {
        Self {
            text: Mutex::new(String::new()),
        }
    }

    async fn contents(&self) -> String {
        self.text.lock().await.clone()
    }
}

#[async_trait]
impl IOBuffer for Recorder {
    async fn write(&self, data: &str) -> Result<(), KirkError> {
        self.text.lock().await.push_str(data);
        Ok(())
    }
}

fn recorder() -> Arc<Recorder> {
    Arc::new(Recorder::new())
}

async fn communicated() -> ShellChannel {
    let mut channel = ShellChannel::new();
    channel.communicate(None).await.expect("communicate");
    channel
}

#[tokio::test]
async fn ping_requires_active() {
    let mut channel = ShellChannel::new();
    let err = channel.ping().await.expect_err("ping while inactive");
    assert!(matches!(err, KirkError::Communication(_)));
}

#[tokio::test]
async fn ping_reports_time() {
    let mut channel = communicated().await;
    let ping_t = channel.ping().await.expect("ping");
    assert!(ping_t > 0.0);
    channel.stop(None).await.expect("stop");
}

#[tokio::test]
async fn double_communicate_errors() {
    let mut channel = communicated().await;
    let err = channel
        .communicate(None)
        .await
        .expect_err("second communicate");
    assert!(matches!(err, KirkError::Communication(_)));
    channel.stop(None).await.expect("stop");
}

#[tokio::test]
async fn ensure_communicate_fails_when_active() {
    let mut channel = communicated().await;
    let err = channel
        .ensure_communicate(None, 1)
        .await
        .expect_err("ensure while active");
    assert!(matches!(err, KirkError::Communication(_)));
    channel.stop(None).await.expect("stop");
}

#[tokio::test]
async fn stop_is_idempotent() {
    let mut channel = ShellChannel::new();
    channel.stop(None).await.expect("stop while inactive");
    assert!(!channel.active().await);

    channel.communicate(None).await.expect("communicate");
    assert!(channel.active().await);
    channel.stop(None).await.expect("stop");
    channel.stop(None).await.expect("second stop");
    assert!(!channel.active().await);
}

#[tokio::test]
async fn communicate_and_stop_concurrently() {
    let channel = ShellChannel::new();
    let mut first = channel.clone();
    let mut second = channel.clone();
    let (communicate_res, stop_res) = tokio::join!(first.communicate(None), second.stop(None));
    communicate_res.expect("communicate");
    stop_res.expect("stop");
    assert!(!channel.active().await);
}

#[tokio::test]
async fn run_command_echo() {
    let mut channel = communicated().await;
    let buf = recorder();
    let res = channel
        .run_command("echo 0", None, None, Some(buf.clone()))
        .await
        .expect("run echo")
        .expect("echo result");

    assert_eq!(res.command, "echo 0");
    assert_eq!(res.returncode, 0);
    assert_eq!(res.stdout.trim(), "0");
    assert!(res.exec_time > 0.0);
    assert!(buf.contents().await.contains('0'));
    channel.stop(None).await.expect("stop");
}

#[tokio::test]
async fn run_command_requires_active() {
    let mut channel = ShellChannel::new();
    let err = channel
        .run_command("echo 0", None, None, None)
        .await
        .expect_err("run while inactive");
    assert!(matches!(err, KirkError::Communication(_)));
}

#[tokio::test]
async fn run_command_rejects_empty() {
    let mut channel = communicated().await;
    for command in ["", "   "] {
        let err = channel
            .run_command(command, None, None, None)
            .await
            .expect_err("empty command must fail");
        assert!(matches!(err, KirkError::Communication(_)));
    }
    channel.stop(None).await.expect("stop");
}

#[tokio::test]
async fn run_command_rejects_shell_syntax() {
    let mut channel = communicated().await;
    for command in [
        "echo hi > /tmp/kirk-shell-x",
        "echo -n $PWD",
        "sleep 1 && echo done",
    ] {
        let err = channel
            .run_command(command, None, None, None)
            .await
            .expect_err("shell syntax must fail");
        assert!(matches!(err, KirkError::Communication(_)), "{command}");
    }
    channel.stop(None).await.expect("stop");
}

#[tokio::test]
async fn run_command_failing_reports_shape_and_merges_stderr() {
    let mut channel = communicated().await;
    let res = channel
        .run_command("ls /kirk-com-shell-definitely-missing", None, None, None)
        .await
        .expect("run failing command")
        .expect("failing result");

    assert_ne!(res.returncode, 0);
    assert!(
        res.stdout.contains("No such file"),
        "stderr merged: {:?}",
        res.stdout
    );
    assert!(res.exec_time > 0.0);
    assert!(res.exec_time < 60.0);
    channel.stop(None).await.expect("stop");
}

#[tokio::test]
async fn run_command_detects_kernel_panic() {
    let mut channel = communicated().await;
    let err = channel
        .run_command("echo 'Kernel panic - not syncing'", None, None, None)
        .await
        .expect_err("panic marker must fail");
    assert!(matches!(err, KirkError::KernelPanic(_)));
    channel.stop(None).await.expect("stop");
}

#[tokio::test]
async fn run_command_caps_output() {
    let mut channel = communicated().await;
    let err = channel
        .run_command("head -c 9000000 /dev/zero", None, None, None)
        .await
        .expect_err("over-cap output must fail");
    assert!(matches!(err, KirkError::Communication(_)));
    channel.stop(None).await.expect("stop");
}

#[tokio::test]
async fn run_command_stop_during_sleep() {
    let channel = communicated().await;
    let mut runner = channel.clone();
    let mut stopper = channel.clone();

    let run = tokio::spawn(async move { runner.run_command("sleep 0.5", None, None, None).await });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    stopper.stop(None).await.expect("stop");

    let res = run
        .await
        .expect("join run")
        .expect("killed run")
        .expect("killed result");
    assert_ne!(res.returncode, 0);
    assert!(res.exec_time < 0.5, "exec_time: {}", res.exec_time);
    assert!(!channel.active().await);
}

#[tokio::test]
async fn run_command_parallel_echoes() {
    let channel = communicated().await;
    let mut tasks = Vec::new();
    for i in 0..8 {
        let mut runner = channel.clone();
        tasks.push(tokio::spawn(async move {
            runner
                .run_command(&format!("echo {i}"), None, None, None)
                .await
        }));
    }
    let mut seen = [false; 8];
    for task in tasks {
        let res = task
            .await
            .expect("join")
            .expect("parallel run")
            .expect("parallel result");
        assert_eq!(res.returncode, 0);
        assert!(res.exec_time > 0.0);
        let i: usize = res.stdout.trim().parse().expect("echoed index");
        seen[i] = true;
    }
    assert!(seen.iter().all(|s| *s));
    let mut channel = channel;
    channel.stop(None).await.expect("stop");
}

#[tokio::test]
async fn run_command_stop_parallel_sleeps() {
    let channel = communicated().await;
    let mut tasks = Vec::new();
    for _ in 0..4 {
        let mut runner = channel.clone();
        tasks.push(tokio::spawn(async move {
            runner.run_command("sleep 0.5", None, None, None).await
        }));
    }
    let mut stopper = channel.clone();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    stopper.stop(None).await.expect("stop");

    for task in tasks {
        let outcome = task.await.expect("join");
        match outcome {
            Ok(Some(res)) => {
                assert_ne!(res.returncode, 0);
                assert!(res.exec_time < 0.5);
            }
            Err(KirkError::Communication(_)) => {}
            other => panic!("unexpected parallel-stop outcome: {other:?}"),
        }
    }
    assert!(!channel.active().await);
}

#[tokio::test]
async fn run_command_cwd() {
    let dir = temp_dir("cwd");
    let canonical = dir.canonicalize().expect("canonicalize");
    let mut channel = communicated().await;
    let res = channel
        .run_command(
            "pwd",
            Some(canonical.to_str().expect("cwd utf-8")),
            None,
            None,
        )
        .await
        .expect("run pwd")
        .expect("pwd result");

    assert_eq!(res.returncode, 0);
    assert_eq!(res.stdout.trim(), canonical.to_str().expect("cwd utf-8"));
    channel.stop(None).await.expect("stop");
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn run_command_env() {
    let mut channel = communicated().await;
    let res = channel
        .run_command(
            "printenv HELLO",
            None,
            Some(&HashMap::from([("HELLO".to_string(), "ciao".to_string())])),
            None,
        )
        .await
        .expect("run printenv")
        .expect("printenv result");

    assert_eq!(res.returncode, 0);
    assert_eq!(res.stdout.trim(), "ciao");
    channel.stop(None).await.expect("stop");
}

#[tokio::test]
async fn fetch_file_bad_args() {
    let mut channel = communicated().await;
    let err = channel
        .fetch_file("")
        .await
        .expect_err("empty path must fail");
    assert!(matches!(err, KirkError::Communication(_)));

    let err = channel
        .fetch_file("/kirk-com-shell-definitely-missing")
        .await
        .expect_err("missing file must fail");
    assert!(matches!(err, KirkError::Communication(_)));
    channel.stop(None).await.expect("stop");
}

#[tokio::test]
async fn fetch_file_requires_active() {
    let mut channel = ShellChannel::new();
    let dir = temp_dir("fetch-inactive");
    let path = dir.join("data");
    std::fs::write(&path, b"mytests").expect("write fixture");
    let err = channel
        .fetch_file(path.to_str().expect("path utf-8"))
        .await
        .expect_err("fetch while inactive");
    assert!(matches!(err, KirkError::Communication(_)));
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn fetch_file_round_trip() {
    let mut channel = communicated().await;
    let dir = temp_dir("fetch");
    for i in 0..5 {
        let path = dir.join(format!("myfile{i}"));
        std::fs::write(&path, b"mytests").expect("write fixture");
        let data = channel
            .fetch_file(path.to_str().expect("path utf-8"))
            .await
            .expect("fetch");
        assert_eq!(data, b"mytests");
    }
    channel.stop(None).await.expect("stop");
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn channel_metadata() {
    let channel = ShellChannel::new();
    assert_eq!(channel.name(), "shell");
    assert!(channel.parallel_execution());
    assert!(channel.config_help().is_empty());

    let renamed = channel.clone_channel_box("newchan");
    assert_eq!(renamed.name(), "newchan");
    assert!(renamed.parallel_execution());
}
