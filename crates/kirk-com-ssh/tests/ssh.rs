//! Live SSH tests, mirroring upstream `pytest.mark.ssh`.
//!
//! All tests are `#[ignore]`d and no-op without the `TEST_SSH_*`
//! environment (host at minimum). With a reachable OpenSSH server, run:
//!
//! ```sh
//! TEST_SSH_HOST=127.0.0.1 TEST_SSH_USER=root TEST_SSH_KEY_FILE=~/.ssh/id_ed25519 \
//!     cargo test -p kirk-com-ssh -- --ignored
//! ```
//!
//! Optional: `TEST_SSH_PORT`, `TEST_SSH_PASSWORD`, `TEST_SSH_KNOWN_HOSTS`,
//! `TEST_SSH_SUDO` (`0`/`1`), `TEST_SSH_RESET_CMD`, `TEST_SSH_FETCH_PATH`.

use std::collections::HashMap;
use std::sync::Arc;

use kirk_com::{ComChannel, IOBuffer};
use kirk_com_ssh::SshChannel;
use kirk_core::KirkError;
use kirk_plugin::Plugin;

struct VecBuffer {
    chunks: tokio::sync::Mutex<Vec<String>>,
}

impl VecBuffer {
    fn new() -> Self {
        Self {
            chunks: tokio::sync::Mutex::new(Vec::new()),
        }
    }
}

#[async_trait::async_trait]
impl IOBuffer for VecBuffer {
    async fn write(&self, data: &str) -> Result<(), KirkError> {
        self.chunks.lock().await.push(data.to_owned());
        Ok(())
    }
}

/// Build channel config from the environment; `None` skips the test.
fn live_config() -> Option<HashMap<String, String>> {
    let host = std::env::var("TEST_SSH_HOST")
        .ok()
        .filter(|h| !h.is_empty())?;
    let mut cfg = HashMap::new();
    cfg.insert("host".to_owned(), host);
    for (key, var) in [
        ("user", "TEST_SSH_USER"),
        ("port", "TEST_SSH_PORT"),
        ("key_file", "TEST_SSH_KEY_FILE"),
        ("password", "TEST_SSH_PASSWORD"),
        ("known_hosts", "TEST_SSH_KNOWN_HOSTS"),
        ("sudo", "TEST_SSH_SUDO"),
        ("reset_cmd", "TEST_SSH_RESET_CMD"),
    ] {
        if let Ok(value) = std::env::var(var)
            && !value.is_empty()
        {
            cfg.insert(key.to_owned(), value);
        }
    }
    Some(cfg)
}

async fn connect() -> Option<SshChannel> {
    let Some(cfg) = live_config() else {
        eprintln!("skipping live SSH test: TEST_SSH_HOST is not set");
        return None;
    };
    let mut channel = SshChannel::new("ssh");
    channel.setup(&cfg).expect("live config must be valid");
    channel
        .communicate(Some(Arc::new(VecBuffer::new())))
        .await
        .expect("live connect must succeed");
    Some(channel)
}

#[tokio::test]
#[ignore = "needs a live SSH server (set TEST_SSH_HOST)"]
async fn live_communicate_ping_stop() {
    let Some(mut channel) = connect().await else {
        return;
    };
    assert!(channel.active().await);
    // Double connect must fail.
    assert!(channel.communicate(None).await.is_err());
    let round_trip = channel.ping().await.expect("ping must succeed");
    assert!(round_trip >= 0.0);
    channel.stop(None).await.expect("stop must succeed");
    assert!(!channel.active().await);
    // Stop is idempotent.
    channel.stop(None).await.expect("second stop must succeed");
}

#[tokio::test]
#[ignore = "needs a live SSH server (set TEST_SSH_HOST)"]
async fn live_run_and_fetch() {
    let Some(mut channel) = connect().await else {
        return;
    };
    let iobuffer = Arc::new(VecBuffer::new());
    let ret = channel
        .run_command("echo hello", None, None, Some(iobuffer.clone()))
        .await
        .expect("run must succeed")
        .expect("result must be present");
    assert_eq!(ret.command, "echo hello");
    assert_eq!(ret.returncode, 0);
    assert!(ret.stdout.contains("hello"), "stdout: {:?}", ret.stdout);

    let ret = channel
        .run_command("echo out; echo err >&2; exit 3", None, None, None)
        .await
        .expect("run must succeed")
        .expect("result must be present");
    assert_eq!(ret.returncode, 3);
    assert!(ret.stdout.contains("out"));
    assert!(ret.stdout.contains("err"));

    let fetch_path =
        std::env::var("TEST_SSH_FETCH_PATH").unwrap_or_else(|_| "/etc/hosts".to_owned());
    let data = channel
        .fetch_file(&fetch_path)
        .await
        .expect("fetch must succeed");
    assert_ne!(data, [] as [u8; 0]);

    channel.stop(None).await.expect("stop must succeed");
}

#[tokio::test]
#[ignore = "needs a live SSH server (set TEST_SSH_HOST)"]
async fn concurrent_run_command_overlaps_not_serializes() {
    let Some(mut channel) = connect().await else {
        return;
    };
    let handle_a = channel
        .concurrent_handle()
        .expect("ssh must offer a handle");
    let handle_b = channel
        .concurrent_handle()
        .expect("ssh must offer a handle");

    let start = std::time::Instant::now();
    let (result_a, result_b) = tokio::join!(
        run_owned(handle_a, "sleep 2"),
        run_owned(handle_b, "sleep 2")
    );
    let elapsed = start.elapsed();

    result_a.expect("run must succeed").expect("result present");
    result_b.expect("run must succeed").expect("result present");
    // Serialized execution would take ~4s; concurrent execution over one
    // shared session should stay well under 2x a single sleep.
    assert!(
        elapsed < std::time::Duration::from_secs(3),
        "expected overlap, took {elapsed:?}"
    );

    channel.stop(None).await.expect("stop must succeed");
}

async fn run_owned(
    mut channel: Box<dyn ComChannel>,
    command: &str,
) -> Result<Option<kirk_com::CmdResult>, KirkError> {
    channel.run_command(command, None, None, None).await
}
