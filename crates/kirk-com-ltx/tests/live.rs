//! Live tests against a real LTX binary, mirroring
//! `kirk/libkirk/tests/test_ltx.py`.
//!
//! Every test is `#[ignore = "needs TEST_LTX_BINARY"]` and returns early unless `TEST_LTX_BINARY`
//! points at an executable LTX binary. Run them with:
//! `TEST_LTX_BINARY=... cargo test -p kirk-com-ltx -- --ignored`

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use kirk_com::ComChannel;
use kirk_com_ltx::{Ltx, LtxChannel, Reply, Request, SlotId};
use kirk_plugin::Plugin;

static LIVE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Path to the LTX binary under test, or `None` when live tests are gated off.
fn ltx_binary() -> Option<PathBuf> {
    let path: PathBuf = std::env::var_os("TEST_LTX_BINARY")?.into();
    (path.is_file() && is_executable(&path)).then_some(path)
}

#[cfg(unix)]
fn is_executable(path: &PathBuf) -> bool {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(path).is_ok_and(|m| m.permissions().mode() & 0o111 != 0)
}

#[cfg(not(unix))]
fn is_executable(path: &PathBuf) -> bool {
    path.is_file()
}

/// Spawned LTX binary attached to a FIFO pair; kills the child on drop.
struct LivePeer {
    infile: PathBuf,
    outfile: PathBuf,
    child: std::process::Child,
    _holders: Vec<std::fs::File>,
}

impl LivePeer {
    fn spawn(tag: &str) -> Option<Self> {
        let binary = ltx_binary()?;
        let id = LIVE_COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir();
        let infile = dir.join(format!(
            "kirk-ltx-live-{tag}-in-{}-{id}",
            std::process::id()
        ));
        let outfile = dir.join(format!(
            "kirk-ltx-live-{tag}-out-{}-{id}",
            std::process::id()
        ));
        for path in [&infile, &outfile] {
            let _ = std::fs::remove_file(path);
            let status = std::process::Command::new("mkfifo")
                .arg(path)
                .status()
                .ok()?;
            if !status.success() {
                return None;
            }
        }
        // Hold both ends open read/write so neither side blocks on open.
        let in_holder = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&infile)
            .ok()?;
        let out_holder = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&outfile)
            .ok()?;
        let child = std::process::Command::new(&binary)
            .stdin(Stdio::from(in_holder.try_clone().ok()?))
            .stdout(Stdio::from(out_holder.try_clone().ok()?))
            .spawn()
            .ok()?;
        Some(Self {
            infile,
            outfile,
            child,
            _holders: vec![in_holder, out_holder],
        })
    }

    fn ltx(&self) -> Ltx {
        Ltx::new(self.infile.clone(), self.outfile.clone())
    }
}

impl Drop for LivePeer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_file(&self.infile);
        let _ = std::fs::remove_file(&self.outfile);
    }
}

async fn gather_timeout(ltx: &Ltx, requests: Vec<Request>) -> Vec<Reply> {
    tokio::time::timeout(Duration::from_secs(30), ltx.gather(requests))
        .await
        .expect("LTX gather timed out")
        .expect("LTX gather failed")
}

#[tokio::test]
#[ignore = "needs TEST_LTX_BINARY"]
async fn live_version() {
    let Some(peer) = LivePeer::spawn("version") else {
        return;
    };
    let ltx = peer.ltx();
    ltx.connect().await.expect("connect");
    let replies = gather_timeout(&ltx, vec![Request::version()]).await;
    assert_eq!(replies, vec![Reply::Version("0.1".to_string())]);
    ltx.disconnect().await.expect("disconnect");
}

#[tokio::test]
#[ignore = "needs TEST_LTX_BINARY"]
async fn live_ping() {
    let Some(peer) = LivePeer::spawn("ping") else {
        return;
    };
    let ltx = peer.ltx();
    ltx.connect().await.expect("connect");
    let replies = gather_timeout(&ltx, vec![Request::ping()]).await;
    assert!(matches!(replies[0], Reply::Ping(end_t) if end_t > 0));
    ltx.disconnect().await.expect("disconnect");
}

#[tokio::test]
#[ignore = "needs TEST_LTX_BINARY"]
async fn live_execute() {
    let Some(peer) = LivePeer::spawn("exec") else {
        return;
    };
    let ltx = peer.ltx();
    ltx.connect().await.expect("connect");
    let slot = SlotId::new(0).expect("slot");
    let replies = gather_timeout(&ltx, vec![Request::execute(slot, "uname").expect("exec")]).await;
    match &replies[0] {
        Reply::Execute {
            si_code,
            si_status,
            stdout,
            ..
        } => {
            assert_eq!(stdout, "Linux\n");
            assert_eq!(*si_code, 1);
            assert_eq!(*si_status, 0);
        }
        other => panic!("expected execute reply, got {other:?}"),
    }
    ltx.disconnect().await.expect("disconnect");
}

#[tokio::test]
#[ignore = "needs TEST_LTX_BINARY"]
async fn live_set_and_get_file() {
    let Some(peer) = LivePeer::spawn("file") else {
        return;
    };
    let ltx = peer.ltx();
    ltx.connect().await.expect("connect");
    let path = std::env::temp_dir().join(format!(
        "kirk-ltx-live-file-{}-{}.bin",
        std::process::id(),
        LIVE_COUNTER.fetch_add(1, Ordering::SeqCst)
    ));
    let data: Vec<u8> = b"AaXa\x00\x01\x02Zz".repeat(1024);
    let replies = gather_timeout(
        &ltx,
        vec![
            Request::set_file(&path.display().to_string(), &data).expect("set"),
            Request::get_file(&path.display().to_string()).expect("get"),
        ],
    )
    .await;
    assert!(matches!(replies[0], Reply::SetFile { .. }));
    match &replies[1] {
        Reply::GetFile { data: fetched, .. } => assert_eq!(*fetched, data),
        other => panic!("expected file reply, got {other:?}"),
    }
    let _ = std::fs::remove_file(&path);
    ltx.disconnect().await.expect("disconnect");
}

#[tokio::test]
#[ignore = "needs TEST_LTX_BINARY"]
async fn live_kill() {
    let Some(peer) = LivePeer::spawn("kill") else {
        return;
    };
    let ltx = peer.ltx();
    ltx.connect().await.expect("connect");
    let slot = SlotId::new(0).expect("slot");
    let replies = gather_timeout(
        &ltx,
        vec![
            Request::execute(slot, "sleep 30").expect("exec"),
            Request::kill(slot),
        ],
    )
    .await;
    match &replies[0] {
        Reply::Execute {
            si_code,
            si_status,
            stdout,
            ..
        } => {
            assert_eq!(*si_code, 2);
            assert_eq!(*si_status, libc_sigkill());
            assert_eq!(stdout, "");
        }
        other => panic!("expected execute reply, got {other:?}"),
    }
    ltx.disconnect().await.expect("disconnect");
}

#[cfg(unix)]
fn libc_sigkill() -> i32 {
    9
}

#[cfg(not(unix))]
fn libc_sigkill() -> i32 {
    9
}

#[tokio::test]
#[ignore = "needs TEST_LTX_BINARY"]
async fn live_env_and_cwd() {
    let Some(peer) = LivePeer::spawn("env") else {
        return;
    };
    let ltx = peer.ltx();
    ltx.connect().await.expect("connect");
    let replies = gather_timeout(
        &ltx,
        vec![
            Request::env(Some(0), "HELLO", "CIAO").expect("env"),
            Request::execute(SlotId::new(0).expect("slot"), "echo -n $HELLO").expect("exec"),
        ],
    )
    .await;
    match &replies[1] {
        Reply::Execute { stdout, .. } => assert_eq!(stdout, "CIAO"),
        other => panic!("expected execute reply, got {other:?}"),
    }
    ltx.disconnect().await.expect("disconnect");
}

#[tokio::test]
#[ignore = "needs TEST_LTX_BINARY"]
async fn live_channel_run_and_fetch() {
    let Some(peer) = LivePeer::spawn("chan") else {
        return;
    };
    let mut channel = LtxChannel::new();
    channel
        .setup(&HashMap::from([
            ("infile".to_string(), peer.infile.display().to_string()),
            ("outfile".to_string(), peer.outfile.display().to_string()),
        ]))
        .expect("setup");
    tokio::time::timeout(Duration::from_secs(30), channel.communicate(None))
        .await
        .expect("communicate timed out")
        .expect("communicate");

    let result = tokio::time::timeout(
        Duration::from_secs(30),
        channel.run_command("echo -n ciao", None, None, None),
    )
    .await
    .expect("run timed out")
    .expect("run")
    .expect("result");
    assert_eq!(result.returncode, 0);
    assert_eq!(result.stdout, "ciao");

    let path = std::env::temp_dir().join(format!(
        "kirk-ltx-live-chan-{}-{}.bin",
        std::process::id(),
        LIVE_COUNTER.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::write(&path, b"channel-bytes").expect("write tmp file");
    let fetched = tokio::time::timeout(
        Duration::from_secs(30),
        channel.fetch_file(&path.display().to_string()),
    )
    .await
    .expect("fetch timed out")
    .expect("fetch");
    assert_eq!(fetched, b"channel-bytes");
    let _ = std::fs::remove_file(&path);

    channel.stop(None).await.expect("stop");
}
