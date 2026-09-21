//! `LTXComChannel` ported from `kirk/libkirk/channels/ltx_chan.py`.
//!
//! Slot pool, `cwd`/`env`/`exec` chaining, `exec_time` accounting and
//! `fetch_file` traversal rejection live here; the FIFO transport lives in
//! [`crate::ltx`].
//!
//! Concurrency note: [`ComChannel::concurrent_handle`] hands out
//! [`LtxChannel`] clones that share the live connection ([`Ltx`] is already
//! `Arc`-backed) and the slot pool (`slots`, behind a `std::sync::Mutex`),
//! mirroring the Python channel's `asyncio.Lock`-guarded slot allocation now
//! that handles genuinely run concurrently instead of being serialized by
//! the borrow checker's `&mut self`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use kirk_com::{CmdResult, ComChannel, IOBuffer};
use kirk_core::KirkError;
use kirk_plugin::Plugin;
use tokio::sync::mpsc;

use crate::ltx::Ltx;
use crate::request::{MAX_SLOTS, Reply, Request, SlotId};

/// Delay between slot-drain polls in [`LtxChannel::stop`], mirroring the
/// Python `asyncio.sleep(1e-2)`.
const STOP_POLL_INTERVAL: Duration = Duration::from_millis(10);

/// Reject unsafe download paths.
///
/// Absolute and plain relative paths are allowed (the live tests fetch
/// absolute paths); parent-directory components and NUL bytes are not.
///
/// # Errors
///
/// Returns [`KirkError::Communication`] for empty or unsafe paths.
fn check_target_path(path: &str) -> Result<(), KirkError> {
    if path.is_empty() {
        return Err(KirkError::Communication("target path is empty".to_string()));
    }
    if path.contains('\0') {
        return Err(KirkError::Communication(
            "target path contains NUL byte".to_string(),
        ));
    }
    if path.split('/').any(|component| component == "..") {
        return Err(KirkError::Communication(format!(
            "target path '{path}' escapes with '..'"
        )));
    }
    Ok(())
}

/// Communication channel driving an LTX executor over a FIFO pair.
///
/// [`Clone`] shares the live connection and slot pool (used by
/// [`ComChannel::concurrent_handle`] for concurrent `run_command`/`stop`);
/// [`Plugin::clone_box`] and [`ComChannel::clone_channel_box`] build a fresh,
/// disconnected instance instead.
#[derive(Debug, Clone)]
pub struct LtxChannel {
    name: String,
    infile: String,
    outfile: String,
    // `Arc<Ltx>` (not bare `Ltx`): `Ltx`'s `Drop` stops the shared poll task
    // when *its* last reference goes away, on the assumption that a
    // dropped `Ltx` means the connection is abandoned. `send_requests`
    // below clones this handle out on every call; cloning the outer `Arc`
    // only bumps a refcount, so a transient per-call handle going out of
    // scope never looks like the connection being abandoned.
    ltx: Arc<std::sync::Mutex<Option<Arc<Ltx>>>>,
    slots: Arc<std::sync::Mutex<Vec<u8>>>,
}

impl LtxChannel {
    /// Create an unconfigured channel named `ltx`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            name: "ltx".to_string(),
            infile: String::new(),
            outfile: String::new(),
            ltx: Arc::new(std::sync::Mutex::new(None)),
            slots: Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }

    /// Build a fresh, disconnected instance with an empty slot pool,
    /// sharing no state with `self`.
    fn disconnected_clone(&self, new_name: &str) -> Self {
        Self {
            name: new_name.to_string(),
            infile: self.infile.clone(),
            outfile: self.outfile.clone(),
            ltx: Arc::new(std::sync::Mutex::new(None)),
            slots: Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }

    fn slots_lock(&self) -> std::sync::MutexGuard<'_, Vec<u8>> {
        self.slots
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn ltx_lock(&self) -> std::sync::MutexGuard<'_, Option<Arc<Ltx>>> {
        self.ltx
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Clone the shared connection handle out of the lock; cloning the
    /// outer `Arc` is a refcount bump, never invoking `Ltx`'s own `Drop`,
    /// so this never holds the lock across an `.await`.
    fn ltx_handle(&self) -> Option<Arc<Ltx>> {
        self.ltx_lock().clone()
    }

    /// Reserve the first free execution slot, scanning `0..MAX_SLOTS` like
    /// the Python `_reserve_slot`. Slots are shared across every
    /// [`ComChannel::concurrent_handle`] clone, so two handles never
    /// reserve the same one.
    fn reserve_slot(&self) -> Result<u8, KirkError> {
        let mut slots = self.slots_lock();
        for id in 0..MAX_SLOTS {
            if !slots.contains(&id) {
                slots.push(id);
                return Ok(id);
            }
        }
        Err(KirkError::Communication(
            "No execution slots available".to_string(),
        ))
    }

    /// Release an execution slot.
    fn release_slot(&self, slot: u8) {
        let mut slots = self.slots_lock();
        if let Some(position) = slots.iter().position(|id| *id == slot) {
            slots.remove(position);
        }
    }

    fn slots_empty(&self) -> bool {
        self.slots_lock().is_empty()
    }

    /// Send `KILL` for every reserved slot; the `stop` half of the Python
    /// slot cleanup (without the drain wait, so tests stay deterministic).
    async fn kill_in_flight(&self) -> Result<(), KirkError> {
        let slots = self.slots_lock().clone();
        if slots.is_empty() {
            return Ok(());
        }
        let kills = slots
            .iter()
            .map(|slot| {
                SlotId::new(*slot)
                    .map(Request::kill)
                    .expect("reserved slots are always valid slot ids")
            })
            .collect();
        self.send_requests(kills).await.map(|_| ())
    }

    /// Gather requests, mapping LTX failures to communication errors like the
    /// Python `_send_requests`.
    async fn send_requests(&self, requests: Vec<Request>) -> Result<Vec<Reply>, KirkError> {
        let ltx = self
            .ltx_handle()
            .ok_or_else(|| KirkError::Communication("LTX connection is not present".to_string()))?;
        ltx.gather(requests)
            .await
            .map_err(|error| KirkError::Communication(error.to_string()))
    }

    /// Run `command` on `slot` with the `cwd`/`env`/`exec` chaining of the
    /// Python `run_command`, streaming stdout into `iobuffer`.
    async fn execute_on_slot(
        &self,
        slot: u8,
        command: &str,
        cwd: Option<&str>,
        env: Option<&HashMap<String, String>>,
        iobuffer: Option<Arc<dyn IOBuffer>>,
    ) -> Result<CmdResult, KirkError> {
        let started = std::time::Instant::now();

        let mut requests = Vec::new();
        if let Some(path) = cwd {
            requests.push(Request::cwd(Some(slot), path)?);
        }
        if let Some(vars) = env {
            for (key, value) in vars {
                requests.push(Request::env(Some(slot), key, value)?);
            }
        }

        let (stdout_tx, stdout_rx) = mpsc::unbounded_channel::<String>();
        let exec = Request::execute_with_stdout(
            SlotId::new(slot).expect("reserved slots are always valid slot ids"),
            command,
            stdout_tx,
        )?;
        requests.push(exec);

        let forward = stdout_rx_task(stdout_rx, iobuffer);
        let replies = match self.send_requests(requests).await {
            Ok(replies) => replies,
            Err(error) => {
                forward.abort();
                return Err(error);
            }
        };
        forward.await.ok();

        let Reply::Execute {
            si_status, stdout, ..
        } = replies
            .into_iter()
            .next_back()
            .unwrap_or(Reply::Version(String::new()))
        else {
            return Err(KirkError::Communication(
                "LTX returned an unexpected reply for EXEC".to_string(),
            ));
        };
        Ok(CmdResult {
            command: command.to_string(),
            returncode: si_status,
            stdout,
            // Deviation: Python subtracts the peer's monotonic timestamp from
            // `time.monotonic()`; without `clock_gettime` in std this measures
            // the local round-trip instead, which is the same quantity on one
            // host plus sub-millisecond gather overhead.
            exec_time: started.elapsed().as_secs_f64(),
        })
    }
}

impl Default for LtxChannel {
    fn default() -> Self {
        Self::new()
    }
}

/// Forward streamed stdout chunks into `iobuffer`; finishes when the `EXEC`
/// request completes and its sender is dropped.
fn stdout_rx_task(
    mut stdout_rx: mpsc::UnboundedReceiver<String>,
    iobuffer: Option<Arc<dyn IOBuffer>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let Some(iobuffer) = iobuffer else {
            return;
        };
        while let Some(data) = stdout_rx.recv().await {
            if iobuffer.write(&data).await.is_err() {
                break;
            }
        }
    })
}

impl Plugin for LtxChannel {
    fn name(&self) -> &str {
        &self.name
    }

    fn config_help(&self) -> HashMap<String, String> {
        HashMap::from([
            (
                "infile".to_string(),
                "file where ltx is reading data".to_string(),
            ),
            (
                "outfile".to_string(),
                "file where ltx is writing data".to_string(),
            ),
        ])
    }

    fn setup(&mut self, cfg: &HashMap<String, String>) -> Result<(), KirkError> {
        let infile = cfg.get("infile").map_or("", String::as_str);
        let outfile = cfg.get("outfile").map_or("", String::as_str);
        if infile.is_empty() || !std::path::Path::new(infile).exists() {
            return Err(KirkError::Communication(format!(
                "'{infile}' input file doesn't exist"
            )));
        }
        if outfile.is_empty() || !std::path::Path::new(outfile).exists() {
            return Err(KirkError::Communication(format!(
                "'{outfile}' output file doesn't exist"
            )));
        }
        self.infile = infile.to_string();
        self.outfile = outfile.to_string();
        Ok(())
    }

    fn clone_box(&self, name: &str) -> Box<dyn Plugin> {
        Box::new(self.disconnected_clone(name))
    }
}

#[async_trait]
impl ComChannel for LtxChannel {
    fn parallel_execution(&self) -> bool {
        true
    }

    async fn active(&self) -> bool {
        self.ltx_lock().as_ref().is_some_and(|ltx| ltx.connected())
    }

    async fn communicate(&mut self, _iobuffer: Option<Arc<dyn IOBuffer>>) -> Result<(), KirkError> {
        if self.active().await {
            return Err(KirkError::Communication(
                "LTX is already running".to_string(),
            ));
        }
        let ltx = Ltx::new(self.infile.clone().into(), self.outfile.clone().into());
        if let Err(error) = ltx.connect().await {
            return Err(KirkError::Communication(error.to_string()));
        }
        if let Err(error) = ltx.gather(vec![Request::version()]).await {
            let _ = ltx.disconnect().await;
            return Err(KirkError::Communication(error.to_string()));
        }
        *self.ltx_lock() = Some(Arc::new(ltx));
        Ok(())
    }

    async fn stop(&mut self, _iobuffer: Option<Arc<dyn IOBuffer>>) -> Result<(), KirkError> {
        if !self.active().await {
            return Ok(());
        }
        self.kill_in_flight().await?;
        while !self.slots_empty() && self.active().await {
            tokio::time::sleep(STOP_POLL_INTERVAL).await;
        }
        let ltx = self.ltx_lock().take();
        if let Some(ltx) = ltx {
            ltx.disconnect()
                .await
                .map_err(|error| KirkError::Communication(error.to_string()))?;
        }
        Ok(())
    }

    async fn ping(&mut self) -> Result<f64, KirkError> {
        if !self.active().await {
            return Err(KirkError::Communication("LTX is not running".to_string()));
        }
        let started = std::time::Instant::now();
        self.send_requests(vec![Request::ping()]).await?;
        // Deviation: see `execute_on_slot` — local round-trip instead of the
        // peer timestamp minus start.
        Ok(started.elapsed().as_secs_f64())
    }

    async fn run_command(
        &mut self,
        command: &str,
        cwd: Option<&str>,
        env: Option<&HashMap<String, String>>,
        iobuffer: Option<Arc<dyn IOBuffer>>,
    ) -> Result<Option<CmdResult>, KirkError> {
        if command.is_empty() {
            return Err(KirkError::Communication("command is empty".to_string()));
        }
        if !self.active().await {
            return Err(KirkError::Communication("LTX is not running".to_string()));
        }
        let slot = self.reserve_slot()?;
        let outcome = self
            .execute_on_slot(slot, command, cwd, env, iobuffer)
            .await;
        self.release_slot(slot);
        outcome.map(Some)
    }

    async fn fetch_file(&mut self, target_path: &str) -> Result<Vec<u8>, KirkError> {
        check_target_path(target_path)?;
        if !self.active().await {
            return Err(KirkError::Communication(
                "LTX connection is not present".to_string(),
            ));
        }
        let request = Request::get_file(target_path)?;
        let mut replies = self.send_requests(vec![request]).await?;
        let Some(Reply::GetFile { data, .. }) = replies.pop() else {
            return Err(KirkError::Communication(
                "LTX returned an unexpected reply for GET_FILE".to_string(),
            ));
        };
        Ok(data)
    }

    fn clone_channel_box(&self, new_name: &str) -> Box<dyn ComChannel> {
        Box::new(self.disconnected_clone(new_name))
    }

    fn concurrent_handle(&self) -> Option<Box<dyn ComChannel>> {
        Some(Box::new(self.clone()))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    use kirk_com::{ComChannel, IOBuffer};
    use kirk_core::KirkError;
    use kirk_plugin::Plugin;

    use super::LtxChannel;
    use crate::ltx::test_support::{Fifos, Seen, run_mock};
    use crate::request::MAX_SLOTS;

    struct Sink(Arc<tokio::sync::Mutex<String>>);

    #[async_trait::async_trait]
    impl IOBuffer for Sink {
        async fn write(&self, data: &str) -> Result<(), KirkError> {
            self.0.lock().await.push_str(data);
            Ok(())
        }
    }

    fn configured(fifos: &Fifos) -> LtxChannel {
        let mut channel = LtxChannel::new();
        channel
            .setup(&HashMap::from([
                ("infile".to_string(), fifos.infile.display().to_string()),
                ("outfile".to_string(), fifos.outfile.display().to_string()),
            ]))
            .expect("setup");
        channel
    }

    #[test]
    fn setup_rejects_missing_files() {
        let mut channel = LtxChannel::new();
        assert!(channel.setup(&HashMap::new()).is_err());
        assert!(
            channel
                .setup(&HashMap::from([
                    ("infile".to_string(), "/nonexistent-in".to_string()),
                    ("outfile".to_string(), "/nonexistent-out".to_string()),
                ]))
                .is_err()
        );
    }

    #[test]
    fn rejects_unsafe_target_paths() {
        assert!(super::check_target_path("").is_err());
        assert!(super::check_target_path("/tmp/../etc/passwd").is_err());
        assert!(super::check_target_path("a/../../b").is_err());
        assert!(super::check_target_path("/tmp/\0evil").is_err());
        assert!(super::check_target_path("/tmp/file.bin").is_ok());
        assert!(super::check_target_path("relative/file").is_ok());
    }

    #[tokio::test]
    async fn slot_pool_exhausts_and_recovers() {
        let channel = LtxChannel::new();
        let mut held = Vec::new();
        for _ in 0..MAX_SLOTS {
            held.push(channel.reserve_slot().expect("slot"));
        }
        assert!(channel.reserve_slot().is_err());
        channel.release_slot(held[0]);
        assert!(channel.reserve_slot().is_ok());
    }

    #[tokio::test]
    async fn inactive_channel_errors() {
        let mut channel = LtxChannel::new();
        assert!(!channel.active().await);
        assert!(channel.stop(None).await.is_ok());
        assert!(channel.ping().await.is_err());
        assert!(channel.fetch_file("/tmp/f").await.is_err());
        assert!(channel.run_command("echo", None, None, None).await.is_err());
        assert!(channel.run_command("", None, None, None).await.is_err());
    }

    #[tokio::test]
    async fn full_loopback_flow() {
        let fifos = Fifos::create("channel");
        let seen: Seen = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicU64::new(0));
        let mock = tokio::spawn(run_mock(
            fifos.infile.clone(),
            fifos.outfile.clone(),
            seen.clone(),
            stop.clone(),
        ));

        let mut channel = configured(&fifos);
        assert!(channel.parallel_execution());
        channel.communicate(None).await.expect("communicate");
        assert!(channel.active().await);
        assert!(channel.communicate(None).await.is_err());

        let ping = channel.ping().await.expect("ping");
        assert!(ping >= 0.0);

        let sink = Arc::new(tokio::sync::Mutex::new(String::new()));
        let env = HashMap::from([("HELLO".to_string(), "CIAO".to_string())]);
        let result = channel
            .run_command(
                "echo hi",
                Some("/tmp"),
                Some(&env),
                Some(Arc::new(Sink(sink.clone())) as Arc<dyn IOBuffer>),
            )
            .await
            .expect("run")
            .expect("result");
        assert_eq!(result.command, "echo hi");
        assert_eq!(result.returncode, 0);
        assert_eq!(result.stdout, "mock-out-0");
        assert!(result.exec_time >= 0.0);
        assert_eq!(sink.lock().await.as_str(), "mock-out-0");

        let data = channel.fetch_file("/tmp/f").await.expect("fetch");
        assert_eq!(data, b"file-bytes");

        channel.stop(None).await.expect("stop");
        assert!(!channel.active().await);
        // Clean shutdown with no in-flight commands sends no KILL.
        assert!(
            !seen.lock().await.contains(&crate::request::OP_KILL),
            "no KILL on clean stop"
        );

        stop.store(1, Ordering::SeqCst);
        mock.await.expect("mock joins");
        fifos.cleanup();
    }

    #[tokio::test]
    async fn stop_kills_in_flight_slots() {
        let fifos = Fifos::create("killstop");
        let seen: Seen = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicU64::new(0));
        let mock = tokio::spawn(run_mock(
            fifos.infile.clone(),
            fifos.outfile.clone(),
            seen.clone(),
            stop.clone(),
        ));

        let mut channel = configured(&fifos);
        channel.communicate(None).await.expect("communicate");
        // Simulate in-flight commands holding slots 0 and 1, then run the
        // kill half of `stop` and check both slots are killed.
        channel.reserve_slot().expect("slot 0");
        channel.reserve_slot().expect("slot 1");
        channel.kill_in_flight().await.expect("kills");

        let mut kills = 0;
        for opcode in seen.lock().await.iter() {
            if *opcode == crate::request::OP_KILL {
                kills += 1;
            }
        }
        assert_eq!(kills, 2, "one KILL per in-flight slot");

        channel.release_slot(0);
        channel.release_slot(1);
        channel.stop(None).await.expect("stop");
        assert!(!channel.active().await);

        stop.store(1, Ordering::SeqCst);
        mock.await.expect("mock joins");
        fifos.cleanup();
    }

    #[tokio::test]
    async fn concurrent_handles_reserve_distinct_slots() {
        let channel = LtxChannel::new();
        let handle_a = channel.clone();
        let handle_b = channel.clone();

        let slot_a = handle_a.reserve_slot().expect("slot a");
        let slot_b = handle_b.reserve_slot().expect("slot b");

        assert_ne!(slot_a, slot_b, "handles share one slot pool");
        handle_a.release_slot(slot_a);
        handle_b.release_slot(slot_b);
        assert!(channel.reserve_slot().is_ok());
    }

    #[tokio::test]
    async fn concurrent_run_command_uses_distinct_slots_and_own_output() {
        let fifos = Fifos::create("concurrent");
        let seen: Seen = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicU64::new(0));
        let mock = tokio::spawn(run_mock(
            fifos.infile.clone(),
            fifos.outfile.clone(),
            seen.clone(),
            stop.clone(),
        ));

        let mut channel = configured(&fifos);
        channel.communicate(None).await.expect("communicate");

        let mut handle_a = channel.concurrent_handle().expect("ltx offers a handle");
        let mut handle_b = channel.concurrent_handle().expect("ltx offers a handle");

        let (result_a, result_b) = tokio::join!(
            handle_a.run_command("echo a", None, None, None),
            handle_b.run_command("echo b", None, None, None)
        );
        let result_a = result_a.expect("run a").expect("result a");
        let result_b = result_b.expect("run b").expect("result b");

        assert_eq!(result_a.command, "echo a");
        assert_eq!(result_b.command, "echo b");
        // Each result's stdout carries the slot it actually ran on
        // (see `reply_for`'s OP_EXEC arm); distinct values prove the two
        // in-flight commands were not cross-delivered onto each other's
        // slot, the failure mode a shared, unsynchronized `slots` field
        // would produce.
        assert_ne!(
            result_a.stdout, result_b.stdout,
            "commands must not cross-deliver replies"
        );
        assert!(result_a.stdout.starts_with("mock-out-"));
        assert!(result_b.stdout.starts_with("mock-out-"));

        channel.stop(None).await.expect("stop");
        stop.store(1, Ordering::SeqCst);
        mock.await.expect("mock joins");
        fifos.cleanup();
    }
}
