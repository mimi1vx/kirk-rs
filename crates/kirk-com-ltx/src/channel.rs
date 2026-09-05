//! `LTXComChannel` ported from `kirk/libkirk/channels/ltx_chan.py`.
//!
//! Slot pool, `cwd`/`env`/`exec` chaining, `exec_time` accounting and
//! `fetch_file` traversal rejection live here; the FIFO transport lives in
//! [`crate::ltx`].
//!
//! Concurrency note: the Python channel serializes slot allocation and file
//! fetches with `asyncio.Lock`s because tasks share one object. Here every
//! mutating [`kirk_com::ComChannel`] method takes `&mut self`, so the borrow
//! checker already serializes access and no locks are needed.

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
#[derive(Debug)]
pub struct LtxChannel {
    name: String,
    infile: String,
    outfile: String,
    ltx: Option<Ltx>,
    slots: Vec<u8>,
}

impl LtxChannel {
    /// Create an unconfigured channel named `ltx`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            name: "ltx".to_string(),
            infile: String::new(),
            outfile: String::new(),
            ltx: None,
            slots: Vec::new(),
        }
    }

    /// Reserve the first free execution slot, scanning `0..MAX_SLOTS` like
    /// the Python `_reserve_slot`.
    fn reserve_slot(&mut self) -> Result<u8, KirkError> {
        for id in 0..MAX_SLOTS {
            if !self.slots.contains(&id) {
                self.slots.push(id);
                return Ok(id);
            }
        }
        Err(KirkError::Communication(
            "No execution slots available".to_string(),
        ))
    }

    /// Release an execution slot.
    fn release_slot(&mut self, slot: u8) {
        if let Some(position) = self.slots.iter().position(|id| *id == slot) {
            self.slots.remove(position);
        }
    }

    /// Send `KILL` for every reserved slot; the `stop` half of the Python
    /// slot cleanup (without the drain wait, so tests stay deterministic).
    async fn kill_in_flight(&self) -> Result<(), KirkError> {
        if self.slots.is_empty() {
            return Ok(());
        }
        let kills = self
            .slots
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
            .ltx
            .as_ref()
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
        Box::new(Self {
            name: name.to_string(),
            infile: self.infile.clone(),
            outfile: self.outfile.clone(),
            ltx: None,
            slots: Vec::new(),
        })
    }
}

#[async_trait]
impl ComChannel for LtxChannel {
    fn parallel_execution(&self) -> bool {
        true
    }

    async fn active(&self) -> bool {
        self.ltx.as_ref().is_some_and(Ltx::connected)
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
        self.ltx = Some(ltx);
        Ok(())
    }

    async fn stop(&mut self, _iobuffer: Option<Arc<dyn IOBuffer>>) -> Result<(), KirkError> {
        if !self.active().await {
            return Ok(());
        }
        self.kill_in_flight().await?;
        while !self.slots.is_empty() && self.active().await {
            tokio::time::sleep(STOP_POLL_INTERVAL).await;
        }
        if let Some(ltx) = self.ltx.take() {
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
        Box::new(Self {
            name: new_name.to_string(),
            infile: self.infile.clone(),
            outfile: self.outfile.clone(),
            ltx: None,
            slots: Vec::new(),
        })
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
        let mut channel = LtxChannel::new();
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
        assert_eq!(result.stdout, "mock-out");
        assert!(result.exec_time >= 0.0);
        assert_eq!(sink.lock().await.as_str(), "mock-out");

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
}
