//! `QEMU` communication channel, ported from
//! `kirk/libkirk/channels/qemu.py` (`QemuComChannel`).
//!
//! The guest is spawned as an `argv` vector with
//! [`tokio::process::Command`]: user-controlled paths (image, kernel,
//! `tmpdir`, `options`) are passed as separate arguments and never joined
//! into a shell string, so no `sh -c` is involved at any point.

pub mod config;
pub mod expect;

use std::collections::HashMap;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use async_trait::async_trait;
use kirk_com::CmdResult;
use kirk_com::ComChannel;
use kirk_com::IOBuffer;
use kirk_core::KirkError;
use kirk_plugin::Plugin;
use tokio::io::AsyncReadExt as _;
use tokio::io::AsyncWriteExt as _;

pub use config::QemuConfig;
pub use config::SerialType;
use expect::ExpectState;
use expect::WaitOptions;

/// Expect timeout for login prompts and command replies.
const EXPECT_TIMEOUT: Duration = Duration::from_secs(300);
/// Grace period for `poweroff` before falling back to `kill()`.
const POWER_OFF_TIMEOUT: Duration = Duration::from_secs(30);
/// Serial drain quantum while waiting for `poweroff`.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(2);

/// Random `echo` marker, drawn from OS-seeded randomness
/// (`thread_rng`, seeded by the OS entropy source) so concurrent sessions
/// cannot collide on the same marker.
fn generate_code() -> String {
    use rand::Rng as _;
    let mut rng = rand::thread_rng();
    (0..10)
        .map(|_| char::from(rng.sample(rand::distributions::Alphanumeric)))
        .collect()
}

/// Read the guest transport file written via `<file > <transport_dev>`.
///
/// Runs on a blocking thread (plain `std::fs` I/O must not block the async
/// runtime). The size is checked before *and* after the read because the
/// guest may still be writing between `metadata` and `read` (`TOCTOU`);
/// anything over [`expect::MAX_TRANSPORT_BYTES`] is rejected.
fn read_transport(path: &str, pos: u64) -> Result<(Vec<u8>, u64), KirkError> {
    use std::io::Read as _;
    use std::io::Seek as _;
    use std::io::SeekFrom;

    let communicate = |err: std::io::Error| KirkError::Communication(err.to_string());
    let size = std::fs::metadata(path).map_err(communicate)?.len();
    if size.saturating_sub(pos) > expect::MAX_TRANSPORT_BYTES {
        return Err(KirkError::Communication(
            "Transport file exceeds size cap".to_string(),
        ));
    }
    let mut file = std::fs::File::open(path).map_err(communicate)?;
    file.seek(SeekFrom::Start(pos)).map_err(communicate)?;
    let mut buf = Vec::new();
    file.take(expect::MAX_TRANSPORT_BYTES + 1)
        .read_to_end(&mut buf)
        .map_err(communicate)?;
    let len = u64::try_from(buf.len()).map_err(|err| KirkError::Communication(err.to_string()))?;
    if len > expect::MAX_TRANSPORT_BYTES {
        return Err(KirkError::Communication(
            "Transport file exceeds size cap".to_string(),
        ));
    }
    Ok((buf, pos + len))
}

/// [`expect::SerialIo`] adapter over the `QEMU` child stdout pipe.
struct ProcSerial<'a> {
    stdout: tokio::sync::MutexGuard<'a, Option<tokio::process::ChildStdout>>,
}

#[async_trait]
impl expect::SerialIo for ProcSerial<'_> {
    async fn read_chunk(&mut self, max: usize) -> std::io::Result<Option<String>> {
        let Some(stdout) = self.stdout.as_mut() else {
            return Ok(None);
        };
        let mut buf = vec![0u8; max];
        let len = stdout.read(&mut buf).await?;
        if len == 0 {
            return Ok(None);
        }
        Ok(Some(String::from_utf8_lossy(&buf[..len]).into_owned()))
    }
}

/// `QEMU` communication channel.
pub struct QemuChannel {
    name: String,
    config: Option<QemuConfig>,
    child: Arc<tokio::sync::Mutex<Option<tokio::process::Child>>>,
    reader: Arc<tokio::sync::Mutex<Option<tokio::process::ChildStdout>>>,
    writer: Arc<tokio::sync::Mutex<Option<tokio::process::ChildStdin>>>,
    stderr_task: Option<tokio::task::JoinHandle<()>>,
    comm_lock: Arc<tokio::sync::Mutex<()>>,
    cmd_lock: Arc<tokio::sync::Mutex<()>>,
    fetch_lock: Arc<tokio::sync::Mutex<()>>,
    stopping: Arc<AtomicBool>,
    panicked: Arc<AtomicBool>,
    logged_in: Arc<AtomicBool>,
    last_read: Arc<tokio::sync::Mutex<String>>,
    last_pos: Arc<AtomicU64>,
}

impl QemuChannel {
    /// Create an unconfigured channel named `qemu`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            name: "qemu".to_string(),
            config: None,
            child: Arc::new(tokio::sync::Mutex::new(None)),
            reader: Arc::new(tokio::sync::Mutex::new(None)),
            writer: Arc::new(tokio::sync::Mutex::new(None)),
            stderr_task: None,
            comm_lock: Arc::new(tokio::sync::Mutex::new(())),
            cmd_lock: Arc::new(tokio::sync::Mutex::new(())),
            fetch_lock: Arc::new(tokio::sync::Mutex::new(())),
            stopping: Arc::new(AtomicBool::new(false)),
            panicked: Arc::new(AtomicBool::new(false)),
            logged_in: Arc::new(AtomicBool::new(false)),
            last_read: Arc::new(tokio::sync::Mutex::new(String::new())),
            last_pos: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Fresh, non-running copy sharing nothing but the configuration.
    fn fresh_clone(&self, name: &str) -> Self {
        let mut clone = Self::new();
        clone.name = name.to_string();
        clone.config.clone_from(&self.config);
        clone
    }

    /// Non-blocking liveness probe (reaps the exit status when present).
    fn is_active_sync(&self) -> bool {
        let Ok(mut guard) = self.child.try_lock() else {
            // Another task is mutating the child; assume it stays alive.
            return true;
        };
        match guard.as_mut() {
            Some(child) => matches!(child.try_wait(), Ok(None)),
            None => false,
        }
    }

    /// Read up to `size` bytes from the serial, forwarding to `iobuffer`.
    async fn read_stdout(
        &self,
        size: usize,
        timeout: Duration,
        iobuffer: Option<Arc<dyn IOBuffer>>,
    ) -> Result<String, KirkError> {
        let mut guard = self.reader.lock().await;
        let Some(stdout) = guard.as_mut() else {
            return Ok(String::new());
        };
        let mut buf = vec![0u8; size];
        let len = tokio::time::timeout(timeout, stdout.read(&mut buf))
            .await
            .map_err(|_| KirkError::Communication("Timed out reading from VM serial".to_string()))?
            .map_err(|err| KirkError::Communication(err.to_string()))?;
        let text = String::from_utf8_lossy(&buf[..len]).into_owned();
        if let Some(iobuffer) = &iobuffer {
            iobuffer.write(&text).await?;
        }
        Ok(text)
    }

    /// Write to the serial; silently dropped when the VM is gone.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Communication`] on I/O failure or timeout,
    /// except for a broken pipe during [`ComChannel::stop`].
    async fn write_stdin(&self, data: &str) -> Result<(), KirkError> {
        if !self.is_active_sync() {
            return Ok(());
        }
        let mut guard = self.writer.lock().await;
        let Some(stdin) = guard.as_mut() else {
            return Ok(());
        };
        let broken = |err: std::io::Error| {
            if err.kind() == std::io::ErrorKind::BrokenPipe && self.stopping.load(Ordering::SeqCst)
            {
                Ok(())
            } else {
                Err(KirkError::Communication(err.to_string()))
            }
        };
        tokio::time::timeout(EXPECT_TIMEOUT, stdin.write_all(data.as_bytes()))
            .await
            .map_err(|_| KirkError::Communication("Timed out writing to VM serial".to_string()))?
            .or_else(broken)?;
        tokio::time::timeout(EXPECT_TIMEOUT, stdin.flush())
            .await
            .map_err(|_| KirkError::Communication("Timed out writing to VM serial".to_string()))?
            .or_else(broken)?;
        Ok(())
    }

    /// Wait for `message` on the serial with [`EXPECT_TIMEOUT`].
    async fn wait_for(
        &self,
        message: &str,
        iobuffer: Option<Arc<dyn IOBuffer>>,
    ) -> Result<String, KirkError> {
        let mut state = ExpectState {
            pending: std::mem::take(&mut *self.last_read.lock().await),
            panicked: false,
        };
        let mut io = ProcSerial {
            stdout: self.reader.lock().await,
        };
        let options = WaitOptions {
            timeout: EXPECT_TIMEOUT,
            panic_settle: expect::PANIC_SETTLE,
        };
        let out = expect::wait_for_message(
            &mut io,
            &mut state,
            message,
            options,
            &self.stopping,
            &|| self.is_active_sync(),
            iobuffer,
        )
        .await;
        *self.last_read.lock().await = std::mem::take(&mut state.pending);
        self.panicked.store(state.panicked, Ordering::SeqCst);
        out
    }

    /// Run a guest shell command, returning `(stdout, retcode, exec_time)`.
    async fn exec(
        &self,
        command: &str,
        iobuffer: Option<Arc<dyn IOBuffer>>,
    ) -> Result<(String, i32, f64), KirkError> {
        let code = generate_code();
        let msg = if command.trim_end().is_empty() {
            format!("echo $?-{code}\n")
        } else {
            format!("{command};echo $?-{code}\n")
        };
        let start = tokio::time::Instant::now();
        self.write_stdin(&msg).await?;
        let stdout = self.wait_for(&code, iobuffer).await?;
        let exec_time = start.elapsed().as_secs_f64();
        if self.stopping.load(Ordering::SeqCst) {
            return Ok((stdout, -1, exec_time));
        }
        let (out, retcode) = expect::parse_reply(&stdout, &code)?;
        Ok((out, retcode, exec_time))
    }

    /// Block until no communicate/run/fetch operation is in flight.
    async fn wait_lockers(&self) {
        let _comm = self.comm_lock.lock().await;
        let _cmd = self.cmd_lock.lock().await;
        let _fetch = self.fetch_lock.lock().await;
    }

    /// Login sequence after spawn: login prompt, `stty`, `dmesg`, `PS1`.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError`] when any expect step times out or setup fails.
    async fn login(
        &self,
        cfg: &QemuConfig,
        iobuffer: Option<Arc<dyn IOBuffer>>,
    ) -> Result<(), KirkError> {
        if let Some(user) = &cfg.user {
            self.wait_for("login:", iobuffer.clone()).await?;
            self.write_stdin(&format!("{user}\n")).await?;
            if let Some(password) = &cfg.password {
                self.wait_for("Password:", iobuffer.clone()).await?;
                self.write_stdin(&format!("{password}\n")).await?;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        self.wait_for(&cfg.prompt, iobuffer.clone()).await?;
        tokio::time::sleep(Duration::from_millis(200)).await;
        self.write_stdin("stty -echo; stty cols 1024\n").await?;
        self.wait_for(&cfg.prompt, None).await?;
        self.write_stdin("dmesg -D\n").await?;
        self.wait_for(&cfg.prompt, None).await?;
        let (_, retcode, _) = self.exec("export PS1=''", None).await?;
        if retcode != 0 {
            return Err(KirkError::Communication(
                "Can't setup prompt string".to_string(),
            ));
        }
        if cfg.virtfs.is_some() {
            let (_, retcode, _) = self
                .exec("mount -t 9p -o trans=virtio host0 /mnt", None)
                .await?;
            if retcode != 0 {
                return Err(KirkError::Communication(
                    "Failed to mount virtfs".to_string(),
                ));
            }
        }
        self.logged_in.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// Shared `stop` body (callable without `&mut`).
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Communication`] when the child cannot be killed.
    async fn stop_inner(&self, iobuffer: Option<Arc<dyn IOBuffer>>) -> Result<(), KirkError> {
        if !self.is_active_sync() {
            return Ok(());
        }
        self.stopping.store(true, Ordering::SeqCst);
        if !self.panicked.load(Ordering::SeqCst) {
            let cmd_busy = self.cmd_lock.try_lock().is_err();
            let fetch_busy = self.fetch_lock.try_lock().is_err();
            if cmd_busy || fetch_busy {
                // Interrupt character (equivalent of CTRL+C).
                self.write_stdin("\x03").await?;
                self.wait_lockers().await;
            }
            if self.logged_in.load(Ordering::SeqCst) {
                self.write_stdin("poweroff; poweroff -f\n").await?;
                let deadline = tokio::time::Instant::now() + POWER_OFF_TIMEOUT;
                while self.is_active_sync() && tokio::time::Instant::now() < deadline {
                    // Drain so the guest never blocks on a full pipe.
                    let _ = self
                        .read_stdout(1024, DRAIN_TIMEOUT, iobuffer.clone())
                        .await;
                }
                if let Some(child) = self.child.lock().await.as_mut() {
                    // Returns immediately when the child already exited;
                    // on timeout fall through to `kill()` below.
                    let _ = tokio::time::timeout(Duration::from_secs(5), child.wait()).await;
                }
            }
        }
        if self.is_active_sync() {
            if let Some(child) = self.child.lock().await.as_mut() {
                child
                    .kill()
                    .await
                    .map_err(|err| KirkError::Communication(err.to_string()))?;
            }
            self.wait_lockers().await;
            if let Some(child) = self.child.lock().await.as_mut() {
                child
                    .wait()
                    .await
                    .map_err(|err| KirkError::Communication(err.to_string()))?;
            }
        }
        self.stopping.store(false, Ordering::SeqCst);
        Ok(())
    }

    fn configured(&self) -> Result<QemuConfig, KirkError> {
        self.config
            .clone()
            .ok_or_else(|| KirkError::Communication("QEMU channel is not configured".to_string()))
    }
}

impl Default for QemuChannel {
    fn default() -> Self {
        Self::new()
    }
}

impl Plugin for QemuChannel {
    fn name(&self) -> &str {
        &self.name
    }

    fn config_help(&self) -> HashMap<String, String> {
        HashMap::from([
            ("image".to_string(), "qemu image location".to_string()),
            ("kernel".to_string(), "kernel image location".to_string()),
            ("initrd".to_string(), "initrd image location".to_string()),
            ("user".to_string(), "user name (default: '')".to_string()),
            (
                "password".to_string(),
                "user password (default: '')".to_string(),
            ),
            (
                "prompt".to_string(),
                "prompt string (default: '#')".to_string(),
            ),
            (
                "system".to_string(),
                "system architecture (default: x86_64)".to_string(),
            ),
            ("ram".to_string(), "RAM of the VM (default: 2G)".to_string()),
            ("smp".to_string(), "number of CPUs (default: 2)".to_string()),
            (
                "serial".to_string(),
                "type of serial protocol. isa|virtio (default: isa)".to_string(),
            ),
            (
                "virtfs".to_string(),
                "directory to mount inside VM".to_string(),
            ),
            ("options".to_string(), "user defined options".to_string()),
        ])
    }

    fn setup(&mut self, cfg: &HashMap<String, String>) -> Result<(), KirkError> {
        self.config = Some(QemuConfig::from_map(cfg)?);
        Ok(())
    }

    fn clone_box(&self, name: &str) -> Box<dyn Plugin> {
        Box::new(self.fresh_clone(name))
    }
}

#[async_trait]
impl ComChannel for QemuChannel {
    fn parallel_execution(&self) -> bool {
        false
    }

    async fn active(&self) -> bool {
        self.is_active_sync()
    }

    async fn communicate(&mut self, iobuffer: Option<Arc<dyn IOBuffer>>) -> Result<(), KirkError> {
        let cfg = self.configured()?;
        if self.is_active_sync() {
            return Err(KirkError::Communication(
                "Virtual machine is already running".to_string(),
            ));
        }
        let _comm = self.comm_lock.lock().await;
        if self.is_active_sync() {
            return Err(KirkError::Communication(
                "Virtual machine is already running".to_string(),
            ));
        }
        self.logged_in.store(false, Ordering::SeqCst);
        self.panicked.store(false, Ordering::SeqCst);
        self.stopping.store(false, Ordering::SeqCst);
        *self.last_read.lock().await = String::new();
        self.last_pos.store(0, Ordering::SeqCst);

        let pid = std::process::id();
        let (program, argv) = cfg.build_argv(pid)?;
        // Spawned as argv directly: never joined into a shell string.
        let mut child = tokio::process::Command::new(&program)
            .args(&argv)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|err| {
                if err.kind() == std::io::ErrorKind::NotFound {
                    KirkError::Communication(format!("Command not found: {program}"))
                } else {
                    KirkError::Communication(err.to_string())
                }
            })?;
        *self.writer.lock().await = child.stdin.take();
        *self.reader.lock().await = child.stdout.take();
        if let Some(stderr) = child.stderr.take() {
            // Drain guest stderr so the VM never blocks on a full pipe;
            // aborted on `stop`, otherwise exits on EOF after the kill.
            let mut stderr = stderr;
            self.stderr_task = Some(tokio::spawn(async move {
                let mut buf = [0u8; 4096];
                loop {
                    match stderr.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {}
                    }
                }
            }));
        }
        *self.child.lock().await = Some(child);

        match self.login(&cfg, iobuffer.clone()).await {
            Ok(()) => Ok(()),
            Err(KirkError::Communication(message)) => {
                // Mirror upstream: shut down unless a stop is in progress,
                // then re-raise as a communication error.
                if !self.stopping.load(Ordering::SeqCst) {
                    self.stop_inner(iobuffer).await?;
                }
                Err(KirkError::Communication(message))
            }
            Err(err) => Err(err),
        }?;
        Ok(())
    }

    async fn stop(&mut self, iobuffer: Option<Arc<dyn IOBuffer>>) -> Result<(), KirkError> {
        let result = self.stop_inner(iobuffer).await;
        if let Some(handle) = self.stderr_task.take() {
            handle.abort();
        }
        result
    }

    async fn ping(&mut self) -> Result<f64, KirkError> {
        if !self.is_active_sync() {
            return Err(KirkError::Communication("Qemu is not running".to_string()));
        }
        let (_, _, exec_time) = self.exec("test .", None).await?;
        Ok(exec_time)
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
        if !self.is_active_sync() {
            return Err(KirkError::Communication(
                "Virtual machine is not running".to_string(),
            ));
        }
        let _cmd = self.cmd_lock.lock().await;
        if !self.is_active_sync() {
            return Err(KirkError::Communication(
                "Virtual machine is not running".to_string(),
            ));
        }
        if let Some(cwd) = cwd {
            let quoted = expect::shell_quote(cwd);
            let (stdout, retcode, _) = self.exec(&format!("cd -- {quoted}"), None).await?;
            if retcode != 0 {
                return Err(KirkError::Communication(format!(
                    "Can't setup current working directory: {stdout}"
                )));
            }
        }
        if let Some(env) = env {
            let mut vars: Vec<(&String, &String)> = env.iter().collect();
            vars.sort_by(|left, right| left.0.cmp(right.0));
            for (key, value) in vars {
                expect::validate_env_key(key)?;
                let quoted = expect::shell_quote(value);
                let (stdout, retcode, _) =
                    self.exec(&format!("export {key}={quoted}"), None).await?;
                if retcode != 0 {
                    return Err(KirkError::Communication(format!(
                        "Can't setup env {key}={value}: {stdout}"
                    )));
                }
            }
        }
        let (stdout, returncode, exec_time) = self.exec(command, iobuffer).await?;
        Ok(Some(CmdResult {
            command: command.to_string(),
            returncode,
            stdout,
            exec_time,
        }))
    }

    async fn fetch_file(&mut self, target_path: &str) -> Result<Vec<u8>, KirkError> {
        if target_path.is_empty() {
            return Err(KirkError::Communication("target path is empty".to_string()));
        }
        if !self.is_active_sync() {
            return Err(KirkError::Communication(
                "Virtual machine is not running".to_string(),
            ));
        }
        let _fetch = self.fetch_lock.lock().await;
        if !self.is_active_sync() {
            return Err(KirkError::Communication(
                "Virtual machine is not running".to_string(),
            ));
        }
        let quoted = expect::shell_quote(target_path);
        let (_, retcode, _) = self.exec(&format!("test -f {quoted}"), None).await?;
        if retcode != 0 {
            return Err(KirkError::Communication(format!(
                "'{target_path}' doesn't exist"
            )));
        }
        let pid = std::process::id();
        let (transport_dev, transport_path) = self.configured()?.transport(pid);
        let (stdout, retcode, _) = self
            .exec(&format!("cat {quoted} > {transport_dev}"), None)
            .await?;
        if self.stopping.load(Ordering::SeqCst) {
            return Ok(Vec::new());
        }
        // Mirror upstream, which tolerates the reader being torn down with
        // the guest (`SIGHUP`/`SIGKILL` overlap shell exit codes 1/9 here).
        if !matches!(retcode, 0 | 1 | 9) {
            return Err(KirkError::Communication(format!(
                "Can't send file to {transport_dev}: {stdout}"
            )));
        }
        let pos = self.last_pos.load(Ordering::SeqCst);
        // Blocking file I/O stays off the async runtime.
        let (data, new_pos) =
            tokio::task::spawn_blocking(move || read_transport(&transport_path, pos))
                .await
                .map_err(|err| KirkError::Communication(err.to_string()))??;
        self.last_pos.store(new_pos, Ordering::SeqCst);
        Ok(data)
    }

    fn clone_channel_box(&self, new_name: &str) -> Box<dyn ComChannel> {
        Box::new(self.fresh_clone(new_name))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transport_read_caps_size() {
        let path =
            std::env::temp_dir().join(format!("kirk-qemu-cap-{}-{}", std::process::id(), "test"));
        std::fs::write(&path, vec![b'x'; 16]).expect("write fixture");
        let path_str = path.to_str().expect("UTF-8").to_string();
        let (data, pos) = read_transport(&path_str, 0).expect("small read works");
        assert_eq!(data.len(), 16);
        assert_eq!(pos, 16);
        let (rest, pos) = read_transport(&path_str, 16).expect("read at end");
        assert!(rest.is_empty());
        assert_eq!(pos, 16);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn transport_read_beyond_cap_fails() {
        let path =
            std::env::temp_dir().join(format!("kirk-qemu-cap-{}-{}", std::process::id(), "big"));
        std::fs::write(&path, vec![b'x'; 32]).expect("write fixture");
        let path_str = path.to_str().expect("UTF-8").to_string();
        // Fake a stale `last_pos` far behind a huge file: metadata fast-fail.
        let err = read_transport(
            &path_str,
            0u64.wrapping_sub(expect::MAX_TRANSPORT_BYTES + 64),
        )
        .expect_err("oversized delta must fail");
        assert!(matches!(err, KirkError::Communication(_)));
        std::fs::remove_file(&path).ok();
    }
}
