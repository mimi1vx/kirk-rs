//! SSH communication channel ported from `kirk/libkirk/channels/ssh.py`.
//!
//! [`SshChannel`] uses `russh` in place of upstream `asyncssh`. Live
//! behavior deliberately differs in a few hardened spots (see [`SshConfig`]
//! docs and `GAPS` below); the wire-visible shape — `communicate`, `stop`,
//! `ping`, `run_command`, `fetch_file` — mirrors upstream.
//!
//! # Gaps vs `ssh.py`
//!
//! - Host-key verification cannot be disabled (`/dev/null` is rejected);
//!   OpenSSH certificates are rejected (fail closed).
//! - `sudo` accepts only `0`/`1` (upstream coerces any other int to false).
//! - `cwd`/env values are shell-quoted and env names validated (upstream
//!   interpolates them raw).
//! - `stop` disconnects the session instead of tracking per-channel
//!   handles; in-flight commands fail closed on the dropped session.
//! - `reset_cmd` runs via argv, never via a shell (upstream uses
//!   `create_subprocess_shell`).
//! - `fetch_file` enforces `FETCH_SIZE_CAP`.
//! - No overall timeout on the `run_command` data loop (matches upstream
//!   `wait_closed`); every initiating call (dial/auth/open/exec) has one.

pub mod config;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use kirk_com::{CmdResult, ComChannel, IOBuffer};
use kirk_core::KirkError;
use kirk_plugin::Plugin;
use russh::client::{AuthResult, Config as RusshConfig, Handle, Handler};
use tokio::time::timeout;
use zeroize::Zeroizing;

pub use config::SshConfig;
use config::{
    DEFAULT_MAX_SESSIONS, FETCH_SIZE_CAP, IO_TIMEOUT, OutputCollector, PROBE_TIMEOUT,
    RESET_TIMEOUT, build_remote_command, parse_max_sessions, quote_sh, split_argv,
};

type Session = Handle<HostKeyVerifier>;

/// russh client handler enforcing known-hosts verification.
///
/// Unknown or changed keys are rejected; there is no accept-unknown path.
struct HostKeyVerifier {
    host: String,
    port: u16,
    known_hosts: String,
}

impl Handler for HostKeyVerifier {
    type Error = russh::Error;

    #[allow(
        clippy::unused_async_trait_impl,
        reason = "russh Handler requires async; verification itself needs no await"
    )]
    async fn check_server_key(
        &mut self,
        key: &russh::keys::PublicKeyOrCertificate,
    ) -> Result<bool, Self::Error> {
        match key {
            russh::keys::PublicKeyOrCertificate::PublicKey { key, .. } => Ok(
                russh::keys::check_known_hosts_path(&self.host, self.port, key, &self.known_hosts)
                    .unwrap_or(false),
            ),
            russh::keys::PublicKeyOrCertificate::Certificate(_) => Ok(false),
        }
    }
}

fn comm(msg: impl Into<String>) -> KirkError {
    KirkError::Communication(msg.into())
}

fn timed_out(what: &str) -> KirkError {
    comm(format!("SSH {what} timed out"))
}

/// SSH communication channel.
pub struct SshChannel {
    name: String,
    config: SshConfig,
    session: tokio::sync::Mutex<Option<Session>>,
    max_sessions: Arc<tokio::sync::Semaphore>,
}

impl SshChannel {
    /// Create an unconfigured channel; call [`Plugin::setup`] before use.
    #[must_use]
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_owned(),
            config: SshConfig::from_map(&HashMap::new())
                .unwrap_or_else(|_| unreachable_default_config()),
            session: tokio::sync::Mutex::new(None),
            max_sessions: Arc::new(tokio::sync::Semaphore::new(DEFAULT_MAX_SESSIONS)),
        }
    }

    /// Borrow the live session handle, or fail when not connected.
    fn session_handle<'a>(
        guard: &'a tokio::sync::MutexGuard<'_, Option<Session>>,
    ) -> Result<&'a Session, KirkError> {
        guard
            .as_ref()
            .ok_or_else(|| comm("SSH connection is not present"))
    }

    /// Open a channel, exec `command`, and collect merged stdout/stderr.
    ///
    /// Channel open and the exec request carry [`IO_TIMEOUT`]; `overall`
    /// bounds the whole operation when set. `cap` bounds collected bytes.
    async fn run_remote(
        handle: &Session,
        command: &str,
        overall: Option<std::time::Duration>,
        cap: Option<usize>,
    ) -> Result<(Option<u32>, Vec<u8>), KirkError> {
        let work = async {
            let mut channel = timeout(IO_TIMEOUT, handle.channel_open_session())
                .await
                .map_err(|_| timed_out("channel open"))?
                .map_err(|err| comm(err.to_string()))?;
            timeout(IO_TIMEOUT, channel.exec(true, command.as_bytes().to_vec()))
                .await
                .map_err(|_| timed_out("exec"))?
                .map_err(|err| comm(err.to_string()))?;
            let mut out = Vec::new();
            let mut status = None;
            while let Some(msg) = channel.wait().await {
                match msg {
                    russh::ChannelMsg::Data { data }
                    | russh::ChannelMsg::ExtendedData { data, .. } => {
                        if cap.is_some_and(|cap| out.len() + data.len() > cap) {
                            let _ = channel.close().await;
                            return Err(comm("remote output exceeds size limit"));
                        }
                        out.extend_from_slice(&data);
                    }
                    russh::ChannelMsg::ExitStatus { exit_status } => status = Some(exit_status),
                    russh::ChannelMsg::Close => break,
                    // Eof and anything else: keep waiting for status/close.
                    _ => {}
                }
            }
            let _ = channel.eof().await;
            let _ = channel.close().await;
            Ok((status, out))
        };
        if let Some(bound) = overall {
            timeout(bound, work)
                .await
                .map_err(|_| timed_out("operation"))?
        } else {
            work.await
        }
    }

    /// Probe the server `MaxSessions`, defaulting when the probe fails.
    async fn probe_max_sessions(handle: &Session) -> usize {
        for probe in [
            "grep -i '^MaxSessions' /etc/ssh/sshd_config",
            "sudo sshd -T 2>/dev/null | grep maxsessions",
        ] {
            if let Ok((_, out)) = Self::run_remote(handle, probe, Some(PROBE_TIMEOUT), None).await
                && let Some(value) = parse_max_sessions(&String::from_utf8_lossy(&out))
            {
                return value.max(1);
            }
        }
        DEFAULT_MAX_SESSIONS
    }

    /// Run the configured `reset_cmd` locally via argv with [`RESET_TIMEOUT`].
    async fn run_reset(
        reset_cmd: &str,
        iobuffer: Option<Arc<dyn IOBuffer>>,
    ) -> Result<(), KirkError> {
        let parts = split_argv(reset_cmd)?;
        let Some((program, args)) = parts.split_first() else {
            return Err(comm("reset command is empty"));
        };
        let mut child = tokio::process::Command::new(program)
            .args(args)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|err| comm(format!("reset command failed to start: {err}")))?;
        let mut stdout = child.stdout.take();
        let collect = async {
            let mut buf = [0_u8; 1024];
            {
                use tokio::io::AsyncReadExt as _;
                if let Some(stream) = stdout.as_mut() {
                    loop {
                        let n = stream
                            .read(&mut buf)
                            .await
                            .map_err(|err| comm(err.to_string()))?;
                        if n == 0 {
                            break;
                        }
                        if let Some(iobuffer) = iobuffer.as_ref() {
                            iobuffer.write(&String::from_utf8_lossy(&buf[..n])).await?;
                        }
                    }
                }
            }
            child
                .wait()
                .await
                .map_err(|err| comm(err.to_string()))
                .map(|_| ())
        };
        timeout(RESET_TIMEOUT, collect)
            .await
            .map_err(|_| timed_out("reset command"))?
    }
}

/// Defaults are valid by construction; this is unreachable.
fn unreachable_default_config() -> SshConfig {
    SshConfig {
        host: config::DEFAULT_HOST.to_owned(),
        user: config::DEFAULT_USER.to_owned(),
        key_file: None,
        password: None,
        port: config::DEFAULT_PORT,
        reset_cmd: None,
        sudo: false,
        known_hosts: config::DEFAULT_KNOWN_HOSTS.to_owned(),
    }
}

impl Plugin for SshChannel {
    fn name(&self) -> &str {
        &self.name
    }

    fn config_help(&self) -> HashMap<String, String> {
        [
            ("host", "IP address of the host (default: localhost)"),
            ("port", "TCP port of the service (default: 22)"),
            ("user", "name of the user (default: root)"),
            ("password", "root password"),
            ("key_file", "private key location"),
            ("reset_cmd", "command to reset the remote target"),
            ("sudo", "use sudo to access to root shell (default: 0)"),
            ("known_hosts", "path to custom known_hosts file"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect()
    }

    fn setup(&mut self, cfg: &HashMap<String, String>) -> Result<(), KirkError> {
        self.config = SshConfig::from_map(cfg)?;
        Ok(())
    }

    fn clone_box(&self, name: &str) -> Box<dyn Plugin> {
        Box::new(Self {
            name: name.to_owned(),
            config: self.config.clone(),
            session: tokio::sync::Mutex::new(None),
            max_sessions: Arc::new(tokio::sync::Semaphore::new(DEFAULT_MAX_SESSIONS)),
        })
    }
}

#[async_trait]
impl ComChannel for SshChannel {
    fn parallel_execution(&self) -> bool {
        true
    }

    async fn active(&self) -> bool {
        self.session.lock().await.is_some()
    }

    async fn communicate(&mut self, _iobuffer: Option<Arc<dyn IOBuffer>>) -> Result<(), KirkError> {
        if self.active().await {
            return Err(comm("SSH client is already connected"));
        }
        let verifier = HostKeyVerifier {
            host: self.config.host.clone(),
            port: self.config.port,
            known_hosts: self.config.known_hosts.clone(),
        };
        let mut handle = timeout(
            IO_TIMEOUT,
            russh::client::connect(
                Arc::new(RusshConfig::default()),
                (self.config.host.as_str(), self.config.port),
                verifier,
            ),
        )
        .await
        .map_err(|_| timed_out("connect"))?
        .map_err(|err| comm(err.to_string()))?;

        let authenticated = if let Some(key_file) = self.config.key_file.clone() {
            authenticate_with_key(
                &mut handle,
                &self.config.user,
                &key_file,
                self.config.password.clone(),
            )
            .await
        } else if let Some(password) = self.config.password.clone() {
            let password: String = password.as_str().to_owned();
            timeout(
                IO_TIMEOUT,
                handle.authenticate_password(self.config.user.as_str(), password),
            )
            .await
            .map_err(|_| timed_out("authentication"))?
            .map_err(|err| comm(err.to_string()))
        } else {
            timeout(
                IO_TIMEOUT,
                handle.authenticate_none(self.config.user.as_str()),
            )
            .await
            .map_err(|_| timed_out("authentication"))?
            .map_err(|err| comm(err.to_string()))
        }?;
        if !matches!(authenticated, AuthResult::Success) {
            let _ = timeout(
                IO_TIMEOUT,
                handle.disconnect(russh::Disconnect::ByApplication, "", ""),
            )
            .await;
            return Err(comm("SSH authentication failed"));
        }

        let max_sessions = Self::probe_max_sessions(&handle).await;
        self.max_sessions = Arc::new(tokio::sync::Semaphore::new(max_sessions));
        *self.session.lock().await = Some(handle);
        Ok(())
    }

    async fn stop(&mut self, iobuffer: Option<Arc<dyn IOBuffer>>) -> Result<(), KirkError> {
        let handle = self.session.lock().await.take();
        let Some(handle) = handle else {
            return Ok(());
        };
        let _ = timeout(
            IO_TIMEOUT,
            handle.disconnect(russh::Disconnect::ByApplication, "", ""),
        )
        .await;
        self.max_sessions = Arc::new(tokio::sync::Semaphore::new(DEFAULT_MAX_SESSIONS));
        if let Some(reset_cmd) = self.config.reset_cmd.clone() {
            Self::run_reset(&reset_cmd, iobuffer).await?;
        }
        Ok(())
    }

    #[allow(
        clippy::await_holding_lock,
        reason = "single non-reentrant session lock; every channel op takes &mut self so no concurrent holder exists, and IOBuffer::write never calls back into the channel"
    )]
    async fn ping(&mut self) -> Result<f64, KirkError> {
        let guard = self.session.lock().await;
        let handle = Self::session_handle(&guard).map_err(|_| comm("SSH client is not running"))?;
        let start = Instant::now();
        let (status, _) = Self::run_remote(handle, "test .", Some(PROBE_TIMEOUT), None).await?;
        if status != Some(0) {
            return Err(comm("SSH ping failed"));
        }
        Ok(start.elapsed().as_secs_f64())
    }

    #[allow(
        clippy::await_holding_lock,
        reason = "single non-reentrant session lock; every channel op takes &mut self so no concurrent holder exists, and IOBuffer::write never calls back into the channel"
    )]
    async fn run_command(
        &mut self,
        command: &str,
        cwd: Option<&str>,
        env: Option<&HashMap<String, String>>,
        iobuffer: Option<Arc<dyn IOBuffer>>,
    ) -> Result<Option<CmdResult>, KirkError> {
        if command.is_empty() {
            return Err(comm("command is empty"));
        }
        let guard = self.session.lock().await;
        let handle = Self::session_handle(&guard)?;
        let _permit = self
            .max_sessions
            .clone()
            .acquire_owned()
            .await
            .map_err(|err| comm(err.to_string()))?;
        let remote = build_remote_command(command, cwd, env, self.config.sudo)?;
        let start = Instant::now();
        let mut channel = timeout(IO_TIMEOUT, handle.channel_open_session())
            .await
            .map_err(|_| timed_out("channel open"))?
            .map_err(|err| comm(err.to_string()))?;
        timeout(IO_TIMEOUT, channel.exec(true, remote.as_bytes().to_vec()))
            .await
            .map_err(|_| timed_out("exec"))?
            .map_err(|err| comm(err.to_string()))?;
        let mut collector = OutputCollector::new();
        let mut status = None;
        while let Some(msg) = channel.wait().await {
            match msg {
                russh::ChannelMsg::Data { data } | russh::ChannelMsg::ExtendedData { data, .. } => {
                    let text = String::from_utf8_lossy(&data);
                    collector.push(&text);
                    if let Some(iobuffer) = iobuffer.as_ref() {
                        // Upstream drops iobuffer failures and keeps the output.
                        let _ = iobuffer.write(&text).await;
                    }
                }
                russh::ChannelMsg::ExitStatus { exit_status } => status = Some(exit_status),
                russh::ChannelMsg::Close => break,
                // Eof and anything else: keep waiting for status/close.
                _ => {}
            }
        }
        let _ = channel.eof().await;
        let _ = channel.close().await;
        let exec_time = start.elapsed().as_secs_f64();
        let (stdout, panicked) = collector.finish();
        if panicked {
            return Err(KirkError::KernelPanic(String::from(
                "kernel panic detected during command execution",
            )));
        }
        Ok(Some(CmdResult {
            command: command.to_owned(),
            returncode: status
                .and_then(|code| i32::try_from(code).ok())
                .unwrap_or(-1),
            stdout,
            exec_time,
        }))
    }

    #[allow(
        clippy::await_holding_lock,
        reason = "single non-reentrant session lock; every channel op takes &mut self so no concurrent holder exists, and IOBuffer::write never calls back into the channel"
    )]
    async fn fetch_file(&mut self, target_path: &str) -> Result<Vec<u8>, KirkError> {
        if target_path.is_empty() {
            return Err(comm("target path is empty"));
        }
        let guard = self.session.lock().await;
        let handle = Self::session_handle(&guard)?;
        let remote = format!("cat -- {}", quote_sh(target_path));
        let (status, data) =
            Self::run_remote(handle, &remote, Some(IO_TIMEOUT), Some(FETCH_SIZE_CAP)).await?;
        if status != Some(0) {
            return Err(comm("failed to fetch remote file"));
        }
        Ok(data)
    }

    fn clone_channel_box(&self, new_name: &str) -> Box<dyn ComChannel> {
        Box::new(Self {
            name: new_name.to_owned(),
            config: self.config.clone(),
            session: tokio::sync::Mutex::new(None),
            max_sessions: Arc::new(tokio::sync::Semaphore::new(DEFAULT_MAX_SESSIONS)),
        })
    }
}

/// Authenticate with a private key file, using the configured password
/// (if any) to decrypt the key.
async fn authenticate_with_key(
    handle: &mut Session,
    user: &str,
    key_file: &str,
    password: Option<Zeroizing<String>>,
) -> Result<AuthResult, KirkError> {
    let key_data: Zeroizing<String> = Zeroizing::new(
        timeout(IO_TIMEOUT, tokio::fs::read_to_string(key_file))
            .await
            .map_err(|_| timed_out("key read"))?
            .map_err(|err| comm(err.to_string()))?,
    );
    let password = password.clone();
    let key = tokio::task::spawn_blocking(move || {
        russh::keys::decode_secret_key(
            key_data.as_str(),
            password.as_ref().map(|entry| entry.as_str()),
        )
    })
    .await
    .map_err(|err| comm(err.to_string()))?
    .map_err(|err| comm(err.to_string()))?;
    let hash_alg = if key.algorithm().is_rsa() {
        timeout(IO_TIMEOUT, handle.best_supported_rsa_hash())
            .await
            .map_err(|_| timed_out("authentication"))?
            .map_err(|err| comm(err.to_string()))?
            .flatten()
    } else {
        None
    };
    timeout(
        IO_TIMEOUT,
        handle.authenticate_publickey(
            user,
            russh::keys::PrivateKeyWithHashAlg::new(Arc::new(key), hash_alg),
        ),
    )
    .await
    .map_err(|_| timed_out("authentication"))?
    .map_err(|err| comm(err.to_string()))
}
