//! SUT abstraction ported from `kirk/libkirk/sut.py`.
//!
//! [`Sut`] mirrors the Python `SUT` plugin base: probe helpers with a 1.5s
//! timeout collapsing to `"unknown"`, [`Sut::get_info`] (sequential, or a
//! [`JoinSet`] gather when [`Sut::optimize`] is set),
//! taint parsing over `TAINTED_MSG`, and fault-injection helpers over
//! [`FAULT_INJECTION_FILES`]. [`GenericSut`](crate::GenericSut) wires these
//! defaults to a concrete channel.
//!
//! Deliberate differences from upstream:
//!
//! * The parallel gather cannot share one channel object: every
//!   [`ComChannel`] method takes `&mut self`, so each
//!   probe runs on an independent clone
//!   ([`ComChannel::clone_channel_box`])
//!   that is communicated first. Probes only run in parallel when the
//!   channel reports [`parallel_execution`](kirk_com::ComChannel::parallel_execution);
//!   otherwise `optimize` falls back to sequential probing with identical results.
//! * The taint cache never holds a lock across an `.await`. A probe in flight
//!   is tracked with a flag under short [`Mutex`](tokio::sync::Mutex)
//!   sections; a concurrent caller reuses the cached value when one exists
//!   and probes alongside otherwise, instead of blocking on the probe lock
//!   like upstream does.
//! * Command strings travel through [`ComChannel::run_command`]
//!   opaquely. Channels must treat them as argv and never hand them to a
//!   shell; probes such as `. /etc/os-release && echo "$ID"` are preserved
//!   byte-for-byte from upstream.

use std::time::Duration;

use async_trait::async_trait;
use kirk_com::{ComChannel, IOBuffer};
use kirk_core::KirkError;
use kirk_plugin::Plugin;
use regex::Regex;
use tokio::task::JoinSet;
use tokio::time::timeout;

/// Value returned by probes on timeout, missing output, or a non-zero exit.
const UNKNOWN: &str = "unknown";

/// Timeout for a single probe command, mirroring `asyncio.wait_for(..., 1.5)`.
const RUN_CMD_TIMEOUT: Duration = Duration::from_millis(1500);

/// Retries for [`ComChannel::ensure_communicate`], mirroring the Python default.
const COMMUNICATE_RETRIES: u32 = 10;

/// Kernel taint messages, index `i` describing bit `i` of
/// `/proc/sys/kernel/tainted`, in upstream order.
const TAINTED_MSG: [&str; 18] = [
    "proprietary module was loaded",
    "module was force loaded",
    "kernel running on an out of specification system",
    "module was force unloaded",
    "processor reported a Machine Check Exception (MCE)",
    "bad page referenced or some unexpected page flags",
    "taint requested by userspace application",
    "kernel died recently, i.e. there was an OOPS or BUG",
    "ACPI table overridden by user",
    "kernel issued warning",
    "staging driver was loaded",
    "workaround for bug in platform firmware applied",
    "externally-built (“out-of-tree”) module was loaded",
    "unsigned module was loaded",
    "soft lockup occurred",
    "kernel has been live patched",
    "auxiliary taint, defined for and used by distros",
    "kernel was built with the struct randomization plugin",
];

/// Kernel fault-injection knobs under `/sys/kernel/debug`.
pub const FAULT_INJECTION_FILES: [&str; 4] = [
    "fail_io_timeout",
    "fail_make_request",
    "fail_page_alloc",
    "failslab",
];

/// Probe commands for [`Sut::get_info`], in upstream gather order.
const PROBE_COMMANDS: [&str; 7] = [
    ". /etc/os-release && echo \"$ID\"",
    ". /etc/os-release && echo \"$VERSION_ID\"",
    "uname -s -r -v",
    "cat /proc/cmdline",
    "uname -m",
    "uname -p",
    "cat /proc/meminfo",
];

/// Command reading the kernel taint code.
const TAINTED_CMD: &str = "cat /proc/sys/kernel/tainted";

/// Command reporting the SUT user id.
const ID_CMD: &str = "id -u";

/// System information gathered by [`Sut::get_info`].
///
/// Mirrors the upstream info dict; every field is `"unknown"` when its probe
/// timed out or failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SutInfo {
    /// Distribution id from `/etc/os-release`.
    pub distro: String,
    /// Distribution version from `/etc/os-release`.
    pub distro_ver: String,
    /// Kernel name, release, and version from `uname -s -r -v`.
    pub kernel: String,
    /// Contents of `/proc/cmdline`.
    pub cmdline: String,
    /// Machine architecture from `uname -m`.
    pub arch: String,
    /// CPU name from `uname -p`.
    pub cpu: String,
    /// Total RAM parsed from `/proc/meminfo`.
    pub ram: String,
    /// Total swap parsed from `/proc/meminfo`.
    pub swap: String,
}

/// Kernel taint status from [`Sut::get_tainted_info`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaintedInfo {
    /// Raw taint code from `/proc/sys/kernel/tainted`.
    pub code: u64,
    /// One message per set bit, in `TAINTED_MSG` order.
    pub messages: Vec<String>,
}

/// Outcome of the taint fast path in [`Sut::taint_begin`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaintBegin {
    /// A probe is in flight and already cached a result; reuse it.
    Cached(TaintedInfo),
    /// No usable cache; run the probe.
    Probe,
}

/// SUT abstraction. Object-safe so schedulers can hold `Box<dyn Sut>`.
///
/// Channel accessors, [`Sut::optimize`] state, and the taint-cache primitives
/// are required; everything else defaults to the upstream behavior so
/// [`GenericSut`](crate::GenericSut) only wires state.
#[async_trait]
pub trait Sut: Plugin + Send + Sync {
    /// Borrow the communication channel.
    ///
    /// The `Box` borrow (rather than `&dyn ComChannel`) keeps the trait
    /// object's `'static` bound intact; shrinking it through the mutable
    /// accessor is unsound under invariance.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Sut`] when no channel is attached yet.
    #[allow(clippy::borrowed_box, reason = "invariance forbids &mut dyn here")]
    fn channel(&self) -> Result<&Box<dyn ComChannel>, KirkError>;

    /// Borrow the communication channel mutably.
    ///
    /// See [`Sut::channel`] for why the `Box` borrow is returned.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Sut`] when no channel is attached yet.
    #[allow(clippy::borrowed_box, reason = "invariance forbids &mut dyn here")]
    fn channel_mut(&mut self) -> Result<&mut Box<dyn ComChannel>, KirkError>;

    /// Whether probes run through the parallel gather.
    fn optimize(&self) -> bool;

    /// Set whether probes run through the parallel gather.
    fn set_optimize(&mut self, optimize: bool);

    /// Taint fast path: cached in-flight result, or permission to probe.
    async fn taint_begin(&mut self) -> TaintBegin;

    /// Record a probe outcome (`None` on failure) and clear the in-flight flag.
    async fn taint_end(&mut self, result: Option<TaintedInfo>);

    /// Start the SUT, communicating the channel unless already running.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError`] when the channel is missing or won't communicate.
    async fn start(
        &mut self,
        iobuffer: Option<std::sync::Arc<dyn IOBuffer>>,
    ) -> Result<(), KirkError> {
        if self.is_running().await? {
            return Ok(());
        }
        self.channel_mut()?
            .ensure_communicate(iobuffer, COMMUNICATE_RETRIES)
            .await
    }

    /// Stop the SUT, unless already stopped.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError`] when the channel is missing or won't stop.
    async fn stop(
        &mut self,
        iobuffer: Option<std::sync::Arc<dyn IOBuffer>>,
    ) -> Result<(), KirkError> {
        if !self.is_running().await? {
            return Ok(());
        }
        self.channel_mut()?.stop(iobuffer).await
    }

    /// Stop, then start the SUT.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError`] when the channel is missing or won't restart.
    async fn restart(
        &mut self,
        iobuffer: Option<std::sync::Arc<dyn IOBuffer>>,
    ) -> Result<(), KirkError> {
        self.stop(iobuffer.clone()).await?;
        self.start(iobuffer).await
    }

    /// Report whether the SUT channel is active.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Sut`] when no channel is attached yet.
    async fn is_running(&self) -> Result<bool, KirkError> {
        Ok(self.channel()?.active().await)
    }

    /// Run `cmd` with a 1.5s timeout, returning `"unknown"` on timeout,
    /// missing output, or a non-zero exit. Channel errors propagate.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError`] when the channel is missing or fails.
    async fn run_cmd(&mut self, cmd: &str) -> Result<String, KirkError> {
        let output = match timeout(
            RUN_CMD_TIMEOUT,
            self.channel_mut()?.run_command(cmd, None, None, None),
        )
        .await
        {
            Err(_) | Ok(Ok(None)) => return Ok(UNKNOWN.to_owned()),
            Ok(Err(err)) => return Err(err),
            Ok(Ok(Some(result))) if result.returncode != 0 => {
                return Ok(UNKNOWN.to_owned());
            }
            Ok(Ok(Some(result))) => result.stdout,
        };
        Ok(output.trim_end().to_owned())
    }

    /// Collect SUT information, probing sequentially or via a
    /// [`JoinSet`] gather when [`Sut::optimize`] is set
    /// and the channel supports parallel execution.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Sut`] when the SUT is not running, and
    /// [`KirkError`] when probing fails.
    async fn get_info(&mut self) -> Result<SutInfo, KirkError> {
        if !self.is_running().await? {
            return Err(KirkError::Sut(String::from("SUT is not running")));
        }
        let parallel = self.optimize() && self.channel()?.parallel_execution();
        let [distro, distro_ver, kernel, cmdline, arch, cpu, meminfo] = if parallel {
            run_probes_parallel(self).await?
        } else {
            run_probes_sequential(self).await?
        };
        let (ram, swap) = parse_meminfo(&meminfo)?;
        Ok(SutInfo {
            distro,
            distro_ver,
            kernel,
            cmdline,
            arch,
            cpu,
            ram,
            swap,
        })
    }

    /// Report the kernel taint code and messages, reusing an in-flight
    /// result when one is cached.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Sut`] when the SUT is not running, when the taint
    /// file is unreadable, or when its content is not a number.
    async fn get_tainted_info(&mut self) -> Result<TaintedInfo, KirkError> {
        if !self.is_running().await? {
            return Err(KirkError::Sut(String::from("SUT is not running")));
        }
        if let TaintBegin::Cached(info) = self.taint_begin().await {
            return Ok(info);
        }
        let probe: Result<TaintedInfo, KirkError> = async {
            let result = self
                .channel_mut()?
                .run_command(TAINTED_CMD, None, None, None)
                .await?
                .ok_or_else(|| {
                    KirkError::Sut(String::from("Can't read tainted kernel information"))
                })?;
            if result.returncode != 0 {
                return Err(KirkError::Sut(String::from(
                    "Can't read tainted kernel information",
                )));
            }
            parse_tainted(&result.stdout)
        }
        .await;
        match probe {
            Ok(info) => {
                self.taint_end(Some(info.clone())).await;
                Ok(info)
            }
            Err(err) => {
                self.taint_end(None).await;
                Err(err)
            }
        }
    }

    /// Report whether the SUT session runs as root.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Sut`] when the SUT is not running, when `id -u`
    /// fails, or when its output is not a number.
    async fn logged_as_root(&mut self) -> Result<bool, KirkError> {
        if !self.is_running().await? {
            return Err(KirkError::Sut(String::from("SUT is not running")));
        }
        let result = self
            .channel_mut()?
            .run_command(ID_CMD, None, None, None)
            .await?
            .ok_or_else(|| {
                KirkError::Sut(String::from("Can't determine if we are running as root"))
            })?;
        if result.returncode != 0 {
            return Err(KirkError::Sut(String::from(
                "Can't determine if we are running as root",
            )));
        }
        let value = result.stdout.trim_end().to_owned();
        let user_id: i64 = value
            .trim()
            .parse()
            .map_err(|_| KirkError::Sut(format!("'id -u' returned {value}")))?;
        Ok(user_id == 0)
    }

    /// Report whether every fault-injection debug directory exists.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Sut`] when the SUT is not running, and
    /// [`KirkError`] when a probe fails.
    async fn is_fault_injection_enabled(&mut self) -> Result<bool, KirkError> {
        if !self.is_running().await? {
            return Err(KirkError::Sut(String::from("SUT is not running")));
        }
        for file in FAULT_INJECTION_FILES {
            let result = self
                .channel_mut()?
                .run_command(
                    &format!("test -d /sys/kernel/debug/{file}"),
                    None,
                    None,
                    None,
                )
                .await?;
            if result.is_none_or(|output| output.returncode != 0) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Configure kernel fault injection; `prob == 0` resets to defaults.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Sut`] when the SUT is not running or a knob
    /// rejects its value, and [`KirkError`] when a write fails.
    async fn setup_fault_injection(&mut self, prob: u32, interval: u32) -> Result<(), KirkError> {
        if !self.is_running().await? {
            return Err(KirkError::Sut(String::from("SUT is not running")));
        }
        let interval = i64::from(if prob == 0 { 1 } else { interval });
        let times: i64 = if prob == 0 { 1 } else { -1 };
        let prob = i64::from(prob);
        for file in FAULT_INJECTION_FILES {
            let base = format!("/sys/kernel/debug/{file}");
            write_fault_value(self.channel_mut()?, 0, &format!("{base}/space")).await?;
            write_fault_value(self.channel_mut()?, times, &format!("{base}/times")).await?;
            write_fault_value(self.channel_mut()?, interval, &format!("{base}/interval")).await?;
            write_fault_value(self.channel_mut()?, prob, &format!("{base}/probability")).await?;
        }
        Ok(())
    }
}

/// Run the [`PROBE_COMMANDS`] sequentially, in order.
async fn run_probes_sequential<S: Sut + ?Sized>(sut: &mut S) -> Result<[String; 7], KirkError> {
    let mut values = std::array::from_fn(|_| String::new());
    for (slot, cmd) in values.iter_mut().zip(PROBE_COMMANDS.iter()) {
        *slot = sut.run_cmd(cmd).await?;
    }
    Ok(values)
}

/// Run the [`PROBE_COMMANDS`] concurrently, one independent channel clone
/// per probe, re-associated by index because [`JoinSet`] completion order is
/// arbitrary.
async fn run_probes_parallel<S: Sut + ?Sized>(sut: &mut S) -> Result<[String; 7], KirkError> {
    let mut set = JoinSet::new();
    for (index, cmd) in PROBE_COMMANDS.iter().enumerate() {
        let mut clone = sut.channel_mut()?.clone_channel_box("sut-probe");
        let cmd = (*cmd).to_owned();
        set.spawn(async move {
            clone.ensure_communicate(None, COMMUNICATE_RETRIES).await?;
            let output =
                match timeout(RUN_CMD_TIMEOUT, clone.run_command(&cmd, None, None, None)).await {
                    Err(_) | Ok(Ok(None)) => UNKNOWN.to_owned(),
                    Ok(Err(err)) => return Err(err),
                    Ok(Ok(Some(result))) if result.returncode != 0 => UNKNOWN.to_owned(),
                    Ok(Ok(Some(result))) => result.stdout.trim_end().to_owned(),
                };
            Ok((index, output))
        });
    }
    let mut values = std::array::from_fn(|_| String::new());
    while let Some(joined) = set.join_next().await {
        let (index, output): (usize, String) =
            joined.map_err(|err| KirkError::Sut(err.to_string()))??;
        let slot = values
            .get_mut(index)
            .ok_or_else(|| KirkError::Sut(format!("probe index {index} out of range")))?;
        *slot = output;
    }
    Ok(values)
}

/// Split `MemTotal`/`SwapTotal` out of `/proc/meminfo`, defaulting to
/// `"unknown"` like upstream.
fn parse_meminfo(meminfo: &str) -> Result<(String, String), KirkError> {
    let memory = mem_value(r"MemTotal:\s+(?P<value>\d+\s+kB)", meminfo)?;
    let swap = mem_value(r"SwapTotal:\s+(?P<value>\d+\s+kB)", meminfo)?;
    Ok((memory, swap))
}

/// The `value` group of the first `pattern` match in `text`, or `"unknown"`
/// when absent.
fn mem_value(pattern: &str, text: &str) -> Result<String, KirkError> {
    let regex = Regex::new(pattern).map_err(|err| KirkError::Framework(err.to_string()))?;
    Ok(regex
        .captures(text)
        .and_then(|captures| captures.name("value"))
        .map_or_else(|| UNKNOWN.to_owned(), |found| found.as_str().to_owned()))
}

/// Parse taint output: digits only, then one `TAINTED_MSG` entry per set
/// bit, least-significant bit first (matching the upstream reversed format).
fn parse_tainted(output: &str) -> Result<TaintedInfo, KirkError> {
    let code_str = output.trim();
    if code_str.is_empty() || !code_str.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(KirkError::Sut(code_str.to_owned()));
    }
    let code: u64 = code_str
        .parse()
        .map_err(|_| KirkError::Sut(code_str.to_owned()))?;
    let mut messages = Vec::new();
    for (index, message) in TAINTED_MSG.iter().enumerate() {
        if (code >> index) & 1 == 1 {
            messages.push((*message).to_owned());
        }
    }
    Ok(TaintedInfo { code, messages })
}

/// Write one fault-injection knob via `echo <value> > <path>`.
async fn write_fault_value(
    channel: &mut Box<dyn ComChannel>,
    value: i64,
    path: &str,
) -> Result<(), KirkError> {
    let result = channel
        .run_command(&format!("echo {value} > {path}"), None, None, None)
        .await?;
    match result {
        Some(output) if output.returncode == 0 => Ok(()),
        Some(output) => Err(KirkError::Sut(format!(
            "Can't setup {path}. {}",
            output.stdout
        ))),
        None => Err(KirkError::Sut(format!("Can't setup {path}"))),
    }
}
