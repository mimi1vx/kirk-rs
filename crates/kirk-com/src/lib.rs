//! Communication channels ported from `kirk/libkirk/com.py`.
//!
//! [`IOBuffer`] mirrors the Python stdout buffer, [`CmdResult`] mirrors the
//! `run_command` result dict `{command, returncode, stdout, exec_time}`,
//! and [`ComChannel`] mirrors the Python `ComChannel` plugin base.
//!
//! Implementations of [`ComChannel::run_command`] take the command as a
//! single string for upstream parity, but must split it into an argv vector
//! and spawn it directly: never forward it to `sh -c` or any other shell.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use kirk_core::KirkError;
use kirk_plugin::Plugin;

pub mod registry;

pub use registry::Registry;

/// Async stdout sink, mirroring Python `IOBuffer`.
#[async_trait]
pub trait IOBuffer: Send + Sync {
    /// Write data into the buffer.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError`] when the write fails.
    async fn write(&self, data: &str) -> Result<(), KirkError>;
}

/// Outcome of [`ComChannel::run_command`].
///
/// Mirrors the upstream dict with keys
/// `command`, `returncode`, `stdout`, `exec_time`.
#[derive(Debug, Clone, PartialEq)]
pub struct CmdResult {
    /// Command that was executed.
    pub command: String,
    /// Process return code.
    pub returncode: i32,
    /// Captured stdout.
    pub stdout: String,
    /// Execution time in seconds.
    pub exec_time: f64,
}

/// Communication channel plugin.
///
/// Object-safe; registries hold `Box<dyn ComChannel>`. All async methods
/// carry a `Send` bound via [`async_trait`], so channels work inside
/// `tokio::spawn` and `select!`. Methods take `&mut self`, so no lock is
/// held across an `.await` inside this crate.
#[async_trait]
pub trait ComChannel: Plugin + Send + Sync {
    /// Whether the channel supports parallel command execution.
    fn parallel_execution(&self) -> bool;

    /// Report whether communication is active.
    async fn active(&self) -> bool;

    /// Start communication.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError`] when communication cannot start.
    async fn communicate(&mut self, iobuffer: Option<Arc<dyn IOBuffer>>) -> Result<(), KirkError>;

    /// Stop communication.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError`] when communication cannot stop.
    async fn stop(&mut self, iobuffer: Option<Arc<dyn IOBuffer>>) -> Result<(), KirkError>;

    /// Ping the target and return the round-trip time in seconds.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError`] when no active connection exists.
    async fn ping(&mut self) -> Result<f64, KirkError>;

    /// Run a command and return its result, or `None` when the callback failed.
    ///
    /// Implementations must treat `command` as argv (split, then spawn
    /// directly) and never hand it to a shell.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError`] on communication failures.
    async fn run_command(
        &mut self,
        command: &str,
        cwd: Option<&str>,
        env: Option<&HashMap<String, String>>,
        iobuffer: Option<Arc<dyn IOBuffer>>,
    ) -> Result<Option<CmdResult>, KirkError>;

    /// Fetch a file from the target.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError`] when the path is invalid or unreadable.
    async fn fetch_file(&mut self, target_path: &str) -> Result<Vec<u8>, KirkError>;

    /// Copy the channel and return a new instance with the given name.
    ///
    /// Typed equivalent of [`Plugin::clone_box`] so registries can clone
    /// without downcasting.
    fn clone_channel_box(&self, new_name: &str) -> Box<dyn ComChannel>;

    /// Retry [`ComChannel::communicate`] up to `retries` times.
    ///
    /// After each failure the channel is stopped before retrying; the last
    /// error is re-raised. Cancellation-safe: no state is held across an
    /// `.await` besides the loop counter, so dropping the future between
    /// attempts loses nothing but the retry count.
    ///
    /// # Errors
    ///
    /// Returns the last [`KirkError`] when every attempt fails, or a
    /// [`ComChannel::stop`] error when cleanup fails.
    async fn ensure_communicate(
        &mut self,
        iobuffer: Option<Arc<dyn IOBuffer>>,
        retries: u32,
    ) -> Result<(), KirkError> {
        let attempts = retries.max(1);
        for attempt in 0..attempts {
            match self.communicate(iobuffer.clone()).await {
                Ok(()) => return Ok(()),
                Err(err) => {
                    if attempt + 1 >= attempts {
                        return Err(err);
                    }
                    self.stop(iobuffer.clone()).await?;
                }
            }
        }
        Ok(())
    }
}
