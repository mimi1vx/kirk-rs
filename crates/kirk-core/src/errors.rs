//! Error hierarchy mirroring `kirk/libkirk/errors.py`.
//!
//! The Python `KirkException` root maps to [`KirkError`] itself; each
//! subclass maps to one enum variant carrying a message.

/// Root error type for kirk-core.
///
/// Mirrors `KirkException` and its 11 subclasses from upstream
/// `kirk/libkirk/errors.py`.
#[derive(Debug, thiserror::Error)]
pub enum KirkError {
    /// Raised when plugins operations have failed.
    #[error("plugin error: {0}")]
    Plugin(String),
    /// Raised when error occurs during channels communication.
    #[error("communication error: {0}")]
    Communication(String),
    /// Raised when error occurs in SUT.
    #[error("SUT error: {0}")]
    Sut(String),
    /// Raised during kernel panic.
    #[error("kernel panic: {0}")]
    KernelPanic(String),
    /// Raised when kernel is tainted.
    #[error("kernel tainted: {0}")]
    KernelTainted(String),
    /// Raised when kernel is not responding anymore.
    #[error("kernel timeout: {0}")]
    KernelTimeout(String),
    /// Raised when an error occurs inside a framework.
    #[error("framework error: {0}")]
    Framework(String),
    /// Raised when an error occurs during Exporter operations.
    #[error("exporter error: {0}")]
    Exporter(String),
    /// Raised when an error occurs during LTX execution.
    #[error("LTX error: {0}")]
    Ltx(String),
    /// Raised when an error occurs during Scheduler operations.
    #[error("scheduler error: {0}")]
    Scheduler(String),
    /// Raised when an error occurs during Session operations.
    #[error("session error: {0}")]
    Session(String),
}
