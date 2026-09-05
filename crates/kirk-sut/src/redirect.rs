//! Stdout redirect buffers ported from `RedirectTestStdout` and
//! `RedirectSUTStdout` in `kirk/libkirk/sut.py`.
//!
//! Both implement [`IOBuffer`] and forward every write to an
//! [`EventRegistry`]. Upstream handlers receive structured arguments (the
//! `Test`/SUT plus the data); [`EventArgs`](kirk_events::EventArgs) carries
//! only the event name and one message, so the payload here is the data and
//! the writer struct itself stays queryable for the test/SUT identity.

use async_trait::async_trait;
use kirk_com::IOBuffer;
use kirk_core::KirkError;
use kirk_core::data::Test;
use kirk_events::EventRegistry;

/// Event fired by [`RedirectTestStdout::write`].
pub const TEST_STDOUT_EVENT: &str = "test_stdout";

/// Event fired by [`RedirectSutStdout::write`] for SUT output.
pub const SUT_STDOUT_EVENT: &str = "sut_stdout";

/// Event fired by [`RedirectSutStdout::write`] for command output.
pub const RUN_CMD_STDOUT_EVENT: &str = "run_cmd_stdout";

/// Redirect test stdout to [`TEST_STDOUT_EVENT`] handlers and accumulate it.
pub struct RedirectTestStdout {
    test: Test,
    stdout: std::sync::Mutex<String>,
    events: EventRegistry,
}

impl RedirectTestStdout {
    /// Build a redirect for `test` firing into `events`.
    #[must_use]
    pub fn new(test: Test, events: EventRegistry) -> Self {
        Self {
            test,
            stdout: std::sync::Mutex::new(String::new()),
            events,
        }
    }

    /// Test whose stdout is redirected.
    #[must_use]
    pub fn test(&self) -> &Test {
        &self.test
    }

    /// Data written so far.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Framework`] when the buffer lock is poisoned.
    pub fn stdout(&self) -> Result<String, KirkError> {
        self.stdout
            .lock()
            .map(|buffer| buffer.clone())
            .map_err(|_| KirkError::Framework(String::from("stdout lock poisoned")))
    }
}

#[async_trait]
impl IOBuffer for RedirectTestStdout {
    /// Fire [`TEST_STDOUT_EVENT`] with `data`, then accumulate it.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError`] when the event cannot fire or the buffer lock
    /// is poisoned.
    async fn write(&self, data: &str) -> Result<(), KirkError> {
        self.events
            .fire(TEST_STDOUT_EVENT, Some(data.to_owned()))
            .await?;
        self.stdout
            .lock()
            .map(|mut buffer| buffer.push_str(data))
            .map_err(|_| KirkError::Framework(String::from("stdout lock poisoned")))?;
        Ok(())
    }
}

/// Redirect SUT stdout to [`SUT_STDOUT_EVENT`], or [`RUN_CMD_STDOUT_EVENT`]
/// when built with `is_cmd`.
pub struct RedirectSutStdout {
    sut_name: String,
    is_cmd: bool,
    events: EventRegistry,
}

impl RedirectSutStdout {
    /// Build a redirect for the SUT named `sut_name` firing into `events`.
    #[must_use]
    pub fn new(sut_name: &str, is_cmd: bool, events: EventRegistry) -> Self {
        Self {
            sut_name: sut_name.to_owned(),
            is_cmd,
            events,
        }
    }

    /// Name of the SUT whose stdout is redirected.
    #[must_use]
    pub fn sut_name(&self) -> &str {
        &self.sut_name
    }

    /// Whether command (rather than SUT) output is redirected.
    #[must_use]
    pub fn is_cmd(&self) -> bool {
        self.is_cmd
    }
}

#[async_trait]
impl IOBuffer for RedirectSutStdout {
    /// Fire the matching event with `data`.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError`] when the event cannot fire.
    async fn write(&self, data: &str) -> Result<(), KirkError> {
        let event = if self.is_cmd {
            RUN_CMD_STDOUT_EVENT
        } else {
            SUT_STDOUT_EVENT
        };
        self.events.fire(event, Some(data.to_owned())).await
    }
}
