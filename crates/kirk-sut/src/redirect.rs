//! Stdout redirect buffers ported from `RedirectTestStdout` and
//! `RedirectSUTStdout` in `kirk/libkirk/sut.py`.
//!
//! Both implement [`IOBuffer`] and forward every write to an
//! [`EventRegistry`], firing the test/SUT identity alongside the data
//! chunk (`EventPayload::TestStdout`/`SutStdout`) so consumers like
//! `JSONFileMonitor` can build the upstream schema without needing separate
//! out-of-band state.

use async_trait::async_trait;
use kirk_com::IOBuffer;
use kirk_core::KirkError;
use kirk_core::data::Test;
use kirk_events::{EventPayload, EventRegistry};

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
            .fire(
                TEST_STDOUT_EVENT,
                EventPayload::TestStdout(self.test.clone(), data.to_owned()),
            )
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
        if self.is_cmd {
            self.events
                .fire(RUN_CMD_STDOUT_EVENT, EventPayload::Text(data.to_owned()))
                .await
        } else {
            self.events
                .fire(
                    SUT_STDOUT_EVENT,
                    EventPayload::SutStdout(self.sut_name.clone(), data.to_owned()),
                )
                .await
        }
    }
}
