//! `Framework` async trait ported from `kirk/libkirk/framework.py`.

use async_trait::async_trait;
use kirk_com::ComChannel;
use kirk_core::KirkError;
use kirk_core::data::{Suite, Test};
use kirk_core::results::TestResults;

/// Framework definition. Implement this trait to support more testing
/// frameworks inside the application.
#[async_trait]
pub trait Framework: Send + Sync {
    /// Return the list of available suites.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError`] when the framework root is missing or the SUT
    /// cannot be reached.
    async fn get_suites(&self, channel: &mut dyn ComChannel) -> Result<Vec<String>, KirkError>;

    /// Search for `command` and return a [`Test`] which can be executed.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError`] when `command` is empty or communication fails.
    async fn find_command(
        &self,
        channel: &mut dyn ComChannel,
        command: &str,
    ) -> Result<Test, KirkError>;

    /// Search for the suite `name` inside the SUT.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError`] when `name` is empty or invalid, the suite does
    /// not exist, or its runtest file cannot be parsed.
    async fn find_suite(
        &self,
        channel: &mut dyn ComChannel,
        name: &str,
    ) -> Result<Suite, KirkError>;

    /// Return test results according to runner output and [`Test`] definition.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError`] when result counters overflow.
    async fn read_result(
        &self,
        test: Test,
        stdout: &str,
        retcode: i32,
        exec_time: f64,
    ) -> Result<TestResults, KirkError>;
}
