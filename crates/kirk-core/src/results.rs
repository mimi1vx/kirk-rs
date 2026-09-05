//! Test result aggregation mirroring `kirk/libkirk/results.py`.

use crate::data::{Suite, Test};

/// Overall status of a test.
pub struct ResultStatus;

impl ResultStatus {
    /// Test has passed.
    pub const PASS: i32 = 0;
    /// Test is broken.
    pub const BROK: i32 = 2;
    /// Test warnings received.
    pub const WARN: i32 = 4;
    /// Test has failed.
    pub const FAIL: i32 = 16;
    /// Test can't run because of configuration error.
    pub const CONF: i32 = 32;
}

/// Test results definition.
#[derive(Debug, Clone, PartialEq)]
pub struct TestResults {
    test: Test,
    failed: u32,
    passed: u32,
    broken: u32,
    skipped: u32,
    warnings: u32,
    exec_time: f64,
    status: i32,
    retcode: i32,
    stdout: String,
}

impl TestResults {
    /// Create results for `test` with zeroed counters and `PASS` status.
    ///
    /// Upstream rejects an empty test object; the owned [`Test`] argument
    /// enforces that statically here.
    #[must_use]
    pub fn new(test: Test) -> Self {
        Self {
            test,
            failed: 0,
            passed: 0,
            broken: 0,
            skipped: 0,
            warnings: 0,
            exec_time: 0.0,
            status: ResultStatus::PASS,
            retcode: 0,
            stdout: String::new(),
        }
    }

    /// Set the number of failures.
    #[must_use]
    pub fn with_failed(mut self, failed: u32) -> Self {
        self.failed = failed;
        self
    }

    /// Set the number of passed tests.
    #[must_use]
    pub fn with_passed(mut self, passed: u32) -> Self {
        self.passed = passed;
        self
    }

    /// Set the number of broken tests.
    #[must_use]
    pub fn with_broken(mut self, broken: u32) -> Self {
        self.broken = broken;
        self
    }

    /// Set the number of skipped tests.
    #[must_use]
    pub fn with_skipped(mut self, skipped: u32) -> Self {
        self.skipped = skipped;
        self
    }

    /// Set the number of warnings.
    #[must_use]
    pub fn with_warnings(mut self, warnings: u32) -> Self {
        self.warnings = warnings;
        self
    }

    /// Set the test execution time in seconds.
    #[must_use]
    pub fn with_exec_time(mut self, exec_time: f64) -> Self {
        self.exec_time = exec_time;
        self
    }

    /// Set the overall status of the test.
    #[must_use]
    pub fn with_status(mut self, status: i32) -> Self {
        self.status = status;
        self
    }

    /// Set the return code of the executed test.
    #[must_use]
    pub fn with_retcode(mut self, retcode: i32) -> Self {
        self.retcode = retcode;
        self
    }

    /// Set the stdout of the test.
    #[must_use]
    pub fn with_stdout(mut self, stdout: &str) -> Self {
        stdout.clone_into(&mut self.stdout);
        self
    }

    /// Test object.
    #[must_use]
    pub fn test(&self) -> &Test {
        &self.test
    }

    /// Return code after execution.
    #[must_use]
    pub fn return_code(&self) -> i32 {
        self.retcode
    }

    /// Test process stdout.
    #[must_use]
    pub fn stdout(&self) -> &str {
        &self.stdout
    }

    /// Test result status.
    #[must_use]
    pub fn status(&self) -> i32 {
        self.status
    }

    /// Test execution time in seconds.
    #[must_use]
    pub fn exec_time(&self) -> f64 {
        self.exec_time
    }

    /// Number of failures.
    #[must_use]
    pub fn failed(&self) -> u32 {
        self.failed
    }

    /// Number of passed tests.
    #[must_use]
    pub fn passed(&self) -> u32 {
        self.passed
    }

    /// Number of broken tests.
    #[must_use]
    pub fn broken(&self) -> u32 {
        self.broken
    }

    /// Number of skipped tests.
    #[must_use]
    pub fn skipped(&self) -> u32 {
        self.skipped
    }

    /// Number of warnings.
    #[must_use]
    pub fn warnings(&self) -> u32 {
        self.warnings
    }
}

/// Testing suite results definition.
#[derive(Debug, Clone, PartialEq)]
pub struct SuiteResults {
    suite: Suite,
    tests: Vec<TestResults>,
    distro: Option<String>,
    distro_ver: Option<String>,
    kernel: Option<String>,
    cmdline: Option<String>,
    arch: Option<String>,
    cpu: Option<String>,
    swap: Option<String>,
    ram: Option<String>,
}

impl SuiteResults {
    /// Create empty results for `suite`.
    ///
    /// Upstream rejects an empty suite object; the owned [`Suite`] argument
    /// enforces that statically here.
    #[must_use]
    pub fn new(suite: Suite) -> Self {
        Self {
            suite,
            tests: Vec::new(),
            distro: None,
            distro_ver: None,
            kernel: None,
            cmdline: None,
            arch: None,
            cpu: None,
            swap: None,
            ram: None,
        }
    }

    /// Set the tests results.
    #[must_use]
    pub fn with_tests(mut self, tests: Vec<TestResults>) -> Self {
        self.tests = tests;
        self
    }

    /// Set the Linux distribution name.
    #[must_use]
    pub fn with_distro(mut self, distro: &str) -> Self {
        self.distro = Some(distro.to_owned());
        self
    }

    /// Set the Linux distribution version.
    #[must_use]
    pub fn with_distro_ver(mut self, distro_ver: &str) -> Self {
        self.distro_ver = Some(distro_ver.to_owned());
        self
    }

    /// Set the kernel version.
    #[must_use]
    pub fn with_kernel(mut self, kernel: &str) -> Self {
        self.kernel = Some(kernel.to_owned());
        self
    }

    /// Set the contents of `/proc/cmdline`.
    #[must_use]
    pub fn with_cmdline(mut self, cmdline: &str) -> Self {
        self.cmdline = Some(cmdline.to_owned());
        self
    }

    /// Set the operating system architecture.
    #[must_use]
    pub fn with_arch(mut self, arch: &str) -> Self {
        self.arch = Some(arch.to_owned());
        self
    }

    /// Set the current CPU type.
    #[must_use]
    pub fn with_cpu(mut self, cpu: &str) -> Self {
        self.cpu = Some(cpu.to_owned());
        self
    }

    /// Set the current swap memory occupation.
    #[must_use]
    pub fn with_swap(mut self, swap: &str) -> Self {
        self.swap = Some(swap.to_owned());
        self
    }

    /// Set the current RAM occupation.
    #[must_use]
    pub fn with_ram(mut self, ram: &str) -> Self {
        self.ram = Some(ram.to_owned());
        self
    }

    /// Testing suite.
    #[must_use]
    pub fn suite(&self) -> &Suite {
        &self.suite
    }

    /// All tests results.
    #[must_use]
    pub fn tests_results(&self) -> &[TestResults] {
        &self.tests
    }

    /// Linux distribution name.
    #[must_use]
    pub fn distro(&self) -> Option<&str> {
        self.distro.as_deref()
    }

    /// Linux distribution version.
    #[must_use]
    pub fn distro_ver(&self) -> Option<&str> {
        self.distro_ver.as_deref()
    }

    /// Kernel version.
    #[must_use]
    pub fn kernel(&self) -> Option<&str> {
        self.kernel.as_deref()
    }

    /// Contents of `/proc/cmdline`.
    #[must_use]
    pub fn cmdline(&self) -> Option<&str> {
        self.cmdline.as_deref()
    }

    /// Operating system architecture.
    #[must_use]
    pub fn arch(&self) -> Option<&str> {
        self.arch.as_deref()
    }

    /// Current CPU type.
    #[must_use]
    pub fn cpu(&self) -> Option<&str> {
        self.cpu.as_deref()
    }

    /// Current swap memory occupation.
    #[must_use]
    pub fn swap(&self) -> Option<&str> {
        self.swap.as_deref()
    }

    /// Current RAM occupation.
    #[must_use]
    pub fn ram(&self) -> Option<&str> {
        self.ram.as_deref()
    }

    /// Total execution time in seconds.
    #[must_use]
    pub fn exec_time(&self) -> f64 {
        self.tests.iter().map(TestResults::exec_time).sum()
    }

    /// Total number of failures.
    #[must_use]
    pub fn failed(&self) -> u32 {
        self.tests.iter().map(TestResults::failed).sum()
    }

    /// Total number of passed tests.
    #[must_use]
    pub fn passed(&self) -> u32 {
        self.tests.iter().map(TestResults::passed).sum()
    }

    /// Total number of broken tests.
    #[must_use]
    pub fn broken(&self) -> u32 {
        self.tests.iter().map(TestResults::broken).sum()
    }

    /// Total number of skipped tests.
    #[must_use]
    pub fn skipped(&self) -> u32 {
        self.tests.iter().map(TestResults::skipped).sum()
    }

    /// Total number of warnings.
    #[must_use]
    pub fn warnings(&self) -> u32 {
        self.tests.iter().map(TestResults::warnings).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test(name: &str) -> Test {
        Test::new(name, "cmd").unwrap()
    }

    #[test]
    fn status_values() {
        assert_eq!(ResultStatus::PASS, 0);
        assert_eq!(ResultStatus::BROK, 2);
        assert_eq!(ResultStatus::WARN, 4);
        assert_eq!(ResultStatus::FAIL, 16);
        assert_eq!(ResultStatus::CONF, 32);
    }

    #[test]
    fn test_results_defaults() {
        let results = TestResults::new(test("t0"));
        assert_eq!(results.failed(), 0);
        assert_eq!(results.passed(), 0);
        assert!((results.exec_time() - 0.0).abs() < f64::EPSILON);
        assert_eq!(results.status(), ResultStatus::PASS);
        assert_eq!(results.return_code(), 0);
        assert_eq!(results.stdout(), "");
    }

    #[test]
    fn suite_sums_counters() {
        let suite = Suite::new("suite0", Vec::new());
        let results = SuiteResults::new(suite).with_tests(vec![
            TestResults::new(test("t0"))
                .with_failed(1)
                .with_passed(2)
                .with_broken(3)
                .with_skipped(4)
                .with_warnings(5)
                .with_exec_time(1.5),
            TestResults::new(test("t1"))
                .with_failed(6)
                .with_passed(7)
                .with_broken(8)
                .with_skipped(9)
                .with_warnings(10)
                .with_exec_time(2.5),
        ]);
        assert_eq!(results.failed(), 7);
        assert_eq!(results.passed(), 9);
        assert_eq!(results.broken(), 11);
        assert_eq!(results.skipped(), 13);
        assert_eq!(results.warnings(), 15);
        assert!((results.exec_time() - 4.0).abs() < f64::EPSILON);
    }

    #[test]
    fn empty_suite_sums_zero() {
        let results = SuiteResults::new(Suite::new("suite0", Vec::new()));
        assert_eq!(results.failed(), 0);
        assert!((results.exec_time() - 0.0).abs() < f64::EPSILON);
    }
}
