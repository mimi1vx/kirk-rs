//! `LTPFramework` ported from `kirk/libkirk/ltp.py` (read-only upstream).
//!
//! # Security
//!
//! `fetch_file` paths are confined to the framework root
//! ([`validate_suite_name`] rejects
//! separators and parent references), `ltp.json` is capped at
//! [`MAX_METADATA_BYTES`], and test
//! stdout is truncated to
//! [`MAX_STDOUT_BYTES`](crate::parse::MAX_STDOUT_BYTES) before parsing.
//! All regexes are linear-time (no nested quantifiers).

use std::collections::HashMap;

use async_trait::async_trait;
use kirk_com::ComChannel;
use kirk_core::KirkError;
use kirk_core::data::{Suite, Test};
use kirk_core::results::TestResults;
use serde_json::{Map, Value};

use crate::framework::Framework;
use crate::parse::{
    MAX_METADATA_BYTES, count_markers, parse_summary, retcode_status, split_cmd_args, strip_ansi,
    truncate_stdout, validate_suite_name,
};

/// Default LTP root, mirroring `os.environ.get("LTPROOT", "/opt/ltp")`.
const DEFAULT_ROOT: &str = "/opt/ltp";

/// Default generic test timeout in seconds, mirroring `__init__`.
const DEFAULT_TIMEOUT: f64 = 30.0;

/// Tags whose presence marks a test as non-parallelizable.
///
/// Byte-parity with upstream `PARALLEL_BLACKLIST`.
pub const PARALLEL_BLACKLIST: &[&str] = &[
    "needs_root",
    "needs_device",
    "mount_device",
    "mntpoint",
    "resource_file",
    "format_device",
    "save_restore",
    "max_runtime",
];

/// Environment variables without the `LTP_` or `TST_` prefix that are still
/// forwarded. Byte-parity with upstream `SUPPORTED_ENV`.
pub const SUPPORTED_ENV: &[&str] = &[
    "PATH",
    "KCONFIG_PATH",
    "KCONFIG_SKIP_CHECK",
    "RHOST",
    "IPV4_LHOST",
    "IPV4_RHOST",
    "IPV6_LHOST",
    "IPV6_RHOST",
    "LHOST_IFACES",
    "RHOST_IFACES",
    "NS_DURATION",
    "NS_TIMES",
    "CONNECTION_TOTAL",
    "IP_TOTAL",
    "IP_TOTAL_FOR_TCPIP",
    "ROUTE_TOTAL",
    "ROUTE_CHANGE_IP",
    "ROUTE_CHANGE_NETLINK",
    "MTU_CHANGE_TIMES",
    "IF_UPDOWN_TIMES",
    "PING_MAX",
    "NS_ICMPV4_SENDER_DATA_MAXSIZE",
    "NS_ICMPV6_SENDER_DATA_MAXSIZE",
    "MCASTNUM_NORMAL",
    "MCASTNUM_HEAVY",
    "DOWNLOAD_BIGFILESIZE",
    "DOWNLOAD_REGFILESIZE",
    "UPLOAD_BIGFILESIZE",
    "UPLOAD_REGFILESIZE",
    "HTTP_DOWNLOAD_DIR",
    "FTP_DOWNLOAD_DIR",
    "FTP_UPLOAD_DIR",
    "FTP_UPLOAD_URLDIR",
    "IPSEC_MODE",
    "IPSEC_PROTO",
    "VIRT_PERF_THRESHOLD",
];

/// Variables set explicitly in [`LtpFramework`] setup; skipped in the
/// forwarding loop. Byte-parity with upstream `_PRESET_ENV`.
const PRESET_ENV: &[&str] = &[
    "LTPROOT",
    "TMPDIR",
    "LTP_COLORIZE_OUTPUT",
    "LTP_TIMEOUT_MUL",
];

/// Linux Test Project framework definition.
pub struct LtpFramework {
    max_runtime: f64,
    root: String,
    tc_folder: String,
    env: HashMap<String, String>,
}

impl LtpFramework {
    /// Create a framework reading `LTPROOT` from the process environment.
    #[must_use]
    pub fn new(max_runtime: f64, timeout: f64) -> Self {
        let root = std::env::var("LTPROOT").unwrap_or_else(|_| DEFAULT_ROOT.to_owned());
        Self::with_root(&root, max_runtime, timeout)
    }

    /// Create a framework rooted at `root`, bypassing the `LTPROOT` lookup.
    ///
    /// Aside from the fixed root this behaves exactly like [`Self::new`];
    /// tests use it to avoid mutating the shared process environment.
    #[must_use]
    pub fn with_root(root: &str, max_runtime: f64, timeout: f64) -> Self {
        let mut framework = Self {
            max_runtime,
            root: root.to_owned(),
            tc_folder: format!("{root}/testcases/bin"),
            env: HashMap::new(),
        };
        framework.update_env_vars(timeout);
        framework
    }

    /// Filter limit in seconds; `0.0` disables filtering.
    #[must_use]
    pub fn max_runtime(&self) -> f64 {
        self.max_runtime
    }

    /// LTP installation root.
    #[must_use]
    pub fn root(&self) -> &str {
        &self.root
    }

    /// Testcases binary folder (`<root>/testcases/bin`).
    #[must_use]
    pub fn tc_folder(&self) -> &str {
        &self.tc_folder
    }

    /// Snapshot of the forwarded environment.
    #[must_use]
    pub fn env(&self) -> &HashMap<String, String> {
        &self.env
    }

    /// Mutable access to the forwarded environment (e.g. to drop `PATH`).
    #[must_use]
    pub fn env_mut(&mut self) -> &mut HashMap<String, String> {
        &mut self.env
    }

    /// Populate `env` with LTP-relevant variables, mirroring `_update_env_vars`.
    fn update_env_vars(&mut self, timeout: f64) {
        self.env.insert("LTPROOT".to_owned(), self.root.clone());
        self.env.insert(
            "TMPDIR".to_owned(),
            std::env::var("TMPDIR").unwrap_or_else(|_| "/tmp".to_owned()),
        );
        self.env.insert(
            "LTP_COLORIZE_OUTPUT".to_owned(),
            std::env::var("LTP_COLORIZE_OUTPUT").unwrap_or_else(|_| "1".to_owned()),
        );

        match std::env::var("LTP_TIMEOUT_MUL") {
            Ok(multiplier) => {
                self.env.insert("LTP_TIMEOUT_MUL".to_owned(), multiplier);
            }
            Err(_) => {
                if timeout != 0.0 {
                    self.env.insert(
                        "LTP_TIMEOUT_MUL".to_owned(),
                        format!("{}", (timeout * 0.9) / 300.0),
                    );
                }
            }
        }

        for (key, value) in std::env::vars() {
            if PRESET_ENV.contains(&key.as_str()) {
                continue;
            }
            if SUPPORTED_ENV.contains(&key.as_str())
                || key.starts_with("LTP_")
                || key.starts_with("TST_")
            {
                self.env.insert(key, value);
            }
        }
    }

    /// Copy of `env` with the testcases folder appended to `PATH`.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Framework`] when `PATH` is unset and the SUT
    /// cannot report it.
    async fn read_path(
        &self,
        channel: &mut dyn ComChannel,
    ) -> Result<HashMap<String, String>, KirkError> {
        let mut env = self.env.clone();
        if let Some(path) = env.get("PATH").cloned() {
            env.insert("PATH".to_owned(), format!("{path}:{}", self.tc_folder));
        } else {
            let reported = channel
                .run_command("echo -n $PATH", None, None, None)
                .await?;
            match reported {
                Some(result) if result.returncode == 0 => {
                    env.insert(
                        "PATH".to_owned(),
                        format!("{}:{}", result.stdout.trim(), self.tc_folder),
                    );
                }
                _ => {
                    return Err(KirkError::Framework(String::from(
                        "Can't read PATH variable",
                    )));
                }
            }
        }
        Ok(env)
    }

    /// Whether a test with `max_runtime` metadata survives filtering.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Framework`] when `max_runtime` is a string or
    /// number that cannot be interpreted as a float.
    fn is_addable(&self, params: &Map<String, Value>) -> Result<bool, KirkError> {
        if self.max_runtime == 0.0 {
            return Ok(true);
        }
        let Some(runtime) = params.get("max_runtime") else {
            return Ok(true);
        };
        if runtime.is_null() {
            return Ok(true);
        }
        let value = match runtime {
            Value::Number(number) => number.as_f64().ok_or_else(|| {
                KirkError::Framework(format!(
                    "metadata contains wrong max_runtime value: {number}"
                ))
            })?,
            Value::String(text) => text.parse::<f64>().map_err(|_| {
                KirkError::Framework(format!("metadata contains wrong max_runtime value: {text}"))
            })?,
            Value::Bool(flag) => {
                if *flag {
                    1.0
                } else {
                    0.0
                }
            }
            _ => return Ok(true),
        };
        if value >= self.max_runtime {
            return Ok(false);
        }
        Ok(true)
    }

    /// Parse a runtest file into a [`Suite`], mirroring `_read_runtest`.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError`] when a line declares no command or a test
    /// cannot be built.
    async fn read_runtest(
        &self,
        channel: &mut dyn ComChannel,
        suite_name: &str,
        content: &str,
        metadata: Option<&Value>,
    ) -> Result<Suite, KirkError> {
        let metadata_tests = metadata
            .and_then(|value| value.get("tests"))
            .and_then(Value::as_object);
        let env = self.read_path(channel).await?;
        let mut tests = Vec::new();

        for raw_line in content.split('\n') {
            let line = raw_line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let parts = split_cmd_args(line);
            if parts.len() < 2 {
                return Err(KirkError::Framework(String::from(
                    "runtest file is not defining test command",
                )));
            }
            let test_name = &parts[0];
            let test_cmd = &parts[1];
            let args = parts[2..].to_vec();
            let mut parallelizable = false;

            if let Some(known) = metadata_tests
                && let Some(Value::Object(params)) = known.get(test_name.as_str())
            {
                if !self.is_addable(params)? {
                    continue;
                }
                parallelizable = PARALLEL_BLACKLIST
                    .iter()
                    .all(|tag| !params.contains_key(*tag));
            }

            tests.push(
                Test::new(test_name, test_cmd)?
                    .with_args(args)
                    .with_cwd(&self.tc_folder)
                    .with_env(env.clone())
                    .with_parallelizable(parallelizable),
            );
        }

        Ok(Suite::new(suite_name, tests))
    }
}

impl Default for LtpFramework {
    /// Upstream defaults: no `max_runtime` filtering, 30s timeout.
    fn default() -> Self {
        Self::new(0.0, DEFAULT_TIMEOUT)
    }
}

#[async_trait]
impl Framework for LtpFramework {
    async fn get_suites(&self, channel: &mut dyn ComChannel) -> Result<Vec<String>, KirkError> {
        let present = channel
            .run_command(&format!("test -d {}", self.root), None, None, None)
            .await?;
        if !matches!(present, Some(result) if result.returncode == 0) {
            return Err(KirkError::Framework(format!(
                "LTP folder doesn't exist: {}",
                self.root
            )));
        }

        let runtest_dir = format!("{}/runtest", self.root);
        let present = channel
            .run_command(&format!("test -d {runtest_dir}"), None, None, None)
            .await?;
        if !matches!(present, Some(result) if result.returncode == 0) {
            return Err(KirkError::Framework(format!(
                "'{runtest_dir}' doesn't exist inside SUT"
            )));
        }

        let listing = channel
            .run_command(
                &format!("ls --format=single-column {runtest_dir}"),
                None,
                None,
                None,
            )
            .await?;
        let Some(result) = listing else {
            return Err(KirkError::Framework(String::from(
                "Can't communicate with SUT",
            )));
        };
        if result.returncode != 0 {
            return Err(KirkError::Framework(format!(
                "command failed with: {}",
                result.stdout
            )));
        }
        Ok(result
            .stdout
            .split('\n')
            .filter(|line| !line.is_empty())
            .map(str::to_owned)
            .collect())
    }

    async fn find_command(
        &self,
        channel: &mut dyn ComChannel,
        command: &str,
    ) -> Result<Test, KirkError> {
        if command.is_empty() {
            return Err(KirkError::Framework(String::from("command is empty")));
        }
        let parts = split_cmd_args(command);
        let Some((name, args)) = parts.split_first() else {
            return Err(KirkError::Framework(String::from("command is empty")));
        };

        let mut cwd: Option<String> = None;
        let mut env = HashMap::new();
        let present = channel
            .run_command(&format!("test -d {}", self.tc_folder), None, None, None)
            .await?;
        if matches!(present, Some(result) if result.returncode == 0) {
            cwd = Some(self.tc_folder.clone());
            env = self.read_path(channel).await?;
        }

        let mut test = Test::new(name, name)?
            .with_args(args.to_vec())
            .with_env(env)
            .with_parallelizable(false);
        if let Some(dir) = cwd {
            test = test.with_cwd(&dir);
        }
        Ok(test)
    }

    async fn find_suite(
        &self,
        channel: &mut dyn ComChannel,
        name: &str,
    ) -> Result<Suite, KirkError> {
        if name.is_empty() {
            return Err(KirkError::Framework(String::from("name is empty")));
        }
        validate_suite_name(name)?;

        let present = channel
            .run_command(&format!("test -d {}", self.root), None, None, None)
            .await?;
        if !matches!(present, Some(result) if result.returncode == 0) {
            return Err(KirkError::Framework(format!(
                "LTP folder doesn't exist: {}",
                self.root
            )));
        }

        let suite_path = format!("{}/runtest/{name}", self.root);
        let present = channel
            .run_command(&format!("test -f {suite_path}"), None, None, None)
            .await?;
        if !matches!(present, Some(result) if result.returncode == 0) {
            return Err(KirkError::Framework(format!(
                "'{name}' suite doesn't exist"
            )));
        }

        let bytes = channel.fetch_file(&suite_path).await?;
        let runtest = String::from_utf8_lossy(&bytes);

        let metadata_path = format!("{}/metadata/ltp.json", self.root);
        let present = channel
            .run_command(&format!("test -f {metadata_path}"), None, None, None)
            .await?;
        let metadata = if matches!(present, Some(result) if result.returncode == 0) {
            let bytes = channel.fetch_file(&metadata_path).await?;
            if bytes.len() > MAX_METADATA_BYTES {
                return Err(KirkError::Framework(format!(
                    "ltp.json exceeds {MAX_METADATA_BYTES} byte cap"
                )));
            }
            Some(
                serde_json::from_slice::<Value>(&bytes)
                    .map_err(|err| KirkError::Framework(format!("invalid ltp.json: {err}")))?,
            )
        } else {
            None
        };

        self.read_runtest(channel, name, &runtest, metadata.as_ref())
            .await
    }

    async fn read_result(
        &self,
        test: Test,
        stdout: &str,
        retcode: i32,
        exec_time: f64,
    ) -> Result<TestResults, KirkError> {
        let stdout = strip_ansi(truncate_stdout(stdout));

        let mut counts = parse_summary(&stdout).unwrap_or_else(|| count_markers(&stdout));
        if counts == crate::parse::Counts::default() {
            match retcode {
                0 => counts.passed = 1,
                4 => counts.warnings = 1,
                32 => counts.skipped = 1,
                -1 => {}
                _ => counts.failed = 1,
            }
        }

        let status = retcode_status(retcode);
        if retcode == -1 {
            counts.broken = 1;
        }

        Ok(TestResults::new(test)
            .with_failed(counts.failed)
            .with_passed(counts.passed)
            .with_broken(counts.broken)
            .with_skipped(counts.skipped)
            .with_warnings(counts.warnings)
            .with_exec_time(exec_time)
            .with_status(status)
            .with_retcode(retcode)
            .with_stdout(&stdout))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blacklist_has_eight_exact_tags() {
        assert_eq!(
            PARALLEL_BLACKLIST,
            &[
                "needs_root",
                "needs_device",
                "mount_device",
                "mntpoint",
                "resource_file",
                "format_device",
                "save_restore",
                "max_runtime",
            ]
        );
    }

    #[test]
    fn env_map_snapshot() {
        let framework = LtpFramework::with_root("/opt/ltp", 0.0, 30.0);
        assert_eq!(framework.root(), "/opt/ltp");
        assert_eq!(framework.tc_folder(), "/opt/ltp/testcases/bin");
        assert_eq!(framework.env()["LTPROOT"], "/opt/ltp");
        let tmpdir = std::env::var("TMPDIR").unwrap_or_else(|_| "/tmp".to_owned());
        assert_eq!(framework.env()["TMPDIR"], tmpdir);
        let colorize = std::env::var("LTP_COLORIZE_OUTPUT").unwrap_or_else(|_| "1".to_owned());
        assert_eq!(framework.env()["LTP_COLORIZE_OUTPUT"], colorize);
        let multiplier = std::env::var("LTP_TIMEOUT_MUL")
            .unwrap_or_else(|_| format!("{}", (30.0 * 0.9) / 300.0));
        assert_eq!(framework.env()["LTP_TIMEOUT_MUL"], multiplier);
    }

    #[test]
    fn max_runtime_filtering() {
        let framework = LtpFramework::with_root("/opt/ltp", 5.0, 30.0);
        let params: Map<String, Value> =
            serde_json::from_value(serde_json::json!({"max_runtime": "10"})).unwrap();
        assert!(!framework.is_addable(&params).unwrap());
        let params: Map<String, Value> =
            serde_json::from_value(serde_json::json!({"max_runtime": "2"})).unwrap();
        assert!(framework.is_addable(&params).unwrap());
        let params: Map<String, Value> = serde_json::from_value(serde_json::json!({})).unwrap();
        assert!(framework.is_addable(&params).unwrap());

        let unfiltered = LtpFramework::with_root("/opt/ltp", 0.0, 30.0);
        let params: Map<String, Value> =
            serde_json::from_value(serde_json::json!({"max_runtime": "999"})).unwrap();
        assert!(unfiltered.is_addable(&params).unwrap());
    }
}
