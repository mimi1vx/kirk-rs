//! Report exporters ported from `kirk/libkirk/export.py`.
//!
//! The JSON schema is a fixed allowlist: only the fields below are ever
//! written, so test output can't leak extra SUT or environment detail into
//! the report.
//!
//! # Security
//!
//! `save_file` refuses to overwrite an existing file and confines `path` to
//! its canonical parent directory (which must already exist).

use std::path::PathBuf;

use kirk_core::KirkError;
use kirk_core::results::{ResultStatus, SuiteResults};
use serde_json::{Map, Value};

use crate::io::AsyncFile;

/// Export testing results into a JSON report file.
pub struct JSONExporter;

impl JSONExporter {
    /// Create an exporter.
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    /// Save `results` as a JSON report at `path`.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Exporter`] when `results` is empty, `path` is
    /// empty or escapes its parent directory, the file already exists, or
    /// the report cannot be written.
    pub async fn save_file(&self, results: &[SuiteResults], path: &str) -> Result<(), KirkError> {
        if results.is_empty() {
            return Err(KirkError::Exporter(String::from("results is empty")));
        }
        if path.is_empty() {
            return Err(KirkError::Exporter(String::from("path is empty")));
        }
        let target = resolve_report_path(path).await?;
        if tokio::fs::try_exists(&target)
            .await
            .map_err(|err| KirkError::Exporter(format!("can't check report path: {err}")))?
        {
            return Err(KirkError::Exporter(format!("'{path}' already exists")));
        }

        let data = report_value(results);
        let text = serde_json::to_string_pretty(&data)
            .map_err(|err| KirkError::Exporter(format!("can't encode report: {err}")))?;

        let mut outfile = AsyncFile::new(&target.to_string_lossy(), "w");
        outfile
            .open()
            .await
            .map_err(|err| KirkError::Exporter(err.to_string()))?;
        outfile
            .write(&text)
            .await
            .map_err(|err| KirkError::Exporter(err.to_string()))?;
        outfile.close().await;
        Ok(())
    }
}

impl Default for JSONExporter {
    fn default() -> Self {
        Self::new()
    }
}

/// Build the fixed-schema report document for `results`.
#[must_use]
pub fn report_value(results: &[SuiteResults]) -> Value {
    let entries: Vec<Value> = results
        .iter()
        .flat_map(|result| result.tests_results().iter().map(test_entry))
        .collect();

    let mut data = Map::new();
    data.insert(String::from("results"), Value::Array(entries));
    data.insert(String::from("stats"), stats_value(results));
    let environment = results
        .first()
        .map_or(Value::Object(Map::new()), environment_value);
    data.insert(String::from("environment"), environment);
    Value::Object(data)
}

/// Build one fixed-schema `results` entry.
fn test_entry(test: &kirk_core::results::TestResults) -> Value {
    let status = status_str(test.status());
    let mut detail = Map::new();
    detail.insert(
        String::from("command"),
        Value::String(test.test().command().to_owned()),
    );
    detail.insert(
        String::from("arguments"),
        Value::Array(
            test.test()
                .arguments()
                .iter()
                .map(|arg| Value::String(arg.clone()))
                .collect(),
        ),
    );
    detail.insert(String::from("log"), Value::String(test.stdout().to_owned()));
    detail.insert(
        String::from("retval"),
        Value::Array(vec![Value::String(test.return_code().to_string())]),
    );
    detail.insert(String::from("duration"), float(test.exec_time()));
    detail.insert(String::from("failed"), number(test.failed()));
    detail.insert(String::from("passed"), number(test.passed()));
    detail.insert(String::from("broken"), number(test.broken()));
    detail.insert(String::from("skipped"), number(test.skipped()));
    detail.insert(String::from("warnings"), number(test.warnings()));
    detail.insert(String::from("result"), Value::String(status.to_owned()));

    let mut entry = Map::new();
    entry.insert(
        String::from("test_fqn"),
        Value::String(test.test().name().to_owned()),
    );
    entry.insert(String::from("status"), Value::String(status.to_owned()));
    entry.insert(String::from("test"), Value::Object(detail));
    Value::Object(entry)
}

/// Build the fixed-schema `stats` document.
fn stats_value(results: &[SuiteResults]) -> Value {
    let mut stats = Map::new();
    stats.insert(
        String::from("runtime"),
        float(results.iter().map(SuiteResults::exec_time).sum()),
    );
    let passed: u32 = results.iter().map(SuiteResults::passed).sum();
    let failed: u32 = results.iter().map(SuiteResults::failed).sum();
    let broken: u32 = results.iter().map(SuiteResults::broken).sum();
    let skipped: u32 = results.iter().map(SuiteResults::skipped).sum();
    let warnings: u32 = results.iter().map(SuiteResults::warnings).sum();
    stats.insert(String::from("passed"), number(passed));
    stats.insert(String::from("failed"), number(failed));
    stats.insert(String::from("broken"), number(broken));
    stats.insert(String::from("skipped"), number(skipped));
    stats.insert(String::from("warnings"), number(warnings));
    Value::Object(stats)
}

/// Build the fixed-schema `environment` document from the first results.
fn environment_value(first: &SuiteResults) -> Value {
    let mut environment = Map::new();
    environment.insert(
        String::from("distribution"),
        Value::String(first.distro().unwrap_or_default().to_owned()),
    );
    environment.insert(
        String::from("distribution_version"),
        Value::String(first.distro_ver().unwrap_or_default().to_owned()),
    );
    environment.insert(
        String::from("kernel"),
        Value::String(first.kernel().unwrap_or_default().to_owned()),
    );
    environment.insert(
        String::from("cmdline"),
        Value::String(first.cmdline().unwrap_or_default().to_owned()),
    );
    environment.insert(
        String::from("arch"),
        Value::String(first.arch().unwrap_or_default().to_owned()),
    );
    environment.insert(
        String::from("cpu"),
        Value::String(first.cpu().unwrap_or_default().to_owned()),
    );
    environment.insert(
        String::from("swap"),
        Value::String(first.swap().unwrap_or_default().to_owned()),
    );
    environment.insert(
        String::from("RAM"),
        Value::String(first.ram().unwrap_or_default().to_owned()),
    );
    Value::Object(environment)
}

/// Map a numeric status to its report string, mirroring upstream.
#[must_use]
pub fn status_str(status: i32) -> &'static str {
    if status == ResultStatus::PASS {
        "pass"
    } else if status == ResultStatus::BROK {
        "brok"
    } else if status == ResultStatus::WARN {
        "warn"
    } else if status == ResultStatus::CONF {
        "conf"
    } else {
        "fail"
    }
}

fn number<T: Into<serde_json::Number>>(value: T) -> Value {
    Value::Number(value.into())
}

fn float(value: f64) -> Value {
    serde_json::Number::from_f64(value).map_or(Value::Null, Value::Number)
}

/// Confine `path` to its canonical parent directory.
async fn resolve_report_path(path: &str) -> Result<PathBuf, KirkError> {
    let raw = PathBuf::from(path);
    let name = raw
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty() && *name != "." && *name != "..")
        .ok_or_else(|| KirkError::Exporter(format!("invalid report path: '{path}'")))?;
    let parent = raw.parent().filter(|p| !p.as_os_str().is_empty());
    if let Some(parent) = parent {
        let owned = parent.to_path_buf();
        let canonical = tokio::task::spawn_blocking(move || owned.canonicalize())
            .await
            .map_err(|err| KirkError::Exporter(format!("can't resolve report path: {err}")))?
            .map_err(|err| KirkError::Exporter(format!("report folder is missing: {err}")))?;
        Ok(canonical.join(name))
    } else {
        let cwd = tokio::task::spawn_blocking(std::env::current_dir)
            .await
            .map_err(|err| KirkError::Exporter(format!("can't resolve report path: {err}")))?
            .map_err(|err| KirkError::Exporter(format!("can't resolve report path: {err}")))?;
        Ok(cwd.join(name))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kirk_core::data::{Suite, Test};

    fn fixture() -> Vec<SuiteResults> {
        let tests = vec![
            Test::new("ls0", "ls").unwrap(),
            Test::new("ls1", "ls")
                .unwrap()
                .with_args(vec![String::from("-l")]),
            Test::new("ls2", "ls")
                .unwrap()
                .with_args(vec![String::from("--error")]),
        ];
        let suite = Suite::new("ls_suite0", tests.clone());
        let results = vec![
            kirk_core::results::TestResults::new(tests[0].clone())
                .with_failed(0)
                .with_passed(1)
                .with_exec_time(1.0)
                .with_retcode(0)
                .with_stdout("folder\nfile.txt")
                .with_status(ResultStatus::PASS),
            kirk_core::results::TestResults::new(tests[1].clone())
                .with_failed(0)
                .with_passed(1)
                .with_exec_time(1.0)
                .with_retcode(0)
                .with_stdout("folder\nfile.txt")
                .with_status(ResultStatus::PASS),
            kirk_core::results::TestResults::new(tests[2].clone())
                .with_failed(1)
                .with_exec_time(1.0)
                .with_retcode(1)
                .with_stdout("")
                .with_status(ResultStatus::FAIL),
        ];
        vec![
            SuiteResults::new(suite)
                .with_tests(results)
                .with_distro("openSUSE-Leap")
                .with_distro_ver("15.3")
                .with_kernel("5.17")
                .with_cmdline("security=selinux selinux=1 enforcing=1 ima_policy=tcb")
                .with_arch("x86_64")
                .with_cpu("x86_64")
                .with_swap("10 kB")
                .with_ram("1000 kB"),
        ]
    }

    #[tokio::test]
    async fn bad_args_fail() {
        let exporter = JSONExporter::new();
        assert!(exporter.save_file(&[], "").await.is_err());
        assert!(exporter.save_file(&fixture(), "").await.is_err());
    }

    #[test]
    fn empty_report_value_does_not_panic() {
        let data = report_value(&[]);
        assert_eq!(data["results"], serde_json::json!([]));
        assert_eq!(data["stats"]["passed"], serde_json::json!(0));
    }

    #[tokio::test]
    async fn refuse_overwrite() {
        let dir = std::env::temp_dir().join("kirk-export-refuse");
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let path = dir.join("output.json");
        tokio::fs::write(&path, "{}").await.unwrap();
        let exporter = JSONExporter::new();
        let err = exporter
            .save_file(&fixture(), &path.to_string_lossy())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("already exists"));
    }

    async fn saved_report(name: &str) -> Value {
        let dir = std::env::temp_dir().join("kirk-export-fixture");
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let path = dir.join(name);
        let _ = tokio::fs::remove_file(&path).await;
        JSONExporter::new()
            .save_file(&fixture(), &path.to_string_lossy())
            .await
            .unwrap();
        let text = tokio::fs::read_to_string(&path).await.unwrap();
        serde_json::from_str(&text).unwrap()
    }

    #[tokio::test]
    async fn save_file_matches_results() {
        let data = saved_report("results.json").await;
        assert_eq!(data["results"].as_array().unwrap().len(), 3);
        assert_eq!(
            data["results"][0],
            serde_json::json!({
                "test": {
                    "command": "ls",
                    "arguments": [],
                    "failed": 0,
                    "passed": 1,
                    "broken": 0,
                    "skipped": 0,
                    "warnings": 0,
                    "duration": 1.0,
                    "result": "pass",
                    "log": "folder\nfile.txt",
                    "retval": ["0"],
                },
                "status": "pass",
                "test_fqn": "ls0",
            })
        );
        assert_eq!(
            data["results"][1],
            serde_json::json!({
                "test": {
                    "command": "ls",
                    "arguments": ["-l"],
                    "failed": 0,
                    "passed": 1,
                    "broken": 0,
                    "skipped": 0,
                    "warnings": 0,
                    "duration": 1.0,
                    "result": "pass",
                    "log": "folder\nfile.txt",
                    "retval": ["0"],
                },
                "status": "pass",
                "test_fqn": "ls1",
            })
        );
        assert_eq!(
            data["results"][2],
            serde_json::json!({
                "test": {
                    "command": "ls",
                    "arguments": ["--error"],
                    "failed": 1,
                    "passed": 0,
                    "broken": 0,
                    "skipped": 0,
                    "warnings": 0,
                    "duration": 1.0,
                    "result": "fail",
                    "log": "",
                    "retval": ["1"],
                },
                "status": "fail",
                "test_fqn": "ls2",
            })
        );
    }

    #[tokio::test]
    async fn save_file_matches_env_and_stats() {
        let data = saved_report("env-stats.json").await;
        assert_eq!(
            data["environment"],
            serde_json::json!({
                "distribution_version": "15.3",
                "distribution": "openSUSE-Leap",
                "kernel": "5.17",
                "cmdline": "security=selinux selinux=1 enforcing=1 ima_policy=tcb",
                "arch": "x86_64",
                "cpu": "x86_64",
                "swap": "10 kB",
                "RAM": "1000 kB",
            })
        );
        assert_eq!(
            data["stats"],
            serde_json::json!({
                "runtime": 3.0,
                "passed": 2,
                "failed": 1,
                "broken": 0,
                "skipped": 0,
                "warnings": 0,
            })
        );
    }
}
