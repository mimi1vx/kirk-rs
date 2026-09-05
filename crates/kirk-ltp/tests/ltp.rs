//! `test_ltp.py` fixture port mirrored to a mock channel over a tempdir LTP root.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use kirk_com::{CmdResult, ComChannel, IOBuffer};
use kirk_core::KirkError;
use kirk_core::data::Test;
use kirk_core::results::ResultStatus;
use kirk_ltp::parse::MAX_STDOUT_BYTES;
use kirk_ltp::{Framework, LtpFramework};
use kirk_plugin::Plugin;

const TESTS_NUM: usize = 6;
const SUITES_NUM: usize = 3;

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Tempdir guard mirroring pytest `tmpdir`; removed on drop.
struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(tag: &str) -> Self {
        let id = TEMP_COUNTER.fetch_add(1, Ordering::SeqCst);
        let path = std::env::temp_dir().join(format!("kirk-ltp-{tag}-{}-{id}", std::process::id()));
        std::fs::create_dir_all(&path).expect("create temp dir");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn write(&self, rel: &str, content: &str) {
        let full = self.path.join(rel);
        if let Some(parent) = full.parent() {
            std::fs::create_dir_all(parent).expect("create parent dirs");
        }
        std::fs::write(&full, content).expect("write fixture file");
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// Mock channel serving a local LTP root, mirroring `ShellComChannel` over
/// the pytest `tmpdir` fixture.
struct MockChannel {
    name: String,
    root: PathBuf,
    path_answer: Option<String>,
    active: bool,
}

impl MockChannel {
    fn new(root: &Path) -> Self {
        Self {
            name: String::from("mock"),
            root: root.to_owned(),
            path_answer: Some(String::from("/usr/bin:/bin")),
            active: true,
        }
    }

    fn ok(command: &str, stdout: String, returncode: i32) -> CmdResult {
        CmdResult {
            command: command.to_owned(),
            returncode,
            stdout,
            exec_time: 0.0,
        }
    }
}

impl Plugin for MockChannel {
    fn name(&self) -> &str {
        &self.name
    }

    fn config_help(&self) -> HashMap<String, String> {
        HashMap::new()
    }

    fn setup(&mut self, _cfg: &HashMap<String, String>) -> Result<(), KirkError> {
        Ok(())
    }

    fn clone_box(&self, name: &str) -> Box<dyn Plugin> {
        Box::new(Self {
            name: name.to_owned(),
            root: self.root.clone(),
            path_answer: self.path_answer.clone(),
            active: false,
        })
    }
}

#[async_trait]
impl ComChannel for MockChannel {
    fn parallel_execution(&self) -> bool {
        false
    }

    async fn active(&self) -> bool {
        self.active
    }

    async fn communicate(&mut self, _iobuffer: Option<Arc<dyn IOBuffer>>) -> Result<(), KirkError> {
        self.active = true;
        Ok(())
    }

    async fn stop(&mut self, _iobuffer: Option<Arc<dyn IOBuffer>>) -> Result<(), KirkError> {
        self.active = false;
        Ok(())
    }

    async fn ping(&mut self) -> Result<f64, KirkError> {
        Ok(0.0)
    }

    async fn run_command(
        &mut self,
        command: &str,
        _cwd: Option<&str>,
        _env: Option<&HashMap<String, String>>,
        _iobuffer: Option<Arc<dyn IOBuffer>>,
    ) -> Result<Option<CmdResult>, KirkError> {
        if let Some(path) = command.strip_prefix("test -d ") {
            let code = i32::from(!Path::new(path).is_dir());
            return Ok(Some(Self::ok(command, String::new(), code)));
        }
        if let Some(path) = command.strip_prefix("test -f ") {
            let code = i32::from(!Path::new(path).is_file());
            return Ok(Some(Self::ok(command, String::new(), code)));
        }
        if let Some(dir) = command.strip_prefix("ls --format=single-column ") {
            let mut names: Vec<String> = std::fs::read_dir(dir)
                .map_err(|err| KirkError::Communication(err.to_string()))?
                .filter_map(Result::ok)
                .map(|entry| entry.file_name().to_string_lossy().into_owned())
                .collect();
            names.sort();
            let mut stdout = names.join("\n");
            if !stdout.is_empty() {
                stdout.push('\n');
            }
            return Ok(Some(Self::ok(command, stdout, 0)));
        }
        if command == "echo -n $PATH" {
            return Ok(self
                .path_answer
                .clone()
                .map(|stdout| Self::ok(command, stdout, 0)));
        }
        Ok(Some(Self::ok(command, String::new(), 1)))
    }

    async fn fetch_file(&mut self, target_path: &str) -> Result<Vec<u8>, KirkError> {
        std::fs::read(target_path).map_err(|err| KirkError::Communication(err.to_string()))
    }

    fn clone_channel_box(&self, new_name: &str) -> Box<dyn ComChannel> {
        Box::new(Self {
            name: new_name.to_owned(),
            root: self.root.clone(),
            path_answer: self.path_answer.clone(),
            active: false,
        })
    }
}

/// Build the `test_ltp.py::prepare_tmpdir` fixture on disk.
fn prepare_ltproot(tag: &str) -> TempDir {
    let tmp = TempDir::new(tag);

    let mut content = String::new();
    for i in 0..TESTS_NUM {
        let _ = writeln!(content, "test0{i} echo ciao");
    }
    for i in 0..SUITES_NUM {
        tmp.write(&format!("runtest/suite{i}"), &content);
    }

    let mut slow = String::new();
    for i in TESTS_NUM..TESTS_NUM * 2 {
        let _ = writeln!(slow, "slow_test0{i} sleep 0.05");
    }
    tmp.write("runtest/slow_suite", &slow);
    tmp.write("runtest/bad_suite", "lonelycommand\n");

    let mut meta = String::from("{\"tests\": {");
    for i in TESTS_NUM..TESTS_NUM * 2 {
        if i > TESTS_NUM {
            meta.push(',');
        }
        let _ = write!(meta, "\"slow_test0{i}\": {{\"max_runtime\": \"10\"}}");
    }
    meta.push_str("}}");
    tmp.write("metadata/ltp.json", &meta);

    tmp.write("testcases/bin/test.sh", "#!/bin/bash\necho $1 $2\n");
    tmp
}

fn harness(tag: &str) -> (TempDir, LtpFramework, MockChannel) {
    let tmp = prepare_ltproot(tag);
    let root = tmp.path().to_string_lossy().into_owned();
    let framework = LtpFramework::with_root(&root, 0.0, 30.0);
    let channel = MockChannel::new(tmp.path());
    (tmp, framework, channel)
}

#[tokio::test]
async fn get_suites_lists_all_fixtures() {
    let (_tmp, framework, mut channel) = harness("suites");
    let suites = framework.get_suites(&mut channel).await.expect("suites");
    for name in ["suite0", "suite1", "suite2", "slow_suite"] {
        assert!(
            suites.contains(&name.to_owned()),
            "{name} missing: {suites:?}"
        );
    }
}

#[tokio::test]
async fn find_command_returns_test_definition() {
    let (tmp, framework, mut channel) = harness("command");
    let test = framework
        .find_command(&mut channel, "test.sh ciao bepi")
        .await
        .expect("command");
    assert_eq!(test.name(), "test.sh");
    assert_eq!(test.command(), "test.sh");
    assert_eq!(test.arguments(), &["ciao".to_owned(), "bepi".to_owned()]);
    assert!(!test.parallelizable());
    let tc_folder = tmp.path().join("testcases").join("bin");
    assert_eq!(test.cwd(), Some(tc_folder.to_string_lossy().as_ref()));
    assert!(!test.env().is_empty());
}

#[tokio::test]
async fn find_command_quote_aware_split() {
    let (_tmp, framework, mut channel) = harness("quote");
    let test = framework
        .find_command(&mut channel, "cmd -c \"a b\"")
        .await
        .expect("command");
    assert_eq!(test.name(), "cmd");
    assert_eq!(test.arguments(), &["-c".to_owned(), "\"a b\"".to_owned()]);
}

#[tokio::test]
async fn find_suite_parses_tests_and_env() {
    let (tmp, framework, mut channel) = harness("suite");
    for i in 0..SUITES_NUM {
        let suite = framework
            .find_suite(&mut channel, &format!("suite{i}"))
            .await
            .expect("suite");
        assert_eq!(suite.tests().len(), TESTS_NUM);
        for (j, test) in suite.tests().iter().enumerate() {
            assert_eq!(test.name(), format!("test0{j}"));
            assert_eq!(test.command(), "echo");
            assert_eq!(test.arguments(), &["ciao".to_owned()]);
            let tc_folder = tmp.path().join("testcases").join("bin");
            assert_eq!(test.cwd(), Some(tc_folder.to_string_lossy().as_ref()));
            assert!(!test.parallelizable());
            for key in ["LTPROOT", "TMPDIR", "LTP_COLORIZE_OUTPUT"] {
                assert!(test.env().contains_key(key), "{key} missing");
            }
        }
    }

    let suite = framework
        .find_suite(&mut channel, "slow_suite")
        .await
        .expect("slow suite");
    assert_eq!(suite.tests().len(), TESTS_NUM);
    for test in suite.tests() {
        assert_eq!(test.command(), "sleep");
        assert_eq!(test.arguments(), &["0.05".to_owned()]);
        // `max_runtime` is in `PARALLEL_BLACKLIST`, so metadata-backed
        // tests stay non-parallelizable.
        assert!(!test.parallelizable());
    }
}

#[tokio::test]
async fn find_suite_forwards_supported_and_prefixed_env() {
    // Additive-only env mutation: parallel tests only ever add keys, and no
    // assertion anywhere depends on their absence.
    // SAFETY: keys are test-unique in effect (values namespaced per key) and
    // restored below; other tests tolerate extra forwarded variables.
    struct Restore {
        keys: Vec<String>,
        previous: HashMap<String, Option<String>>,
    }
    impl Drop for Restore {
        fn drop(&mut self) {
            for key in &self.keys {
                match self.previous.get(key) {
                    Some(Some(value)) => unsafe { std::env::set_var(key, value) },
                    _ => unsafe { std::env::remove_var(key) },
                }
            }
        }
    }

    let mut net_vars: HashMap<String, String> = kirk_ltp::ltp::SUPPORTED_ENV
        .iter()
        .filter(|key| **key != "PATH")
        .map(|key| (key.to_string(), format!("test_value_{key}")))
        .collect();
    net_vars.insert(String::from("TST_USE_NETNS"), String::from("yes"));
    net_vars.insert(String::from("LTP_RSH"), String::from("ssh -nq"));

    let previous = net_vars
        .keys()
        .map(|key| (key.clone(), std::env::var(key).ok()))
        .collect();
    let restore = Restore {
        keys: net_vars.keys().cloned().collect(),
        previous,
    };
    for (key, value) in &net_vars {
        unsafe { std::env::set_var(key, value) };
    }

    let (_tmp, _, mut channel) = harness("netvars");
    let root = channel.root.clone();
    let framework = LtpFramework::with_root(&root.to_string_lossy(), 0.0, 30.0);
    let suite = framework
        .find_suite(&mut channel, "suite0")
        .await
        .expect("suite");
    for test in suite.tests() {
        for (key, value) in &net_vars {
            assert_eq!(test.env().get(key), Some(value), "{key} not forwarded");
        }
    }
    drop(restore);
}

#[tokio::test]
async fn find_suite_max_runtime_filters_slow_tests() {
    let (_tmp, _, mut channel) = harness("max-runtime");
    let root = channel.root.clone();
    let framework = LtpFramework::with_root(&root.to_string_lossy(), 5.0, 30.0);
    let suite = framework
        .find_suite(&mut channel, "slow_suite")
        .await
        .expect("suite");
    assert!(suite.tests().is_empty());
}

#[tokio::test]
async fn read_result_passed() {
    let (_tmp, framework, _) = harness("result-pass");
    let test = Test::new("test", "echo")
        .expect("test")
        .with_args(vec!["ciao".to_owned()]);
    let result = framework
        .read_result(test.clone(), "ciao\n", 0, 0.1)
        .await
        .expect("result");
    assert_eq!(result.passed(), 1);
    assert_eq!(result.failed(), 0);
    assert_eq!(result.broken(), 0);
    assert_eq!(result.skipped(), 0);
    assert_eq!(result.warnings(), 0);
    assert!((result.exec_time() - 0.1).abs() < f64::EPSILON);
    assert_eq!(result.test(), &test);
    assert_eq!(result.return_code(), 0);
    assert_eq!(result.stdout(), "ciao\n");
    assert_eq!(result.status(), ResultStatus::PASS);
}

#[tokio::test]
async fn read_result_failure() {
    let (_tmp, framework, _) = harness("result-fail");
    let test = Test::new("test", "echo").expect("test");
    let result = framework
        .read_result(test.clone(), "", 1, 0.1)
        .await
        .expect("result");
    assert_eq!(result.passed(), 0);
    assert_eq!(result.failed(), 1);
    assert_eq!(result.status(), ResultStatus::FAIL);
    assert_eq!(result.return_code(), 1);
    assert_eq!(result.stdout(), "");
}

#[tokio::test]
async fn read_result_broken() {
    let (_tmp, framework, _) = harness("result-brok");
    let test = Test::new("test", "echo").expect("test");
    let result = framework
        .read_result(test.clone(), "", -1, 0.1)
        .await
        .expect("result");
    assert_eq!(result.broken(), 1);
    assert_eq!(result.failed(), 0);
    assert_eq!(result.status(), ResultStatus::BROK);
    assert_eq!(result.return_code(), -1);
}

#[tokio::test]
async fn read_result_skipped_and_warnings() {
    let (_tmp, framework, _) = harness("result-skip");
    let test = Test::new("test", "echo").expect("test");
    let result = framework
        .read_result(test.clone(), "mydata", 32, 0.1)
        .await
        .expect("result");
    assert_eq!(result.skipped(), 1);
    assert_eq!(result.status(), ResultStatus::CONF);
    assert_eq!(result.stdout(), "mydata");

    let result = framework
        .read_result(test.clone(), "", 4, 0.1)
        .await
        .expect("result");
    assert_eq!(result.warnings(), 1);
    assert_eq!(result.status(), ResultStatus::WARN);
}

#[tokio::test]
async fn read_result_with_summary() {
    let (_tmp, framework, _) = harness("result-summary");
    let stdout =
        "some output\nSummary:\npassed   3\nfailed   1\nbroken   0\nskipped  2\nwarnings 1\n";
    let test = Test::new("test", "echo").expect("test");
    let result = framework
        .read_result(test, stdout, 0, 0.5)
        .await
        .expect("result");
    assert_eq!(result.passed(), 3);
    assert_eq!(result.failed(), 1);
    assert_eq!(result.broken(), 0);
    assert_eq!(result.skipped(), 2);
    assert_eq!(result.warnings(), 1);
}

#[tokio::test]
async fn read_result_tpass_markers() {
    let (_tmp, framework, _) = harness("result-markers");
    let stdout = "test 1 TPASS: ok\ntest 2 TPASS: ok\ntest 3 TFAIL: bad\n";
    let test = Test::new("test", "echo").expect("test");
    let result = framework
        .read_result(test, stdout, 1, 0.1)
        .await
        .expect("result");
    assert_eq!(result.passed(), 2);
    assert_eq!(result.failed(), 1);
}

#[tokio::test]
async fn read_result_strips_ansi() {
    let (_tmp, framework, _) = harness("result-ansi");
    let test = Test::new("test", "echo").expect("test");
    let result = framework
        .read_result(test, "\u{1b}[32mTPASS\u{1b}[0m: ok\n", 0, 0.1)
        .await
        .expect("result");
    assert_eq!(result.stdout(), "TPASS: ok\n");
    assert_eq!(result.passed(), 1);
}

#[tokio::test]
async fn read_result_truncates_huge_stdout() {
    let (_tmp, framework, _) = harness("result-cap");
    let test = Test::new("test", "echo").expect("test");
    let huge = "x".repeat(MAX_STDOUT_BYTES + 1024);
    let result = framework
        .read_result(test, &huge, 0, 0.1)
        .await
        .expect("result");
    assert!(result.stdout().len() <= MAX_STDOUT_BYTES);
}

#[tokio::test]
async fn invalid_inputs_error() {
    let (_tmp, framework, mut channel) = harness("errors");
    assert!(framework.find_command(&mut channel, "").await.is_err());
    assert!(framework.find_command(&mut channel, "   ").await.is_err());
    assert!(framework.find_suite(&mut channel, "").await.is_err());
    assert!(
        framework
            .find_suite(&mut channel, "nonexistent_suite_xyz")
            .await
            .is_err()
    );
    assert!(
        framework
            .find_suite(&mut channel, "bad_suite")
            .await
            .is_err()
    );
    for traversal in ["../evil", "a/b", ".."] {
        assert!(
            framework.find_suite(&mut channel, traversal).await.is_err(),
            "{traversal} must be rejected"
        );
    }

    let missing = LtpFramework::with_root("/nonexistent-ltp-root-xyz", 0.0, 30.0);
    assert!(missing.get_suites(&mut channel).await.is_err());
    assert!(missing.find_suite(&mut channel, "suite0").await.is_err());
}

#[tokio::test]
async fn timeout_mul_from_env() {
    // SAFETY: single key set and restored; other tests tolerate the extra var.
    let previous = std::env::var("LTP_TIMEOUT_MUL").ok();
    unsafe { std::env::set_var("LTP_TIMEOUT_MUL", "2.5") };
    let framework = LtpFramework::with_root("/opt/ltp", 0.0, 30.0);
    assert_eq!(framework.env()["LTP_TIMEOUT_MUL"], "2.5");
    match previous {
        Some(value) => unsafe { std::env::set_var("LTP_TIMEOUT_MUL", value) },
        None => unsafe { std::env::remove_var("LTP_TIMEOUT_MUL") },
    }
}

#[tokio::test]
async fn read_path_without_path_env() {
    let (_tmp, mut framework, mut channel) = harness("nopath");
    framework.env_mut().remove("PATH");
    let root = channel.root.clone();
    let _ = &root;
    let suite = framework
        .find_suite(&mut channel, "suite0")
        .await
        .expect("suite");
    let tc_folder = framework.tc_folder().to_owned();
    for test in suite.tests() {
        let path = test.env().get("PATH").expect("PATH present");
        assert_eq!(path, &format!("/usr/bin:/bin:{tc_folder}"));
    }
}
