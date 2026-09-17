//! End-to-end CLI snapshots exercising `run_session` directly (not UI
//! methods), covering upstream-style output per mode.
//!
//! `LTPROOT` is process-global, so every test in this file serializes
//! behind [`ENV_LOCK`] around the env mutation and the run it drives.

use std::sync::Arc;

use clap::Parser as _;
use kirk_cli::args::Args;
use kirk_cli::session::run_session;
use kirk_support::VecPrinter;
use tokio::sync::Mutex;

static ENV_LOCK: Mutex<()> = Mutex::const_new(());

struct Fixture {
    root: std::path::PathBuf,
    tmp_dir: std::path::PathBuf,
}

impl Fixture {
    fn new(name: &str) -> Self {
        let root = std::env::temp_dir().join(format!("kirk-cli-snap-ltproot-{name}"));
        let tmp_dir = std::env::temp_dir().join(format!("kirk-cli-snap-tmp-{name}"));
        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_dir_all(&tmp_dir);
        std::fs::create_dir_all(root.join("runtest")).expect("mkdir runtest");
        std::fs::create_dir_all(root.join("metadata")).expect("mkdir metadata");
        std::fs::create_dir_all(root.join("testcases/bin")).expect("mkdir testcases/bin");
        std::fs::create_dir_all(&tmp_dir).expect("mkdir tmpdir");
        Self { root, tmp_dir }
    }

    fn write_runtest(&self, name: &str, content: &str) {
        std::fs::write(self.root.join("runtest").join(name), content).expect("write runtest");
    }

    fn write_metadata(&self, content: &str) {
        std::fs::write(self.root.join("metadata/ltp.json"), content).expect("write metadata");
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
        let _ = std::fs::remove_dir_all(&self.tmp_dir);
    }
}

/// Set `LTPROOT`, run `run_session` under the fixture, and return the
/// captured output plus the exit code.
async fn run(fixture: &Fixture, extra_args: &[&str]) -> (String, i32) {
    let _guard = ENV_LOCK.lock().await;
    // SAFETY: `_guard` serializes every test in this binary around this
    // mutation and the run it drives.
    unsafe { std::env::set_var("LTPROOT", &fixture.root) };

    let mut cli_args = vec![
        "kirk",
        "--tmp-dir",
        fixture.tmp_dir.to_str().expect("tmp_dir utf-8"),
        "--no-colors",
    ];
    cli_args.extend_from_slice(extra_args);
    let args = Args::parse_from(cli_args);

    let printer = Arc::new(VecPrinter::new());
    let code = run_session(&args, printer.clone())
        .await
        .expect("run_session");
    (printer.contents(), code)
}

#[tokio::test]
async fn serial_success_and_failure() {
    let fixture = Fixture::new("serial");
    fixture.write_runtest("mysuite", "test01 true\ntest02 false\n");
    fixture.write_metadata(r#"{"tests": {"test01": {}, "test02": {}}}"#);

    let (out, code) = run(&fixture, &["--run-suite", "mysuite"]).await;
    assert_eq!(code, 0, "session must succeed even with a failing test");
    assert!(out.contains("test01: "), "prefix for passing test: {out}");
    assert!(out.contains("test02: "), "prefix for failing test: {out}");
    assert!(out.contains("pass"), "passing result: {out}");
    assert!(out.contains("fail"), "failing result: {out}");
}

#[tokio::test]
async fn verbose_streaming_shows_full_command() {
    let fixture = Fixture::new("verbose");
    fixture.write_runtest("mysuite", "test01 echo hello\n");
    fixture.write_metadata(r#"{"tests": {"test01": {}}}"#);

    let (out, code) = run(&fixture, &["--run-suite", "mysuite", "--verbose"]).await;
    assert_eq!(code, 0);
    assert!(out.contains("Executing: "), "verbose executing line: {out}");
    assert!(out.contains("echo hello"), "full command shown: {out}");
    assert!(out.contains("Duration:"), "verbose duration footer: {out}");
}

#[tokio::test]
async fn verbose_streams_test_stdout_before_duration_footer() {
    let fixture = Fixture::new("verbose-stream");
    fixture.write_runtest(
        "mysuite",
        "test01 echo KIRK_STDOUT_MARKER_OK\ntest02 echo KIRK_STDOUT_MARKER_FAIL ; exit 1\n",
    );
    fixture.write_metadata(r#"{"tests": {"test01": {}, "test02": {}}}"#);

    let (out, code) = run(&fixture, &["--run-suite", "mysuite", "--verbose"]).await;
    assert_eq!(code, 0, "a failing test is not a session failure");

    // Isolate test02's block on its own section header, so its marker is
    // checked against its own command header/duration footer, not test01's.
    let (block01, block02) = out
        .split_once("      test02\n")
        .expect("test02 section header present");

    for (marker, block) in [
        ("KIRK_STDOUT_MARKER_OK", block01),
        ("KIRK_STDOUT_MARKER_FAIL", block02),
    ] {
        // The command header also echoes `marker` (it is `echo <marker>`),
        // so isolate the body between the blank line that ends "Executing:
        // ..." and the duration footer before counting.
        let exec_pos = block
            .find("Executing: ")
            .unwrap_or_else(|| panic!("executing header missing for {marker}: {block}"));
        let body_start = exec_pos
            + block[exec_pos..].find("\n\n").unwrap_or_else(|| {
                panic!("command header not blank-terminated for {marker}: {block}")
            })
            + 2;
        let duration_pos = block[body_start..]
            .find("Duration:")
            .unwrap_or_else(|| panic!("duration footer missing for {marker}: {block}"));
        let body = &block[body_start..body_start + duration_pos];
        assert_eq!(
            body.matches(marker).count(),
            1,
            "marker {marker} must appear exactly once between the command header and duration footer: {block}"
        );
    }
}

#[tokio::test]
async fn parallel_progress_counts_tests() {
    let fixture = Fixture::new("parallel");
    fixture.write_runtest("mysuite", "test01 true\ntest02 true\n");
    fixture.write_metadata(r#"{"tests": {"test01": {}, "test02": {}}}"#);

    let (out, code) = run(&fixture, &["--run-suite", "mysuite", "--workers", "2"]).await;
    assert_eq!(code, 0);
    assert!(
        out.contains("Following tests will run in parallel:"),
        "parallel listing: {out}"
    );
    assert!(out.contains("(1/2)"), "first progress marker: {out}");
    assert!(out.contains("(2/2)"), "second progress marker: {out}");
}

#[tokio::test]
async fn dry_run_groups_parallel_and_serial() {
    let fixture = Fixture::new("dryrun");
    fixture.write_runtest("mysuite", "test01 true\ntest02 true\n");
    fixture.write_metadata(r#"{"tests": {"test01": {}, "test02": {"needs_root": "1"}}}"#);

    let (out, code) = run(&fixture, &["--run-suite", "mysuite", "--dry-run"]).await;
    assert_eq!(code, 0);
    assert!(out.contains("Suite: mysuite"), "suite header: {out}");
    assert!(out.contains("Parallel tests:"), "parallel section: {out}");
    assert!(out.contains("Serial tests:"), "serial section: {out}");
    assert!(
        out.contains("Total tests: 2 (not executed)"),
        "not-executed footer: {out}"
    );
}

#[tokio::test]
async fn suite_timeout_marks_leftover_tests() {
    let fixture = Fixture::new("timeout");
    fixture.write_runtest("mysuite", "test01 sleep 2\ntest02 sleep 2\n");
    fixture.write_metadata(r#"{"tests": {"test01": {}, "test02": {}}}"#);

    let (out, code) = run(
        &fixture,
        &["--run-suite", "mysuite", "--suite-timeout", "1"],
    )
    .await;
    assert_eq!(code, 0, "a suite timeout is not a session failure");
    assert!(
        out.contains("timed out after"),
        "suite timeout message: {out}"
    );
}

#[tokio::test]
async fn kernel_panic_marker_is_detected() {
    let fixture = Fixture::new("panic");
    fixture.write_runtest("mysuite", "test01 echo 'Kernel panic - not syncing'\n");
    fixture.write_metadata(r#"{"tests": {"test01": {}}}"#);

    let (out, _code) = run(&fixture, &["--run-suite", "mysuite"]).await;
    assert!(out.contains("kernel panic"), "kernel panic message: {out}");
}

#[tokio::test]
async fn final_summary_lists_failures() {
    let fixture = Fixture::new("summary");
    fixture.write_runtest("mysuite", "test01 true\ntest02 false\n");
    fixture.write_metadata(r#"{"tests": {"test01": {}, "test02": {}}}"#);

    let (out, code) = run(&fixture, &["--run-suite", "mysuite"]).await;
    assert_eq!(code, 0);
    assert!(out.contains("TEST SUMMARY"), "summary header: {out}");
    assert!(out.contains("Failures:"), "failures section: {out}");
    assert!(out.contains("test02"), "failing test named: {out}");
}
