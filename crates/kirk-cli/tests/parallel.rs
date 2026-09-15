//! Proves `--workers 2` actually overlaps parallelizable test execution
//! through the CLI adapter, rather than serializing on a shared SUT lock.

use clap::Parser as _;
use kirk_cli::args::Args;
use kirk_cli::session::run_session;

fn write_ltp_root(root: &std::path::Path) {
    std::fs::create_dir_all(root.join("runtest")).expect("mkdir runtest");
    std::fs::create_dir_all(root.join("metadata")).expect("mkdir metadata");
    // Test cwd (see `kirk_ltp::LtpFramework::tc_folder`) must exist for the
    // channel to spawn commands there.
    std::fs::create_dir_all(root.join("testcases/bin")).expect("mkdir testcases/bin");
    std::fs::write(
        root.join("runtest/mysuite"),
        "test01 sleep 1\ntest02 sleep 1\n",
    )
    .expect("write runtest");
    std::fs::write(
        root.join("metadata/ltp.json"),
        r#"{"tests": {"test01": {}, "test02": {}}}"#,
    )
    .expect("write metadata");
}

#[tokio::test]
async fn two_parallel_second_long_tests_overlap_with_two_workers() {
    let root =
        std::env::temp_dir().join(format!("kirk-cli-parallel-ltproot-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    write_ltp_root(&root);
    // SAFETY: this test is the sole consumer of LTPROOT in this binary.
    unsafe { std::env::set_var("LTPROOT", &root) };

    let tmp_dir =
        std::env::temp_dir().join(format!("kirk-cli-parallel-tmp-{}", std::process::id()));
    std::fs::create_dir_all(&tmp_dir).expect("mkdir tmpdir");

    let args = Args::parse_from([
        "kirk",
        "--no-colors",
        "--tmp-dir",
        tmp_dir.to_str().expect("tmp_dir utf-8"),
        "--run-suite",
        "mysuite",
        "--workers",
        "2",
    ]);

    let printer = std::sync::Arc::new(kirk_support::VecPrinter::new());
    let start = std::time::Instant::now();
    let code = run_session(&args, printer).await.expect("run_session");
    let elapsed = start.elapsed();

    assert_eq!(code, 0, "session must succeed");
    // Sequential execution of two ~1s tests takes >= 2s; two workers running
    // them concurrently should finish well under that, with generous slack
    // for process spawn overhead in CI.
    assert!(
        elapsed.as_secs_f64() < 1.8,
        "expected overlap, took {elapsed:?}"
    );

    std::fs::remove_dir_all(&root).ok();
    std::fs::remove_dir_all(&tmp_dir).ok();
}
