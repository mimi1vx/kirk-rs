//! Port of `TestSuiteScheduler` from `test_scheduler.py`.

mod support;

use std::time::Duration;

use kirk_core::KirkError;
use kirk_core::data::{Suite, Test};
use kirk_core::results::ResultStatus;
use kirk_scheduler::SuiteScheduler;
use support::{FakeFramework, FakeSut, echo_test, sleep_test};

fn scheduler(
    sut: FakeSut,
    suite_timeout: f64,
    exec_timeout: f64,
    workers: usize,
) -> SuiteScheduler<FakeSut, FakeFramework> {
    SuiteScheduler::new(
        sut,
        FakeFramework::new(),
        suite_timeout,
        exec_timeout,
        workers,
    )
}

fn suite(name: &str, tests: Vec<Test>) -> Suite {
    Suite::new(name, tests)
}

#[tokio::test]
async fn schedule_runs_all_suites() {
    for workers in [1, 10] {
        let tests: Vec<Test> = (0..10).map(echo_test).collect();
        let sched = scheduler(FakeSut::new(), 3600.0, 3600.0, workers);

        sched
            .schedule(&[suite("suite01", tests)])
            .await
            .expect("schedule succeeds");

        let results = sched.results().await;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].tests_results().len(), 10);
        assert_eq!(results[0].distro(), Some("openSUSE"));
    }
}

#[tokio::test]
async fn max_workers_bounds_concurrency() {
    let sut = FakeSut::new();
    let sched = scheduler(sut.clone(), 3600.0, 3600.0, 3);
    let tests: Vec<Test> = (0..9).map(|index| sleep_test(index, "0.2")).collect();
    let suites = [suite("suite01", tests)];

    sched.schedule(&suites).await.expect("schedule succeeds");

    let results = sched.results().await;
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].tests_results().len(), 9);
    assert!(sut.max_concurrent() > 1 && sut.max_concurrent() <= 3);
}

#[tokio::test]
async fn schedule_rejects_empty_jobs() {
    let sched = scheduler(FakeSut::new(), 3600.0, 3600.0, 1);
    let error = sched.schedule(&[]).await.unwrap_err();
    assert!(matches!(error, KirkError::Scheduler(_)));
}

#[tokio::test]
async fn schedule_stop_cuts_execution_short() {
    for workers in [1, 10] {
        let count = workers * 2;
        let tests: Vec<Test> = (0..count).map(|index| sleep_test(index, "1.0")).collect();
        let sut = FakeSut::new();
        let sched = scheduler(sut.clone(), 3600.0, 3600.0, workers);
        let suites = [suite("suite01", tests)];

        tokio::join!(sched.schedule(&suites), async {
            tokio::time::sleep(Duration::from_millis(200)).await;
            sched.stop().await;
        })
        .0
        .expect("stopped schedule succeeds");

        assert!(sched.stopped());
        assert_eq!(sut.stops(), 0);
        let results = sched.results().await;
        assert_eq!(results.len(), 1);
        assert!(!results[0].tests_results().is_empty());
    }
}

#[tokio::test]
async fn schedule_reboots_on_kernel_tainted() {
    let sut = FakeSut::tainting();
    let sched = scheduler(sut.clone(), 3600.0, 3600.0, 1);
    let tests: Vec<Test> = (0..2).map(echo_test).collect();

    sched
        .schedule(&[suite("suite01", tests)])
        .await
        .expect("tainted suites recover via reboot");

    // Sequential: each test taints in turn, rebooting after each one.
    assert_eq!(sut.restarts(), 2);
    let results = sched.results().await;
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].tests_results().len(), 2);
}

#[tokio::test]
async fn schedule_reboots_on_kernel_tainted_parallel() {
    let sut = FakeSut::tainting();
    let sched = scheduler(sut.clone(), 3600.0, 3600.0, 10);
    let tests: Vec<Test> = (0..2).map(echo_test).collect();

    sched
        .schedule(&[suite("suite01", tests)])
        .await
        .expect("tainted suites recover via reboot");

    assert!(sut.restarts() >= 1);
    let results = sched.results().await;
    assert_eq!(results.len(), 1);
    assert!(results[0].tests_results().len() >= 2);
}

#[tokio::test]
async fn schedule_reboots_on_kernel_panic() {
    for workers in [1, 10] {
        let mut tests: Vec<Test> = (0..9).map(echo_test).collect();
        let mut panic = Test::new("test9", "echo")
            .expect("valid test")
            .with_args(vec![
                String::from("-n"),
                String::from("Kernel"),
                String::from("panic"),
            ]);
        panic.force_parallel();
        tests.push(panic);

        let sut = FakeSut::new();
        let sched = scheduler(sut.clone(), 3600.0, 3600.0, workers);
        sched
            .schedule(&[suite("suite01", tests)])
            .await
            .expect("panicking suites recover via reboot");

        assert_eq!(sut.restarts(), 1);
        let results = sched.results().await;
        assert_eq!(results.len(), 1);
        assert!(results[0].tests_results().len() >= 10);
    }
}

#[tokio::test]
async fn schedule_reboots_on_kernel_timeout() {
    for workers in [1, 10] {
        let tests: Vec<Test> = (0..10).map(|index| sleep_test(index, "0.1")).collect();
        let sut = FakeSut::hanging();
        let sched = scheduler(sut.clone(), 3600.0, 0.05, workers);

        sched
            .schedule(&[suite("suite01", tests)])
            .await
            .expect("timed-out suites recover via reboot");

        assert!(sut.restarts() > 0);
        let results = sched.results().await;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].tests_results().len(), 10);
    }
}

#[tokio::test]
async fn schedule_marks_leftover_tests_on_suite_timeout() {
    for workers in [1, 10] {
        let tests: Vec<Test> = (0..10).map(|index| sleep_test(index, "0.5")).collect();
        let sched = scheduler(FakeSut::new(), 0.1, 3600.0, workers);

        sched
            .schedule(&[suite("suite01", tests)])
            .await
            .expect("suite timeout fills leftovers");

        let results = sched.results().await;
        assert_eq!(results.len(), 1);
        assert!((results[0].exec_time() - 0.0).abs() < f64::EPSILON);
        for (index, result) in results[0].tests_results().iter().enumerate() {
            assert_eq!(result.test().name(), format!("test{index}"));
            assert_eq!(result.passed(), 0);
            assert_eq!(result.failed(), 0);
            assert_eq!(result.broken(), 0);
            assert_eq!(result.skipped(), 1);
            assert_eq!(result.warnings(), 0);
            assert!(result.exec_time() >= 0.0 && result.exec_time() < 0.4);
            assert_eq!(result.return_code(), 32);
            assert_eq!(result.stdout(), "");
            assert_eq!(result.status(), ResultStatus::CONF);
        }
    }
}
