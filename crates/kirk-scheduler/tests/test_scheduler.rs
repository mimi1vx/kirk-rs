//! Port of `TestTestScheduler` from `test_scheduler.py`.

mod support;

use std::time::Duration;

use kirk_core::KirkError;
use kirk_core::data::Test;
use kirk_scheduler::TestScheduler;
use support::{FakeFramework, FakeSut, echo_test, sleep_test};

fn scheduler(
    sut: FakeSut,
    test_timeout: f64,
    workers: usize,
) -> TestScheduler<FakeSut, FakeFramework> {
    TestScheduler::new(sut, FakeFramework::new(), test_timeout, workers)
}

#[tokio::test]
async fn schedule_runs_all_tests() {
    for workers in [1, 10] {
        let tests: Vec<Test> = (0..10).map(echo_test).collect();
        let sched = scheduler(FakeSut::new(), 3600.0, workers);

        sched.schedule(&tests).await.expect("schedule succeeds");

        let mut results = sched.results().await;
        assert_eq!(results.len(), tests.len());
        results.sort_by(|left, right| left.test().name().cmp(right.test().name()));
        for (index, result) in results.iter().enumerate() {
            assert_eq!(result.test().name(), format!("test{index}"));
            assert_eq!(result.passed(), 1);
            assert_eq!(result.failed(), 0);
            assert_eq!(result.broken(), 0);
            assert_eq!(result.skipped(), 0);
            assert_eq!(result.warnings(), 0);
            assert!(result.exec_time() > 0.0 && result.exec_time() < 1.0);
            assert_eq!(result.return_code(), 0);
            assert_eq!(result.stdout(), "ciao");
        }
    }
}

#[tokio::test]
async fn schedule_rejects_empty_jobs() {
    let sched = scheduler(FakeSut::new(), 3600.0, 1);
    let error = sched.schedule(&[]).await.unwrap_err();
    assert!(matches!(error, KirkError::Scheduler(_)));
}

#[tokio::test]
async fn sequential_mode_preserves_order() {
    let tests: Vec<Test> = (0..10).map(echo_test).collect();
    let sched = scheduler(FakeSut::new(), 3600.0, 1);

    sched.schedule(&tests).await.expect("schedule succeeds");

    let results = sched.results().await;
    assert_eq!(results.len(), 10);
    for (index, result) in results.iter().enumerate() {
        assert_eq!(result.test().name(), format!("test{index}"));
    }
}

#[tokio::test]
async fn max_workers_bounds_concurrency() {
    let sut = FakeSut::new();
    let sched = scheduler(sut.clone(), 3600.0, 3);
    let tests: Vec<Test> = (0..9).map(|index| sleep_test(index, "0.02")).collect();

    sched.schedule(&tests).await.expect("schedule succeeds");

    assert_eq!(sched.results().await.len(), 9);
    assert!(sut.max_concurrent() > 1 && sut.max_concurrent() <= 3);
}

#[tokio::test]
async fn single_worker_runs_one_at_a_time() {
    let sut = FakeSut::new();
    let sched = scheduler(sut.clone(), 3600.0, 1);
    let tests: Vec<Test> = (0..4).map(|index| sleep_test(index, "0.05")).collect();

    sched.schedule(&tests).await.expect("schedule succeeds");

    assert_eq!(sut.max_concurrent(), 1);
}

#[tokio::test]
async fn schedule_stop_cuts_execution_short() {
    for workers in [1, 10] {
        let count = workers * 2;
        let tests: Vec<Test> = (0..count).map(|index| sleep_test(index, "0.1")).collect();
        let sut = FakeSut::new();
        let sched = scheduler(sut.clone(), 3600.0, workers);

        tokio::join!(sched.schedule(&tests), async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            sched.stop().await;
        })
        .0
        .expect("stopped schedule succeeds");

        assert!(sched.stopped());
        // A single stop lets tests finish: the SUT is never forced down.
        assert_eq!(sut.stops(), 0);
        assert!(sched.results().await.len() < count);
    }
}

#[tokio::test]
async fn schedule_raises_on_kernel_tainted() {
    for workers in [1, 10] {
        let tests: Vec<Test> = (0..10).map(echo_test).collect();
        let sched = scheduler(FakeSut::tainting(), 3600.0, workers);

        let error = sched.schedule(&tests).await.unwrap_err();

        assert!(matches!(error, KirkError::KernelTainted(_)));
        assert!(!sched.results().await.is_empty());
    }
}

#[tokio::test]
async fn schedule_raises_on_kernel_panic() {
    let mut panic = Test::new("test0", "echo")
        .expect("valid test")
        .with_args(vec![String::from("Kernel"), String::from("panic")]);
    panic.force_parallel();
    let mut tests = vec![panic];
    tests.extend((1..10).map(|index| sleep_test(index, "0.2")));

    let sut = FakeSut::new();
    let sched = scheduler(sut.clone(), 3600.0, 1);
    let error = sched.schedule(&tests).await.unwrap_err();
    assert!(matches!(error, KirkError::KernelPanic(_)));
    // Rebooting is the suite scheduler's job: the test scheduler only raises.
    assert_eq!(sut.restarts(), 0);

    // Sequential mode aborts on the first error: only the panic row exists.
    let results = sched.results().await;
    assert_eq!(results.len(), 1);
    let result = &results[0];
    assert_eq!(result.test().name(), "test0");
    assert_eq!(result.passed(), 0);
    assert_eq!(result.failed(), 0);
    assert_eq!(result.broken(), 1);
    assert_eq!(result.skipped(), 0);
    assert_eq!(result.warnings(), 0);
    assert!(result.exec_time() >= 0.0 && result.exec_time() < 0.2);
    assert_eq!(result.return_code(), -1);
    assert_eq!(result.stdout(), "Kernel panic\n");
}

#[tokio::test]
async fn schedule_drains_parallel_tasks_on_kernel_panic() {
    let mut panic = Test::new("test0", "echo")
        .expect("valid test")
        .with_args(vec![String::from("Kernel"), String::from("panic")]);
    panic.force_parallel();
    let mut tests = vec![panic];
    tests.extend((1..10).map(|index| sleep_test(index, "0.2")));

    let sched = scheduler(FakeSut::new(), 3600.0, 10);
    let error = sched.schedule(&tests).await.unwrap_err();
    assert!(matches!(error, KirkError::KernelPanic(_)));

    // Unlike `asyncio.gather`, the JoinSet drain is deterministic: every
    // task completes and records its result before the error is raised.
    let results = sched.results().await;
    assert_eq!(results.len(), 10);
    let panicked = results
        .iter()
        .find(|result| result.test().name() == "test0")
        .expect("panic row is recorded");
    assert_eq!(panicked.broken(), 1);
    assert_eq!(panicked.return_code(), -1);
}

#[tokio::test]
async fn schedule_raises_on_kernel_timeout() {
    for workers in [1, 10] {
        let tests: Vec<Test> = (0..10).map(|index| sleep_test(index, "0.01")).collect();
        let sched = scheduler(FakeSut::hanging(), 0.02, workers);

        let error = sched.schedule(&tests).await.unwrap_err();

        assert!(matches!(error, KirkError::KernelTimeout(_)));
    }
}

#[tokio::test]
async fn schedule_records_test_timeout_without_raising() {
    for workers in [1, 10] {
        let tests: Vec<Test> = (0..10).map(|index| sleep_test(index, "0.05")).collect();
        let sched = scheduler(FakeSut::new(), 0.02, workers);

        sched.schedule(&tests).await.expect("schedule succeeds");

        let results = sched.results().await;
        assert_eq!(results.len(), tests.len());
        for result in &results {
            assert!(result.test().name().starts_with("test"));
            assert_eq!(result.passed(), 0);
            assert_eq!(result.failed(), 0);
            assert_eq!(result.broken(), 1);
            assert_eq!(result.skipped(), 0);
            assert_eq!(result.warnings(), 0);
            assert!(result.exec_time() > 0.0 && result.exec_time() < 0.4);
            assert_eq!(result.return_code(), -1);
            assert_eq!(result.stdout(), "");
        }
    }
}
