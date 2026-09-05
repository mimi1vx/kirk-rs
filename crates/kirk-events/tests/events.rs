//! Ports of `kirk/libkirk/tests/test_events.py`.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use kirk_events::{BoxFuture, EventArgs, EventRegistry, Handler, HandlerResult};

fn ok_handler() -> Handler {
    Arc::new(|_: EventArgs| -> BoxFuture<HandlerResult> { Box::pin(async move { Ok(()) }) })
}

fn run_loop(registry: &EventRegistry) -> tokio::task::JoinHandle<()> {
    let worker = registry.clone();
    tokio::spawn(async move { worker.start().await })
}

async fn stop_loop(registry: &EventRegistry, running: tokio::task::JoinHandle<()>) {
    registry.stop();
    assert!(running.await.is_ok());
}

async fn wait_for<F, Fut>(mut ready: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !ready().await {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for handlers"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

#[tokio::test]
async fn register_and_reset() {
    let registry = EventRegistry::new();
    assert!(
        registry
            .register("myevent", ok_handler(), false)
            .await
            .is_ok()
    );
    assert!(matches!(registry.is_registered("myevent").await, Ok(true)));

    registry.reset().await;
    assert!(matches!(registry.is_registered("myevent").await, Ok(false)));
}

#[tokio::test]
async fn empty_names_are_rejected() {
    let registry = EventRegistry::new();
    assert!(registry.register("", ok_handler(), false).await.is_err());
    assert!(registry.is_registered("").await.is_err());
    assert!(registry.unregister("", None).await.is_err());
    assert!(registry.fire("", None).await.is_err());
}

#[tokio::test]
async fn unregister_single_handler() {
    let registry = EventRegistry::new();
    let hits: Arc<tokio::sync::Mutex<Vec<&'static str>>> = Arc::default();

    let first: Handler = {
        let hits = Arc::clone(&hits);
        Arc::new(move |_: EventArgs| -> BoxFuture<HandlerResult> {
            let hits = Arc::clone(&hits);
            Box::pin(async move {
                hits.lock().await.push("first");
                Ok(())
            })
        })
    };
    let second: Handler = {
        let hits = Arc::clone(&hits);
        Arc::new(move |_: EventArgs| -> BoxFuture<HandlerResult> {
            let hits = Arc::clone(&hits);
            Box::pin(async move {
                hits.lock().await.push("second");
                Ok(())
            })
        })
    };

    assert!(!matches!(registry.is_registered("myevent").await, Ok(true)));
    assert!(
        registry
            .register("myevent", Arc::clone(&first), false)
            .await
            .is_ok()
    );
    assert!(matches!(registry.is_registered("myevent").await, Ok(true)));
    assert!(registry.register("myevent", second, false).await.is_ok());

    // Removing a handler that was never registered is a no-op.
    assert!(
        registry
            .unregister("myevent", Some(&ok_handler()))
            .await
            .is_ok()
    );
    assert!(matches!(registry.is_registered("myevent").await, Ok(true)));

    assert!(registry.unregister("myevent", Some(&first)).await.is_ok());
    assert!(matches!(registry.is_registered("myevent").await, Ok(true)));

    let running = run_loop(&registry);
    assert!(registry.fire("myevent", None).await.is_ok());
    let hits_probe = Arc::clone(&hits);
    wait_for(|| async { !hits_probe.lock().await.is_empty() }).await;
    stop_loop(&registry, running).await;

    let guard = hits.lock().await;
    assert_eq!(*guard, vec!["second"]);
}

#[tokio::test]
async fn unregister_entire_event_and_missing_name_noop() {
    let registry = EventRegistry::new();
    assert!(
        registry
            .register("myevent", ok_handler(), false)
            .await
            .is_ok()
    );
    assert!(matches!(registry.is_registered("myevent").await, Ok(true)));

    assert!(registry.unregister("myevent", None).await.is_ok());
    assert!(matches!(registry.is_registered("myevent").await, Ok(false)));

    // Unlike upstream, removing an unknown event is a no-op success.
    assert!(registry.unregister("not_registered", None).await.is_ok());
    assert!(
        registry
            .unregister("not_registered", Some(&ok_handler()))
            .await
            .is_ok()
    );

    // Firing an unknown event is a silent no-op.
    let running = run_loop(&registry);
    assert!(registry.fire("not_registered", None).await.is_ok());
    stop_loop(&registry, running).await;
}

#[tokio::test]
async fn fire_fanout_delivers_every_payload() {
    const TIMES: usize = 100;
    let registry = EventRegistry::new();
    let called: Arc<tokio::sync::Mutex<Vec<usize>>> = Arc::default();

    let handler: Handler = {
        let called = Arc::clone(&called);
        Arc::new(move |args: EventArgs| -> BoxFuture<HandlerResult> {
            let called = Arc::clone(&called);
            Box::pin(async move {
                let value: usize = args
                    .message
                    .as_deref()
                    .unwrap_or("")
                    .parse()
                    .map_err(|_| String::from("bad payload"))?;
                called.lock().await.push(value);
                Ok(())
            })
        })
    };
    assert!(registry.register("myevent", handler, false).await.is_ok());
    assert!(matches!(registry.is_registered("myevent").await, Ok(true)));

    let running = run_loop(&registry);
    for i in 0..TIMES {
        assert!(registry.fire("myevent", Some(i.to_string())).await.is_ok());
    }
    let called_probe = Arc::clone(&called);
    wait_for(|| async { called_probe.lock().await.len() >= TIMES }).await;
    stop_loop(&registry, running).await;

    let mut guard = called.lock().await;
    guard.sort_unstable();
    assert_eq!(guard.len(), TIMES);
    for (index, value) in guard.iter().enumerate() {
        assert_eq!(*value, index);
    }
}

#[tokio::test]
async fn ordered_handlers_run_serially_in_registration_order() {
    let registry = EventRegistry::new();
    let order: Arc<tokio::sync::Mutex<Vec<usize>>> = Arc::default();

    // Decreasing delays: only serial execution completes in order.
    for (index, delay_ms) in [(0_usize, 50_u64), (1, 20), (2, 5)] {
        let sink = Arc::clone(&order);
        let handler: Handler = Arc::new(move |_: EventArgs| -> BoxFuture<HandlerResult> {
            let sink = Arc::clone(&sink);
            Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                sink.lock().await.push(index);
                Ok(())
            })
        });
        assert!(
            registry
                .register("ordered_evt", handler, true)
                .await
                .is_ok()
        );
    }

    let running = run_loop(&registry);
    assert!(registry.fire("ordered_evt", None).await.is_ok());
    let order_probe = Arc::clone(&order);
    wait_for(|| async { order_probe.lock().await.len() >= 3 }).await;
    stop_loop(&registry, running).await;

    let guard = order.lock().await;
    assert_eq!(*guard, vec![0, 1, 2]);
}

#[tokio::test]
async fn handler_error_reaches_internal_error() {
    let registry = EventRegistry::new();
    let errors: Arc<tokio::sync::Mutex<Vec<EventArgs>>> = Arc::default();

    let bad: Handler = Arc::new(|_: EventArgs| -> BoxFuture<HandlerResult> {
        Box::pin(async move { Err(String::from("test error")) })
    });
    let catcher: Handler = {
        let errors = Arc::clone(&errors);
        Arc::new(move |args: EventArgs| -> BoxFuture<HandlerResult> {
            let errors = Arc::clone(&errors);
            Box::pin(async move {
                errors.lock().await.push(args);
                Ok(())
            })
        })
    };
    assert!(registry.register("bad_event", bad, false).await.is_ok());
    assert!(
        registry
            .register("internal_error", catcher, false)
            .await
            .is_ok()
    );

    let running = run_loop(&registry);
    assert!(registry.fire("bad_event", None).await.is_ok());
    let errors_probe = Arc::clone(&errors);
    wait_for(|| async { !errors_probe.lock().await.is_empty() }).await;
    stop_loop(&registry, running).await;

    let guard = errors.lock().await;
    assert_eq!(guard.len(), 1);
    assert_eq!(guard[0].event, "bad_event");
    assert_eq!(guard[0].message.as_deref(), Some("test error"));
}

#[tokio::test]
async fn handler_panic_reaches_internal_error() {
    let registry = EventRegistry::new();
    let errors: Arc<tokio::sync::Mutex<Vec<EventArgs>>> = Arc::default();

    let explosive: Handler = Arc::new(|_: EventArgs| -> BoxFuture<HandlerResult> {
        Box::pin(async move { std::panic::panic_any("boom") })
    });
    let catcher: Handler = {
        let errors = Arc::clone(&errors);
        Arc::new(move |args: EventArgs| -> BoxFuture<HandlerResult> {
            let errors = Arc::clone(&errors);
            Box::pin(async move {
                errors.lock().await.push(args);
                Ok(())
            })
        })
    };
    assert!(
        registry
            .register("bad_event", explosive, false)
            .await
            .is_ok()
    );
    assert!(
        registry
            .register("internal_error", catcher, false)
            .await
            .is_ok()
    );

    let running = run_loop(&registry);
    assert!(registry.fire("bad_event", None).await.is_ok());
    let errors_probe = Arc::clone(&errors);
    wait_for(|| async { !errors_probe.lock().await.is_empty() }).await;
    stop_loop(&registry, running).await;

    let guard = errors.lock().await;
    assert_eq!(guard.len(), 1);
    assert_eq!(guard[0].event, "bad_event");
    assert_eq!(guard[0].message.as_deref(), Some("boom"));
}
