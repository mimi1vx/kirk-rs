//! Async event registry ported from upstream `kirk/libkirk/evt.py`.
//!
//! [`EventRegistry`] maps event names to handlers and runs them through a FIFO
//! queue: [`EventRegistry::fire`] snapshots the handlers registered for an
//! event and enqueues them, [`EventRegistry::start`] consumes the queue until
//! [`EventRegistry::stop`] enqueues the stop sentinel.
//!
//! Deliberate differences from upstream:
//!
//! * Handlers are fallible: returning `Err` models a Python handler raising.
//!   Panics are still isolated via owned [`JoinSet`]
//!   tasks and forwarded like any other failure.
//! * Failures reach `INTERNAL_ERROR` handlers as [`EventArgs`] holding the
//!   failing event name plus the failure description. Failures raised *by*
//!   `INTERNAL_ERROR` handlers are dropped instead of propagated, so a
//!   broken error handler can neither recurse nor kill the loop.
//! * [`EventRegistry::unregister`] on an unknown event is a no-op success;
//!   upstream raises `ValueError`.
//! * Empty event names map to [`KirkError::Framework`], since neither upstream
//!   (`ValueError`) nor [`KirkError`] defines an events variant.
//! * Registration calls are `async`: state lives behind `tokio::sync::Mutex`
//!   guards held only for short, `await`-free snapshots.

use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use kirk_core::KirkError;
use tokio::sync::{Mutex, mpsc};
use tokio::task::{JoinError, JoinSet};

/// Built-in event receiving handler failures.
///
/// Present from construction with no handlers, mirroring `EventsHandler`.
/// [`EventRegistry::reset`] removes it (as upstream does); re-register
/// handlers to receive failures again.
const INTERNAL_ERROR: &str = "internal_error";

/// Arguments delivered to every handler of a fired event.
///
/// Upstream passes arbitrary `*args`/`**kwargs`; this port carries only what
/// call sites need. For `INTERNAL_ERROR` deliveries, `event` is the
/// *failing* event name and `message` is the failure description.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventArgs {
    /// Event being handled, or the failing event for internal errors.
    pub event: String,
    /// Optional payload; the failure description for internal errors.
    pub message: Option<String>,
}

/// Boxed future every handler returns; `Send` so a [`JoinSet`] can own it.
pub type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

/// Outcome of one handler invocation; `Err` holds the failure description.
pub type HandlerResult = Result<(), String>;

/// Event handler: maps [`EventArgs`] to a [`HandlerResult`] future.
pub type Handler = Arc<dyn Fn(EventArgs) -> BoxFuture<HandlerResult> + Send + Sync>;

fn empty_name_error() -> KirkError {
    KirkError::Framework(String::from("event_name is empty"))
}

fn panic_message(payload: &Box<dyn Any + Send>) -> String {
    if let Some(text) = payload.downcast_ref::<String>() {
        text.clone()
    } else if let Some(text) = payload.downcast_ref::<&'static str>() {
        (*text).to_string()
    } else {
        String::from("event handler panicked")
    }
}

fn join_failure(error: JoinError) -> String {
    if error.is_panic() {
        panic_message(&error.into_panic())
    } else {
        String::from("event handler was cancelled")
    }
}

struct EventEntry {
    handlers: Vec<Handler>,
    ordered: bool,
}

enum QueueItem {
    Dispatch {
        event: String,
        handlers: Vec<Handler>,
        ordered: bool,
        args: EventArgs,
    },
    Stop,
}

struct Inner {
    events: Mutex<HashMap<String, EventEntry>>,
    tx: mpsc::UnboundedSender<QueueItem>,
    rx: Mutex<mpsc::UnboundedReceiver<QueueItem>>,
}

/// Cloneable handle to a shared async event registry.
///
/// All methods take `&self`; clones share the same events and queue.
#[derive(Clone)]
pub struct EventRegistry {
    inner: Arc<Inner>,
}

impl EventRegistry {
    /// Build an empty registry with a handler-less `INTERNAL_ERROR` event.
    #[must_use]
    pub fn new() -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let mut events = HashMap::new();
        events.insert(
            String::from(INTERNAL_ERROR),
            EventEntry {
                handlers: Vec::new(),
                ordered: false,
            },
        );
        Self {
            inner: Arc::new(Inner {
                events: Mutex::new(events),
                tx,
                rx: Mutex::new(rx),
            }),
        }
    }

    /// Register `handler` under `event_name`.
    ///
    /// A first registration fixes the event's `ordered` flag; later
    /// registrations for the same name keep it, mirroring `setdefault`
    /// upstream.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError`] when `event_name` is empty.
    pub async fn register(
        &self,
        event_name: &str,
        handler: Handler,
        ordered: bool,
    ) -> Result<(), KirkError> {
        if event_name.is_empty() {
            return Err(empty_name_error());
        }
        let mut events = self.inner.events.lock().await;
        events
            .entry(String::from(event_name))
            .or_insert_with(|| EventEntry {
                handlers: Vec::new(),
                ordered,
            })
            .handlers
            .push(handler);
        Ok(())
    }

    /// Remove one handler, or the whole event when `handler` is `None`.
    ///
    /// Removing a handler that was never registered — or an event that does
    /// not exist — is a no-op success.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError`] when `event_name` is empty.
    pub async fn unregister(
        &self,
        event_name: &str,
        handler: Option<&Handler>,
    ) -> Result<(), KirkError> {
        if event_name.is_empty() {
            return Err(empty_name_error());
        }
        let mut events = self.inner.events.lock().await;
        match handler {
            Some(target) => {
                if let Some(entry) = events.get_mut(event_name) {
                    entry.handlers.retain(|known| !Arc::ptr_eq(known, target));
                }
            }
            None => {
                events.remove(event_name);
            }
        }
        Ok(())
    }

    /// Report whether `event_name` has at least one handler.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError`] when `event_name` is empty.
    pub async fn is_registered(&self, event_name: &str) -> Result<bool, KirkError> {
        if event_name.is_empty() {
            return Err(empty_name_error());
        }
        let events = self.inner.events.lock().await;
        Ok(events
            .get(event_name)
            .is_some_and(|entry| !entry.handlers.is_empty()))
    }

    /// Clear all registrations, including `INTERNAL_ERROR`, mirroring upstream.
    pub async fn reset(&self) {
        self.inner.events.lock().await.clear();
    }

    /// Snapshot the event's handlers and enqueue them for [`EventRegistry::start`].
    ///
    /// Unknown events, and events with no handlers, are a silent no-op.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError`] when `event_name` is empty.
    pub async fn fire(&self, event_name: &str, message: Option<String>) -> Result<(), KirkError> {
        if event_name.is_empty() {
            return Err(empty_name_error());
        }
        let snapshot = {
            let events = self.inner.events.lock().await;
            events
                .get(event_name)
                .map(|entry| (entry.handlers.clone(), entry.ordered))
        };
        let Some((handlers, ordered)) = snapshot else {
            return Ok(());
        };
        if handlers.is_empty() {
            return Ok(());
        }
        let args = EventArgs {
            event: String::from(event_name),
            message,
        };
        let _ = self.inner.tx.send(QueueItem::Dispatch {
            event: String::from(event_name),
            handlers,
            ordered,
            args,
        });
        Ok(())
    }

    /// Consume the queue until [`EventRegistry::stop`], then flush leftovers.
    pub async fn start(&self) {
        loop {
            let item = {
                let mut rx = self.inner.rx.lock().await;
                rx.recv().await
            };
            let Some(item) = item else { break };
            if !self.handle(item).await {
                break;
            }
        }
        loop {
            let item = {
                let mut rx = self.inner.rx.lock().await;
                rx.try_recv().ok()
            };
            let Some(item) = item else { break };
            if !self.handle(item).await {
                break;
            }
        }
    }

    /// Enqueue the stop sentinel; a running [`EventRegistry::start`] drains
    /// queued items behind it, then returns.
    pub fn stop(&self) {
        let _ = self.inner.tx.send(QueueItem::Stop);
    }

    /// Run one queue item; returns `false` once the stop sentinel is seen.
    async fn handle(&self, item: QueueItem) -> bool {
        match item {
            QueueItem::Stop => false,
            QueueItem::Dispatch {
                event,
                handlers,
                ordered,
                args,
            } => {
                self.execute(&event, handlers, ordered, args).await;
                true
            }
        }
    }

    /// Run one dispatch: serially for `ordered`, concurrently otherwise.
    ///
    /// Every handler runs in an owned [`JoinSet`](tokio::task::JoinSet) task so
    /// a panic surfaces as a forwardable failure instead of unwinding the loop.
    async fn execute(&self, event: &str, handlers: Vec<Handler>, ordered: bool, args: EventArgs) {
        if ordered {
            for handler in handlers {
                if let Err(error) = run_owned(handler, args.clone()).await {
                    self.report_error(event, error).await;
                }
            }
            return;
        }
        let mut set = JoinSet::new();
        for handler in handlers {
            let args = args.clone();
            set.spawn(async move { handler(args).await });
        }
        while let Some(outcome) = set.join_next().await {
            match outcome {
                Ok(Ok(())) => {}
                Ok(Err(error)) => self.report_error(event, error).await,
                Err(join) => self.report_error(event, join_failure(join)).await,
            }
        }
    }

    /// Deliver a handler failure to [`INTERNAL_ERROR`] handlers.
    ///
    /// Failures of [`INTERNAL_ERROR`] itself, and deliveries with nowhere to
    /// go (removed by [`EventRegistry::reset`]), are dropped.
    async fn report_error(&self, event: &str, error: String) {
        if event == INTERNAL_ERROR {
            return;
        }
        let handlers = {
            let events = self.inner.events.lock().await;
            events
                .get(INTERNAL_ERROR)
                .map(|entry| entry.handlers.clone())
        };
        let Some(handlers) = handlers else { return };
        if handlers.is_empty() {
            return;
        }
        let args = EventArgs {
            event: String::from(event),
            message: Some(error),
        };
        for handler in handlers {
            let _ = run_owned(handler, args.clone()).await;
        }
    }
}

impl Default for EventRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Run one handler to completion inside an owned task.
///
/// A panic becomes `Err` with the panic message; an `Err` passes through.
async fn run_owned(handler: Handler, args: EventArgs) -> HandlerResult {
    let mut set = JoinSet::new();
    set.spawn(async move { handler(args).await });
    match set.join_next().await {
        Some(Ok(result)) => result,
        Some(Err(join)) => Err(join_failure(join)),
        // Unreachable: exactly one task was just spawned.
        None => Ok(()),
    }
}
