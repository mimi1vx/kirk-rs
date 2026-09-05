//! Generic SUT ported from `kirk/libkirk/sut_base.py`.
//!
//! [`GenericSut`] wires the [`Sut`] defaults to one channel looked up in a
//! [`Registry`](kirk_com::Registry) via [`GenericSut::setup_with_registry`],
//! replacing the upstream global channel list. Lifecycle delegates to
//! [`ComChannel::ensure_communicate`](kirk_com::ComChannel::ensure_communicate),
//! [`ComChannel::stop`](kirk_com::ComChannel::stop), and
//! [`ComChannel::active`](kirk_com::ComChannel::active).

use std::collections::HashMap;

use async_trait::async_trait;
use kirk_com::{ComChannel, Registry};
use kirk_core::KirkError;
use kirk_plugin::Plugin;
use tokio::sync::Mutex;

use super::sut::{Sut, TaintBegin, TaintedInfo};

/// Lazily reported taint state; only touched in short sections, never held
/// across an `.await` (see [`Sut::get_tainted_info`]).
#[derive(Debug, Default)]
struct TaintState {
    /// Whether a taint probe is currently running.
    in_flight: bool,
    /// Last successful probe result, served to concurrent callers.
    cached: Option<TaintedInfo>,
}

/// Generic SUT named `default`, communicating over one channel.
pub struct GenericSut {
    name: String,
    com_name: String,
    channel: Option<Box<dyn ComChannel>>,
    optimize: bool,
    taint: Mutex<TaintState>,
}

impl GenericSut {
    /// Build an uninitialized SUT; call [`GenericSut::setup_with_registry`]
    /// before use.
    #[must_use]
    pub fn new() -> Self {
        Self {
            name: String::from("default"),
            com_name: String::from("shell"),
            channel: None,
            optimize: false,
            taint: Mutex::new(TaintState::default()),
        }
    }

    /// Full setup mirroring `GenericSUT.setup(com="shell")`: validate the
    /// requested channel name, then attach a copy of the matching registered
    /// channel.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Sut`] when `cfg["com"]` is empty, when the
    /// registry holds no channels, or when no channel matches.
    pub fn setup_with_registry(
        &mut self,
        cfg: &HashMap<String, String>,
        registry: &Registry,
    ) -> Result<(), KirkError> {
        self.com_name = com_name_from_cfg(cfg)?;
        let channels = registry.get_channels();
        if channels.is_empty() {
            return Err(KirkError::Sut(String::from(
                "No communication channels are provided",
            )));
        }
        let found = channels
            .iter()
            .find(|channel| channel.name() == self.com_name)
            .ok_or_else(|| {
                KirkError::Sut(format!(
                    "Can't find communication channel '{}'",
                    self.com_name
                ))
            })?;
        self.channel = Some(found.clone_channel_box(found.name()));
        Ok(())
    }
}

impl Default for GenericSut {
    fn default() -> Self {
        Self::new()
    }
}

/// Read the requested channel name, defaulting to `"shell"` like upstream.
fn com_name_from_cfg(cfg: &HashMap<String, String>) -> Result<String, KirkError> {
    let name = cfg.get("com").map_or("shell", String::as_str);
    if name.is_empty() {
        return Err(KirkError::Sut(String::from(
            "Communication channel has not been defined",
        )));
    }
    Ok(name.to_owned())
}

#[async_trait]
impl Plugin for GenericSut {
    fn name(&self) -> &str {
        &self.name
    }

    fn config_help(&self) -> HashMap<String, String> {
        HashMap::from([(
            String::from("com"),
            String::from("Communication channel name (default: shell)"),
        )])
    }

    /// Validate `cfg["com"]` and remember it; use
    /// [`GenericSut::setup_with_registry`] to also attach the channel, since
    /// the [`Plugin`] interface has no registry access.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Sut`] when `cfg["com"]` is empty.
    fn setup(&mut self, cfg: &HashMap<String, String>) -> Result<(), KirkError> {
        self.com_name = com_name_from_cfg(cfg)?;
        Ok(())
    }

    /// Copy as a fresh uninitialized instance under `name`, mirroring
    /// upstream `Plugin.clone` (which re-runs the constructor).
    fn clone_box(&self, name: &str) -> Box<dyn Plugin> {
        let mut copy = Self::new();
        name.clone_into(&mut copy.name);
        Box::new(copy)
    }
}

#[async_trait]
impl Sut for GenericSut {
    #[allow(clippy::borrowed_box, reason = "matches the Sut interface")]
    fn channel(&self) -> Result<&Box<dyn ComChannel>, KirkError> {
        self.channel
            .as_ref()
            .ok_or_else(|| KirkError::Sut(String::from("SUT is not initialized")))
    }

    #[allow(clippy::borrowed_box, reason = "matches the Sut interface")]
    fn channel_mut(&mut self) -> Result<&mut Box<dyn ComChannel>, KirkError> {
        self.channel
            .as_mut()
            .ok_or_else(|| KirkError::Sut(String::from("SUT is not initialized")))
    }

    fn optimize(&self) -> bool {
        self.optimize
    }

    fn set_optimize(&mut self, optimize: bool) {
        self.optimize = optimize;
    }

    async fn taint_begin(&mut self) -> TaintBegin {
        let mut state = self.taint.lock().await;
        if state.in_flight {
            if let Some(cached) = state.cached.clone() {
                return TaintBegin::Cached(cached);
            }
            // A probe runs but cached nothing yet; probe alongside instead
            // of waiting, so the lock is never held across an await.
            return TaintBegin::Probe;
        }
        state.in_flight = true;
        TaintBegin::Probe
    }

    async fn taint_end(&mut self, result: Option<TaintedInfo>) {
        let mut state = self.taint.lock().await;
        state.in_flight = false;
        if let Some(info) = result {
            state.cached = Some(info);
        }
    }
}
