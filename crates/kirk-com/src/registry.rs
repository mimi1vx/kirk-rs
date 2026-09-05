//! In-process channel registry, ported from `com.get_channels/clone_channel`.
//!
//! Channels are statically linked into the `kirk` binary: dynamic channel
//! plugins were tried and removed, because every channel needs the tokio
//! reactor and a dylib statically links its own tokio copy whose
//! runtime-context thread-locals cannot see the host runtime (`no reactor
//! running`, then abort). A shared-tokio dylib shim would be needed to make
//! dynamic loading sound, which is not worth the build complexity.

use kirk_core::KirkError;

use super::ComChannel;

/// Owned set of channels, mirroring upstream's channel list.
#[derive(Default)]
pub struct Registry {
    channels: Vec<Box<dyn ComChannel>>,
}

impl Registry {
    /// Build an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            channels: Vec::new(),
        }
    }

    /// Register a channel.
    pub fn register(&mut self, channel: Box<dyn ComChannel>) {
        self.channels.push(channel);
    }

    /// Borrow the registered channels.
    #[must_use]
    pub fn get_channels(&self) -> &[Box<dyn ComChannel>] {
        &self.channels
    }

    /// Names of the registered channels, in registry order.
    #[must_use]
    pub fn names(&self) -> Vec<&str> {
        self.channels.iter().map(|channel| channel.name()).collect()
    }

    /// Clone channel `name` under `new_name` and register the copy.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Plugin`] when no channel is named `name`.
    pub fn clone_channel(&mut self, name: &str, new_name: &str) -> Result<(), KirkError> {
        let channel = self
            .channels
            .iter()
            .find(|channel| channel.name() == name)
            .ok_or_else(|| KirkError::Plugin(format!("Can't find plugin '{name}'")))?;
        self.channels.push(channel.clone_channel_box(new_name));
        Ok(())
    }
}
