//! Generic plugin definition ported from `kirk/libkirk/plugin.py`.
//!
//! [`Plugin`] mirrors the Python base class: a unique [`Plugin::name`],
//! a [`Plugin::config_help`] map for `--help` text, fallible
//! [`Plugin::setup`], and [`Plugin::clone_box`] which copies the plugin
//! under a new name (upstream `Plugin.clone`).

use std::collections::HashMap;

use kirk_core::KirkError;

/// Generic plugin. Object-safe so registries can hold `Box<dyn Plugin>`.
pub trait Plugin: Send + Sync {
    /// Unique name identifier of the plugin.
    fn name(&self) -> &str;

    /// Map each configuration option to its help message.
    fn config_help(&self) -> HashMap<String, String>;

    /// Initialize the plugin from a configuration map.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError`] when the configuration is invalid.
    fn setup(&mut self, cfg: &HashMap<String, String>) -> Result<(), KirkError>;

    /// Copy the plugin and return a new instance with the given name.
    ///
    /// The caller must ensure the name is unique, as upstream does.
    fn clone_box(&self, name: &str) -> Box<dyn Plugin>;
}
