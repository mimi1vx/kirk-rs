//! `libloading` channel registry, ported from `com.discover/get_channels/clone_channel`.
//!
//! Only `*.so`/`*.dylib`/`*.dll` files exposing
//! `extern "C" fn kirk_plugin() -> *mut dyn ComChannel` are loaded;
//! anything else is skipped without error. The plugin directory is a trust
//! boundary: a malicious dylib runs with full process privileges, so load
//! only from operator-controlled paths.
//!
//! # ABI
//!
//! Trait objects across a dylib boundary are fragile: host and plugin must
//! be built with the same rustc and the same `kirk-com` version, and the
//! exported constructor must return an owned `Box<dyn ComChannel>` via
//! `Box::into_raw` (null means "no channel", which is skipped). The
//! [`Registry`] keeps every loaded [`libloading::Library`] alive so channel
//! vtables never dangle.

use std::path::Path;

use kirk_core::KirkError;

use super::ComChannel;

/// Version of the dylib constructor ABI described above.
pub const PLUGIN_ABI_VERSION: u32 = 1;

/// Constructor a channel dylib must export as `kirk_plugin`.
///
/// Must return an owned `Box<dyn ComChannel>` leaked with
/// `Box::into_raw`, or null when it has nothing to offer. Must not unwind.
#[allow(
    improper_ctypes_definitions,
    reason = "dylib ABI is intentionally coupled: host and plugin share rustc and kirk-com so the trait-object vtable matches; documented above"
)]
pub type PluginCtor = unsafe extern "C" fn() -> *mut dyn ComChannel;

/// Owned set of discovered channels plus the libraries backing them.
#[derive(Default)]
pub struct Registry {
    channels: Vec<Box<dyn ComChannel>>,
    libs: Vec<libloading::Library>,
}

impl Registry {
    /// Build an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            channels: Vec::new(),
            libs: Vec::new(),
        }
    }

    /// Register an in-process channel (used by tests and static plugins).
    pub fn register(&mut self, channel: Box<dyn ComChannel>) {
        self.channels.push(channel);
    }

    /// Borrow the loaded channels.
    #[must_use]
    pub fn get_channels(&self) -> &[Box<dyn ComChannel>] {
        &self.channels
    }

    /// Names of the loaded channels, in registry order.
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

    /// Discover channel dylibs in `dir`, mirroring `com.discover`.
    ///
    /// When `extend` is false, previously loaded channels are cleared first.
    /// Loaded channels end up sorted by name, as upstream does.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Plugin`] when `dir` is not a directory or its
    /// entries cannot be listed. Individual files that fail to load, lack
    /// the `kirk_plugin` symbol, or return null are skipped.
    pub fn discover(&mut self, dir: &Path, extend: bool) -> Result<(), KirkError> {
        if !dir.is_dir() {
            return Err(KirkError::Plugin(String::from(
                "Discover folder doesn't exist",
            )));
        }
        if !extend {
            self.channels.clear();
            self.libs.clear();
        }
        let entries = std::fs::read_dir(dir).map_err(|err| KirkError::Plugin(err.to_string()))?;
        for entry in entries {
            let Ok(entry) = entry else { continue };
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let is_dylib = path
                .extension()
                .and_then(|ext| ext.to_str())
                .is_some_and(|ext| ext == "so" || ext == "dylib" || ext == "dll");
            if !is_dylib {
                continue;
            }
            // SAFETY: dlopening a third-party dylib executes its
            // initializers with full process privileges. The directory is a
            // trust boundary (see module docs); only operator-controlled
            // paths may be passed.
            let lib = unsafe {
                match libloading::Library::new(&path) {
                    Ok(lib) => lib,
                    Err(_) => continue,
                }
            };
            // SAFETY: the symbol, when present, must have the `PluginCtor`
            // signature and must not unwind; anything else is skipped via
            // the null check below or rejected by the loader.
            let ctor: libloading::Symbol<'_, PluginCtor> = unsafe {
                match lib.get(b"kirk_plugin") {
                    Ok(ctor) => ctor,
                    Err(_) => continue,
                }
            };
            // SAFETY: `ctor` promises an owned `Box<dyn ComChannel>`
            // leaked with `Box::into_raw`, built with the same rustc and
            // `kirk-com` version so the vtable layout matches. Null means
            // "no channel" and is skipped; the `Library` is retained below
            // so the vtable never dangles.
            let raw = unsafe { ctor() };
            if raw.is_null() {
                continue;
            }
            let channel: Box<dyn ComChannel> = unsafe { Box::from_raw(raw) };
            self.channels.push(channel);
            self.libs.push(lib);
        }
        self.channels
            .sort_by(|left, right| left.name().cmp(right.name()));
        Ok(())
    }
}
