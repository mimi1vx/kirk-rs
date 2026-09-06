//! Validated `QEMU` channel configuration, ported from
//! `QemuComChannel.setup`, `_get_command` and `_get_transport`.
//!
//! The guest is always spawned as an `argv` vector via
//! [`tokio::process::Command`]; this module only builds the vector, it never
//! joins it into a shell string.

use std::collections::HashMap;
use std::path::Path;

use kirk_core::KirkError;

/// Serial transport type (the `serial` option).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SerialType {
    /// ISA serial transport (`/dev/ttyS1`).
    #[default]
    Isa,
    /// `VirtIO` serial transport (`/dev/vport1p1`).
    Virtio,
}

impl SerialType {
    /// Parse the `serial` option value.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Communication`] for anything but `isa`/`virtio`.
    pub fn parse(value: &str) -> Result<Self, KirkError> {
        match value {
            "isa" => Ok(Self::Isa),
            "virtio" => Ok(Self::Virtio),
            other => Err(KirkError::Communication(format!(
                "Serial protocol must be isa or virtio, got '{other}'"
            ))),
        }
    }

    /// Option spelling used in logs and on the `QEMU` command line.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Isa => "isa",
            Self::Virtio => "virtio",
        }
    }

    /// Guest device node used for file transport.
    #[must_use]
    pub fn transport_dev(self) -> &'static str {
        match self {
            Self::Isa => "/dev/ttyS1",
            Self::Virtio => "/dev/vport1p1",
        }
    }

    /// Guest console name for `-append console=`.
    #[must_use]
    pub fn console(self) -> &'static str {
        match self {
            Self::Isa => "ttyS0",
            Self::Virtio => "hvc0",
        }
    }
}

/// Validated `QEMU` channel configuration (mirrors `setup` kwargs).
#[derive(Debug, Clone)]
#[allow(
    clippy::module_name_repetitions,
    reason = "plan mandates config.rs holding QemuConfig; the name keeps upstream parity"
)]
pub struct QemuConfig {
    /// Host directory for `ttyS0` logs and transport files.
    pub tmpdir: String,
    /// Guest disk image.
    pub image: Option<String>,
    /// Guest kernel image.
    pub kernel: Option<String>,
    /// Guest initrd image.
    pub initrd: Option<String>,
    /// Login user (no login step when `None`).
    pub user: Option<String>,
    /// Login password.
    pub password: Option<String>,
    /// Shell prompt to expect after login.
    pub prompt: String,
    /// Guest architecture (`qemu-system-<system>`).
    pub system: String,
    /// Guest RAM (e.g. `2G`).
    pub ram: String,
    /// Guest CPU count.
    pub smp: String,
    /// Serial transport type.
    pub serial: SerialType,
    /// Host directory shared via `virtfs`.
    pub virtfs: Option<String>,
    /// Extra user-defined `QEMU` options (shell-word split, never shelled out).
    pub options: Option<String>,
}

impl QemuConfig {
    /// Build from the string map passed to [`kirk_plugin::Plugin::setup`].
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Communication`] when directories/files are
    /// missing, `ram`/`smp` are empty, or `serial` is not `isa`/`virtio`.
    pub fn from_map(cfg: &HashMap<String, String>) -> Result<Self, KirkError> {
        let get = |key: &str| cfg.get(key).cloned();
        let non_empty = |key: &str| get(key).filter(|value| !value.is_empty());
        let serial = SerialType::parse(get("serial").as_deref().unwrap_or("isa"))?;
        let config = Self {
            tmpdir: get("tmpdir").unwrap_or_default(),
            image: non_empty("image"),
            kernel: non_empty("kernel"),
            initrd: non_empty("initrd"),
            user: non_empty("user"),
            password: non_empty("password"),
            prompt: get("prompt").unwrap_or_else(|| "#".to_string()),
            system: get("system").unwrap_or_else(|| "x86_64".to_string()),
            ram: get("ram").unwrap_or_else(|| "2G".to_string()),
            smp: get("smp").unwrap_or_else(|| "2".to_string()),
            serial,
            virtfs: non_empty("virtfs"),
            options: non_empty("options"),
        };
        config.validate()
    }

    fn validate(self) -> Result<Self, KirkError> {
        if self.tmpdir.is_empty() || !Path::new(&self.tmpdir).is_dir() {
            return Err(KirkError::Communication(format!(
                "Temporary directory doesn't exist: {}",
                self.tmpdir
            )));
        }
        for (label, path) in [
            ("Image", &self.image),
            ("Kernel", &self.kernel),
            ("initrd", &self.initrd),
        ] {
            if let Some(path) = path
                && !Path::new(path).is_file()
            {
                return Err(KirkError::Communication(format!(
                    "{label} location doesn't exist: {path}"
                )));
            }
        }
        if self.ram.is_empty() {
            return Err(KirkError::Communication("RAM is not defined".to_string()));
        }
        if self.smp.is_empty() {
            return Err(KirkError::Communication("CPU is not defined".to_string()));
        }
        if let Some(virtfs) = &self.virtfs
            && !Path::new(virtfs).is_dir()
        {
            return Err(KirkError::Communication(format!(
                "Virtual FS directory doesn't exist: {virtfs}"
            )));
        }
        Ok(self)
    }

    /// `qemu-system-<arch>` binary name.
    #[must_use]
    pub fn qemu_cmd(&self) -> String {
        format!("qemu-system-{}", self.system)
    }

    /// `(transport_dev, transport_file)` for the given host pid.
    ///
    /// The file side is per-process so concurrent sessions never share it.
    #[must_use]
    pub fn transport(&self, pid: u32) -> (&'static str, String) {
        (
            self.serial.transport_dev(),
            format!("{}/transport-{pid}", self.tmpdir),
        )
    }

    /// `(program, argv)` for spawning `QEMU` directly (never via a shell).
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Communication`] when `options` contains an
    /// unterminated quote.
    pub fn build_argv(&self, pid: u32) -> Result<(String, Vec<String>), KirkError> {
        let tty_log = format!("{}/ttyS0-{pid}.log", self.tmpdir);
        let (_, transport_file) = self.transport(pid);
        let mut args = vec![
            "-enable-kvm".to_string(),
            "-display".to_string(),
            "none".to_string(),
            "-m".to_string(),
            self.ram.clone(),
            "-smp".to_string(),
            self.smp.clone(),
            "-device".to_string(),
            "virtio-rng-pci".to_string(),
            "-chardev".to_string(),
            format!("stdio,id=tty,logfile={tty_log}"),
        ];
        match self.serial {
            SerialType::Isa => {
                args.push("-serial".to_string());
                args.push("chardev:tty".to_string());
                args.push("-serial".to_string());
                args.push("chardev:transport".to_string());
            }
            SerialType::Virtio => {
                args.push("-device".to_string());
                args.push("virtio-serial".to_string());
                args.push("-device".to_string());
                args.push("virtconsole,chardev=tty".to_string());
                args.push("-device".to_string());
                args.push("virtserialport,chardev=transport".to_string());
            }
        }
        args.push("-chardev".to_string());
        args.push(format!("file,id=transport,path={transport_file}"));
        if let Some(virtfs) = &self.virtfs {
            args.push("-virtfs".to_string());
            args.push(format!(
                "local,path={virtfs},mount_tag=host0,security_model=mapped-xattr,readonly=on"
            ));
        }
        if let Some(image) = &self.image {
            args.push("-drive".to_string());
            args.push(format!("if=virtio,cache=unsafe,file={image}"));
        }
        if let Some(initrd) = &self.initrd {
            args.push("-initrd".to_string());
            args.push(initrd.clone());
        }
        if let Some(kernel) = &self.kernel {
            args.push("-append".to_string());
            args.push(format!("console={} ignore_loglevel", self.serial.console()));
            args.push("-kernel".to_string());
            args.push(kernel.clone());
        }
        if let Some(options) = &self.options {
            args.extend(crate::expect::split_options(options)?);
        }
        Ok((self.qemu_cmd(), args))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_map(tmpdir: &str) -> HashMap<String, String> {
        HashMap::from([("tmpdir".to_string(), tmpdir.to_string())])
    }

    #[test]
    fn defaults_match_upstream() {
        let dir = std::env::temp_dir();
        let cfg = QemuConfig::from_map(&valid_map(dir.to_str().expect("temp dir is UTF-8")))
            .expect("temp dir exists");
        assert_eq!(cfg.prompt, "#");
        assert_eq!(cfg.system, "x86_64");
        assert_eq!(cfg.ram, "2G");
        assert_eq!(cfg.smp, "2");
        assert_eq!(cfg.serial, SerialType::Isa);
        assert_eq!(cfg.qemu_cmd(), "qemu-system-x86_64");
    }

    #[test]
    fn bad_tmpdir_is_communication_error() {
        let err = QemuConfig::from_map(&valid_map("/no-such-kirk-dir-xyz"))
            .expect_err("missing tmpdir must fail");
        assert!(matches!(err, KirkError::Communication(_)));
    }

    #[test]
    fn bad_serial_is_communication_error() {
        let dir = std::env::temp_dir();
        let mut map = valid_map(dir.to_str().expect("temp dir is UTF-8"));
        map.insert("serial".to_string(), "usb".to_string());
        let err = QemuConfig::from_map(&map).expect_err("bad serial must fail");
        assert!(matches!(err, KirkError::Communication(_)));
    }

    #[test]
    fn transport_dev_per_serial() {
        let dir = std::env::temp_dir().to_str().expect("UTF-8").to_string();
        let base = QemuConfig::from_map(&valid_map(&dir)).expect("valid");
        assert_eq!(base.transport(7).0, "/dev/ttyS1");
        let virtio = QemuConfig {
            serial: SerialType::Virtio,
            ..base
        };
        assert_eq!(virtio.transport(7).0, "/dev/vport1p1");
        assert!(virtio.transport(7).1.ends_with("/transport-7"));
    }
}
