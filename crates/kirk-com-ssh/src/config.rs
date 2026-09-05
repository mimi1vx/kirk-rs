//! SSH channel configuration and remote-command helpers.
//!
//! [`SshConfig`] mirrors the options accepted by upstream
//! `SSHComChannel.setup` (`host`, `port`, `user`, `password`,
//! `key_file`, `reset_cmd`, `sudo`, `known_hosts`). Passwords are held
//! in [`Zeroizing`] and the [`Debug`](std::fmt::Debug) impl redacts
//! them, so secrets never reach logs.
//!
//! Command building hardens upstream `_create_command`: `cwd` and env
//! values are POSIX single-quoted and env names are validated, instead
//! of being interpolated raw.

use std::collections::HashMap;
use std::time::Duration;

use kirk_core::KirkError;
use zeroize::Zeroizing;

/// Default SSH host, mirroring upstream.
pub const DEFAULT_HOST: &str = "localhost";
/// Default SSH user, mirroring upstream.
pub const DEFAULT_USER: &str = "root";
/// Default SSH port, mirroring upstream.
pub const DEFAULT_PORT: u16 = 22;
/// Default known-hosts path, mirroring upstream.
pub const DEFAULT_KNOWN_HOSTS: &str = "~/.ssh/known_hosts";
/// Fallback session count when the `MaxSessions` probe fails, mirroring upstream.
pub const DEFAULT_MAX_SESSIONS: usize = 10;
/// Cap on bytes collected by `fetch_file`; upstream has none, this bounds memory.
pub const FETCH_SIZE_CAP: usize = 16 * 1024 * 1024;
/// Timeout for TCP dial, authentication, channel open/exec and disconnect.
pub const IO_TIMEOUT: Duration = Duration::from_secs(10);
/// Timeout for the `MaxSessions` probe and for `ping`.
pub const PROBE_TIMEOUT: Duration = Duration::from_secs(5);
/// Timeout for the local `reset_cmd` spawned by `stop`.
pub const RESET_TIMEOUT: Duration = Duration::from_secs(10);
/// Remote output marker that raises [`KirkError::KernelPanic`], mirroring upstream.
pub const PANIC_MARKER: &str = "Kernel panic";

/// Validated SSH channel configuration.
#[derive(Clone)]
pub struct SshConfig {
    /// Remote host.
    pub host: String,
    /// Login user.
    pub user: String,
    /// Private key path, when key authentication is used.
    pub key_file: Option<String>,
    /// Password for password auth (or key decryption). Zeroized on drop.
    pub password: Option<Zeroizing<String>>,
    /// TCP port, always `1..=65535`.
    pub port: u16,
    /// Local command run by `stop` to reset the target.
    pub reset_cmd: Option<String>,
    /// Whether remote commands run under `sudo /bin/sh -c`.
    pub sudo: bool,
    /// Expanded known-hosts path. Host-key verification is always on.
    pub known_hosts: String,
}

impl std::fmt::Debug for SshConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SshConfig")
            .field("host", &self.host)
            .field("user", &self.user)
            .field("key_file", &self.key_file)
            .field("password", &"<redacted>")
            .field("port", &self.port)
            .field("reset_cmd", &self.reset_cmd)
            .field("sudo", &self.sudo)
            .field("known_hosts", &self.known_hosts)
            .finish()
    }
}

fn nonempty(cfg: &HashMap<String, String>, key: &str) -> Option<String> {
    cfg.get(key).and_then(|value| {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_owned())
    })
}

fn parse_port(raw: Option<&str>) -> Result<u16, KirkError> {
    let Some(raw) = raw else {
        return Ok(DEFAULT_PORT);
    };
    match raw.trim().parse::<i64>() {
        Ok(port) if (1..=65535).contains(&port) => u16::try_from(port).map_err(|_| {
            KirkError::Communication(String::from("'port' must be an integer between 1-65535"))
        }),
        _ => Err(KirkError::Communication(String::from(
            "'port' must be an integer between 1-65535",
        ))),
    }
}

fn parse_sudo(raw: Option<&str>) -> Result<bool, KirkError> {
    let Some(raw) = raw else {
        return Ok(false);
    };
    // Upstream coerces any non-1 int to false; reject anything but 0/1 instead.
    match raw.trim().parse::<i64>() {
        Ok(0) => Ok(false),
        Ok(1) => Ok(true),
        _ => Err(KirkError::Communication(String::from(
            "'sudo' must be 0 or 1",
        ))),
    }
}

/// Expand a known-hosts path, rejecting `/dev/null`.
///
/// Upstream maps `/dev/null` to disabled host-key verification; that is
/// refused here so verification stays enforced.
///
/// # Errors
///
/// Returns [`KirkError::Communication`] when the path is `/dev/null` or
/// `~` cannot be expanded.
pub fn expand_known_hosts(raw: &str, home: Option<&str>) -> Result<String, KirkError> {
    if raw == "/dev/null" {
        return Err(KirkError::Communication(String::from(
            "known_hosts '/dev/null' disables host-key verification; refusing",
        )));
    }
    if let Some(rest) = raw.strip_prefix('~') {
        let Some(home) = home else {
            return Err(KirkError::Communication(String::from(
                "cannot expand '~' in known_hosts: $HOME is not set",
            )));
        };
        if rest.is_empty() || rest.starts_with('/') {
            return Ok(format!("{home}{rest}"));
        }
        return Err(KirkError::Communication(String::from(
            "cannot expand '~user' in known_hosts",
        )));
    }
    Ok(raw.to_owned())
}

impl SshConfig {
    /// Build validated config from the plugin setup map.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Communication`] when `port` is not an integer
    /// in `1..=65535`, `sudo` is not `0`/`1`, or `known_hosts` disables
    /// host-key verification or cannot be expanded.
    #[allow(
        clippy::implicit_hasher,
        reason = "mirrors Plugin::setup, which passes HashMap<String, String>"
    )]
    pub fn from_map(cfg: &HashMap<String, String>) -> Result<Self, KirkError> {
        let raw_port = nonempty(cfg, "port");
        let raw_sudo = nonempty(cfg, "sudo");
        let raw_known = nonempty(cfg, "known_hosts");
        let home = std::env::var("HOME").ok();
        Ok(Self {
            host: nonempty(cfg, "host").unwrap_or_else(|| DEFAULT_HOST.to_owned()),
            user: nonempty(cfg, "user").unwrap_or_else(|| DEFAULT_USER.to_owned()),
            key_file: nonempty(cfg, "key_file"),
            password: nonempty(cfg, "password").map(Zeroizing::new),
            port: parse_port(raw_port.as_deref())?,
            reset_cmd: nonempty(cfg, "reset_cmd"),
            sudo: parse_sudo(raw_sudo.as_deref())?,
            known_hosts: expand_known_hosts(
                raw_known.as_deref().unwrap_or(DEFAULT_KNOWN_HOSTS),
                home.as_deref(),
            )?,
        })
    }
}

/// POSIX single-quote a value for remote-shell interpolation.
#[must_use]
pub fn quote_sh(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

/// Whether `name` is a valid shell variable name; values are always quoted.
fn valid_env_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .enumerate()
            .all(|(i, b)| b == b'_' || b.is_ascii_alphabetic() || (i > 0 && b.is_ascii_digit()))
}

/// Build the remote command string, mirroring upstream `_create_command`
/// but quoting `cwd`/env values and validating env names.
///
/// # Errors
///
/// Returns [`KirkError::Communication`] when an env name is not a valid
/// shell variable name. Only the name is reported, never the value.
#[allow(
    clippy::implicit_hasher,
    reason = "mirrors ComChannel::run_command, which passes HashMap<String, String>"
)]
pub fn build_remote_command(
    cmd: &str,
    cwd: Option<&str>,
    env: Option<&HashMap<String, String>>,
    sudo: bool,
) -> Result<String, KirkError> {
    let mut script = String::new();
    if let Some(cwd) = cwd.filter(|cwd| !cwd.is_empty()) {
        script.push_str("cd ");
        script.push_str(&quote_sh(cwd));
        script.push_str(" && ");
    }
    if let Some(env) = env {
        let mut names: Vec<&String> = env.keys().collect();
        names.sort();
        for name in names {
            if !valid_env_name(name) {
                return Err(KirkError::Communication(format!(
                    "invalid environment variable name: {name}"
                )));
            }
            script.push_str("export ");
            script.push_str(name);
            script.push('=');
            script.push_str(&quote_sh(&env[name]));
            script.push_str(" && ");
        }
    }
    script.push_str(cmd);
    if sudo {
        return Ok(format!("sudo /bin/sh -c {}", quote_sh(&script)));
    }
    Ok(script)
}

/// Parse the server `MaxSessions` from probe output, accepting both the
/// `sshd_config` form (`MaxSessions N`) and the `sshd -T` form
/// (`maxsessions N`). Returns `None` when no line matches.
#[must_use]
pub fn parse_max_sessions(output: &str) -> Option<usize> {
    for line in output.lines() {
        let line = line.trim();
        let rest = line
            .strip_prefix("MaxSessions ")
            .or_else(|| line.strip_prefix("maxsessions "));
        if let Some(rest) = rest
            && let Some(first) = rest.split_whitespace().next()
            && let Ok(value) = first.parse::<usize>()
        {
            return Some(value);
        }
    }
    None
}

/// Merged stdout/stderr accumulator with [`PANIC_MARKER`] detection.
///
/// Chunks are scanned with overlap against previously buffered output so
/// a marker split across two chunks is still found.
#[derive(Debug, Default)]
pub struct OutputCollector {
    buf: String,
    panic: bool,
}

impl OutputCollector {
    /// Create an empty collector.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Append a chunk, updating panic detection.
    pub fn push(&mut self, chunk: &str) {
        if self.panic {
            self.buf.push_str(chunk);
            return;
        }
        // Scan the new chunk plus enough preceding tail to catch a
        // marker straddling the boundary. The marker is ASCII, so it
        // cannot start inside a multibyte sequence.
        let from = self.buf.len().saturating_sub(PANIC_MARKER.len());
        let mut from = from.min(self.buf.len());
        while !self.buf.is_char_boundary(from) {
            from += 1;
        }
        self.buf.push_str(chunk);
        if self.buf[from..].contains(PANIC_MARKER) {
            self.panic = true;
        }
    }

    /// Whether [`PANIC_MARKER`] has been seen.
    #[must_use]
    pub fn panicked(&self) -> bool {
        self.panic
    }

    /// Consume into `(output, panicked)`.
    #[must_use]
    pub fn finish(self) -> (String, bool) {
        (self.buf, self.panic)
    }
}

/// Split a local command into argv (minimal POSIX quoting).
///
/// # Errors
///
/// Returns [`KirkError::Communication`] on unterminated quotes.
pub fn split_argv(cmd: &str) -> Result<Vec<String>, KirkError> {
    let mut argv = Vec::new();
    let mut current = String::new();
    let mut in_arg = false;
    let mut quote: Option<char> = None;
    let mut chars = cmd.chars();
    while let Some(c) = chars.next() {
        if let Some(q) = quote {
            if c == q {
                quote = None;
            } else if q == '"' && c == '\\' {
                if let Some(next) = chars.next() {
                    current.push(next);
                }
            } else {
                current.push(c);
            }
        } else if c == '\'' || c == '"' {
            quote = Some(c);
            in_arg = true;
        } else if c == '\\' {
            if let Some(next) = chars.next() {
                current.push(next);
                in_arg = true;
            }
        } else if c.is_whitespace() {
            if in_arg {
                argv.push(std::mem::take(&mut current));
                in_arg = false;
            }
        } else {
            current.push(c);
            in_arg = true;
        }
    }
    if quote.is_some() {
        return Err(KirkError::Communication(String::from(
            "reset command has an unterminated quote",
        )));
    }
    if in_arg {
        argv.push(current);
    }
    Ok(argv)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg_with(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    #[test]
    fn defaults() {
        let cfg = SshConfig::from_map(&cfg_with(&[])).unwrap();
        assert_eq!(cfg.host, "localhost");
        assert_eq!(cfg.user, "root");
        assert_eq!(cfg.port, 22);
        assert!(!cfg.sudo);
        assert!(cfg.key_file.is_none());
        assert!(cfg.password.is_none());
        assert!(cfg.reset_cmd.is_none());
    }

    #[test]
    fn bad_ports_rejected() {
        for port in ["0", "65536", "-1", "abc", "22.5", "99999999999999999999"] {
            let err = SshConfig::from_map(&cfg_with(&[("port", port)])).unwrap_err();
            assert!(
                matches!(err, KirkError::Communication(_)),
                "port {port}: {err:?}"
            );
        }
        assert_eq!(
            SshConfig::from_map(&cfg_with(&[("port", "2222")]))
                .unwrap()
                .port,
            2222
        );
        assert_eq!(
            SshConfig::from_map(&cfg_with(&[("port", " 22 ")]))
                .unwrap()
                .port,
            22
        );
    }

    #[test]
    fn bad_sudo_rejected() {
        for sudo in ["2", "-1", "yes", "true", ""] {
            if sudo.is_empty() {
                // Empty means unset: defaults to false.
                assert!(
                    !SshConfig::from_map(&cfg_with(&[("sudo", sudo)]))
                        .unwrap()
                        .sudo
                );
                continue;
            }
            let err = SshConfig::from_map(&cfg_with(&[("sudo", sudo)])).unwrap_err();
            assert!(
                matches!(err, KirkError::Communication(_)),
                "sudo {sudo}: {err:?}"
            );
        }
        assert!(
            SshConfig::from_map(&cfg_with(&[("sudo", "1")]))
                .unwrap()
                .sudo
        );
        assert!(
            !SshConfig::from_map(&cfg_with(&[("sudo", "0")]))
                .unwrap()
                .sudo
        );
    }

    #[test]
    fn dev_null_known_hosts_rejected() {
        let err = SshConfig::from_map(&cfg_with(&[("known_hosts", "/dev/null")])).unwrap_err();
        assert!(matches!(err, KirkError::Communication(_)));
    }

    #[test]
    fn known_hosts_expansion() {
        assert_eq!(
            expand_known_hosts("/etc/ssh/known_hosts", None).unwrap(),
            "/etc/ssh/known_hosts"
        );
        assert_eq!(
            expand_known_hosts("~/.ssh/known_hosts", Some("/home/u")).unwrap(),
            "/home/u/.ssh/known_hosts"
        );
        assert!(expand_known_hosts("~/.ssh/known_hosts", None).is_err());
        assert!(expand_known_hosts("~other/x", Some("/home/u")).is_err());
        assert!(expand_known_hosts("/dev/null", Some("/home/u")).is_err());
    }

    #[test]
    fn debug_redacts_password() {
        let cfg = SshConfig::from_map(&cfg_with(&[("password", "s3cr3t")])).unwrap();
        let rendered = format!("{cfg:?}");
        assert!(!rendered.contains("s3cr3t"));
        assert!(rendered.contains("<redacted>"));
    }

    #[test]
    fn quoting() {
        assert_eq!(quote_sh("plain"), "'plain'");
        assert_eq!(quote_sh("a b"), "'a b'");
        assert_eq!(quote_sh("o'clock"), "'o'\\''clock'");
        assert_eq!(quote_sh("x'; rm -rf / #'"), "'x'\\''; rm -rf / #'\\'''");
    }

    #[test]
    fn remote_command_shapes() {
        assert_eq!(build_remote_command("ls", None, None, false).unwrap(), "ls");
        assert_eq!(
            build_remote_command("ls", Some("/tmp/a b"), None, false).unwrap(),
            "cd '/tmp/a b' && ls"
        );
        let env = cfg_with(&[("A", "1"), ("B", "x y")]);
        assert_eq!(
            build_remote_command("ls", None, Some(&env), false).unwrap(),
            "export A='1' && export B='x y' && ls"
        );
        assert_eq!(
            build_remote_command("ls", None, None, true).unwrap(),
            "sudo /bin/sh -c 'ls'"
        );
        let evil = cfg_with(&[("E", "x'; touch pwned; echo '")]);
        let built = build_remote_command("id", None, Some(&evil), false).unwrap();
        assert_eq!(built, "export E='x'\\''; touch pwned; echo '\\''' && id");
        let bad = cfg_with(&[("not valid", "1")]);
        assert!(build_remote_command("id", None, Some(&bad), false).is_err());
        let semi = cfg_with(&[("A;B", "1")]);
        assert!(build_remote_command("id", None, Some(&semi), false).is_err());
    }

    #[test]
    fn max_sessions_parsing() {
        assert_eq!(parse_max_sessions("MaxSessions 4\n"), Some(4));
        assert_eq!(parse_max_sessions("maxsessions 20\n"), Some(20));
        assert_eq!(parse_max_sessions("nothing here\n"), None);
        assert_eq!(parse_max_sessions("MaxSessions nope\n"), None);
    }

    #[test]
    fn collector_merges_and_detects_panic() {
        let mut out = OutputCollector::new();
        out.push("hello ");
        out.push("world");
        assert!(!out.panicked());
        out.push("oops Kernel panic");
        assert!(out.panicked());
        let (text, panicked) = out.finish();
        assert_eq!(text, "hello worldoops Kernel panic");
        assert!(panicked);
    }

    #[test]
    fn collector_detects_split_marker() {
        let mut out = OutputCollector::new();
        out.push("boot log Kernel pa");
        assert!(!out.panicked());
        out.push("nic - not syncing");
        assert!(out.panicked());
    }

    #[test]
    fn argv_splitting() {
        assert_eq!(
            split_argv("echo hello world").unwrap(),
            ["echo", "hello", "world"]
        );
        assert_eq!(
            split_argv("echo 'a b' \"c d\"").unwrap(),
            ["echo", "a b", "c d"]
        );
        assert!(split_argv("echo 'oops").is_err());
        assert_eq!(split_argv("").unwrap(), [] as [String; 0]);
    }
}
