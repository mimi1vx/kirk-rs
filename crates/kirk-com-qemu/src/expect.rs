//! Serial expect-loop, retcode parsing and shell quoting, ported from
//! `QemuComChannel._wait_for`, `_exec`, `run_command` and `fetch_file`.
//!
//! [`wait_for_message`] is the single expect-loop used by the channel; its
//! [`SerialIo`] input keeps it unit-testable with an in-process fake serial
//! (no `KVM` needed).

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use kirk_com::IOBuffer;
use kirk_core::KirkError;

/// Hard cap for one serial capture, so a chatty guest cannot `OOM` the host.
pub const MAX_STDOUT_BYTES: usize = 8 * 1024 * 1024;
/// Hard cap for a fetched transport file.
pub const MAX_TRANSPORT_BYTES: u64 = 64 * 1024 * 1024;
/// Settle delay after `Kernel panic` so the full message arrives on serial.
pub const PANIC_SETTLE: Duration = Duration::from_secs(2);
/// Back-off when the serial has no data ready (avoids a hot spin).
const IDLE_BACKOFF: Duration = Duration::from_millis(5);

/// Async source of serial text, chunk by chunk.
#[async_trait::async_trait]
pub trait SerialIo: Send {
    /// Return the next chunk, or `None` when no data is ready yet.
    ///
    /// # Errors
    ///
    /// Returns [`std::io::Error`] when the underlying read fails.
    async fn read_chunk(&mut self, max: usize) -> std::io::Result<Option<String>>;
}

/// Carry-over state between [`wait_for_message`] calls (mirrors `_last_read`).
#[derive(Debug, Default)]
pub struct ExpectState {
    /// Unconsumed tail from the previous wait.
    pub pending: String,
    /// Set once `Kernel panic` is observed.
    pub panicked: bool,
}

/// Tuning for [`wait_for_message`].
#[derive(Debug, Clone, Copy)]
pub struct WaitOptions {
    /// Total time to wait for the message (the expect-loop timeout).
    pub timeout: Duration,
    /// Settle delay after `Kernel panic` before the final drain.
    pub panic_settle: Duration,
}

impl Default for WaitOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(300),
            panic_settle: PANIC_SETTLE,
        }
    }
}

/// Wait until `message` appears on the serial.
///
/// Returns the full output up to and including `message`; the tail after
/// `message` is kept in `state.pending` for the next call. The whole wait is
/// bounded by `options.timeout`, and the capture by [`MAX_STDOUT_BYTES`].
///
/// # Errors
///
/// Returns [`KirkError::KernelPanic`] when `Kernel panic` is observed,
/// [`KirkError::Communication`] on timeout, oversized output, or I/O failure.
/// When `cancel` is set or `is_alive` reports `false`, the wait aborts and
/// the output so far is returned without error (mirrors upstream, where
/// `_wait_for` breaks on `_stop`/dead process and lets the caller decide).
pub async fn wait_for_message(
    io: &mut impl SerialIo,
    state: &mut ExpectState,
    message: &str,
    options: WaitOptions,
    cancel: &AtomicBool,
    is_alive: &(dyn Fn() -> bool + Send + Sync),
    iobuffer: Option<Arc<dyn IOBuffer>>,
) -> Result<String, KirkError> {
    let deadline = tokio::time::Instant::now() + options.timeout;
    let mut stdout = std::mem::take(&mut state.pending);
    state.panicked = false;

    loop {
        if cancel.load(Ordering::SeqCst) || !is_alive() {
            let out = stdout.clone();
            state.pending = stdout;
            return Ok(out);
        }
        if let Some(pos) = stdout.find(message) {
            state.pending = stdout[pos + message.len()..].to_string();
            return Ok(stdout);
        }
        if stdout.contains("Kernel panic") {
            tokio::time::sleep(options.panic_settle).await;
            if let Ok(tail) =
                tokio::time::timeout(Duration::from_secs(5), io.read_chunk(1024 * 1024)).await
                && let Ok(Some(data)) = tail
            {
                stdout.push_str(&data);
            }
            state.panicked = true;
            let start = stdout.len().saturating_sub(4096);
            let excerpt = &stdout[stdout.floor_char_boundary(start)..];
            return Err(KirkError::KernelPanic(format!(
                "guest kernel panic: {excerpt}"
            )));
        }
        let now = tokio::time::Instant::now();
        if now >= deadline {
            state.pending = stdout;
            return Err(KirkError::Communication(format!(
                "Timed out waiting for {message:?}"
            )));
        }
        let remaining = deadline - now;
        let chunk = match tokio::time::timeout(remaining, io.read_chunk(1024)).await {
            Err(_) => {
                state.pending = stdout;
                return Err(KirkError::Communication(format!(
                    "Timed out waiting for {message:?}"
                )));
            }
            Ok(Err(err)) => {
                state.pending = stdout;
                return Err(KirkError::Communication(err.to_string()));
            }
            Ok(Ok(chunk)) => chunk,
        };
        match chunk {
            Some(data) if !data.is_empty() => {
                if let Some(iobuffer) = &iobuffer {
                    iobuffer.write(&data).await?;
                }
                if stdout.len() + data.len() > MAX_STDOUT_BYTES {
                    state.pending = stdout;
                    return Err(KirkError::Communication(
                        "Serial output exceeds 8 MiB cap".to_string(),
                    ));
                }
                stdout.push_str(&data);
            }
            _ => tokio::time::sleep(IDLE_BACKOFF).await,
        }
    }
}

/// Parse an `echo $?</>-<code>` reply (mirrors `_exec`).
///
/// Returns `(stdout_without_marker, retcode)`; a blank reply yields the
/// input with `-1`, mirroring upstream where an empty reply skips parsing.
///
/// # Errors
///
/// Returns [`KirkError::Communication`] when the reply is non-blank but the
/// `retcode-<code>` marker is missing or unparsable.
pub fn parse_reply(stdout: &str, code: &str) -> Result<(String, i32), KirkError> {
    if stdout.trim().is_empty() {
        return Ok((stdout.to_string(), -1));
    }
    let pattern = format!(r"(?P<retcode>\d+)-{}", regex::escape(code));
    let re =
        regex::Regex::new(&pattern).map_err(|err| KirkError::Communication(err.to_string()))?;
    let captures = re.captures(stdout).ok_or_else(|| {
        KirkError::Communication(format!("Can't read return code from reply {stdout:?}"))
    })?;
    let marker = captures.get(0).map_or(0, |matched| matched.start());
    // Upstream strips the leading newline: `stdout[1:match.start()]`.
    let out = stdout.get(1..marker).unwrap_or_default().to_string();
    let retcode = captures
        .name("retcode")
        .map_or("-1", |matched| matched.as_str())
        .parse::<i32>()
        .map_err(|err| KirkError::Communication(err.to_string()))?;
    Ok((out, retcode))
}

/// Quote `arg` for `POSIX sh` (single-quote, escaping embedded quotes).
#[must_use]
pub fn shell_quote(arg: &str) -> String {
    if arg.is_empty() {
        return "''".to_string();
    }
    format!("'{}'", arg.replace('\'', "'\\''"))
}

/// Validate an `export NAME=` identifier before interpolating it.
///
/// # Errors
///
/// Returns [`KirkError::Communication`] when `key` is not a valid shell name.
pub fn validate_env_key(key: &str) -> Result<(), KirkError> {
    let mut bytes = key.bytes();
    let first_ok = bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_');
    if first_ok && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_') {
        Ok(())
    } else {
        Err(KirkError::Communication(format!(
            "Invalid environment variable name: {key:?}"
        )))
    }
}

/// Split user `options` into `argv` words (quotes/backslash aware).
///
/// This replaces the shell word-splitting that upstream got for free from
/// `create_subprocess_shell`; the words are spawned directly, never shelled.
///
/// # Errors
///
/// Returns [`KirkError::Communication`] on an unterminated quote.
pub fn split_options(options: &str) -> Result<Vec<String>, KirkError> {
    let mut words = Vec::new();
    let mut current = String::new();
    let mut in_word = false;
    let mut quote: Option<char> = None;
    let mut rest = options.chars();
    while let Some(ch) = rest.next() {
        if let Some(open) = quote {
            if ch == open {
                quote = None;
            } else if ch == '\\' && open == '"' {
                if let Some(next) = rest.next() {
                    current.push(next);
                }
            } else {
                current.push(ch);
            }
        } else {
            match ch {
                '\'' | '"' => {
                    quote = Some(ch);
                    in_word = true;
                }
                '\\' => {
                    if let Some(next) = rest.next() {
                        current.push(next);
                    }
                    in_word = true;
                }
                ch if ch.is_whitespace() => {
                    if in_word {
                        words.push(std::mem::take(&mut current));
                        in_word = false;
                    }
                }
                _ => {
                    current.push(ch);
                    in_word = true;
                }
            }
        }
    }
    if quote.is_some() {
        return Err(KirkError::Communication(
            "Unterminated quote in QEMU options".to_string(),
        ));
    }
    if in_word {
        words.push(current);
    }
    Ok(words)
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use super::*;

    struct FakeSerial {
        chunks: VecDeque<String>,
    }

    impl FakeSerial {
        fn new(chunks: &[&str]) -> Self {
            Self {
                chunks: chunks.iter().map(ToString::to_string).collect(),
            }
        }
    }

    #[async_trait::async_trait]
    impl SerialIo for FakeSerial {
        async fn read_chunk(&mut self, _max: usize) -> std::io::Result<Option<String>> {
            Ok(self.chunks.pop_front())
        }
    }

    fn test_options() -> WaitOptions {
        WaitOptions {
            timeout: Duration::from_secs(5),
            panic_settle: Duration::from_millis(0),
        }
    }

    #[tokio::test]
    async fn message_split_across_chunks() {
        let mut io = FakeSerial::new(&["hel", "lo wor", "ld#", " next"]);
        let mut state = ExpectState::default();
        let cancel = AtomicBool::new(false);
        let out = wait_for_message(
            &mut io,
            &mut state,
            "hello world",
            test_options(),
            &cancel,
            &|| true,
            None,
        )
        .await
        .expect("message arrives across chunks");
        assert!(out.contains("hello world"));
        // The loop stops at the first match; `" next"` stays unread.
        assert_eq!(state.pending, "#");
    }

    #[tokio::test]
    async fn pending_carryover_is_used() {
        let mut io = FakeSerial::new(&[]);
        let mut state = ExpectState {
            pending: "old # prompt".to_string(),
            panicked: false,
        };
        let cancel = AtomicBool::new(false);
        let out = wait_for_message(
            &mut io,
            &mut state,
            "#",
            test_options(),
            &cancel,
            &|| true,
            None,
        )
        .await
        .expect("pending already holds the message");
        assert_eq!(out, "old # prompt");
        assert_eq!(state.pending, " prompt");
    }

    #[tokio::test]
    async fn timeout_errors() {
        let mut io = FakeSerial::new(&["noise "]);
        let mut state = ExpectState::default();
        let cancel = AtomicBool::new(false);
        let err = wait_for_message(
            &mut io,
            &mut state,
            "never-comes",
            WaitOptions {
                timeout: Duration::from_millis(50),
                panic_settle: Duration::from_millis(0),
            },
            &cancel,
            &|| true,
            None,
        )
        .await
        .expect_err("missing message must time out");
        assert!(matches!(err, KirkError::Communication(_)));
    }

    #[tokio::test]
    async fn kernel_panic_raises() {
        let mut io = FakeSerial::new(&["booting\nKernel panic - not syncing"]);
        let mut state = ExpectState::default();
        let cancel = AtomicBool::new(false);
        let err = wait_for_message(
            &mut io,
            &mut state,
            "#",
            test_options(),
            &cancel,
            &|| true,
            None,
        )
        .await
        .expect_err("panic must raise");
        assert!(matches!(err, KirkError::KernelPanic(_)));
        assert!(state.panicked);
    }

    #[tokio::test]
    async fn cancel_aborts_cleanly() {
        let mut io = FakeSerial::new(&[]);
        let mut state = ExpectState::default();
        let cancel = AtomicBool::new(true);
        let out = wait_for_message(
            &mut io,
            &mut state,
            "#",
            test_options(),
            &cancel,
            &|| true,
            None,
        )
        .await
        .expect("cancel aborts without error");
        assert!(out.is_empty());
    }

    #[test]
    fn retcode_parse_ok() {
        let (out, retcode) = parse_reply("\nhello\n42-abc123\n# ", "abc123").expect("parses");
        assert_eq!(out, "hello\n");
        assert_eq!(retcode, 42);
    }

    #[test]
    fn retcode_parse_missing_marker() {
        let err = parse_reply("\nsome output\n# ", "abc123").expect_err("must fail");
        assert!(matches!(err, KirkError::Communication(_)));
    }

    #[test]
    fn retcode_parse_blank_yields_minus_one() {
        let (out, retcode) = parse_reply("  \n ", "abc123").expect("blank is fine");
        assert_eq!(retcode, -1);
        assert_eq!(out, "  \n ");
    }

    #[test]
    fn quoting_cases() {
        assert_eq!(shell_quote(""), "''");
        assert_eq!(shell_quote("/tmp/a b"), "'/tmp/a b'");
        assert_eq!(shell_quote("a'b"), "'a'\\''b'");
    }

    #[test]
    fn env_key_cases() {
        validate_env_key("FOO_BAR1").expect("valid");
        for bad in ["", "1BAD", "A-B", "A B", "A=B"] {
            validate_env_key(bad).expect_err("invalid key");
        }
    }

    #[test]
    fn options_split_cases() {
        assert_eq!(
            split_options("-m 2G -device virtio-rng-pci").expect("splits"),
            vec!["-m", "2G", "-device", "virtio-rng-pci"]
        );
        assert_eq!(
            split_options("-append 'a b' -m 2G").expect("quotes"),
            vec!["-append", "a b", "-m", "2G"]
        );
        split_options("-append 'oops").expect_err("unterminated");
    }
}
