//! Shell communication channel ported from `kirk/libkirk/channels/shell.py`.
//!
//! [`ShellChannel`] mirrors upstream `ShellComChannel`: `communicate` flips an
//! active flag, `stop` kills every tracked process group, `run_command`
//! spawns argv directly, and `fetch_file` reads a local path.
//!
//! # Security
//!
//! argv-exec only: `run_command` splits `command` into words and spawns them
//! with [`tokio::process::Command`]. No shell is ever invoked, so there is no
//! expansion, redirection, or pipelines. Commands relying on shell syntax
//! (unquoted `|`, `>`, `<`, `;`, `&`, backticks, `$()`/`$VAR`, `(`, `)`) are
//! rejected with [`KirkError::Communication`] instead of silently
//! misbehaving, which would invalidate test runs.
//!
//! # Bounds
//!
//! Combined stdout+stderr per command is capped at [`MAX_OUTPUT_BYTES`];
//! `fetch_file` is capped at [`MAX_FETCH_BYTES`]. Past the cap the child is
//! killed and the call fails, so a runaway command cannot OOM the runner.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use async_trait::async_trait;
use kirk_com::{CmdResult, ComChannel, IOBuffer};
use kirk_core::KirkError;
use kirk_plugin::Plugin;
use tokio::io::AsyncReadExt as _;
use tokio::process::Command;

/// Cap for combined stdout+stderr bytes kept per [`ComChannel::run_command`].
const MAX_OUTPUT_BYTES: usize = 8 * 1024 * 1024;

/// Cap for bytes returned by [`ComChannel::fetch_file`].
const MAX_FETCH_BYTES: usize = 64 * 1024 * 1024;

/// Chunk size for streaming child output.
const READ_CHUNK: usize = 8192;

/// Marker scanned for in command output, mirroring upstream.
const KERNEL_PANIC_MARKER: &str = "Kernel panic";

/// Shared live state behind every [`ShellChannel`] handle.
///
/// Handles are cheap clones (`Clone` shares the [`Inner`]); concurrent
/// `run_command`/`stop` from different handles interleave exactly like
/// upstream coroutines sharing one `ShellComChannel`.
#[derive(Default)]
struct Inner {
    active: AtomicBool,
    pids: tokio::sync::Mutex<Vec<u32>>,
    fetch_lock: tokio::sync::Mutex<()>,
}

/// Shell communication channel.
///
/// [`Clone`] shares the live session (use it for concurrent `stop` while a
/// command runs); [`Plugin::clone_box`] and
/// [`ComChannel::clone_channel_box`] create an independent inactive channel.
pub struct ShellChannel {
    name: String,
    inner: Arc<Inner>,
}

impl ShellChannel {
    /// Build an inactive channel named `shell`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            name: String::from("shell"),
            inner: Arc::new(Inner::default()),
        }
    }

    /// Whether the inner session is active.
    fn is_active(&self) -> bool {
        self.inner.active.load(Ordering::SeqCst)
    }
}

impl Default for ShellChannel {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for ShellChannel {
    /// Clone the handle, sharing the live session state.
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            inner: Arc::clone(&self.inner),
        }
    }
}

/// Kill a tracked child and its process group, best effort.
///
/// Children are spawned detached (`process_group(0)`), so the pid is the
/// group leader; killing the group first also reaches backgrounded members,
/// mirroring upstream's `/proc` session scan. Failures are ignored: the pid
/// may already be gone.
#[cfg(unix)]
fn kill_process_group(pid: u32) {
    let Ok(pid) = i32::try_from(pid) else {
        return;
    };
    // SAFETY: `killpg`/`kill` with `SIGKILL` only signal a tracked child
    // group; errors (already exited, reused pid) are intentionally ignored.
    unsafe {
        libc::killpg(pid, libc::SIGKILL);
        libc::kill(pid, libc::SIGKILL);
    }
}

#[cfg(not(unix))]
fn kill_process_group(_pid: u32) {}

/// Wait until `kill(pid, 0)` reports the process gone, bounded by a deadline
/// so `stop` never hangs on an unreaped zombie.
#[cfg(unix)]
async fn wait_for_exit(pid: u32) {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        let Ok(pid) = i32::try_from(pid) else {
            break;
        };
        // SAFETY: signal 0 performs an existence check without delivering
        // anything; nonzero means gone (or zombie awaiting its owner).
        let alive = unsafe { libc::kill(pid, 0) == 0 };
        if !alive {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

#[cfg(not(unix))]
async fn wait_for_exit(_pid: u32) {}

/// Map an exit status to a return code, mirroring Python's negative signal
/// numbers (`-9` for `SIGKILL`).
fn exit_code(status: std::process::ExitStatus) -> i32 {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt as _;
        status
            .code()
            .or_else(|| status.signal().map(|signal| -signal))
            .unwrap_or(-1)
    }
    #[cfg(not(unix))]
    {
        status.code().unwrap_or(-1)
    }
}

/// Split `command` into argv words.
///
/// Handles single/double quotes and backslash escapes; unquoted shell control
/// operators are rejected (see module docs) instead of being executed
/// literally with surprising results.
///
/// # Errors
///
/// Returns [`KirkError::Communication`] when the command is empty, has an
/// unclosed quote, or relies on shell syntax.
fn split_argv(command: &str) -> Result<Vec<String>, KirkError> {
    let shell_syntax = || {
        KirkError::Communication(String::from(
            "shell syntax is not supported; pass plain argv words",
        ))
    };
    let mut args = Vec::new();
    let mut current = String::new();
    let mut in_arg = false;
    let mut quote: Option<char> = None;
    let mut chars = command.chars().peekable();

    while let Some(ch) = chars.next() {
        if let Some(open) = quote {
            if ch == open {
                quote = None;
            } else if open == '"' && ch == '\\' {
                match chars.next() {
                    Some(escaped) => current.push(escaped),
                    None => current.push('\\'),
                }
            } else {
                current.push(ch);
            }
            continue;
        }
        match ch {
            '\'' | '"' => {
                quote = Some(ch);
                in_arg = true;
            }
            c if c.is_whitespace() => {
                if in_arg {
                    args.push(std::mem::take(&mut current));
                    in_arg = false;
                }
            }
            '\\' => {
                if let Some(escaped) = chars.next() {
                    current.push(escaped);
                } else {
                    current.push('\\');
                }
                in_arg = true;
            }
            '|' | ';' | '`' | '>' | '<' | '&' | '(' | ')' => return Err(shell_syntax()),
            '$' => {
                let substitution = matches!(
                    chars.peek(),
                    Some(next)
                        if next.is_alphanumeric()
                            || matches!(next, '_' | '{' | '(' | '*' | '?' | '#' | '$' | '-' | '!' | '@')
                );
                if substitution {
                    return Err(shell_syntax());
                }
                current.push(ch);
                in_arg = true;
            }
            _ => {
                current.push(ch);
                in_arg = true;
            }
        }
    }

    if quote.is_some() {
        return Err(KirkError::Communication(String::from(
            "unclosed quote in command",
        )));
    }
    if in_arg {
        args.push(current);
    }
    if args.is_empty() {
        return Err(KirkError::Communication(String::from("command is empty")));
    }
    Ok(args)
}

/// Check the bytes appended by the latest chunk for the panic marker.
///
/// Only the overlap region is scanned, so streaming stays linear.
fn tail_contains_panic(text: &str, new_bytes: usize) -> bool {
    let from = text
        .len()
        .saturating_sub(new_bytes + KERNEL_PANIC_MARKER.len());
    let from = text.floor_char_boundary(from);
    text[from..].contains(KERNEL_PANIC_MARKER)
}

/// Byte-level variant of [`tail_contains_panic`] for the stderr buffer.
fn bytes_tail_contains_panic(buf: &[u8], new_bytes: usize) -> bool {
    let needle = KERNEL_PANIC_MARKER.as_bytes();
    let from = buf.len().saturating_sub(new_bytes + needle.len());
    buf[from..]
        .windows(needle.len())
        .any(|window| window == needle)
}

/// Drain `pipe` to EOF, counting bytes against the shared `used` budget.
///
/// Past the cap the child group is killed and the rest is discarded, keeping
/// memory bounded while letting the child reach EOF so [`tokio::process`]
/// reaping never deadlocks.
async fn drain_pipe<R>(
    mut pipe: R,
    pid: u32,
    used: &AtomicUsize,
    over_cap: &AtomicBool,
    saw_panic: &AtomicBool,
) -> std::io::Result<Vec<u8>>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut bytes = Vec::new();
    let mut chunk = vec![0u8; READ_CHUNK];
    loop {
        let n = pipe.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        let total = used.fetch_add(n, Ordering::SeqCst) + n;
        if total > MAX_OUTPUT_BYTES {
            over_cap.store(true, Ordering::SeqCst);
            kill_process_group(pid);
            continue;
        }
        bytes.extend_from_slice(&chunk[..n]);
        if bytes_tail_contains_panic(&bytes, n) {
            saw_panic.store(true, Ordering::SeqCst);
        }
    }
    Ok(bytes)
}

/// Read a file with a size cap, run off-thread so the runtime never blocks.
fn read_capped(path: &str) -> Result<Vec<u8>, KirkError> {
    let len = std::fs::metadata(path).map_err(|err| KirkError::Communication(err.to_string()))?;
    if len.len() > MAX_FETCH_BYTES as u64 {
        return Err(KirkError::Communication(format!(
            "file '{path}' exceeds {MAX_FETCH_BYTES} byte fetch cap"
        )));
    }
    let bytes = std::fs::read(path).map_err(|err| KirkError::Communication(err.to_string()))?;
    if bytes.len() > MAX_FETCH_BYTES {
        return Err(KirkError::Communication(format!(
            "file '{path}' exceeds {MAX_FETCH_BYTES} byte fetch cap"
        )));
    }
    Ok(bytes)
}

#[async_trait]
impl Plugin for ShellChannel {
    fn name(&self) -> &str {
        &self.name
    }

    fn config_help(&self) -> HashMap<String, String> {
        HashMap::new()
    }

    /// Accept any configuration, mirroring upstream's no-op `setup`.
    ///
    /// # Errors
    ///
    /// Never fails.
    fn setup(&mut self, _cfg: &HashMap<String, String>) -> Result<(), KirkError> {
        Ok(())
    }

    /// Copy the channel as a new independent instance with the given name.
    fn clone_box(&self, name: &str) -> Box<dyn Plugin> {
        Box::new(Self {
            name: name.to_string(),
            inner: Arc::new(Inner::default()),
        })
    }
}

#[async_trait]
impl ComChannel for ShellChannel {
    fn parallel_execution(&self) -> bool {
        true
    }

    async fn active(&self) -> bool {
        self.is_active()
    }

    /// Start communication.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Communication`] when already active.
    async fn communicate(&mut self, _iobuffer: Option<Arc<dyn IOBuffer>>) -> Result<(), KirkError> {
        if self.inner.active.swap(true, Ordering::SeqCst) {
            return Err(KirkError::Communication(String::from("Shell is running")));
        }
        Ok(())
    }

    /// Stop communication, killing every tracked process group.
    ///
    /// Idempotent: stopping an inactive channel succeeds without doing
    /// anything. Waits briefly for an in-flight `fetch_file`, mirroring
    /// upstream.
    ///
    /// # Errors
    ///
    /// Never fails; kill errors are best effort.
    async fn stop(&mut self, _iobuffer: Option<Arc<dyn IOBuffer>>) -> Result<(), KirkError> {
        if !self.is_active() {
            return Ok(());
        }
        let pids = std::mem::take(&mut *self.inner.pids.lock().await);
        for pid in &pids {
            kill_process_group(*pid);
        }
        // Wait concurrently: each wait can block up to 5s on an unreaped
        // zombie owned by its `run_command`, so sequential waits would
        // multiply the bound per pid.
        let mut waits = tokio::task::JoinSet::new();
        for pid in pids {
            waits.spawn(wait_for_exit(pid));
        }
        while waits.join_next().await.is_some() {}
        {
            let _guard = self.inner.fetch_lock.lock().await;
        }
        self.inner.active.store(false, Ordering::SeqCst);
        Ok(())
    }

    /// Ping via a timed `test` run.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Communication`] when inactive or when the probe
    /// yields no result.
    async fn ping(&mut self) -> Result<f64, KirkError> {
        if !self.is_active() {
            return Err(KirkError::Communication(String::from(
                "Shell is not running",
            )));
        }
        match self.run_command("test .", None, None, None).await? {
            Some(result) => Ok(result.exec_time),
            None => Err(KirkError::Communication(String::from(
                "'test' command failed in shell",
            ))),
        }
    }

    /// Run `command` as argv and return its result.
    ///
    /// Stdout and stderr are both captured (stderr appended after stdout),
    /// chunks stream to `iobuffer` as they arrive, and `exec_time` measures
    /// spawn to reaping. Always returns `Some` on success, even for nonzero
    /// exits; the return code carries the failure.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Communication`] when inactive, when the command
    /// is empty or needs a shell, on spawn/I/O failures, or past
    /// [`MAX_OUTPUT_BYTES`]. Returns [`KirkError::KernelPanic`] when the
    /// output contains `Kernel panic`.
    #[allow(
        clippy::too_many_lines,
        reason = "single spawn-stream-reap flow; splitting would scatter pid bookkeeping"
    )]
    async fn run_command(
        &mut self,
        command: &str,
        cwd: Option<&str>,
        env: Option<&HashMap<String, String>>,
        iobuffer: Option<Arc<dyn IOBuffer>>,
    ) -> Result<Option<CmdResult>, KirkError> {
        if !self.is_active() {
            return Err(KirkError::Communication(String::from(
                "Shell is not running",
            )));
        }
        let argv = split_argv(command)?;
        let (program, program_args) = argv
            .split_first()
            .ok_or_else(|| KirkError::Communication(String::from("command is empty")))?;

        let mut cmd = Command::new(program);
        cmd.args(program_args)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt as _;
            cmd.as_std_mut().process_group(0);
        }
        if let Some(dir) = cwd {
            cmd.current_dir(dir);
        }
        if let Some(vars) = env
            && !vars.is_empty()
        {
            // Upstream replaces the environment when one is given.
            cmd.env_clear().envs(vars);
        }

        let mut child = cmd
            .spawn()
            .map_err(|err| KirkError::Communication(err.to_string()))?;
        let pid = child
            .id()
            .ok_or_else(|| KirkError::Communication(String::from("spawned process has no pid")))?;
        self.inner.pids.lock().await.push(pid);

        let start = std::time::Instant::now();
        let used = Arc::new(AtomicUsize::new(0));
        let over_cap = Arc::new(AtomicBool::new(false));
        let saw_panic = Arc::new(AtomicBool::new(false));

        let mut child_stdout = child
            .stdout
            .take()
            .ok_or_else(|| KirkError::Communication(String::from("child stdout is not piped")))?;
        let child_stderr = child
            .stderr
            .take()
            .ok_or_else(|| KirkError::Communication(String::from("child stderr is not piped")))?;
        let stderr_task = tokio::spawn({
            let (used, over_cap, saw_panic) = (
                Arc::clone(&used),
                Arc::clone(&over_cap),
                Arc::clone(&saw_panic),
            );
            async move { drain_pipe(child_stderr, pid, &used, &over_cap, &saw_panic).await }
        });

        let mut stdout = String::new();
        let mut chunk = vec![0u8; READ_CHUNK];
        loop {
            let n = child_stdout
                .read(&mut chunk)
                .await
                .map_err(|err| KirkError::Communication(err.to_string()))?;
            if n == 0 {
                break;
            }
            let total = used.fetch_add(n, Ordering::SeqCst) + n;
            if total > MAX_OUTPUT_BYTES {
                over_cap.store(true, Ordering::SeqCst);
                kill_process_group(pid);
                continue;
            }
            let text = String::from_utf8_lossy(&chunk[..n]);
            if let Some(buffer) = &iobuffer {
                buffer.write(&text).await?;
            }
            stdout.push_str(&text);
            if tail_contains_panic(&stdout, text.len()) {
                saw_panic.store(true, Ordering::SeqCst);
            }
        }

        let status = child
            .wait()
            .await
            .map_err(|err| KirkError::Communication(err.to_string()))?;
        let stderr_bytes = stderr_task
            .await
            .map_err(|err| KirkError::Communication(err.to_string()))?
            .map_err(|err| KirkError::Communication(err.to_string()))?;

        self.inner
            .pids
            .lock()
            .await
            .retain(|tracked| *tracked != pid);
        // Reap stragglers sharing the group, mirroring upstream's
        // finally-block kill.
        kill_process_group(pid);

        if saw_panic.load(Ordering::SeqCst) {
            return Err(KirkError::KernelPanic(String::from(
                "kernel panic detected in command output",
            )));
        }
        if over_cap.load(Ordering::SeqCst) {
            return Err(KirkError::Communication(format!(
                "command output exceeds {MAX_OUTPUT_BYTES} byte cap"
            )));
        }

        let stderr_text = String::from_utf8_lossy(&stderr_bytes);
        if !stderr_text.is_empty() {
            if let Some(buffer) = &iobuffer {
                buffer.write(&stderr_text).await?;
            }
            stdout.push_str(&stderr_text);
        }

        Ok(Some(CmdResult {
            command: command.to_string(),
            returncode: exit_code(status),
            stdout,
            exec_time: start.elapsed().as_secs_f64(),
        }))
    }

    /// Fetch a local file, capped at [`MAX_FETCH_BYTES`].
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Communication`] when the path is empty, missing,
    /// unreadable, over the cap, or the channel is inactive.
    async fn fetch_file(&mut self, target_path: &str) -> Result<Vec<u8>, KirkError> {
        if target_path.is_empty() {
            return Err(KirkError::Communication(String::from(
                "target path is empty",
            )));
        }
        if !Path::new(target_path).is_file() {
            return Err(KirkError::Communication(format!(
                "'{target_path}' file doesn't exist"
            )));
        }
        if !self.is_active() {
            return Err(KirkError::Communication(String::from(
                "Shell is not running",
            )));
        }
        let _guard = self.inner.fetch_lock.lock().await;
        let owned = target_path.to_string();
        tokio::task::spawn_blocking(move || read_capped(&owned))
            .await
            .map_err(|err| KirkError::Communication(err.to_string()))?
    }

    /// Copy the channel as a new independent instance with the given name.
    fn clone_channel_box(&self, new_name: &str) -> Box<dyn ComChannel> {
        Box::new(Self {
            name: new_name.to_string(),
            inner: Arc::new(Inner::default()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_simple_words() {
        assert_eq!(
            split_argv("echo 0").expect("simple split"),
            vec!["echo", "0"]
        );
    }

    #[test]
    fn split_quotes_and_escapes() {
        assert_eq!(
            split_argv(r#"echo 'a b' "c d" e\ f"#).expect("quoted split"),
            vec!["echo", "a b", "c d", "e f"]
        );
    }

    #[test]
    fn split_rejects_empty_and_blank() {
        assert!(split_argv("").is_err());
        assert!(split_argv("   ").is_err());
    }

    #[test]
    fn split_rejects_shell_operators() {
        for command in [
            "echo hi > /tmp/x",
            "echo a | grep a",
            "sleep 1; echo done",
            "sleep 1 && echo done",
            "echo `id`",
            "echo $(id)",
            "echo (group)",
        ] {
            assert!(split_argv(command).is_err(), "{command} must be rejected");
        }
    }

    #[test]
    fn split_rejects_substitution_but_keeps_bare_dollar() {
        assert!(split_argv("echo -n $PWD").is_err());
        assert_eq!(split_argv("echo price: $").expect("bare dollar").len(), 3);
        for command in ["echo $@", "echo $-", "echo $!", "echo $*"] {
            assert!(split_argv(command).is_err(), "{command} must be rejected");
        }
    }

    #[test]
    fn split_rejects_unclosed_quote() {
        assert!(split_argv("echo 'oops").is_err());
    }
}
