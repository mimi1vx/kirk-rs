//! LTX transport ported from the `LTX` class in
//! `kirk/libkirk/channels/ltx.py`.
//!
//! Requests are packed with msgpack and written to the input FIFO; a single
//! owned poll task (held in a [`tokio::task::JoinSet`], aborted and drained
//! on [`Ltx::disconnect`] so no task outlives the connection) reads reply
//! frames from the output FIFO and feeds the pending requests in order.
//!
//! Security bounds: the streaming buffer is capped at
//! `MAX_BUFFERED_BYTES`, frames decode with a nesting depth cap of
//! `MAX_DECODE_DEPTH`, and every offset/length uses `checked_*` or
//! `try_from` conversions.

use std::collections::{HashMap, VecDeque};
use std::io::{Cursor, ErrorKind, Read, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use kirk_core::KirkError;
use serde::Deserialize;
use tokio::io::unix::AsyncFd;
use tokio::sync::Mutex;
use tokio::task::JoinSet;

use crate::request::{Field, Frame, OP_ERROR, Reply, Request};

/// Read chunk size, mirroring Python `LTX.BUFFSIZE`.
pub(crate) const READ_CHUNK: usize = 1 << 21;
/// Maximum buffered-but-undecoded bytes; bounds transient allocations.
const MAX_BUFFERED_BYTES: usize = 8 << 20;
/// Maximum msgpack nesting depth accepted while decoding.
const MAX_DECODE_DEPTH: usize = 4;
/// Delay between reply polls, mirroring the Python `asyncio.sleep(0.005)`.
const POLL_INTERVAL: Duration = Duration::from_millis(5);

/// Non-blocking FIFO handle.
///
/// `tokio::fs` performs blocking reads, which cannot be cancelled by task
/// abort (a thread stuck in `read()` on a FIFO survives `close()` on macOS
/// and pins runtime teardown). Opening with `O_NONBLOCK` and driving reads
/// through [`AsyncFd`] keeps every read abort-safe; writes loop on the
/// non-blocking fd directly (see [`Fifo::write_all`]).
struct Fifo {
    inner: AsyncFd<std::fs::File>,
}

impl Fifo {
    /// Open the FIFO for reading; never waits for a writer.
    ///
    /// Writing ends use [`Fifo::open`] directly; a write-only open fails fast
    /// with `ENXIO` when no reader is present instead of blocking forever.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Ltx`] when the FIFO cannot be opened.
    fn open_read(path: &Path) -> Result<Self, KirkError> {
        Self::open(path, true)
    }

    fn open(path: &Path, read: bool) -> Result<Self, KirkError> {
        let direction = if read { "output" } else { "input" };
        Self::open_raw(path, read)
            .map(|inner| Self { inner })
            .map_err(|e| {
                KirkError::Ltx(format!(
                    "can't open LTX {direction} '{}': {e}",
                    path.display()
                ))
            })
    }

    fn open_raw(path: &Path, read: bool) -> std::io::Result<AsyncFd<std::fs::File>> {
        let mut options = std::fs::OpenOptions::new();
        if read {
            options.read(true);
        } else {
            options.write(true);
        }
        options.custom_flags(libc::O_NONBLOCK);
        options.open(path).and_then(AsyncFd::new)
    }

    /// Read one chunk; resolves to `0` once all writers went away.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Ltx`] on I/O failure.
    async fn read_chunk(&self, buf: &mut [u8]) -> Result<usize, KirkError> {
        loop {
            let mut guard = self
                .inner
                .readable()
                .await
                .map_err(|e| KirkError::Ltx(format!("can't wait for LTX output: {e}")))?;
            match guard.try_io(|file| file.get_ref().read(buf)) {
                Ok(Ok(count)) => return Ok(count),
                Ok(Err(error)) if error.kind() == ErrorKind::WouldBlock => {}
                Ok(Err(error)) => {
                    return Err(KirkError::Ltx(format!("can't read LTX output: {error}")));
                }
                Err(_not_ready) => {}
            }
        }
    }

    /// Write the whole buffer.
    ///
    /// A plain non-blocking write loop: the fd is `O_NONBLOCK`, so the kernel
    /// never parks this task inside `write`. On backpressure (`WouldBlock`)
    /// yield so the peer's reader runs instead of spinning. Epoll writability
    /// is deliberately not used here: a freshly opened FIFO write end has no
    /// state transition for edge-triggered readiness to report, and the wait
    /// can stall forever on Linux while the pipe sits empty with a live
    /// reader.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Ltx`] on I/O failure (including a vanished reader).
    async fn write_all(&self, mut bytes: &[u8]) -> Result<(), KirkError> {
        while !bytes.is_empty() {
            match self.inner.get_ref().write(bytes) {
                Ok(0) => {
                    return Err(KirkError::Ltx(
                        "can't write to LTX input: no reader".to_string(),
                    ));
                }
                Ok(count) => {
                    bytes = bytes
                        .get(count..)
                        .ok_or_else(|| KirkError::Ltx("short LTX write".to_string()))?;
                }
                Err(error) if error.kind() == ErrorKind::WouldBlock => {
                    tokio::task::yield_now().await;
                }
                Err(error) => {
                    return Err(KirkError::Ltx(format!("can't write to LTX input: {error}")));
                }
            }
        }
        Ok(())
    }
}
/// Whether a decode failure means "need more bytes" rather than corruption.
fn is_truncation(error: &rmp_serde::decode::Error) -> bool {
    use rmp_serde::decode::Error as DecodeError;
    matches!(
        error,
        DecodeError::InvalidMarkerRead(io) | DecodeError::InvalidDataRead(io)
            if io.kind() == ErrorKind::UnexpectedEof
    )
}

/// Decode one frame from the head of `buf`.
///
/// Returns `Ok(None)` when `buf` holds a truncated frame and more bytes are
/// needed, and `Err` on corrupt frames or nesting/size violations.
///
/// # Errors
///
/// Returns [`KirkError::Ltx`] for corrupt frames, over-long fields, nested
/// values, or non-array messages.
pub(crate) fn decode_one(buf: &[u8]) -> Result<Option<(Vec<Field>, usize)>, KirkError> {
    if buf.is_empty() {
        return Ok(None);
    }
    let mut decoder = rmp_serde::Deserializer::new(Cursor::new(buf));
    decoder.set_max_depth(MAX_DECODE_DEPTH);
    match Frame::deserialize(&mut decoder) {
        Ok(frame) => {
            let used = usize::try_from(decoder.position())
                .map_err(|e| KirkError::Ltx(format!("frame offset does not fit in usize: {e}")))?;
            Ok(Some((frame.into_inner(), used)))
        }
        Err(error) if is_truncation(&error) => Ok(None),
        Err(error) => Err(KirkError::Ltx(format!("can't decode LTX frame: {error}"))),
    }
}

#[derive(Debug)]
struct Pending {
    id: u64,
    request: Request,
}

#[derive(Debug)]
struct State {
    pending: VecDeque<Pending>,
    replies: HashMap<u64, Reply>,
    next_id: u64,
    fatal: Option<String>,
}

#[derive(Debug)]
struct Shared {
    state: Mutex<State>,
    stop: AtomicBool,
    running: AtomicBool,
}

/// Multiplexes [`Request`]s over an input/output FIFO pair.
///
/// Cheap to clone: clones share the pending queue and the poll task.
#[derive(Debug, Clone)]
pub struct Ltx {
    infile: PathBuf,
    outfile: PathBuf,
    shared: Arc<Shared>,
    poll: Arc<Mutex<Option<JoinSet<()>>>>,
}

impl Ltx {
    /// Create an unconnected client for the given FIFO pair.
    #[must_use]
    pub fn new(infile: PathBuf, outfile: PathBuf) -> Self {
        Self {
            infile,
            outfile,
            shared: Arc::new(Shared {
                state: Mutex::new(State {
                    pending: VecDeque::new(),
                    replies: HashMap::new(),
                    next_id: 0,
                    fatal: None,
                }),
                stop: AtomicBool::new(false),
                running: AtomicBool::new(false),
            }),
            poll: Arc::new(Mutex::new(None)),
        }
    }

    /// Whether the poll task is running.
    #[must_use]
    pub(crate) fn connected(&self) -> bool {
        self.shared.running.load(Ordering::SeqCst)
    }

    /// Start the poll task; a no-op when already connected.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Ltx`] when the output FIFO cannot be opened.
    pub async fn connect(&self) -> Result<(), KirkError> {
        if self.connected() {
            return Ok(());
        }
        // Probe the output FIFO before spawning so missing paths are reported
        // synchronously instead of surfacing as a background fatal error.
        // The open is non-blocking: readiness for the peer is established by
        // the version handshake in `communicate`, not here.
        Fifo::open_read(&self.outfile)?;

        self.shared.stop.store(false, Ordering::SeqCst);
        {
            let mut state = self.shared.state.lock().await;
            state.fatal = None;
        }
        let task = self.clone();
        let mut poll = self.poll.lock().await;
        let mut set = JoinSet::new();
        set.spawn(async move { task.poll_loop().await });
        *poll = Some(set);
        self.shared.running.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// Stop the poll task and wait for it, so no task outlives the
    /// connection. A no-op when not connected.
    ///
    /// # Errors
    ///
    /// Returns the recorded [`KirkError::Ltx`] when the poll task failed.
    pub async fn disconnect(&self) -> Result<(), KirkError> {
        if !self.connected() {
            return Ok(());
        }
        self.shared.stop.store(true, Ordering::SeqCst);
        // Abort first (unblocks a read stuck on the FIFO), then drain so the
        // task is reaped before returning: no orphaned poll tasks.
        let mut poll = self.poll.lock().await;
        if let Some(set) = poll.as_mut() {
            set.abort_all();
            while set.join_next().await.is_some() {}
        }
        *poll = None;
        drop(poll);

        self.shared.stop.store(false, Ordering::SeqCst);
        self.shared.running.store(false, Ordering::SeqCst);

        let state = self.shared.state.lock().await;
        if let Some(fatal) = &state.fatal {
            return Err(KirkError::Ltx(fatal.clone()));
        }
        Ok(())
    }

    /// Pack requests, queue them, and write them to the input FIFO,
    /// preserving order. Returns the request ids.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Ltx`] when no requests are given, when not
    /// connected, or when packing/writing fails.
    async fn send(&self, requests: Vec<Request>) -> Result<Vec<u64>, KirkError> {
        if requests.is_empty() {
            return Err(KirkError::Ltx("No requests given".to_string()));
        }
        if !self.connected() {
            return Err(KirkError::Ltx("Client is not connected to LTX".to_string()));
        }
        {
            let state = self.shared.state.lock().await;
            if let Some(fatal) = &state.fatal {
                return Err(KirkError::Ltx(fatal.clone()));
            }
        }

        let mut packed = Vec::new();
        for request in &requests {
            packed.extend_from_slice(&request.pack()?);
        }

        let mut ids = Vec::with_capacity(requests.len());
        {
            let mut state = self.shared.state.lock().await;
            for request in requests {
                let id = state.next_id;
                state.next_id = state
                    .next_id
                    .checked_add(1)
                    .ok_or_else(|| KirkError::Ltx("request id overflow".to_string()))?;
                ids.push(id);
                state.pending.push_back(Pending { id, request });
            }
        }

        self.write_infile(&packed).await?;
        Ok(ids)
    }

    /// Send requests and wait for every reply, preserving request order.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Ltx`] when sending fails, the peer reports an
    /// error, or the connection drops while waiting.
    pub async fn gather(&self, requests: Vec<Request>) -> Result<Vec<Reply>, KirkError> {
        let count = requests.len();
        let ids = self.send(requests).await?;
        loop {
            {
                let mut state = self.shared.state.lock().await;
                if let Some(fatal) = &state.fatal {
                    return Err(KirkError::Ltx(fatal.clone()));
                }
                if !self.connected() {
                    return Err(KirkError::Ltx("Client is not connected to LTX".to_string()));
                }
                if state.replies.len() >= count
                    && ids.iter().all(|id| state.replies.contains_key(id))
                {
                    let mut replies = Vec::with_capacity(count);
                    for id in &ids {
                        if let Some(reply) = state.replies.remove(id) {
                            replies.push(reply);
                        }
                    }
                    return Ok(replies);
                }
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    async fn write_infile(&self, bytes: &[u8]) -> Result<(), KirkError> {
        // The peer opens the read end at startup; retry while it is missing
        // (like a blocking open would wait), aborting on disconnect. The
        // sleeps keep the retry abort-safe: no thread ever blocks in `open`.
        let fifo = loop {
            match Fifo::open_raw(&self.infile, false) {
                Ok(inner) => break Fifo { inner },
                Err(error) if error.raw_os_error() == Some(libc::ENXIO) => {
                    if !self.connected() {
                        return Err(KirkError::Ltx("Client is not connected to LTX".to_string()));
                    }
                    tokio::time::sleep(POLL_INTERVAL).await;
                }
                Err(error) => {
                    return Err(KirkError::Ltx(format!(
                        "can't open LTX input '{}': {error}",
                        self.infile.display()
                    )));
                }
            }
        };
        fifo.write_all(bytes).await
    }

    fn record_fatal(&self, message: String) {
        // `poll_loop` is the only writer besides tests; `try_lock` keeps it
        // non-blocking, a missed update only delays the error by one frame.
        if let Ok(mut state) = self.shared.state.try_lock() {
            state.fatal.get_or_insert(message);
        }
    }

    async fn dispatch(&self, msg: Vec<Field>) {
        if msg.is_empty() {
            return;
        }
        if msg[0].as_u8().ok() == Some(OP_ERROR) {
            let detail = msg
                .get(1)
                .and_then(|f| f.as_str().ok())
                .unwrap_or("unknown LTX error");
            self.record_fatal(format!("LTX error: {detail}"));
            return;
        }
        if msg[0].as_u8().is_err() {
            self.record_fatal("LTX error: message opcode must be an integer".to_string());
            return;
        }
        let mut state = self.shared.state.lock().await;
        let mut position = 0;
        while position < state.pending.len() {
            let feed = state.pending[position].request.feed(&msg);
            match feed {
                Ok(Some(reply)) => {
                    let pending = state.pending.remove(position).expect("pending in range");
                    state.replies.insert(pending.id, reply);
                }
                Ok(None) => {
                    position = position.saturating_add(1);
                }
                Err(error) => {
                    state.fatal.get_or_insert(error.to_string());
                    break;
                }
            }
        }
    }

    async fn poll_loop(&self) {
        let outfile = match Fifo::open_read(&self.outfile) {
            Ok(fifo) => fifo,
            Err(error) => {
                self.record_fatal(error.to_string());
                self.shared.running.store(false, Ordering::SeqCst);
                return;
            }
        };

        let mut buffered: Vec<u8> = Vec::new();
        let mut chunk = vec![0u8; READ_CHUNK];
        loop {
            if self.shared.stop.load(Ordering::SeqCst) {
                break;
            }
            let count = match outfile.read_chunk(&mut chunk).await {
                Ok(count) => count,
                Err(error) => {
                    self.record_fatal(error.to_string());
                    break;
                }
            };
            if count == 0 {
                // All writers went away: the peer is gone.
                self.record_fatal("LTX output closed".to_string());
                break;
            }
            let total = buffered.len().saturating_add(count);
            if total > MAX_BUFFERED_BYTES {
                self.record_fatal(format!(
                    "buffered {total} bytes exceed {MAX_BUFFERED_BYTES} bytes"
                ));
                break;
            }
            buffered.extend_from_slice(&chunk[..count]);

            loop {
                match decode_one(&buffered) {
                    Ok(Some((msg, used))) => {
                        // `used` is bounded by `buffered.len()` by construction.
                        buffered.drain(..used);
                        self.dispatch(msg).await;
                        if self.shared.state.lock().await.fatal.is_some() {
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(error) => {
                        self.record_fatal(error.to_string());
                        break;
                    }
                }
            }
            if self.shared.state.lock().await.fatal.is_some() {
                break;
            }
        }
        self.shared.running.store(false, Ordering::SeqCst);
    }
}

impl Drop for Ltx {
    /// Backstop so a leaked `Ltx` never leaves a poll task behind; the normal
    /// path is [`Ltx::disconnect`], which also drains the task.
    fn drop(&mut self) {
        if self.connected() {
            self.shared.stop.store(true, Ordering::SeqCst);
            if let Ok(mut poll) = self.poll.try_lock()
                && let Some(set) = poll.as_mut()
            {
                set.abort_all();
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    //! Loopback harness: FIFO pair plus a mock peer answering requests.
    //! Shared by the `ltx` and `channel` unit tests; no LTX binary needed.

    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tokio::sync::Mutex;

    use crate::request::{
        OP_CWD, OP_DATA, OP_ENV, OP_EXEC, OP_GET_FILE, OP_KILL, OP_LOG, OP_PING, OP_PONG,
        OP_RESULT, OP_SET_FILE, OP_VERSION,
    };

    use super::{Fifo, READ_CHUNK, decode_one};

    static FIFO_COUNTER: AtomicU64 = AtomicU64::new(0);

    /// Create a FIFO pair; files are removed by the caller via [`Fifos::cleanup`].
    pub(crate) struct Fifos {
        /// Path the client writes to.
        pub infile: PathBuf,
        /// Path the client reads from.
        pub outfile: PathBuf,
    }

    impl Fifos {
        pub(crate) fn create(tag: &str) -> Self {
            let id = FIFO_COUNTER.fetch_add(1, Ordering::SeqCst);
            let dir = std::env::temp_dir();
            let infile = dir.join(format!("kirk-ltx-{tag}-in-{}-{id}", std::process::id()));
            let outfile = dir.join(format!("kirk-ltx-{tag}-out-{}-{id}", std::process::id()));
            for path in [&infile, &outfile] {
                if path.exists() {
                    std::fs::remove_file(path).expect("remove stale fifo");
                }
                let status = std::process::Command::new("mkfifo")
                    .arg(path)
                    .status()
                    .expect("run mkfifo");
                assert!(status.success(), "mkfifo failed");
            }
            Self { infile, outfile }
        }

        pub(crate) fn cleanup(&self) {
            for path in [&self.infile, &self.outfile] {
                let _ = std::fs::remove_file(path);
            }
        }
    }

    /// Recorded opcodes seen by the mock peer.
    pub(crate) type Seen = Arc<Mutex<Vec<u8>>>;

    fn now_ns() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .and_then(|elapsed| u64::try_from(elapsed.as_nanos()).ok())
            .unwrap_or(0)
    }

    fn pack<T: serde::Serialize>(value: &T) -> Vec<u8> {
        rmp_serde::to_vec(value).expect("mock reply packs")
    }

    fn reply_for(msg: &[crate::request::Field]) -> Vec<Vec<u8>> {
        let opcode = msg.first().and_then(|f| f.as_u8().ok()).unwrap_or(0xFF);
        let slot = msg.get(1).and_then(|f| f.as_u8().ok()).unwrap_or(0);
        match opcode {
            OP_VERSION => vec![pack(&(OP_VERSION, "0.1-test"))],
            OP_PING => vec![pack(&(OP_PING,)), pack(&(OP_PONG, now_ns()))],
            OP_ENV => {
                let (key, value) = (
                    msg.get(2)
                        .and_then(|f| f.as_str().ok())
                        .unwrap_or("")
                        .to_string(),
                    msg.get(3)
                        .and_then(|f| f.as_str().ok())
                        .unwrap_or("")
                        .to_string(),
                );
                vec![pack(&(OP_ENV, slot, key, value))]
            }
            OP_CWD => {
                let path = msg
                    .get(2)
                    .and_then(|f| f.as_str().ok())
                    .unwrap_or("")
                    .to_string();
                vec![pack(&(OP_CWD, slot, path))]
            }
            OP_GET_FILE => {
                let path = msg
                    .get(1)
                    .and_then(|f| f.as_str().ok())
                    .unwrap_or("")
                    .to_string();
                vec![
                    pack(&(OP_DATA, crate::request::Bin(b"file-bytes"))),
                    pack(&(OP_GET_FILE, path)),
                ]
            }
            OP_SET_FILE => {
                let path = msg
                    .get(1)
                    .and_then(|f| f.as_str().ok())
                    .unwrap_or("")
                    .to_string();
                vec![pack(&(OP_SET_FILE, path))]
            }
            OP_EXEC => vec![
                pack(&(OP_EXEC, slot)),
                pack(&(OP_LOG, slot, 0_u8, "mock-out")),
                pack(&(OP_RESULT, slot, now_ns(), 1_u8, 0_u8)),
            ],
            OP_KILL => vec![pack(&(OP_KILL, slot))],
            _ => vec![],
        }
    }

    /// Run a mock peer until `stop` is set; records seen opcodes in `seen`.
    pub(crate) async fn run_mock(
        infile: PathBuf,
        outfile: PathBuf,
        seen: Seen,
        stop: Arc<AtomicU64>,
    ) {
        let reader = Fifo::open_read(&infile).expect("mock opens infile");
        // The write end needs the client reading first; retry until then.
        let writer = loop {
            if stop.load(Ordering::SeqCst) != 0 {
                return;
            }
            match Fifo::open(&outfile, false) {
                Ok(writer) => break writer,
                Err(_) => tokio::time::sleep(std::time::Duration::from_millis(5)).await,
            }
        };
        let mut buffered = Vec::new();
        let mut chunk = vec![0u8; READ_CHUNK];
        while stop.load(Ordering::SeqCst) == 0 {
            let count = tokio::select! {
                biased;
                result = reader.read_chunk(&mut chunk) => result.expect("mock reads"),
                () = tokio::time::sleep(std::time::Duration::from_millis(50)) => continue,
            };
            if count == 0 {
                // No writers right now (sends open transiently); yield so the
                // client task can run instead of spinning without yielding,
                // which would starve a current-thread runtime.
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                continue;
            }
            buffered.extend_from_slice(&chunk[..count]);
            while let Some((msg, used)) = decode_one(&buffered).expect("mock decodes client frames")
            {
                buffered.drain(..used);
                if let Some(opcode) = msg.first().and_then(|f| f.as_u8().ok()) {
                    seen.lock().await.push(opcode);
                }
                for reply in reply_for(&msg) {
                    writer.write_all(&reply).await.expect("mock replies");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;

    use crate::request::{Reply, Request, SlotId};

    use super::Ltx;
    use super::test_support::{Fifos, Seen, run_mock};

    async fn connected_pair(
        tag: &str,
    ) -> (Fifos, Ltx, Arc<AtomicU64>, tokio::task::JoinHandle<()>) {
        let fifos = Fifos::create(tag);
        let seen: Seen = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicU64::new(0));
        let mock = tokio::spawn(run_mock(
            fifos.infile.clone(),
            fifos.outfile.clone(),
            seen,
            stop.clone(),
        ));
        let ltx = Ltx::new(fifos.infile.clone(), fifos.outfile.clone());
        ltx.connect().await.expect("connect");
        assert!(ltx.connected());
        (fifos, ltx, stop, mock)
    }

    async fn shutdown(
        fifos: Fifos,
        ltx: Ltx,
        stop: Arc<AtomicU64>,
        mock: tokio::task::JoinHandle<()>,
    ) {
        ltx.disconnect().await.expect("disconnect");
        assert!(!ltx.connected());
        stop.store(1, Ordering::SeqCst);
        mock.await.expect("mock joins");
        fifos.cleanup();
    }

    use std::sync::atomic::Ordering;

    #[tokio::test]
    async fn loopback_round_trip() {
        let (fifos, ltx, stop, mock) = connected_pair("roundtrip").await;

        let replies = ltx.gather(vec![Request::version()]).await.expect("version");
        assert_eq!(replies, vec![Reply::Version("0.1-test".to_string())]);

        let replies = ltx.gather(vec![Request::ping()]).await.expect("ping");
        assert!(matches!(replies[0], Reply::Ping(_)));

        let slot = SlotId::new(0).expect("slot");
        let replies = ltx
            .gather(vec![
                Request::cwd(Some(0), "/tmp").expect("cwd"),
                Request::env(Some(0), "HELLO", "CIAO").expect("env"),
                Request::execute(slot, "echo hi").expect("exec"),
            ])
            .await
            .expect("chain");
        assert_eq!(replies.len(), 3);
        match &replies[2] {
            Reply::Execute {
                si_status, stdout, ..
            } => {
                assert_eq!(*si_status, 0);
                assert_eq!(stdout, "mock-out");
            }
            other => panic!("expected execute reply, got {other:?}"),
        }

        let replies = ltx
            .gather(vec![
                Request::set_file("/tmp/f", b"data").expect("set"),
                Request::get_file("/tmp/f").expect("get"),
            ])
            .await
            .expect("files");
        assert!(matches!(replies[0], Reply::SetFile { .. }));
        match &replies[1] {
            Reply::GetFile { data, .. } => assert_eq!(data, b"file-bytes"),
            other => panic!("expected file reply, got {other:?}"),
        }

        let replies = ltx.gather(vec![Request::kill(slot)]).await.expect("kill");
        assert_eq!(replies, vec![Reply::Kill { slot: 0 }]);

        shutdown(fifos, ltx, stop, mock).await;
    }

    #[tokio::test]
    async fn send_validates_input() {
        let ltx = Ltx::new("/nonexistent-in".into(), "/nonexistent-out".into());
        assert!(ltx.gather(vec![]).await.is_err());
        assert!(ltx.gather(vec![Request::version()]).await.is_err());
    }

    #[tokio::test]
    async fn peer_error_reaches_gather() {
        let fifos = Fifos::create("error");
        // Peer under test: emit a single ERROR frame, then hold the FIFOs.
        let outfile = fifos.outfile.clone();
        let infile = fifos.infile.clone();
        let peer = tokio::spawn(async move {
            use super::Fifo;
            // Hold the input open so the client's send succeeds; the ERROR
            // frame must be what fails the gather.
            let _reader = Fifo::open_read(&infile).expect("peer opens infile");
            let writer = loop {
                match Fifo::open(&outfile, false) {
                    Ok(writer) => break writer,
                    Err(_) => tokio::time::sleep(std::time::Duration::from_millis(5)).await,
                }
            };
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            let frame =
                rmp_serde::to_vec(&(crate::request::OP_ERROR, "boom")).expect("error frame packs");
            writer.write_all(&frame).await.expect("peer writes");
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        });

        let ltx = Ltx::new(fifos.infile.clone(), fifos.outfile.clone());
        ltx.connect().await.expect("connect");
        let result = ltx.gather(vec![Request::version()]).await;
        assert!(result.is_err());
        peer.abort();
        let _ = ltx.disconnect().await;
        fifos.cleanup();
    }

    #[tokio::test]
    async fn disconnect_aborts_poll_task() {
        let (fifos, ltx, stop, mock) = connected_pair("abort").await;
        // Second disconnect is a no-op success.
        ltx.disconnect().await.expect("disconnect");
        ltx.disconnect().await.expect("idempotent");
        assert!(!ltx.connected());
        // Reconnect works after a disconnect.
        ltx.connect().await.expect("reconnect");
        assert!(ltx.connected());
        shutdown(fifos, ltx, stop, mock).await;
    }
}
