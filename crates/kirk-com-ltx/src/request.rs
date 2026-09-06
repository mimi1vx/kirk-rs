//! LTX protocol requests ported from `Request`/`Requests` in
//! `kirk/libkirk/channels/ltx.py`.
//!
//! Each request packs itself into msgpack bytes (`Request::pack`) and
//! consumes decoded reply frames (`Request::feed`) until completed, exactly
//! like the Python echo state machines. `Request::feed` returns the
//! request's [`Reply`] once, when the request completes.
//!
//! Security bounds (see `sota-code-security`): frames hold at most
//! `MAX_FIELDS` scalar fields, nested arrays/maps are rejected, individual
//! string/binary fields are capped at `MAX_FIELD_BYTES`, accumulated
//! `DATA`/`LOG` payloads are capped (`MAX_FILE_BYTES`/`MAX_STDOUT_BYTES`)
//! with `checked_*` arithmetic, and every length conversion uses `try_from`.

use kirk_core::KirkError;
use serde::de::{self, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};

/// Opcode sent by the peer when a request failed.
pub const OP_ERROR: u8 = 0xFF;
/// Opcode of the version request/reply.
pub const OP_VERSION: u8 = 0x00;
/// Opcode of the ping request echo.
pub const OP_PING: u8 = 0x01;
/// Opcode of the ping reply.
pub const OP_PONG: u8 = 0x02;
/// Opcode of the file-download request echo.
pub const OP_GET_FILE: u8 = 0x03;
/// Opcode of the file-upload request echo.
pub const OP_SET_FILE: u8 = 0x04;
/// Opcode of the environment request echo.
pub const OP_ENV: u8 = 0x05;
/// Opcode of the working-directory request echo.
pub const OP_CWD: u8 = 0x06;
/// Opcode of the execute request echo.
pub const OP_EXEC: u8 = 0x07;
/// Opcode of the execute result.
pub const OP_RESULT: u8 = 0x08;
/// Opcode of execute stdout chunks.
pub const OP_LOG: u8 = 0x09;
/// Opcode of file-download chunks.
pub const OP_DATA: u8 = 0xA0;
/// Opcode of the kill request echo.
pub const OP_KILL: u8 = 0xA1;

/// Highest assignable execution slot, mirroring `Request.MAX_SLOTS`.
pub const MAX_SLOTS: u8 = 127;
/// Broadcast slot id for `ENV`/`CWD`, mirroring `Request.ALL_SLOTS`.
pub const ALL_SLOTS: u8 = 128;

/// Maximum number of fields accepted in a single frame.
pub(crate) const MAX_FIELDS: usize = 8;
/// Maximum size of a single string/binary field.
pub(crate) const MAX_FIELD_BYTES: usize = 8 << 20;
/// Maximum accumulated stdout for one `EXECUTE` request.
pub(crate) const MAX_STDOUT_BYTES: usize = 16 << 20;
/// Maximum accumulated payload for one `GET_FILE` request.
pub(crate) const MAX_FILE_BYTES: usize = 64 << 20;

/// Execution slot id, mirroring the `slot_id` bounds check of the Python
/// `execute`/`kill` requests (`0..=MAX_SLOTS`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SlotId(u8);

impl SlotId {
    /// Validate a raw slot id.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Ltx`] when `id` exceeds [`MAX_SLOTS`].
    pub fn new(id: u8) -> Result<Self, KirkError> {
        if id <= MAX_SLOTS {
            Ok(Self(id))
        } else {
            Err(KirkError::Ltx(format!(
                "Out of bounds slot ID [0-{MAX_SLOTS}]"
            )))
        }
    }

    /// Return the raw slot id.
    #[must_use]
    pub fn get(self) -> u8 {
        self.0
    }
}

/// Validate an `ENV`/`CWD` slot id (`0..=ALL_SLOTS`, `None` maps to the
/// broadcast id like the Python requests do).
///
/// # Errors
///
/// Returns [`KirkError::Ltx`] when `slot` exceeds [`ALL_SLOTS`].
fn check_broadcast_slot(slot: Option<u8>) -> Result<u8, KirkError> {
    match slot {
        None => Ok(ALL_SLOTS),
        Some(id) if id <= ALL_SLOTS => Ok(id),
        Some(_) => Err(KirkError::Ltx(format!(
            "Out of bounds slot ID [0-{ALL_SLOTS}]"
        ))),
    }
}

/// A single scalar msgpack value.
///
/// Nested arrays/maps are rejected at decode time, so decoded frames are
/// always flat (depth cap); binary data arrives as [`Field::Bin`], text as
/// [`Field::Str`], mirroring Python `msgpack` with `raw=False`.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Field {
    Nil,
    Bool(bool),
    U64(u64),
    I64(i64),
    F64(f64),
    Str(String),
    Bin(Vec<u8>),
}

impl Field {
    /// Interpret the field as an opcode byte.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Ltx`] for non-integer or out-of-range values.
    pub(crate) fn as_u8(&self) -> Result<u8, KirkError> {
        match *self {
            Self::U64(v) => u8::try_from(v).map_err(|_| {
                KirkError::Ltx(format!("integer {v} does not fit in a msgpack opcode"))
            }),
            Self::I64(v) => u8::try_from(v).map_err(|_| {
                KirkError::Ltx(format!("integer {v} does not fit in a msgpack opcode"))
            }),
            _ => Err(KirkError::Ltx(
                "message opcode must be an integer".to_string(),
            )),
        }
    }

    /// Interpret the field as a nanosecond timestamp.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Ltx`] for non-integer or negative values.
    pub(crate) fn as_u64(&self) -> Result<u64, KirkError> {
        match *self {
            Self::U64(v) => Ok(v),
            Self::I64(v) => {
                u64::try_from(v).map_err(|_| KirkError::Ltx(format!("negative timestamp {v}")))
            }
            _ => Err(KirkError::Ltx("expected an integer field".to_string())),
        }
    }

    /// Interpret the field as a 32-bit status code.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Ltx`] for non-integer or out-of-range values.
    pub(crate) fn as_i32(&self) -> Result<i32, KirkError> {
        let v = match *self {
            Self::U64(v) => i64::try_from(v)
                .map_err(|_| KirkError::Ltx(format!("integer {v} does not fit in i32")))?,
            Self::I64(v) => v,
            _ => return Err(KirkError::Ltx("expected an integer field".to_string())),
        };
        i32::try_from(v).map_err(|_| KirkError::Ltx(format!("integer {v} does not fit in i32")))
    }

    /// Interpret the field as text.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Ltx`] for non-string fields.
    pub(crate) fn as_str(&self) -> Result<&str, KirkError> {
        match self {
            Self::Str(s) => Ok(s),
            _ => Err(KirkError::Ltx("expected a string field".to_string())),
        }
    }

    /// Interpret the field as raw bytes (binary or text frames).
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Ltx`] for non-string, non-binary fields.
    pub(crate) fn as_bytes(&self) -> Result<&[u8], KirkError> {
        match self {
            Self::Bin(b) => Ok(b),
            Self::Str(s) => Ok(s.as_bytes()),
            _ => Err(KirkError::Ltx("expected a bytes field".to_string())),
        }
    }
}

/// Reject over-long fields; the streaming buffer cap already bounds the
/// transient allocation, this enforces the per-field policy.
fn check_field_len(len: usize) -> Result<(), String> {
    if len <= MAX_FIELD_BYTES {
        Ok(())
    } else {
        Err(format!(
            "field of {len} bytes exceeds {MAX_FIELD_BYTES} bytes"
        ))
    }
}

struct FieldVisitor;

impl<'de> Visitor<'de> for FieldVisitor {
    type Value = Field;

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("a scalar msgpack value")
    }

    fn visit_none<E: de::Error>(self) -> Result<Field, E> {
        Ok(Field::Nil)
    }

    fn visit_unit<E: de::Error>(self) -> Result<Field, E> {
        Ok(Field::Nil)
    }

    fn visit_bool<E: de::Error>(self, v: bool) -> Result<Field, E> {
        Ok(Field::Bool(v))
    }

    fn visit_u8<E: de::Error>(self, v: u8) -> Result<Field, E> {
        Ok(Field::U64(u64::from(v)))
    }

    fn visit_u16<E: de::Error>(self, v: u16) -> Result<Field, E> {
        Ok(Field::U64(u64::from(v)))
    }

    fn visit_u32<E: de::Error>(self, v: u32) -> Result<Field, E> {
        Ok(Field::U64(u64::from(v)))
    }

    fn visit_u64<E: de::Error>(self, v: u64) -> Result<Field, E> {
        Ok(Field::U64(v))
    }

    fn visit_i8<E: de::Error>(self, v: i8) -> Result<Field, E> {
        Ok(Field::I64(i64::from(v)))
    }

    fn visit_i16<E: de::Error>(self, v: i16) -> Result<Field, E> {
        Ok(Field::I64(i64::from(v)))
    }

    fn visit_i32<E: de::Error>(self, v: i32) -> Result<Field, E> {
        Ok(Field::I64(i64::from(v)))
    }

    fn visit_i64<E: de::Error>(self, v: i64) -> Result<Field, E> {
        Ok(Field::I64(v))
    }

    fn visit_f32<E: de::Error>(self, v: f32) -> Result<Field, E> {
        Ok(Field::F64(f64::from(v)))
    }

    fn visit_f64<E: de::Error>(self, v: f64) -> Result<Field, E> {
        Ok(Field::F64(v))
    }

    fn visit_str<E: de::Error>(self, v: &str) -> Result<Field, E> {
        check_field_len(v.len()).map_err(E::custom)?;
        Ok(Field::Str(v.to_owned()))
    }

    fn visit_string<E: de::Error>(self, v: String) -> Result<Field, E> {
        check_field_len(v.len()).map_err(E::custom)?;
        Ok(Field::Str(v))
    }

    fn visit_borrowed_str<E: de::Error>(self, v: &'de str) -> Result<Field, E> {
        check_field_len(v.len()).map_err(E::custom)?;
        Ok(Field::Str(v.to_owned()))
    }

    fn visit_bytes<E: de::Error>(self, v: &[u8]) -> Result<Field, E> {
        check_field_len(v.len()).map_err(E::custom)?;
        Ok(Field::Bin(v.to_vec()))
    }

    fn visit_borrowed_bytes<E: de::Error>(self, v: &'de [u8]) -> Result<Field, E> {
        check_field_len(v.len()).map_err(E::custom)?;
        Ok(Field::Bin(v.to_vec()))
    }

    fn visit_byte_buf<E: de::Error>(self, v: Vec<u8>) -> Result<Field, E> {
        check_field_len(v.len()).map_err(E::custom)?;
        Ok(Field::Bin(v))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, _seq: A) -> Result<Field, A::Error> {
        Err(de::Error::custom("nested arrays are not valid LTX fields"))
    }

    fn visit_map<A: de::MapAccess<'de>>(self, _map: A) -> Result<Field, A::Error> {
        Err(de::Error::custom("nested maps are not valid LTX fields"))
    }
}

impl<'de> Deserialize<'de> for Field {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(FieldVisitor)
    }
}

/// A decoded frame: a flat array of scalar fields.
#[derive(Debug)]
pub(crate) struct Frame(pub Vec<Field>);

impl Frame {
    pub(crate) fn into_inner(self) -> Vec<Field> {
        self.0
    }
}

struct FrameVisitor;

impl<'de> Visitor<'de> for FrameVisitor {
    type Value = Frame;

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("an LTX message array")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Frame, A::Error> {
        let mut fields = Vec::new();
        while let Some(field) = seq.next_element::<Field>()? {
            let count = fields
                .len()
                .checked_add(1)
                .ok_or_else(|| de::Error::custom("message field count overflow"))?;
            if count > MAX_FIELDS {
                return Err(de::Error::custom("message has too many fields"));
            }
            fields.push(field);
        }
        Ok(Frame(fields))
    }
}

impl<'de> Deserialize<'de> for Frame {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(FrameVisitor)
    }
}

/// Wrapper forcing BIN encoding for byte slices.
///
/// `rmp-serde` packs small all-`u8` tuples as int arrays by default, which the
/// LTX peer would not decode as file data; `serialize_bytes` always emits BIN.
pub(crate) struct Bin<'a>(pub(crate) &'a [u8]);

impl Serialize for Bin<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(self.0)
    }
}

/// Reply produced when a request completes; tuple order mirrors the Python
/// `gather` reply tuples.
#[derive(Debug, Clone, PartialEq)]
pub enum Reply {
    /// `VERSION` reply: peer version string.
    Version(String),
    /// `PONG` reply: peer timestamp in nanoseconds.
    Ping(u64),
    /// `ENV` echo: slot, key, value.
    Env {
        slot: u8,
        key: String,
        value: String,
    },
    /// `CWD` echo: slot, path.
    Cwd { slot: u8, path: String },
    /// `GET_FILE` echo: path plus the accumulated `DATA` payload.
    GetFile { path: String, data: Vec<u8> },
    /// `SET_FILE` echo: path plus the written payload.
    SetFile { path: String, data: Vec<u8> },
    /// `RESULT`: peer timestamp, `si_code`, `si_status` and accumulated stdout.
    Execute {
        time_ns: u64,
        si_code: i32,
        si_status: i32,
        stdout: String,
    },
    /// `KILL` echo: slot.
    Kill { slot: u8 },
}

/// LTX request state machine, mirroring one `Requests.*` Python class each.
#[derive(Debug)]
pub enum Request {
    /// Version request.
    Version(VersionReq),
    /// Ping request.
    Ping(PingReq),
    /// Environment request.
    Env(EnvReq),
    /// Working-directory request.
    Cwd(CwdReq),
    /// File-download request.
    GetFile(GetFileReq),
    /// File-upload request.
    SetFile(SetFileReq),
    /// Execute request.
    Execute(ExecuteReq),
    /// Kill request.
    Kill(KillReq),
}

impl Request {
    /// Create a `VERSION` request.
    #[must_use]
    pub fn version() -> Self {
        Self::Version(VersionReq { completed: false })
    }

    /// Create a `PING` request.
    #[must_use]
    pub fn ping() -> Self {
        Self::Ping(PingReq {
            completed: false,
            echoed: false,
        })
    }

    /// Create an `ENV` request for `slot` (`None` broadcasts to all slots).
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Ltx`] for out-of-range slots or empty key/value.
    pub fn env(slot: Option<u8>, key: &str, value: &str) -> Result<Self, KirkError> {
        let slot = check_broadcast_slot(slot)?;
        if key.is_empty() {
            return Err(KirkError::Ltx("key is empty".to_string()));
        }
        if value.is_empty() {
            return Err(KirkError::Ltx("value is empty".to_string()));
        }
        Ok(Self::Env(EnvReq {
            completed: false,
            slot,
            key: key.to_string(),
            value: value.to_string(),
        }))
    }

    /// Create a `CWD` request for `slot` (`None` broadcasts to all slots).
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Ltx`] for out-of-range slots or an empty path.
    pub fn cwd(slot: Option<u8>, path: &str) -> Result<Self, KirkError> {
        let slot = check_broadcast_slot(slot)?;
        if path.is_empty() {
            return Err(KirkError::Ltx("path is empty".to_string()));
        }
        Ok(Self::Cwd(CwdReq {
            completed: false,
            slot,
            path: path.to_string(),
        }))
    }

    /// Create a `GET_FILE` request.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Ltx`] for an empty path.
    pub fn get_file(path: &str) -> Result<Self, KirkError> {
        if path.is_empty() {
            return Err(KirkError::Ltx("path is empty".to_string()));
        }
        Ok(Self::GetFile(GetFileReq {
            completed: false,
            path: path.to_string(),
            data: Vec::new(),
        }))
    }

    /// Create a `SET_FILE` request.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Ltx`] for an empty path or empty data.
    pub fn set_file(path: &str, data: &[u8]) -> Result<Self, KirkError> {
        if path.is_empty() {
            return Err(KirkError::Ltx("path is empty".to_string()));
        }
        if data.is_empty() {
            return Err(KirkError::Ltx("data is empty".to_string()));
        }
        Ok(Self::SetFile(SetFileReq {
            completed: false,
            path: path.to_string(),
            data: data.to_vec(),
        }))
    }

    /// Create an `EXEC` request.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Ltx`] for an empty command.
    pub fn execute(slot: SlotId, command: &str) -> Result<Self, KirkError> {
        Self::execute_inner(slot, command, None)
    }

    /// Create an `EXEC` request streaming stdout chunks into `stdout_tx`.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Ltx`] for an empty command.
    pub(crate) fn execute_with_stdout(
        slot: SlotId,
        command: &str,
        stdout_tx: tokio::sync::mpsc::UnboundedSender<String>,
    ) -> Result<Self, KirkError> {
        Self::execute_inner(slot, command, Some(stdout_tx))
    }

    fn execute_inner(
        slot: SlotId,
        command: &str,
        stdout_tx: Option<tokio::sync::mpsc::UnboundedSender<String>>,
    ) -> Result<Self, KirkError> {
        if command.is_empty() {
            return Err(KirkError::Ltx("Command is empty".to_string()));
        }
        Ok(Self::Execute(ExecuteReq {
            completed: false,
            echoed: false,
            slot: slot.get(),
            command: command.to_string(),
            stdout_tx,
            stdout: String::new(),
        }))
    }

    /// Create a `KILL` request.
    #[must_use]
    pub fn kill(slot: SlotId) -> Self {
        Self::Kill(KillReq {
            completed: false,
            slot: slot.get(),
        })
    }

    /// Pack the request into msgpack bytes.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Ltx`] when serialization fails.
    pub(crate) fn pack(&self) -> Result<Vec<u8>, KirkError> {
        match self {
            Self::Version(_) => VersionReq::pack(),
            Self::Ping(_) => PingReq::pack(),
            Self::Env(req) => req.pack(),
            Self::Cwd(req) => req.pack(),
            Self::GetFile(req) => req.pack(),
            Self::SetFile(req) => req.pack(),
            Self::Execute(req) => req.pack(),
            Self::Kill(req) => req.pack(),
        }
    }

    /// Feed a decoded frame; returns the [`Reply`] exactly once, when the
    /// request completes.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Ltx`] on protocol violations (e.g. `PONG` without
    /// a `PING` echo) or malformed frames.
    pub(crate) fn feed(&mut self, msg: &[Field]) -> Result<Option<Reply>, KirkError> {
        match self {
            Self::Version(req) => req.feed(msg),
            Self::Ping(req) => req.feed(msg),
            Self::Env(req) => Ok(req.feed(msg)),
            Self::Cwd(req) => Ok(req.feed(msg)),
            Self::GetFile(req) => req.feed(msg),
            Self::SetFile(req) => Ok(req.feed(msg)),
            Self::Execute(req) => req.feed(msg),
            Self::Kill(req) => Ok(req.feed(msg)),
        }
    }
}

/// Opcode of a frame, or `None` when the frame is empty.
fn frame_opcode(msg: &[Field]) -> Option<u8> {
    msg.first()?.as_u8().ok()
}

/// Whether `msg[1]` addresses `slot`; frames without a slot field (such as a
/// bare `[PING]`) always match, mirroring the Python `len(message) > 1` guard.
fn frame_targets_slot(msg: &[Field], slot: u8) -> bool {
    if msg.len() <= 1 {
        return true;
    }
    matches!(msg.get(1), Some(Field::U64(id)) if *id == u64::from(slot))
}

/// `VERSION` request state.
#[derive(Debug)]
pub struct VersionReq {
    completed: bool,
}

impl VersionReq {
    fn pack() -> Result<Vec<u8>, KirkError> {
        rmp_serde::to_vec(&(OP_VERSION,))
            .map_err(|e| KirkError::Ltx(format!("Can't pack VERSION request: {e}")))
    }

    fn feed(&mut self, msg: &[Field]) -> Result<Option<Reply>, KirkError> {
        if self.completed {
            return Ok(None);
        }
        if frame_opcode(msg) == Some(OP_VERSION) {
            let version = msg
                .get(1)
                .ok_or_else(|| KirkError::Ltx("malformed VERSION reply".to_string()))?
                .as_str()?
                .to_string();
            self.completed = true;
            Ok(Some(Reply::Version(version)))
        } else {
            Ok(None)
        }
    }
}

/// `PING` request state.
#[derive(Debug)]
pub struct PingReq {
    completed: bool,
    echoed: bool,
}

impl PingReq {
    fn pack() -> Result<Vec<u8>, KirkError> {
        rmp_serde::to_vec(&(OP_PING,))
            .map_err(|e| KirkError::Ltx(format!("Can't pack PING request: {e}")))
    }

    fn feed(&mut self, msg: &[Field]) -> Result<Option<Reply>, KirkError> {
        if self.completed {
            return Ok(None);
        }
        match frame_opcode(msg) {
            Some(OP_PING) => {
                self.echoed = true;
                Ok(None)
            }
            Some(OP_PONG) => {
                if !self.echoed {
                    return Err(KirkError::Ltx(
                        "PONG received without PING echo".to_string(),
                    ));
                }
                let end_t = msg
                    .get(1)
                    .ok_or_else(|| KirkError::Ltx("malformed PONG reply".to_string()))?
                    .as_u64()?;
                self.completed = true;
                Ok(Some(Reply::Ping(end_t)))
            }
            _ => Ok(None),
        }
    }
}

/// `ENV` request state.
#[derive(Debug)]
pub struct EnvReq {
    completed: bool,
    slot: u8,
    key: String,
    value: String,
}

impl EnvReq {
    fn pack(&self) -> Result<Vec<u8>, KirkError> {
        rmp_serde::to_vec(&(OP_ENV, self.slot, &self.key, &self.value))
            .map_err(|e| KirkError::Ltx(format!("Can't pack ENV request: {e}")))
    }

    fn feed(&mut self, msg: &[Field]) -> Option<Reply> {
        if self.completed {
            return None;
        }
        if !frame_targets_slot(msg, self.slot) {
            return None;
        }
        if frame_opcode(msg) == Some(OP_ENV) {
            self.completed = true;
            Some(Reply::Env {
                slot: self.slot,
                key: self.key.clone(),
                value: self.value.clone(),
            })
        } else {
            None
        }
    }
}

/// `CWD` request state.
#[derive(Debug)]
pub struct CwdReq {
    completed: bool,
    slot: u8,
    path: String,
}

impl CwdReq {
    fn pack(&self) -> Result<Vec<u8>, KirkError> {
        rmp_serde::to_vec(&(OP_CWD, self.slot, &self.path))
            .map_err(|e| KirkError::Ltx(format!("Can't pack CWD request: {e}")))
    }

    fn feed(&mut self, msg: &[Field]) -> Option<Reply> {
        if self.completed {
            return None;
        }
        if !frame_targets_slot(msg, self.slot) {
            return None;
        }
        if frame_opcode(msg) == Some(OP_CWD) {
            self.completed = true;
            Some(Reply::Cwd {
                slot: self.slot,
                path: self.path.clone(),
            })
        } else {
            None
        }
    }
}

/// `GET_FILE` request state.
#[derive(Debug)]
pub struct GetFileReq {
    completed: bool,
    path: String,
    data: Vec<u8>,
}

impl GetFileReq {
    fn pack(&self) -> Result<Vec<u8>, KirkError> {
        rmp_serde::to_vec(&(OP_GET_FILE, &self.path))
            .map_err(|e| KirkError::Ltx(format!("Can't pack GET_FILE request: {e}")))
    }

    fn feed(&mut self, msg: &[Field]) -> Result<Option<Reply>, KirkError> {
        if self.completed {
            return Ok(None);
        }
        match frame_opcode(msg) {
            Some(OP_DATA) => {
                let chunk = msg
                    .get(1)
                    .ok_or_else(|| KirkError::Ltx("malformed DATA frame".to_string()))?
                    .as_bytes()?;
                let total =
                    self.data.len().checked_add(chunk.len()).ok_or_else(|| {
                        KirkError::Ltx("GET_FILE payload size overflow".to_string())
                    })?;
                if total > MAX_FILE_BYTES {
                    return Err(KirkError::Ltx(format!(
                        "GET_FILE payload of {total} bytes exceeds {MAX_FILE_BYTES} bytes"
                    )));
                }
                self.data.extend_from_slice(chunk);
                Ok(None)
            }
            Some(OP_GET_FILE) => {
                self.completed = true;
                Ok(Some(Reply::GetFile {
                    path: self.path.clone(),
                    data: std::mem::take(&mut self.data),
                }))
            }
            _ => Ok(None),
        }
    }
}

/// `SET_FILE` request state.
#[derive(Debug)]
pub struct SetFileReq {
    completed: bool,
    path: String,
    data: Vec<u8>,
}

impl SetFileReq {
    fn pack(&self) -> Result<Vec<u8>, KirkError> {
        rmp_serde::to_vec(&(OP_SET_FILE, &self.path, Bin(&self.data)))
            .map_err(|e| KirkError::Ltx(format!("Can't pack SET_FILE request: {e}")))
    }

    fn feed(&mut self, msg: &[Field]) -> Option<Reply> {
        if self.completed {
            return None;
        }
        if frame_opcode(msg) == Some(OP_SET_FILE)
            && msg.get(1).and_then(|f| f.as_str().ok()) == Some(self.path.as_str())
        {
            self.completed = true;
            Some(Reply::SetFile {
                path: self.path.clone(),
                data: std::mem::take(&mut self.data),
            })
        } else {
            None
        }
    }
}

/// `EXEC` request state.
#[derive(Debug)]
pub struct ExecuteReq {
    completed: bool,
    echoed: bool,
    slot: u8,
    command: String,
    stdout_tx: Option<tokio::sync::mpsc::UnboundedSender<String>>,
    stdout: String,
}

impl ExecuteReq {
    fn pack(&self) -> Result<Vec<u8>, KirkError> {
        rmp_serde::to_vec(&(OP_EXEC, self.slot, &self.command))
            .map_err(|e| KirkError::Ltx(format!("Can't pack EXEC request: {e}")))
    }

    fn feed(&mut self, msg: &[Field]) -> Result<Option<Reply>, KirkError> {
        if self.completed {
            return Ok(None);
        }
        if !frame_targets_slot(msg, self.slot) {
            return Ok(None);
        }
        match frame_opcode(msg) {
            Some(OP_EXEC) => {
                self.echoed = true;
                Ok(None)
            }
            Some(OP_LOG) => {
                if !self.echoed {
                    return Err(KirkError::Ltx("LOG received without EXEC echo".to_string()));
                }
                let log = msg
                    .get(3)
                    .ok_or_else(|| KirkError::Ltx("malformed LOG frame".to_string()))?
                    .as_str()?;
                if !log.is_empty() {
                    let total =
                        self.stdout.len().checked_add(log.len()).ok_or_else(|| {
                            KirkError::Ltx("EXEC stdout size overflow".to_string())
                        })?;
                    if total > MAX_STDOUT_BYTES {
                        return Err(KirkError::Ltx(format!(
                            "EXEC stdout of {total} bytes exceeds {MAX_STDOUT_BYTES} bytes"
                        )));
                    }
                    self.stdout.push_str(log);
                    if let Some(tx) = &self.stdout_tx {
                        let _ = tx.send(log.to_string());
                    }
                }
                Ok(None)
            }
            Some(OP_RESULT) => {
                if !self.echoed {
                    return Err(KirkError::Ltx(
                        "RESULT received without EXEC echo".to_string(),
                    ));
                }
                let time_ns = msg
                    .get(2)
                    .ok_or_else(|| KirkError::Ltx("malformed RESULT frame".to_string()))?
                    .as_u64()?;
                let si_code = msg
                    .get(3)
                    .ok_or_else(|| KirkError::Ltx("malformed RESULT frame".to_string()))?
                    .as_i32()?;
                let si_status = msg
                    .get(4)
                    .ok_or_else(|| KirkError::Ltx("malformed RESULT frame".to_string()))?
                    .as_i32()?;
                self.completed = true;
                Ok(Some(Reply::Execute {
                    time_ns,
                    si_code,
                    si_status,
                    stdout: std::mem::take(&mut self.stdout),
                }))
            }
            _ => Ok(None),
        }
    }
}

/// `KILL` request state.
#[derive(Debug)]
pub struct KillReq {
    completed: bool,
    slot: u8,
}

impl KillReq {
    fn pack(&self) -> Result<Vec<u8>, KirkError> {
        rmp_serde::to_vec(&(OP_KILL, self.slot))
            .map_err(|e| KirkError::Ltx(format!("Can't pack KILL request: {e}")))
    }

    fn feed(&mut self, msg: &[Field]) -> Option<Reply> {
        if self.completed {
            return None;
        }
        if !frame_targets_slot(msg, self.slot) {
            return None;
        }
        if frame_opcode(msg) == Some(OP_KILL) {
            self.completed = true;
            Some(Reply::Kill { slot: self.slot })
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode(bytes: &[u8]) -> Vec<Field> {
        crate::ltx::decode_one(bytes)
            .expect("decode succeeds")
            .expect("one full frame")
            .0
    }

    fn enc<T: serde::Serialize>(value: &T) -> Vec<Field> {
        let bytes = rmp_serde::to_vec(value).expect("test frame packs");
        decode(&bytes)
    }

    #[test]
    fn opcode_values_match_python() {
        assert_eq!(OP_ERROR, 0xFF);
        assert_eq!(OP_VERSION, 0x00);
        assert_eq!(OP_PING, 0x01);
        assert_eq!(OP_PONG, 0x02);
        assert_eq!(OP_GET_FILE, 0x03);
        assert_eq!(OP_SET_FILE, 0x04);
        assert_eq!(OP_ENV, 0x05);
        assert_eq!(OP_CWD, 0x06);
        assert_eq!(OP_EXEC, 0x07);
        assert_eq!(OP_RESULT, 0x08);
        assert_eq!(OP_LOG, 0x09);
        assert_eq!(OP_DATA, 0xA0);
        assert_eq!(OP_KILL, 0xA1);
        assert_eq!(MAX_SLOTS, 127);
        assert_eq!(ALL_SLOTS, 128);
    }

    #[test]
    fn slot_id_bounds() {
        assert_eq!(SlotId::new(0).expect("slot 0").get(), 0);
        assert_eq!(SlotId::new(127).expect("slot 127").get(), 127);
        assert!(SlotId::new(128).is_err());
    }

    #[test]
    fn pack_opcode_bytes() {
        assert_eq!(Request::version().pack().expect("pack"), vec![0x91, 0x00]);
        assert_eq!(Request::ping().pack().expect("pack"), vec![0x91, 0x01]);

        let fields = decode(
            &Request::env(Some(3), "K", "V")
                .expect("env")
                .pack()
                .expect("pack"),
        );
        assert_eq!(
            fields,
            vec![
                Field::U64(u64::from(OP_ENV)),
                Field::U64(3),
                Field::Str("K".to_string()),
                Field::Str("V".to_string()),
            ]
        );

        let fields = decode(
            &Request::cwd(None, "/tmp")
                .expect("cwd")
                .pack()
                .expect("pack"),
        );
        assert_eq!(fields[1], Field::U64(u64::from(ALL_SLOTS)));

        let kill = Request::kill(SlotId::new(9).expect("slot"));
        let fields = decode(&kill.pack().expect("pack"));
        assert_eq!(fields, vec![Field::U64(u64::from(OP_KILL)), Field::U64(9)]);

        let exec = Request::execute(SlotId::new(0).expect("slot"), "uname").expect("exec");
        let fields = decode(&exec.pack().expect("pack"));
        assert_eq!(
            fields,
            vec![
                Field::U64(u64::from(OP_EXEC)),
                Field::U64(0),
                Field::Str("uname".to_string()),
            ]
        );
    }

    #[test]
    fn set_file_packs_data_as_bin() {
        let data = vec![0u8, 1, 2, 250];
        let req = Request::set_file("/tmp/f", &data).expect("set_file");
        let packed = req.pack().expect("pack");
        let fields = decode(&packed);
        assert_eq!(
            fields,
            vec![
                Field::U64(u64::from(OP_SET_FILE)),
                Field::Str("/tmp/f".to_string()),
                Field::Bin(data),
            ]
        );
    }

    #[test]
    fn constructor_validation() {
        assert!(Request::env(Some(129), "K", "V").is_err());
        assert!(Request::env(Some(1), "", "V").is_err());
        assert!(Request::env(Some(1), "K", "").is_err());
        assert!(Request::cwd(Some(1), "").is_err());
        assert!(Request::get_file("").is_err());
        assert!(Request::set_file("", b"x").is_err());
        assert!(Request::set_file("/x", b"").is_err());
        assert!(Request::execute(SlotId::new(0).expect("slot"), "").is_err());
    }

    #[test]
    fn version_round_trip() {
        let mut req = Request::version();
        let reply = req
            .feed(&enc(&(OP_VERSION, "0.1")))
            .expect("feed")
            .expect("completed");
        assert_eq!(reply, Reply::Version("0.1".to_string()));
        assert!(req.feed(&[]).expect("feed").is_none());
    }

    #[test]
    fn ping_needs_echo() {
        let mut req = Request::ping();
        let pong = enc(&(OP_PONG, 7_u64));
        assert!(req.feed(&pong).is_err());

        let mut req = Request::ping();
        let ping_echo = enc(&(OP_PING,));
        assert!(req.feed(&ping_echo).expect("echo").is_none());
        let reply = req.feed(&pong).expect("pong").expect("completed");
        assert_eq!(reply, Reply::Ping(7));
    }

    #[test]
    fn env_echo_and_slot_filter() {
        let mut req = Request::env(Some(2), "HELLO", "CIAO").expect("env");
        let other = enc(&(OP_ENV, 5_u8, "HELLO", "CIAO"));
        assert!(req.feed(&other).expect("feed").is_none());
        let echo = enc(&(OP_ENV, 2_u8, "HELLO", "CIAO"));
        let reply = req.feed(&echo).expect("feed").expect("completed");
        assert_eq!(
            reply,
            Reply::Env {
                slot: 2,
                key: "HELLO".to_string(),
                value: "CIAO".to_string(),
            }
        );
    }

    #[test]
    fn get_file_accumulates_data() {
        let mut req = Request::get_file("/tmp/f").expect("get_file");
        let data = enc(&(OP_DATA, Bin(&[1_u8, 2, 3])));
        assert!(req.feed(&data).expect("feed").is_none());
        let data = enc(&(OP_DATA, Bin(&[4_u8, 5])));
        assert!(req.feed(&data).expect("feed").is_none());
        let echo = enc(&(OP_GET_FILE, "/tmp/f"));
        let reply = req.feed(&echo).expect("feed").expect("completed");
        assert_eq!(
            reply,
            Reply::GetFile {
                path: "/tmp/f".to_string(),
                data: vec![1, 2, 3, 4, 5],
            }
        );
    }

    #[test]
    fn execute_flow_with_stdout_callback() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let mut req =
            Request::execute_with_stdout(SlotId::new(0).expect("slot"), "uname", tx).expect("exec");

        let echo = enc(&(OP_EXEC, 0_u8));
        assert!(req.feed(&echo).expect("feed").is_none());

        let log = enc(&(OP_LOG, 0_u8, 0_u8, "Linux\n"));
        assert!(req.feed(&log).expect("feed").is_none());
        assert_eq!(rx.try_recv().expect("stdout chunk"), "Linux\n");

        let result = enc(&(OP_RESULT, 0_u8, 1_000_u64, 1_u8, 0_u8));
        let reply = req.feed(&result).expect("feed").expect("completed");
        assert_eq!(
            reply,
            Reply::Execute {
                time_ns: 1_000,
                si_code: 1,
                si_status: 0,
                stdout: "Linux\n".to_string(),
            }
        );
    }

    #[test]
    fn execute_guards() {
        let mut req = Request::execute(SlotId::new(0).expect("slot"), "uname").expect("exec");
        let log = enc(&(OP_LOG, 0_u8, 0_u8, "x"));
        assert!(req.feed(&log).is_err());

        let mut req = Request::execute(SlotId::new(0).expect("slot"), "uname").expect("exec");
        let result = enc(&(OP_RESULT, 0_u8, 1_u64, 1_u8, 0_u8));
        assert!(req.feed(&result).is_err());
    }

    #[test]
    fn decoder_rejects_nesting_and_overflow() {
        // `[PING, [1]]`: nested array, hand-encoded (fixarray(2), 1, fixarray(1), 1).
        let nested = vec![0x92, 0x01, 0x91, 0x01];
        assert!(crate::ltx::decode_one(&nested).is_err());

        // Array of MAX_FIELDS + 1 ints, hand-encoded (array16 header).
        let mut wide = vec![
            0xDC,
            0x00,
            u8::try_from(MAX_FIELDS + 1).expect("small count"),
        ];
        wide.extend(std::iter::repeat_n(0x01, MAX_FIELDS + 1));
        assert!(crate::ltx::decode_one(&wide).is_err());

        assert!(crate::ltx::decode_one(&[0x91]).expect("partial").is_none());
        assert!(crate::ltx::decode_one(&[]).expect("empty").is_none());
    }
}
