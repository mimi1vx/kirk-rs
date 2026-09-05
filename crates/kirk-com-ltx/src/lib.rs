//! LTX communication channel ported from `kirk/libkirk/channels/ltx.py`
//! (`Request`/`Requests`/`LTX`) and `kirk/libkirk/channels/ltx_chan.py`
//! (`LTXComChannel`).
//!
//! The wire format is msgpack (via `rmp-serde`, byte-compatible with the
//! Python `msgpack<=1.1.2` packing): every frame is a flat array whose first
//! element is an opcode byte.

pub mod channel;
pub mod ltx;
pub mod request;

pub use channel::LtxChannel;
pub use ltx::Ltx;
pub use request::{
    ALL_SLOTS, MAX_SLOTS, OP_CWD, OP_DATA, OP_ENV, OP_ERROR, OP_EXEC, OP_GET_FILE, OP_KILL, OP_LOG,
    OP_PING, OP_PONG, OP_RESULT, OP_SET_FILE, OP_VERSION, Reply, Request, SlotId,
};
