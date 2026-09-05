//! `kirk` command-line interface: clap parsing, validation, and session
//! startup ported from `kirk/libkirk/main.py`.
//!
//! Library code reports failures as [`kirk_core::KirkError`]; only the
//! binary in `main.rs` renders them through `anyhow`.

pub mod args;
pub mod session;
pub mod validate;
