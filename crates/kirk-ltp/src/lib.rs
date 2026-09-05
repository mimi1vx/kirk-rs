//! LTP framework ported from `kirk/libkirk/framework.py` + `kirk/libkirk/ltp.py`.
//!
//! [`Framework`] mirrors the upstream `Framework` base class and
//! [`LtpFramework`] mirrors `LTPFramework`. Upstream is read-only: this crate
//! depends only on the stable [`kirk_com`], [`kirk_core`] APIs, never on
//! `kirk-sut` (which evolves concurrently).

pub mod framework;
pub mod ltp;
pub mod parse;

pub use framework::Framework;
pub use ltp::LtpFramework;
