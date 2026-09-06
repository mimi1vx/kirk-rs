//! System under test, ported from `kirk/libkirk/sut.py` and
//! `kirk/libkirk/sut_base.py`.
//!
//! [`Sut`] is the plugin interface with upstream-defaulted probes;
//! [`GenericSut`] attaches those defaults to one looked-up channel.

pub mod generic;
pub mod redirect;
pub mod sut;

pub use generic::GenericSut;
pub use redirect::{
    RUN_CMD_STDOUT_EVENT, RedirectSutStdout, RedirectTestStdout, SUT_STDOUT_EVENT,
    TEST_STDOUT_EVENT,
};
pub use sut::{FAULT_INJECTION_FILES, Sut, SutInfo, TaintBegin, TaintedInfo};
