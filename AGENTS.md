# AGENTS.md

Rust workspace (`resolver = "2"`, `edition = "2024"`), members in `crates/*` (14 crates).

## Layout

- Binary: `crates/kirk-cli` (`kirk` binary). `main.rs` parses args, validates, runs session; `lib.rs` exposes `args`, `session`, `validate`.
- `kirk-core` holds shared `Test`/`Suite` types, result counters, `KirkError`. `kirk-plugin` holds the minimal `Plugin` trait.
- Channels (`kirk-com-*`: `shell`, `ssh`, `qemu`, `ltx`) are **statically linked** into the `kirk` binary. Do not attempt dynamic/dylib channel plugins (removed: a dylib links its own tokio copy whose runtime thread-locals can't see the host runtime).
- Error convention: library code returns `kirk_core::KirkError`; only `kirk-cli/src/main.rs` uses `anyhow` (rendering at the binary edge).

## Commands

```bash
cargo build
cargo test
cargo run -p kirk-cli -- --help
cargo test -p <crate>          # focused, e.g. -p kirk-session
cargo clippy --all-targets     # workspace sets clippy `all = "deny"`, `pedantic = "warn"`
cargo fmt --check
```

No CI workflows, rust-toolchain file, or `.cargo/config.toml` in repo; default `cargo` behavior applies.

## Testing quirks

- Plain `cargo test` is safe offline: live tests are `#[ignore]`d and no-op without env vars. Run them only with the documented vars:
  - SSH: `TEST_SSH_HOST` (+ `TEST_SSH_USER`, `TEST_SSH_KEY_FILE`, optional `PORT`/`PASSWORD`/`KNOWN_HOSTS`/`SUDO`/`RESET_CMD`/`FETCH_PATH`) → `cargo test -p kirk-com-ssh -- --ignored`
  - QEMU: `TEST_QEMU_IMAGE`/`TEST_QEMU_USERNAME`/`TEST_QEMU_PASSWORD` (+ `KERNEL`/`BUSYBOX` for busybox case) → `cargo test -p kirk-com-qemu -- --ignored`
  - LTX: `TEST_LTX_BINARY` → `cargo test -p kirk-com-ltx -- --ignored`
- LTP unit tests build a mock LTP root in a tempdir; no real LTP tree needed. Real suite runs need `$LTPROOT` or `/opt/ltp` with `runtest/*` files.
- Exit codes from `kirk` binary: `0` ok, `1` session failure, `2` argument/validation error, `130` interrupted.

## Notes

- `plans/` is untracked scaffolding, not source of truth; never reference it in code, comments, or commits.
