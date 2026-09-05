# kirk-rs

Kirk is an all-in-one Linux testing framework. This repository is a Rust workspace (`cargo`) that runs LTP-style test suites against a System Under Test (SUT) reachable through a communication channel.

## Workspace

| Crate | Purpose |
|---|---|
| `kirk-cli` | `kirk` binary: argument parsing, validation, session wiring |
| `kirk-core` | Shared `Test`/`Suite` types, result counters, error enum |
| `kirk-plugin` | Minimal `Plugin` trait (setup, config help, boxed clone) |
| `kirk-com` | `ComChannel` trait plus `libloading`-based channel registry |
| `kirk-com-shell` | Local process execution channel |
| `kirk-com-ssh` | SSH execution channel library |
| `kirk-com-qemu` | QEMU guest-serial execution channel library |
| `kirk-com-ltx` | LTX msgpack-over-FIFO execution channel library |
| `kirk-sut` | SUT abstraction with host probes, taint tracking, fault injection |
| `kirk-ltp` | LTP framework: runtest parsing, suite discovery, result parsing |
| `kirk-scheduler` | Test and suite schedulers (parallelism, timeouts, retries) |
| `kirk-session` | Session orchestration: filtering, iteration, restore, export |
| `kirk-events` | Async event registry backing UI, monitor, and hooks |
| `kirk-support` | Temp dirs, file I/O, JSON report export, monitor, console UIs |

## Prerequisites

- Rust toolchain that supports `edition = "2024"` with `cargo`.
- A writable temporary directory (default `/tmp`, must exist).
- An LTP tree at `$LTPROOT` or `/opt/ltp` with `runtest/*` files for suite runs.
- QEMU binaries and image files for QEMU runs; a reachable SSH server with `known_hosts` for SSH runs; a FIFO pair for LTX runs.

## Build and test

```bash
cargo build
cargo test
cargo run -p kirk-cli -- --help
```

## Usage

```text
kirk [OPTIONS]
```

General options:

- `-v, --verbose`: verbose output.
- `-n, --no-colors`: disable colors.
- `-d, --tmp-dir <TMP_DIR>`: temporary directory (default `/tmp`).
- `-r, --restore <RESTORE>`: restore a session from a directory containing an `executed` file.
- `-o, --json-report <JSON_REPORT>`: write a JSON report to a path that must not exist yet.
- `-m, --monitor <MONITOR>`: append single-line JSON events to a file whose parent directory must exist.
- `-P, --plugins <PLUGINS>`: directory of external `*.so`/`*.dylib`/`*.dll` channel plugins exporting `kirk_plugin`.

Configuration options:

- `-C, --com <COM>`: communication channel parameters, repeatable, as `name:key=value:...`. `--com help` lists supported channels.
- `-u, --sut <SUT>`: System Under Test parameters (default `default`). `--sut help` lists supported SUTs.
- `-s, --skip-tests <SKIP_TESTS>`: skip tests matching a regex.
- `-S, --skip-file <SKIP_FILE>`: skip file with one regex per line; blank lines and `#` comments are ignored.

Execution options:

- `-f, --run-suite [<RUN_SUITE>...]`: suites to run.
- `-p, --run-pattern <RUN_PATTERN>`: regex selecting tests within the suites; requires `--run-suite`.
- `-c, --run-command <RUN_COMMAND>`: run a single command instead of suites.
- `-T, --suite-timeout <SUITE_TIMEOUT>`: per-suite timeout (default `1h`).
- `-t, --exec-timeout <EXEC_TIMEOUT>`: per-execution timeout (default `1h`). Durations accept `30s`, `4m`, `5h`, `20d`; a bare number means seconds.
- `-R, --randomize`: randomize test execution order.
- `-I, --runtime <RUNTIME>`: session runtime in seconds (default `0`, meaning run once).
- `-i, --suite-iterate <SUITE_ITERATE>`: repeat suites N times (default `1`); repeats are named `suite[i]`.
- `-w, --workers <WORKERS>`: parallel workers (default `1`).
- `-W, --force-parallel`: force parallel execution of all tests.
- `-F, --fault-injection <FAULT_INJECTION>`: fault-injection probability `0-100` (default `0`).
- `--fault-interval <FAULT_INTERVAL>`: fault-injection interval (default `1`).
- `-O, --optimize-sut`: query SUT host info in parallel where the channel allows it.
- `-D, --dry-run`: list selected tests without executing them.

## Channels and SUTs

`--com help` reports the channels wired into the CLI:

```text
--com option supports the following syntax:

	<name>:<param1>=<value1>:<param2>=<value2>:..
```

The `shell` channel runs commands locally and takes no configuration. `kirk-com-ssh`, `kirk-com-qemu`, and `kirk-com-ltx` are libraries in this workspace and are not wired into the CLI channel setup.

`--sut help` reports the SUTs wired into the CLI. The `default` SUT takes one key:

```text
com: Communication channel name (default: shell)
```

Additional channels are attached with repeated `--com` flags and referenced by SUT config; a channel instance is cloned with `id=<name>`.

## Test selection and execution

At least one of `--run-suite` or `--run-command` is required. `--run-pattern` filters tests inside the given suites. Skip rules combine the `--skip-tests` regex with the `--skip-file` entries. A suite run requires the named runtest file under `<ltp-root>/runtest/<name>`; an optional `metadata/ltp.json` caps at 8 MiB.

Non-parallelizable tests always run sequentially; parallelizable tests run on up to `--workers` concurrent slots. `--force-parallel` runs everything concurrently. `--workers 1` runs everything sequentially.

Timeouts: a per-test expiry records a timeout result; a kernel timeout also triggers SUT restart handling. A per-suite expiry marks leftover tests as `CONF` and emits a suite-timeout event.

## Sessions, restore, and reports

Each run appends `suite::test` lines to an `executed` file under the session temp dir. `--restore <dir>` reads that file and skips already-executed tests. Every run writes `results.json` into the session temp dir and additionally to `--json-report` when given. `--monitor` receives one JSON object per event line. Kernel panic, taint, and SUT-not-responding conditions are reported as events and reflected in results.

## Exit codes

- `0`: success.
- `1`: session failure.
- `2`: argument or validation error.
- `130`: interrupted (Ctrl-C).
