//! Pure parsing helpers ported from `kirk/libkirk/ltp.py`.
//!
//! The regexes below are linear-time (no nested quantifiers), so hostile
//! command output cannot trigger catastrophic backtracking. Inputs are
//! size-capped before parsing.

use std::sync::OnceLock;

use kirk_core::KirkError;
use kirk_core::results::ResultStatus;
use regex::Regex;

/// Cap for stdout bytes inspected by `read_result`.
///
/// Past the cap the output is truncated on a char boundary, so a runaway
/// test cannot OOM the runner.
pub const MAX_STDOUT_BYTES: usize = 8 * 1024 * 1024;

/// Cap for `ltp.json` metadata bytes accepted by `find_suite`.
pub const MAX_METADATA_BYTES: usize = 8 * 1024 * 1024;

/// Parsed `TPASS`/`TFAIL`/`TBROK`/`TSKIP`/`TWARN` counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Counts {
    /// Number of passed tests.
    pub passed: u32,
    /// Number of failed tests.
    pub failed: u32,
    /// Number of broken tests.
    pub broken: u32,
    /// Number of skipped tests.
    pub skipped: u32,
    /// Number of warnings.
    pub warnings: u32,
}

fn cmd_matcher() -> &'static Regex {
    static MATCHER: OnceLock<Regex> = OnceLock::new();
    MATCHER.get_or_init(|| {
        Regex::new(r#"(?:"[^"]*"|'[^']*'|\S+)"#).expect("cmd matcher is a static literal")
    })
}

/// Split a runtest line into command + arguments.
///
/// Quoted groups stay intact with quotes retained, mirroring Python
/// `findall`, e.g. `cmd -c "a b"` splits into `cmd`, `-c`, `"a b"`.
///
/// # Panics
///
/// Panics only if the static matcher fails to compile, which cannot happen
/// for a literal pattern.
#[must_use]
pub fn split_cmd_args(line: &str) -> Vec<String> {
    cmd_matcher()
        .find_iter(line)
        .map(|mat| mat.as_str().to_owned())
        .collect()
}

fn ansi_escape() -> &'static Regex {
    static MATCHER: OnceLock<Regex> = OnceLock::new();
    MATCHER.get_or_init(|| {
        Regex::new("\u{1b}\\[[0-9;]+[a-zA-Z]").expect("ANSI regex is a static literal")
    })
}

/// Strip ANSI color escapes from test output.
///
/// # Panics
///
/// Panics only if the static pattern fails to compile, which cannot happen
/// for a literal pattern.
#[must_use]
pub fn strip_ansi(stdout: &str) -> String {
    ansi_escape().replace_all(stdout, "").into_owned()
}

fn summary_pattern() -> &'static Regex {
    static MATCHER: OnceLock<Regex> = OnceLock::new();
    MATCHER.get_or_init(|| {
        Regex::new(
            "Summary:\npassed\\s*(?P<passed>\\d+)\nfailed\\s*(?P<failed>\\d+)\nbroken\\s*(?P<broken>\\d+)\nskipped\\s*(?P<skipped>\\d+)\nwarnings\\s*(?P<warnings>\\d+)\n",
        )
        .expect("summary regex is a static literal")
    })
}

/// Parse a `Summary:` block; returns `None` when absent or unparsable.
///
/// # Panics
///
/// Panics only if the static pattern fails to compile, which cannot happen
/// for a literal pattern.
#[must_use]
pub fn parse_summary(stdout: &str) -> Option<Counts> {
    let captures = summary_pattern().captures(stdout)?;
    let get = |name: &str| -> Option<u32> { captures.name(name)?.as_str().parse::<u32>().ok() };
    Some(Counts {
        passed: get("passed")?,
        failed: get("failed")?,
        broken: get("broken")?,
        skipped: get("skipped")?,
        warnings: get("warnings")?,
    })
}

/// Count `TPASS`/`TFAIL`/`TBROK`/`TSKIP`/`TWARN` markers via substring scan.
#[must_use]
pub fn count_markers(stdout: &str) -> Counts {
    let count = |marker: &str| u32::try_from(stdout.matches(marker).count()).unwrap_or(u32::MAX);
    Counts {
        passed: count("TPASS"),
        failed: count("TFAIL"),
        broken: count("TBROK"),
        skipped: count("TSKIP"),
        warnings: count("TWARN"),
    }
}

/// Map an LTP return code to a [`ResultStatus`], mirroring `_RETCODE_STATUS`.
#[must_use]
pub fn retcode_status(retcode: i32) -> i32 {
    match retcode {
        0 => ResultStatus::PASS,
        2 | -1 => ResultStatus::BROK,
        4 => ResultStatus::WARN,
        32 => ResultStatus::CONF,
        _ => ResultStatus::FAIL,
    }
}

/// Reject suite names that could escape the runtest directory on fetch.
///
/// Upstream performs no check; this is a hardening addition so a hostile
/// `name` cannot turn `fetch_file` into a path traversal.
///
/// # Errors
///
/// Returns [`KirkError::Framework`] when `name` contains a separator,
/// parent reference, or NUL byte.
pub fn validate_suite_name(name: &str) -> Result<(), KirkError> {
    if name.contains('/') || name.contains('\\') || name.contains('\0') || name.contains("..") {
        return Err(KirkError::Framework(format!(
            "invalid suite name: {name:?}"
        )));
    }
    Ok(())
}

/// Truncate `stdout` to [`MAX_STDOUT_BYTES`] on a char boundary.
#[must_use]
pub fn truncate_stdout(stdout: &str) -> &str {
    if stdout.len() <= MAX_STDOUT_BYTES {
        stdout
    } else {
        let end = stdout.floor_char_boundary(MAX_STDOUT_BYTES);
        &stdout[..end]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_keeps_quoted_groups() {
        assert_eq!(
            split_cmd_args(r#"cmd -c "cmd2 -g arg1 -t arg2""#),
            vec!["cmd", "-c", "\"cmd2 -g arg1 -t arg2\""]
        );
        assert_eq!(
            split_cmd_args("test.sh ciao bepi"),
            vec!["test.sh", "ciao", "bepi"]
        );
        assert_eq!(split_cmd_args("   "), [] as [String; 0]);
    }

    #[test]
    fn ansi_stripped() {
        assert_eq!(strip_ansi("\u{1b}[32mTPASS\u{1b}[0m: ok"), "TPASS: ok");
        assert_eq!(strip_ansi("plain"), "plain");
    }

    #[test]
    fn summary_parsed() {
        let stdout =
            "some output\nSummary:\npassed   3\nfailed   1\nbroken   0\nskipped  2\nwarnings 1\n";
        assert_eq!(
            parse_summary(stdout),
            Some(Counts {
                passed: 3,
                failed: 1,
                broken: 0,
                skipped: 2,
                warnings: 1,
            })
        );
        assert_eq!(parse_summary("no summary here"), None);
    }

    #[test]
    fn markers_counted() {
        let counts = count_markers("test 1 TPASS: ok\ntest 2 TPASS: ok\ntest 3 TFAIL: bad\n");
        assert_eq!(counts.passed, 2);
        assert_eq!(counts.failed, 1);
        assert_eq!(counts.broken, 0);
    }

    #[test]
    fn retcode_mapping_matches_upstream() {
        assert_eq!(retcode_status(0), ResultStatus::PASS);
        assert_eq!(retcode_status(2), ResultStatus::BROK);
        assert_eq!(retcode_status(-1), ResultStatus::BROK);
        assert_eq!(retcode_status(4), ResultStatus::WARN);
        assert_eq!(retcode_status(32), ResultStatus::CONF);
        assert_eq!(retcode_status(1), ResultStatus::FAIL);
        assert_eq!(retcode_status(99), ResultStatus::FAIL);
    }

    #[test]
    fn traversal_names_rejected() {
        for name in ["../evil", "a/b", "..", "a\\b", "a\0b"] {
            assert!(
                validate_suite_name(name).is_err(),
                "{name:?} must be rejected"
            );
        }
        assert!(validate_suite_name("suite0").is_ok());
    }
}
