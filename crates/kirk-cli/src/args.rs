//! Clap parser ported flag-for-flag from `main.py::run`.
//!
//! Value parsers mirror `_time_config`, `_iterate_config`,
//! `_finjection_config`, `_finterval_config`, and the `name:k=v` dict
//! parsers. Error strings keep upstream wording so `--help` and failure
//! output stay comparable. Config values are never logged: validation
//! errors name the plugin, never its `k=v` parameters.

use std::collections::HashMap;

use clap::Parser;

/// Maximum number of communication objects, mirroring `MAX_COM_INSTANCES`.
pub const MAX_COM_INSTANCES: usize = 128;

/// Success return code, mirroring `RC_OK`.
pub const RC_OK: i32 = 0;
/// Generic failure return code, mirroring `RC_ERROR`.
pub const RC_ERROR: i32 = 1;
/// Keyboard-interrupt return code, mirroring `RC_INTERRUPT`.
pub const RC_INTERRUPT: i32 = 130;

/// Parse `30s`, `4m`, `5h`, `20d` (bare numbers mean seconds).
///
/// # Errors
///
/// Returns a message when the format is not `<digits>[smhd>]`.
pub fn parse_time_config(value: &str) -> Result<u64, String> {
    let indata = value.trim();
    if indata.is_empty() {
        return Err(format!("Incorrect time format '{value}'"));
    }
    let (digits, mult) = match indata.as_bytes()[indata.len() - 1] {
        b's' => (&indata[..indata.len() - 1], 1_u64),
        b'm' => (&indata[..indata.len() - 1], 60_u64),
        b'h' => (&indata[..indata.len() - 1], 3600_u64),
        b'd' => (&indata[..indata.len() - 1], 86_400_u64),
        byte if byte.is_ascii_digit() => (indata, 1_u64),
        _ => return Err(format!("Incorrect time format '{indata}'")),
    };
    let amount: u64 = digits
        .trim_end()
        .parse()
        .map_err(|_| format!("Incorrect time format '{indata}'"))?;
    amount
        .checked_mul(mult)
        .ok_or_else(|| format!("Incorrect time format '{indata}'"))
}

/// Parse the suite-iterate value, clamping to `>= 1`.
///
/// # Errors
///
/// Returns a message when the value is not a number.
pub fn parse_iterate(value: &str) -> Result<usize, String> {
    if value.is_empty() {
        return Ok(1);
    }
    let ret: i64 = value.parse().map_err(|_| String::from("Invalid number"))?;
    Ok(usize::try_from(ret.max(1)).unwrap_or(1))
}

/// Parse the fault-injection probability, clamping to `0..=100`.
///
/// # Errors
///
/// Returns a message when the value is not a number.
pub fn parse_finjection(value: &str) -> Result<u32, String> {
    if value.is_empty() {
        return Ok(0);
    }
    let ret: i64 = value.parse().map_err(|_| String::from("Invalid number"))?;
    Ok(u32::try_from(ret.clamp(0, 100)).unwrap_or(0))
}

/// Parse the fault-injection interval, clamping to `>= 1`.
///
/// # Errors
///
/// Returns a message when the value is not a number.
pub fn parse_finterval(value: &str) -> Result<u32, String> {
    if value.is_empty() {
        return Ok(1);
    }
    let ret: i64 = value.parse().map_err(|_| String::from("Invalid number"))?;
    Ok(u32::try_from(ret.max(1)).unwrap_or(1))
}

/// Parse a `name:k=v:k=v` dict, mirroring `_dict_config`.
///
/// # Errors
///
/// Returns a message on empty input, a missing `=`, or an empty key/value.
pub fn parse_dict_config(value: &str) -> Result<HashMap<String, String>, String> {
    if value == "help" {
        return Ok(HashMap::from([(String::from("help"), String::new())]));
    }
    if value.is_empty() {
        return Err(String::from("Parameters list can't be empty"));
    }
    let mut parts = value.split(':');
    let name = parts.next().unwrap_or_default();
    let mut config = HashMap::new();
    for param in parts {
        let (key, param_value) = param
            .split_once('=')
            .ok_or_else(|| format!("Missing '=' assignment in '{param}' parameter"))?;
        if key.is_empty() {
            return Err(format!("Empty key for '{param}' parameter"));
        }
        if param_value.is_empty() {
            return Err(format!("Empty value for '{param}' parameter"));
        }
        config.insert(key.to_owned(), param_value.to_owned());
    }
    config.insert(String::from("name"), name.to_owned());
    Ok(config)
}

/// Command-line arguments, flag-for-flag with upstream argparse groups.
#[allow(
    clippy::struct_excessive_bools,
    reason = "flag-for-flag port of the argparse options"
)]
#[derive(Parser, Debug)]
#[command(
    name = "kirk",
    version,
    about = "Kirk - All-in-one Linux Testing Framework"
)]
pub struct Args {
    /// Verbose mode.
    #[arg(short = 'v', long, help_heading = "General options")]
    pub verbose: bool,

    /// If defined, no colors are shown.
    #[arg(short = 'n', long = "no-colors", help_heading = "General options")]
    pub no_colors: bool,

    /// Temporary directory.
    #[arg(
        short = 'd',
        long = "tmp-dir",
        default_value = "/tmp",
        help_heading = "General options"
    )]
    pub tmp_dir: String,

    /// Restore a specific session.
    #[arg(short = 'r', long, help_heading = "General options")]
    pub restore: Option<String>,

    /// JSON output report.
    #[arg(short = 'o', long = "json-report", help_heading = "General options")]
    pub json_report: Option<String>,

    /// Location of the monitor file.
    #[arg(short = 'm', long, help_heading = "General options")]
    pub monitor: Option<String>,

    /// Communication channel parameters. For help please use '--com help'.
    #[arg(short = 'C', long, value_parser = parse_dict_config, help_heading = "Configuration options")]
    pub com: Vec<HashMap<String, String>>,

    /// System Under Test parameters. For help please use '--sut help'.
    #[arg(short = 'u', long, default_value = "default", value_parser = parse_dict_config, help_heading = "Configuration options")]
    pub sut: HashMap<String, String>,

    /// Skip specific tests.
    #[arg(
        short = 's',
        long = "skip-tests",
        help_heading = "Configuration options"
    )]
    pub skip_tests: Option<String>,

    /// Skip specific tests using a skip file (newline separated item).
    #[arg(
        short = 'S',
        long = "skip-file",
        help_heading = "Configuration options"
    )]
    pub skip_file: Option<String>,

    /// List of suites to run.
    #[arg(short = 'f', long = "run-suite", num_args = 0.., help_heading = "Execution options")]
    pub run_suite: Option<Vec<String>>,

    /// Run all tests matching the regex pattern.
    #[arg(short = 'p', long = "run-pattern", help_heading = "Execution options")]
    pub run_pattern: Option<String>,

    /// Command to run.
    #[arg(short = 'c', long = "run-command", help_heading = "Execution options")]
    pub run_command: Option<String>,

    /// Timeout before stopping the suite (default: 1h).
    #[arg(short = 'T', long = "suite-timeout", default_value = "1h", value_parser = parse_time_config, help_heading = "Execution options")]
    pub suite_timeout: u64,

    /// Timeout before stopping a single execution (default: 1h).
    #[arg(short = 't', long = "exec-timeout", default_value = "1h", value_parser = parse_time_config, help_heading = "Execution options")]
    pub exec_timeout: u64,

    /// Randomize tests execution order.
    #[arg(short = 'R', long, help_heading = "Execution options")]
    pub randomize: bool,

    /// Set for how long we want to run the session in seconds.
    #[arg(short = 'I', long, default_value = "0", value_parser = parse_time_config, help_heading = "Execution options")]
    pub runtime: u64,

    /// Number of times to repeat testing suites.
    #[arg(short = 'i', long = "suite-iterate", default_value = "1", value_parser = parse_iterate, help_heading = "Execution options")]
    pub suite_iterate: usize,

    /// Number of workers to execute tests in parallel.
    #[arg(
        short = 'w',
        long,
        default_value_t = 1,
        help_heading = "Execution options"
    )]
    pub workers: usize,

    /// Force parallelization execution of all tests.
    #[arg(
        short = 'W',
        long = "force-parallel",
        help_heading = "Execution options"
    )]
    pub force_parallel: bool,

    /// Probability of failure (0-100).
    #[arg(short = 'F', long = "fault-injection", default_value = "0", value_parser = parse_finjection, help_heading = "Execution options")]
    pub fault_injection: u32,

    /// Fault injection interval (default: 1).
    #[arg(long = "fault-interval", default_value = "1", value_parser = parse_finterval, help_heading = "Execution options")]
    pub fault_interval: u32,

    /// Communicate with SUT using commands parallelization (default: false).
    #[arg(short = 'O', long = "optimize-sut", help_heading = "Execution options")]
    pub optimize_sut: bool,

    /// Performs a dry run listing tests (no execution).
    #[arg(short = 'D', long = "dry-run", help_heading = "Execution options")]
    pub dry_run: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn time_suffixes() {
        assert_eq!(parse_time_config("5m"), Ok(300));
        assert_eq!(parse_time_config("2h"), Ok(7200));
        assert_eq!(parse_time_config("1d"), Ok(86_400));
        assert_eq!(parse_time_config("30s"), Ok(30));
        assert_eq!(parse_time_config("60"), Ok(60));
        assert_eq!(parse_time_config("0"), Ok(0));
    }

    #[test]
    fn time_rejects_garbage() {
        for bad in ["", "abc", "1x", "m", "1.5h", "-5m", "99999999999999999999h"] {
            assert!(parse_time_config(bad).is_err(), "{bad} must fail");
        }
    }

    #[test]
    fn iterate_clamps_to_one() {
        assert_eq!(parse_iterate(""), Ok(1));
        assert_eq!(parse_iterate("0"), Ok(1));
        assert_eq!(parse_iterate("-3"), Ok(1));
        assert_eq!(parse_iterate("4"), Ok(4));
        assert!(parse_iterate("abc").is_err());
    }

    #[test]
    fn finjection_clamps() {
        assert_eq!(parse_finjection(""), Ok(0));
        assert_eq!(parse_finjection("200"), Ok(100));
        assert_eq!(parse_finjection("-5"), Ok(0));
        assert_eq!(parse_finjection("42"), Ok(42));
        assert!(parse_finjection("abc").is_err());
    }

    #[test]
    fn finterval_clamps() {
        assert_eq!(parse_finterval(""), Ok(1));
        assert_eq!(parse_finterval("-5"), Ok(1));
        for val in ["1", "20", "100", "2000"] {
            assert_eq!(parse_finterval(val), Ok(val.parse().unwrap()));
        }
        assert!(parse_finterval("abc").is_err());
    }

    #[test]
    fn dict_parsers() {
        assert_eq!(
            parse_dict_config("help"),
            Ok(HashMap::from([(String::from("help"), String::new())]))
        );
        assert!(parse_dict_config("").is_err());
        assert!(parse_dict_config("noequalssign:k").is_err());
        assert!(parse_dict_config("name:=value").is_err());
        assert!(parse_dict_config("name:key=").is_err());
        let cfg = parse_dict_config("shell:id=myshell").unwrap();
        assert_eq!(cfg["name"], "shell");
        assert_eq!(cfg["id"], "myshell");
        let cfg = parse_dict_config("default").unwrap();
        assert_eq!(cfg["name"], "default");
    }

    #[test]
    fn defaults_match_upstream() {
        let args = Args::try_parse_from(["kirk", "--run-command", "ls"]).unwrap();
        assert_eq!(args.suite_timeout, 3600);
        assert_eq!(args.exec_timeout, 3600);
        assert_eq!(args.runtime, 0);
        assert_eq!(args.suite_iterate, 1);
        assert_eq!(args.workers, 1);
        assert_eq!(args.fault_injection, 0);
        assert_eq!(args.fault_interval, 1);
        assert_eq!(args.tmp_dir, "/tmp");
        assert_eq!(args.sut["name"], "default");
        assert!(!args.verbose);
        assert!(!args.no_colors);
    }

    #[test]
    fn help_lists_every_upstream_flag() {
        let mut help = Vec::new();
        <Args as clap::CommandFactory>::command()
            .write_long_help(&mut help)
            .unwrap();
        let help = String::from_utf8(help).unwrap();
        for flag in [
            "--com",
            "-C",
            "--sut",
            "-u",
            "--run-suite",
            "-f",
            "--run-pattern",
            "-p",
            "--run-command",
            "-c",
            "--suite-timeout",
            "-T",
            "--exec-timeout",
            "-t",
            "--runtime",
            "-I",
            "--suite-iterate",
            "-i",
            "--workers",
            "-w",
            "--force-parallel",
            "-W",
            "--fault-injection",
            "-F",
            "--fault-interval",
            "--optimize-sut",
            "-O",
            "--dry-run",
            "-D",
            "--skip-tests",
            "-s",
            "--skip-file",
            "-S",
            "--tmp-dir",
            "-d",
            "--restore",
            "-r",
            "--json-report",
            "-o",
            "--monitor",
            "-m",
            "--verbose",
            "-v",
            "--no-colors",
            "-n",
            "--randomize",
            "-R",
        ] {
            assert!(help.contains(flag), "help must list {flag}");
        }
        assert!(help.contains("1h"));
    }
}
