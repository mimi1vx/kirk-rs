//! Argument validation in upstream `run()` order.
//!
//! Directory/file flags are canonicalized before the kind check, so a
//! symlinked or `..`-laden path cannot bypass the existence gate. Error
//! messages name the plugin or path, never the `k=v` parameter values.

use std::collections::HashMap;

use kirk_core::KirkError;

use super::args::{Args, MAX_COM_INSTANCES};

/// Outcome of [`validate`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Validation {
    /// Arguments are usable; start the session.
    Proceed,
    /// `--com help` was requested.
    ComHelp,
    /// `--sut help` was requested.
    SutHelp,
}

/// Name plus `config_help` of one builtin plugin, for help and name checks.
#[derive(Debug, Clone)]
pub struct PluginInfo {
    /// Plugin name.
    pub name: String,
    /// Configuration help map.
    pub config_help: HashMap<String, String>,
}

/// Validate `args` exactly in upstream `run()` order.
///
/// # Errors
///
/// Returns [`KirkError`] describing the first failing check.
pub fn validate(
    args: &Args,
    coms: &[PluginInfo],
    suts: &[PluginInfo],
) -> Result<Validation, KirkError> {
    if let Some(dir) = args.plugins.as_deref()
        && !is_existing_dir(dir)
    {
        return Err(KirkError::Session(format!(
            "'{dir}' plugins directory doesn't exist"
        )));
    }

    if args.com.iter().any(|obj| obj.contains_key("help")) {
        return Ok(Validation::ComHelp);
    }

    if args.com.len() >= MAX_COM_INSTANCES {
        return Err(KirkError::Session(format!(
            "Maximum number of communication objects is {MAX_COM_INSTANCES}"
        )));
    }

    for config in &args.com {
        let name = config.get("name").map_or("", String::as_str);
        if !coms.iter().any(|plugin| plugin.name == name) {
            return Err(KirkError::Session(format!(
                "Can't find communication handler with name '{name}'"
            )));
        }
    }

    if args.sut.contains_key("help") {
        return Ok(Validation::SutHelp);
    }

    let sut_name = args.sut.get("name").map_or("", String::as_str);
    if !suts.iter().any(|plugin| plugin.name == sut_name) {
        return Err(KirkError::Session(format!(
            "'{sut_name}' SUT is not available"
        )));
    }

    if let Some(report) = args.json_report.as_deref()
        && std::path::Path::new(report).exists()
    {
        return Err(KirkError::Session(format!(
            "JSON report file already exists: {report}"
        )));
    }

    let suites = args.run_suite.as_deref().unwrap_or_default();
    if args.run_pattern.is_some() && suites.is_empty() {
        return Err(KirkError::Session(String::from(
            "--run-pattern must be used with --run-suite",
        )));
    }

    if suites.is_empty() && args.run_command.is_none() {
        return Err(KirkError::Session(String::from(
            "--run-suite/--run-command are required",
        )));
    }

    if let Some(skip) = args.skip_file.as_deref()
        && !is_existing_file(skip)
    {
        return Err(KirkError::Session(format!(
            "'{skip}' skip file doesn't exist"
        )));
    }

    if !args.tmp_dir.is_empty() && !is_existing_dir(&args.tmp_dir) {
        return Err(KirkError::Session(format!(
            "'{}' temporary folder doesn't exist",
            args.tmp_dir
        )));
    }

    Ok(Validation::Proceed)
}

/// Render `--com`/`--sut` plugin help, mirroring `_print_plugin_help`.
#[must_use]
pub fn plugin_help(opt_name: &str, plugins: &[PluginInfo]) -> String {
    use std::fmt::Write as _;
    let mut msg = format!("{opt_name} option supports the following syntax:\n");
    msg.push_str("\n\t<name>:<param1>=<value1>:<param2>=<value2>:..\n");
    msg.push_str("\nSupported plugins: | ");
    for plugin in plugins {
        msg.push_str(&plugin.name);
        msg.push_str(" | ");
    }
    msg.push('\n');
    for plugin in plugins {
        if plugin.config_help.is_empty() {
            let _ = writeln!(msg, "\n{} has not configuration", plugin.name);
        } else {
            let _ = writeln!(msg, "\n{} configuration:", plugin.name);
            let mut opts: Vec<(&String, &String)> = plugin.config_help.iter().collect();
            opts.sort_by(|left, right| left.0.cmp(right.0));
            for (opt, desc) in opts {
                let _ = writeln!(msg, "\t{opt}: {desc}");
            }
        }
    }
    msg
}

/// Canonicalize-aware directory check: symlinks resolve, missing paths fail.
fn is_existing_dir(path: &str) -> bool {
    let canonical = std::fs::canonicalize(path).unwrap_or_else(|_| std::path::PathBuf::from(path));
    canonical.is_dir()
}

/// Canonicalize-aware file check: symlinks resolve, missing paths fail.
fn is_existing_file(path: &str) -> bool {
    let canonical = std::fs::canonicalize(path).unwrap_or_else(|_| std::path::PathBuf::from(path));
    canonical.is_file()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::args::Args;
    use clap::Parser;

    fn plugins() -> (Vec<PluginInfo>, Vec<PluginInfo>) {
        (
            vec![PluginInfo {
                name: String::from("shell"),
                config_help: HashMap::new(),
            }],
            vec![PluginInfo {
                name: String::from("default"),
                config_help: HashMap::from([(String::from("com"), String::from("help"))]),
            }],
        )
    }

    fn parse(extra: &[&str]) -> Args {
        let mut argv = vec!["kirk"];
        argv.extend(extra);
        Args::try_parse_from(argv).unwrap()
    }

    #[test]
    fn no_run_option_fails() {
        let (coms, suts) = plugins();
        assert!(validate(&parse(&["--sut", "default"]), &coms, &suts).is_err());
    }

    #[test]
    fn pattern_without_suite_fails() {
        let (coms, suts) = plugins();
        assert!(validate(&parse(&["--run-pattern", "test.*"]), &coms, &suts).is_err());
    }

    #[test]
    fn invalid_plugins_dir_fails() {
        let (coms, suts) = plugins();
        let args = parse(&["--plugins", "/nonexistent", "--sut", "help"]);
        assert!(validate(&args, &coms, &suts).is_err());
    }

    #[test]
    fn invalid_tmp_dir_fails() {
        let (coms, suts) = plugins();
        let args = parse(&["--tmp-dir", "/nonexistent_dir", "--run-command", "ls"]);
        assert!(validate(&args, &coms, &suts).is_err());
    }

    #[test]
    fn invalid_skip_file_fails() {
        let (coms, suts) = plugins();
        let args = parse(&["--skip-file", "/nonexistent", "--run-suite", "suite01"]);
        assert!(validate(&args, &coms, &suts).is_err());
    }

    #[test]
    fn existing_report_fails() {
        let dir = std::env::temp_dir().join("kirk-cli-report-check");
        std::fs::create_dir_all(&dir).unwrap();
        let report = dir.join("report.json");
        std::fs::write(&report, "{}").unwrap();
        let (coms, suts) = plugins();
        let args = parse(&[
            "--tmp-dir",
            dir.to_str().unwrap(),
            "--json-report",
            report.to_str().unwrap(),
            "--run-suite",
            "suite01",
        ]);
        assert!(validate(&args, &coms, &suts).is_err());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn com_help_short_circuits() {
        let (coms, suts) = plugins();
        assert!(matches!(
            validate(&parse(&["--com", "help"]), &coms, &suts),
            Ok(Validation::ComHelp)
        ));
    }

    #[test]
    fn sut_help_short_circuits() {
        let (coms, suts) = plugins();
        assert!(matches!(
            validate(&parse(&["--sut", "help"]), &coms, &suts),
            Ok(Validation::SutHelp)
        ));
    }

    #[test]
    fn unknown_com_fails() {
        let (coms, suts) = plugins();
        let args = parse(&["--com", "nonexistent", "--run-command", "ls"]);
        assert!(validate(&args, &coms, &suts).is_err());
    }

    #[test]
    fn com_limit_fails() {
        let (coms, suts) = plugins();
        let mut raw = vec![
            String::from("kirk"),
            String::from("--run-command"),
            String::from("ls"),
        ];
        for _ in 0..MAX_COM_INSTANCES {
            raw.push(String::from("--com"));
            raw.push(String::from("shell"));
        }
        let parsed = Args::try_parse_from(raw).unwrap();
        assert!(validate(&parsed, &coms, &suts).is_err());
    }

    #[test]
    fn valid_command_args_proceed() {
        let (coms, suts) = plugins();
        assert!(matches!(
            validate(&parse(&["--run-command", "ls"]), &coms, &suts),
            Ok(Validation::Proceed)
        ));
    }

    #[test]
    fn plugin_help_renders() {
        let (coms, _) = plugins();
        let text = plugin_help("--com", &coms);
        assert!(text.contains("--com option supports the following syntax"));
        assert!(text.contains("shell"));
    }
}
