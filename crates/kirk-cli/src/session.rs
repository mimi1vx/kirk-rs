//! Session startup ported from `main.py::_start_session`.
//!
//! [`CliSut`] and [`CliFramework`] adapt the real [`GenericSut`] and
//! [`LtpFramework`] to the [`Session`] traits, so no new glue crates are
//! needed. The SUT owns its channel; the framework borrows it through the
//! shared `SutHandle`. UI selection mirrors upstream (`workers > 1` →
//! parallel, `verbose` → verbose, else simple); `Ctrl-C` maps to
//! [`RC_INTERRUPT`].

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use kirk_com::{CmdResult, ComChannel, Registry};
use kirk_com_ltx::LtxChannel;
use kirk_com_qemu::QemuChannel;
use kirk_com_shell::ShellChannel;
use kirk_com_ssh::SshChannel;
use kirk_core::KirkError;
use kirk_events::EventRegistry;
use kirk_ltp::{Framework as LtpFrameworkTrait, LtpFramework};
use kirk_plugin::Plugin as _;
use kirk_scheduler::{Framework as SchedFramework, StdoutBuffer, Sut as SchedSut};
use kirk_session::{RunOptions, Session, SessionConfig, SessionFramework, SessionSut};
use kirk_support::{JSONFileMonitor, StdoutPrinter, TempDir, attach_console};
use kirk_sut::{GenericSut, Sut as SutTrait};
use tokio::sync::Mutex;

use super::args::{Args, RC_ERROR, RC_INTERRUPT, RC_OK};
use super::validate::PluginInfo;

/// Shared ownership of the [`GenericSut`], bridging the SUT and framework
/// adapters the way upstream globals do.
type SutHandle = Arc<Mutex<GenericSut>>;

/// Which console UI to wire, mirroring the upstream selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UiKind {
    /// More than one worker.
    Parallel,
    /// `--verbose` with a single worker.
    Verbose,
    /// Default single-worker UI.
    Simple,
}

/// `u64` seconds into the `f64` the session APIs take; real
/// timeouts are orders of magnitude below the precision-loss range.
#[allow(
    clippy::cast_precision_loss,
    reason = "timeouts are small; u64 seconds fit f64 exactly there"
)]
fn secs(value: u64) -> f64 {
    value as f64
}

/// Pick the UI from worker count and verbosity.
///
/// # Panics
///
/// Never panics; pure function over its arguments.
#[must_use]
fn select_ui(workers: usize, verbose: bool) -> UiKind {
    if workers > 1 {
        UiKind::Parallel
    } else if verbose {
        UiKind::Verbose
    } else {
        UiKind::Simple
    }
}

/// Builtin plugins: the statically linked channels and the `default` SUT.
#[must_use]
pub fn builtin_plugins() -> (Vec<PluginInfo>, Vec<PluginInfo>) {
    let coms = [
        Box::new(ShellChannel::new()) as Box<dyn ComChannel>,
        Box::new(SshChannel::new("ssh")),
        Box::new(QemuChannel::new()),
        Box::new(LtxChannel::new()),
    ]
    .into_iter()
    .map(|channel| PluginInfo {
        name: channel.name().to_owned(),
        config_help: channel.config_help(),
    })
    .collect();
    let suts = vec![PluginInfo {
        name: String::from("default"),
        config_help: GenericSut::new().config_help(),
    }];
    (coms, suts)
}

/// Build a fresh template of the statically linked channel `name`.
fn builtin_template(name: &str, id: &str) -> Option<Box<dyn ComChannel>> {
    let template: Box<dyn ComChannel> = match name {
        "shell" => Box::new(ShellChannel::new()),
        "ssh" => Box::new(SshChannel::new("ssh")),
        "qemu" => Box::new(QemuChannel::new()),
        "ltx" => Box::new(LtxChannel::new()),
        _ => return None,
    };
    Some(template.clone_channel_box(id))
}

/// Combine `--skip-tests` with `--skip-file`, mirroring `_get_skip_tests`.
///
/// # Errors
///
/// Returns [`KirkError::Session`] when the skip file cannot be read.
async fn get_skip_tests(
    skip_tests: Option<&str>,
    skip_file: Option<&str>,
) -> Result<String, KirkError> {
    let mut parts = Vec::new();
    if let Some(path) = skip_file.filter(|p| !p.is_empty()) {
        let text = tokio::fs::read_to_string(path)
            .await
            .map_err(|_| KirkError::Session(String::from("can't read skip file")))?;
        let kept: Vec<&str> = text
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
            .collect();
        if !kept.is_empty() {
            parts.push(kept.join("|"));
        }
    }
    if let Some(skip) = skip_tests.filter(|s| !s.is_empty()) {
        parts.push(skip.to_owned());
    }
    Ok(parts.join("|"))
}

/// Resolve `--restore`, following a trailing symlink like upstream.
///
/// # Errors
///
/// Returns [`KirkError::Session`] when the folder does not exist.
fn resolve_restore(path: Option<&str>) -> Result<Option<String>, KirkError> {
    let Some(path) = path.filter(|p| !p.is_empty()) else {
        return Ok(None);
    };
    let missing = || KirkError::Session(format!("Can't restore '{path}'. Folder doesn't exist"));
    let canonical = std::fs::canonicalize(path).map_err(|_| missing())?;
    if !canonical.is_dir() {
        return Err(missing());
    }
    Ok(Some(canonical.to_string_lossy().into_owned()))
}

/// Build one live channel per `--com` entry, mirroring `_init_channels`.
///
/// Entries with `id=` clone the named template under the new id; templates
/// resolve from already-built instances first, then the statically linked
/// channels. With no entries, one default `shell` instance is returned so
/// the SUT can attach.
///
/// # Errors
///
/// Returns [`KirkError`] when a template is missing or setup fails. Only the
/// channel name is reported, never parameter values.
fn setup_channels(
    configs: &[HashMap<String, String>],
    tmpdir: &str,
) -> Result<Vec<Box<dyn ComChannel>>, KirkError> {
    if configs.is_empty() {
        let mut shell = builtin_template("shell", "shell")
            .ok_or_else(|| KirkError::Plugin(String::from("Can't find plugin 'shell'")))?;
        shell.setup(&HashMap::from([(
            String::from("tmpdir"),
            tmpdir.to_owned(),
        )]))?;
        return Ok(vec![shell]);
    }
    let mut instances: Vec<Box<dyn ComChannel>> = Vec::new();
    for config in configs {
        let name = config.get("name").map_or("", String::as_str);
        let id = config.get("id").map_or(name, String::as_str);
        let template = lookup_template(name, id, &instances)?;
        let mut instance = template.clone_channel_box(id);
        let mut full = config.clone();
        full.insert(String::from("tmpdir"), tmpdir.to_owned());
        instance.setup(&full)?;
        instances.push(instance);
    }
    Ok(instances)
}

/// Clone the `name` template under `id`, preferring an already-built
/// instance (for `id=` chaining) over the statically linked channels.
fn lookup_template(
    name: &str,
    id: &str,
    instances: &[Box<dyn ComChannel>],
) -> Result<Box<dyn ComChannel>, KirkError> {
    if let Some(found) = instances.iter().find(|channel| channel.name() == name) {
        return Ok(found.clone_channel_box(id));
    }
    builtin_template(name, id)
        .ok_or_else(|| KirkError::Plugin(format!("Can't find plugin '{name}'")))
}

/// [`GenericSut`] adapted to the scheduler [`Sut`](SchedSut) surface.
pub struct CliSut {
    /// Shared SUT handle.
    sut: SutHandle,
    /// Cached plugin name (`name()` is sync and cannot lock).
    name: String,
    /// Cached parallel-execution support.
    parallel: bool,
}

impl CliSut {
    /// Wrap a shared SUT, caching the sync accessors.
    #[must_use]
    fn new(sut: SutHandle) -> Self {
        let (name, parallel) = sut.try_lock().map_or_else(
            |_| (String::from("default"), true),
            |guard| {
                (
                    guard.name().to_owned(),
                    guard
                        .channel()
                        .is_ok_and(|channel| channel.parallel_execution()),
                )
            },
        );
        Self {
            sut,
            name,
            parallel,
        }
    }
}

#[async_trait]
impl SchedSut for CliSut {
    async fn get_tainted_info(&self) -> Result<(i64, Vec<String>), KirkError> {
        let mut guard = self.sut.lock().await;
        let info = guard.get_tainted_info().await?;
        let code = i64::try_from(info.code)
            .map_err(|_| KirkError::Sut(String::from("invalid taint code")))?;
        Ok((code, info.messages))
    }

    async fn run_command(
        &self,
        command: &str,
        cwd: Option<&str>,
        env: &HashMap<String, String>,
        capture: &StdoutBuffer,
    ) -> Result<Option<CmdResult>, KirkError> {
        let mut guard = self.sut.lock().await;
        let result = guard
            .channel_mut()?
            .run_command(command, cwd, Some(env), None)
            .await?;
        if let Some(row) = &result {
            capture.push(&row.stdout).await;
        }
        Ok(result)
    }

    async fn ping(&self) -> Result<f64, KirkError> {
        self.sut.lock().await.channel_mut()?.ping().await
    }

    async fn stop(&self) -> Result<(), KirkError> {
        self.sut.lock().await.stop(None).await
    }

    async fn restart(&self) -> Result<(), KirkError> {
        self.sut.lock().await.restart(None).await
    }

    async fn get_info(&self) -> Result<HashMap<String, String>, KirkError> {
        let mut guard = self.sut.lock().await;
        let info = guard.get_info().await?;
        Ok(HashMap::from([
            (String::from("distro"), info.distro),
            (String::from("distro_ver"), info.distro_ver),
            (String::from("kernel"), info.kernel),
            (String::from("cmdline"), info.cmdline),
            (String::from("arch"), info.arch),
            (String::from("cpu"), info.cpu),
            (String::from("ram"), info.ram),
            (String::from("swap"), info.swap),
        ]))
    }

    fn name(&self) -> String {
        self.name.clone()
    }
}

#[async_trait]
impl SessionSut for CliSut {
    async fn session_start(&self) -> Result<(), KirkError> {
        self.sut.lock().await.start(None).await
    }

    async fn session_stop(&self) -> Result<(), KirkError> {
        self.sut.lock().await.stop(None).await
    }

    async fn session_is_running(&self) -> Result<bool, KirkError> {
        self.sut.lock().await.is_running().await
    }

    fn session_parallel_execution(&self) -> bool {
        self.parallel
    }

    async fn session_logged_as_root(&self) -> Result<bool, KirkError> {
        self.sut.lock().await.logged_as_root().await
    }

    async fn session_fault_enabled(&self) -> Result<bool, KirkError> {
        self.sut.lock().await.is_fault_injection_enabled().await
    }

    async fn session_setup_fault(&self, prob: u32, interval: u32) -> Result<(), KirkError> {
        self.sut
            .lock()
            .await
            .setup_fault_injection(prob, interval)
            .await
    }

    async fn session_run_command(
        &self,
        full_command: &str,
        cwd: Option<&str>,
        env: &HashMap<String, String>,
    ) -> Result<Option<CmdResult>, KirkError> {
        self.sut
            .lock()
            .await
            .channel_mut()?
            .run_command(full_command, cwd, Some(env), None)
            .await
    }
}

/// [`LtpFramework`] adapted to the scheduler and session framework surfaces.
///
/// The SUT lock is held across channel calls because the channel is owned
/// by the SUT; no other task needs the lock concurrently in these paths
/// (suite discovery runs before scheduling starts).
pub struct CliFramework {
    /// LTP framework definition.
    framework: LtpFramework,
    /// Shared SUT handle, for channel access.
    sut: SutHandle,
}

#[async_trait]
impl SchedFramework for CliFramework {
    async fn read_result(
        &self,
        test: &kirk_core::data::Test,
        stdout: &str,
        retcode: i32,
        exec_time: f64,
    ) -> Result<kirk_core::results::TestResults, KirkError> {
        self.framework
            .read_result(test.clone(), stdout, retcode, exec_time)
            .await
    }
}

#[async_trait]
impl SessionFramework for CliFramework {
    async fn session_find_suite(&self, name: &str) -> Result<kirk_core::data::Suite, KirkError> {
        let mut guard = self.sut.lock().await;
        let channel: &mut dyn ComChannel = &mut **guard.channel_mut()?;
        self.framework.find_suite(channel, name).await
    }

    async fn session_find_command(
        &self,
        command: &str,
    ) -> Result<kirk_core::data::Test, KirkError> {
        let mut guard = self.sut.lock().await;
        let channel: &mut dyn ComChannel = &mut **guard.channel_mut()?;
        self.framework.find_command(channel, command).await
    }
}

/// Start the session, mirroring `_start_session`.
///
/// Setup failures propagate for the caller to report as argument errors
/// (exit 2, like upstream `parser.error`); a failed run resolves to
/// [`RC_ERROR`] after the `session_error` event reaches the UI.
///
/// # Errors
///
/// Returns [`KirkError`] when skip/restore/tmpdir/channel/SUT setup fails.
#[allow(
    clippy::too_many_lines,
    reason = "single startup sequence mirroring _start_session"
)]
pub async fn run_session(args: &Args) -> Result<i32, KirkError> {
    let skip_tests = get_skip_tests(args.skip_tests.as_deref(), args.skip_file.as_deref()).await?;
    if !skip_tests.is_empty() {
        regex::Regex::new(&skip_tests).map_err(|_| {
            KirkError::Session(format!("'{skip_tests}' is not a valid regular expression"))
        })?;
    }

    if let Some(pattern) = args.run_pattern.as_deref() {
        regex::Regex::new(pattern).map_err(|_| {
            KirkError::Session(format!("'{pattern}' is not a valid regular expression"))
        })?;
    }

    let restore_dir = resolve_restore(args.restore.as_deref())?;

    let tmpdir = if args.tmp_dir.is_empty() {
        TempDir::new(None, 5)
    } else {
        TempDir::new(Some(args.tmp_dir.as_str()), 5)
    }?;
    let tmpdir_str = tmpdir.abspath().to_string_lossy().into_owned();

    let instances = setup_channels(&args.com, &tmpdir_str)?;
    let mut registry = Registry::new();
    for instance in instances {
        registry.register(instance);
    }
    let mut sut_cfg = args.sut.clone();
    sut_cfg.insert(String::from("tmpdir"), tmpdir_str);
    let mut sut = GenericSut::new();
    sut.setup_with_registry(&sut_cfg, &registry)?;
    sut.set_optimize(args.optimize_sut);
    drop(registry);

    let shared: SutHandle = Arc::new(Mutex::new(sut));
    let cli_sut = CliSut::new(Arc::clone(&shared));
    let cli_framework = CliFramework {
        framework: LtpFramework::new(0.0, secs(args.exec_timeout)),
        sut: shared,
    };

    let events = EventRegistry::new();
    // All three UI kinds share the `ConsoleUi` event surface (structured
    // per-test summaries need direct calls the string registry cannot
    // carry), so one console is wired while `select_ui` records the mode.
    let _kind = select_ui(args.workers, args.verbose);
    let printer = Arc::new(StdoutPrinter::new());
    let console = Arc::new(kirk_support::ConsoleUi::new(args.no_colors, printer));
    attach_console(&console, &events).await?;
    drop(console);

    let monitor = if let Some(path) = args.monitor.as_deref() {
        let monitor = JSONFileMonitor::new(path)?;
        monitor.attach(&events).await?;
        Some(monitor)
    } else {
        None
    };

    let session = Session::new(
        tmpdir,
        cli_sut,
        cli_framework,
        events.clone(),
        SessionConfig {
            exec_timeout: secs(args.exec_timeout),
            suite_timeout: secs(args.suite_timeout),
            workers: args.workers,
            force_parallel: args.force_parallel,
        },
    );
    let opts = RunOptions {
        command: args.run_command.clone(),
        suites: args.run_suite.clone().unwrap_or_default(),
        pattern: args.run_pattern.clone(),
        skip_tests: if skip_tests.is_empty() {
            None
        } else {
            Some(skip_tests)
        },
        report_path: args.json_report.clone(),
        restore_path: restore_dir,
        suite_iterate: args.suite_iterate,
        randomize: args.randomize,
        runtime: secs(args.runtime),
        fault_prob: args.fault_injection,
        fault_interval: args.fault_interval,
        dry_run: args.dry_run,
    };

    let events_bg = events.clone();
    let events_task = tokio::spawn(async move { events_bg.start().await });
    let code = tokio::select! {
        result = session.run(&opts) => match result {
            Ok(()) => RC_OK,
            Err(_) => RC_ERROR,
        },
        _ = tokio::signal::ctrl_c() => {
            session.stop().await;
            RC_INTERRUPT
        }
    };
    if let Some(monitor) = &monitor {
        let _ = monitor.detach(&events).await;
    }
    events.stop();
    let _ = events_task.await;
    Ok(code)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ui_selection() {
        assert_eq!(select_ui(4, false), UiKind::Parallel);
        assert_eq!(select_ui(4, true), UiKind::Parallel);
        assert_eq!(select_ui(1, true), UiKind::Verbose);
        assert_eq!(select_ui(1, false), UiKind::Simple);
        assert_eq!(select_ui(0, false), UiKind::Simple);
    }

    #[test]
    fn builtin_names() {
        let (coms, suts) = builtin_plugins();
        assert_eq!(
            coms.iter().map(|p| p.name.as_str()).collect::<Vec<_>>(),
            ["shell", "ssh", "qemu", "ltx"]
        );
        assert_eq!(
            suts.iter().map(|p| p.name.as_str()).collect::<Vec<_>>(),
            ["default"]
        );
    }

    #[tokio::test]
    async fn skip_tests_combines_file_and_flag() {
        let dir = std::env::temp_dir().join("kirk-cli-skip");
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let path = dir.join("skip");
        tokio::fs::write(&path, "test01\n# comment\ntest02\n\n")
            .await
            .unwrap();
        let result = get_skip_tests(Some("test03"), Some(path.to_str().unwrap()))
            .await
            .unwrap();
        assert_eq!(result, "test01|test02|test03");
        assert_eq!(get_skip_tests(None, None).await.unwrap(), "");
        tokio::fs::remove_dir_all(&dir).await.unwrap();
    }

    #[tokio::test]
    async fn skip_tests_trims_whitespace() {
        let dir = std::env::temp_dir().join("kirk-cli-skip-trim");
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let path = dir.join("skip");
        tokio::fs::write(&path, "  test01  \n   # indented comment\n\ttest02\t\n")
            .await
            .unwrap();
        let result = get_skip_tests(None, Some(path.to_str().unwrap()))
            .await
            .unwrap();
        assert_eq!(result, "test01|test02");
        tokio::fs::remove_dir_all(&dir).await.unwrap();
    }

    #[tokio::test]
    async fn skip_tests_missing_file_fails() {
        assert!(
            get_skip_tests(None, Some("/nonexistent-skip-file"))
                .await
                .is_err()
        );
    }

    #[test]
    fn restore_missing_dir_fails() {
        assert!(resolve_restore(Some("/nonexistent-restore-dir")).is_err());
        assert_eq!(resolve_restore(None).unwrap(), None);
        assert_eq!(resolve_restore(Some("")).unwrap(), None);
    }

    #[test]
    fn restore_symlink_resolves() {
        let dir = std::env::temp_dir().join("kirk-cli-restore");
        std::fs::create_dir_all(dir.join("real")).unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(dir.join("real"), dir.join("link")).unwrap();
        #[cfg(unix)]
        {
            let resolved = resolve_restore(Some(dir.join("link").to_str().unwrap()))
                .unwrap()
                .unwrap();
            assert!(resolved.ends_with("real"));
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn channels_default_to_shell() {
        let instances = setup_channels(&[], "/tmp").unwrap();
        assert_eq!(instances.len(), 1);
        assert_eq!(instances[0].name(), "shell");
    }

    #[test]
    fn channels_clone_by_id() {
        let cfg = HashMap::from([
            (String::from("name"), String::from("shell")),
            (String::from("id"), String::from("myshell")),
        ]);
        let instances = setup_channels(&[cfg], "/tmp").unwrap();
        assert_eq!(instances[0].name(), "myshell");
    }

    #[test]
    fn all_builtin_templates_resolve() {
        for name in ["shell", "ssh", "qemu", "ltx"] {
            let template = lookup_template(name, name, &[]).unwrap();
            assert_eq!(template.name(), name);
        }
    }

    #[test]
    fn channels_missing_template_errors() {
        let cfg = HashMap::from([(String::from("name"), String::from("nonexistent"))]);
        assert!(setup_channels(&[cfg], "/tmp").is_err());
    }

    #[tokio::test]
    async fn dry_run_command_needs_no_ltp_root() {
        let (coms, suts) = builtin_plugins();
        let dir = std::env::temp_dir().join("kirk-cli-dryrun");
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let args = Args {
            verbose: false,
            no_colors: true,
            tmp_dir: dir.to_string_lossy().into_owned(),
            restore: None,
            json_report: None,
            monitor: None,
            com: Vec::new(),
            sut: HashMap::from([(String::from("name"), String::from("default"))]),
            skip_tests: None,
            skip_file: None,
            run_suite: None,
            run_pattern: None,
            run_command: Some(String::from("echo hello")),
            suite_timeout: 3600,
            exec_timeout: 60,
            randomize: false,
            runtime: 0,
            suite_iterate: 1,
            workers: 1,
            force_parallel: false,
            fault_injection: 0,
            fault_interval: 1,
            optimize_sut: false,
            dry_run: true,
        };
        assert!(crate::validate::validate(&args, &coms, &suts).is_ok());
        assert_eq!(run_session(&args).await.unwrap(), RC_OK);
        tokio::fs::remove_dir_all(&dir).await.unwrap();
    }
}
