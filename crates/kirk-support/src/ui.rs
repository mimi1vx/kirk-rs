//! Console user interfaces ported from `kirk/libkirk/ui.py`.
//!
//! Rendering is split from delivery: the `ConsoleUi` family formats text and
//! funnels it through one fair mutex plus blocking writes, so lines stay
//! ordered without an extra `printf` event. [`VecPrinter`] captures output
//! for snapshot tests; [`StdoutPrinter`] writes to the console.
//!
//! Event wiring is best-effort by design: the registry only carries strings,
//! so `attach` renders the plain-string events faithfully and leaves the
//! structured summaries (`session_completed`, `session_dry_run`) to direct
//! calls.

use std::sync::{Arc, Mutex as StdMutex};

use kirk_core::data::{Suite, Test};
use kirk_core::results::{SuiteResults, TestResults};
use kirk_events::EventRegistry;

/// Output sink for user interfaces.
pub trait Printer: Send + Sync {
    /// Write one chunk (ending included by the caller).
    fn print(&self, chunk: &str);
}

/// Printer capturing output for snapshot tests.
#[derive(Default)]
pub struct VecPrinter {
    chunks: StdMutex<Vec<String>>,
}

impl VecPrinter {
    /// Create an empty capturing printer.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// All captured output concatenated.
    #[must_use]
    pub fn contents(&self) -> String {
        self.chunks
            .lock()
            .map_or_else(|_| String::new(), |c| c.join(""))
    }
}

impl Printer for VecPrinter {
    fn print(&self, chunk: &str) {
        if let Ok(mut chunks) = self.chunks.lock() {
            chunks.push(chunk.to_owned());
        }
    }
}

/// Printer writing to standard output.
#[derive(Default)]
pub struct StdoutPrinter;

impl StdoutPrinter {
    /// Create a stdout printer.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl Printer for StdoutPrinter {
    fn print(&self, chunk: &str) {
        use std::io::Write;
        let stdout = std::io::stdout();
        let mut handle = stdout.lock();
        let _ = handle.write_all(chunk.as_bytes());
        let _ = handle.flush();
    }
}

/// Console based user interface.
pub struct ConsoleUi {
    no_colors: bool,
    printer: Arc<dyn Printer>,
    lock: tokio::sync::Mutex<()>,
    num_suites: tokio::sync::Mutex<usize>,
}

impl ConsoleUi {
    /// ANSI colors, mirroring upstream constants.
    pub const WHITE: &'static str = "\x1b[1;37m";
    pub const GREEN: &'static str = "\x1b[1;32m";
    pub const YELLOW: &'static str = "\x1b[1;33m";
    pub const RED: &'static str = "\x1b[1;31m";
    pub const CYAN: &'static str = "\x1b[1;36m";
    pub const RESET_COLOR: &'static str = "\x1b[0m";
    pub const RESET_SCREEN: &'static str = "\x1b[2J";

    /// Create a console UI writing to `printer`.
    pub fn new(no_colors: bool, printer: Arc<dyn Printer>) -> Self {
        Self {
            no_colors,
            printer,
            lock: tokio::sync::Mutex::new(()),
            num_suites: tokio::sync::Mutex::new(1),
        }
    }

    /// Whether colors are disabled.
    #[must_use]
    pub fn no_colors(&self) -> bool {
        self.no_colors
    }

    /// Print a raw message chunk.
    pub async fn print_message(&self, msg: &str, end: &str) {
        let text = format!("{msg}{end}");
        let _guard = self.lock.lock().await;
        let printer = Arc::clone(&self.printer);
        let _ = tokio::task::spawn_blocking(move || printer.print(&text)).await;
    }

    /// Format and deliver one message, honoring `--no-colors`.
    pub async fn styled(&self, msg: &str, color: Option<&str>, end: &str) {
        let mut text: String = msg.replace(Self::RESET_SCREEN, "").replace('\r', "");
        if let Some(color) = color
            && !self.no_colors
        {
            text = format!("{color}{text}{}", Self::RESET_COLOR);
        }
        self.print_message(&text, end).await;
    }

    /// User-friendly duration, mirroring `_user_friendly_duration`.
    #[must_use]
    pub fn user_friendly_duration(duration: f64) -> String {
        if duration == 0.0 {
            return String::from("0h 0m 0s");
        }
        let total = duration;
        let minutes = (total / 60.0).floor();
        let seconds = total - minutes * 60.0;
        let hours = (minutes / 60.0).floor();
        let minutes = minutes - hours * 60.0;
        if hours > 0.0 {
            format!("{hours:.0}h {minutes:.0}m {seconds:.0}s")
        } else if minutes > 0.0 {
            format!("{minutes:.0}m {seconds:.0}s")
        } else {
            format!("{seconds:.3}s")
        }
    }

    /// Format a kernel cmdline across lines, mirroring `_format_cmdline`.
    #[must_use]
    pub fn format_cmdline(cmdline: Option<&str>) -> String {
        let Some(cmdline) = cmdline else {
            return String::new();
        };
        if cmdline.is_empty() {
            return String::new();
        }
        let parts: Vec<&str> = cmdline.split_whitespace().collect();
        if parts.is_empty() {
            return cmdline.to_owned();
        }
        let mut formatted = String::from(parts[0]);
        for part in &parts[1..] {
            formatted.push_str("\n          ");
            formatted.push_str(part);
        }
        formatted
    }

    /// Result label and color, mirroring `_result_color`.
    #[must_use]
    pub fn result_color(results: &TestResults) -> (&'static str, &'static str) {
        if results.failed() > 0 {
            return ("fail", Self::RED);
        }
        if results.skipped() > 0 {
            return ("skip", Self::CYAN);
        }
        if results.broken() > 0 {
            return ("broken", Self::RED);
        }
        ("pass", Self::GREEN)
    }

    /// Print an underlined message.
    pub async fn print_underline(&self, msg: &str) {
        let line = "─".repeat(msg.chars().count());
        self.styled(&format!("{msg}\n{line}"), None, "\n").await;
    }

    /// Print a section title surrounded by lines.
    pub async fn print_section(&self, msg: &str) {
        let line = "─".repeat(msg.chars().count() + 12);
        self.styled(&format!("{line}\n      {msg}\n{line}"), None, "\n")
            .await;
    }

    /// Print target information for the first suite results.
    pub async fn print_target_info(&self, results: &SuiteResults) {
        let message = format!(
            "Kernel:   {}\nCmdline:  {}\nMachine:  {}\nArch:     {}\nRAM:      {}\nSwap:     {}\nDistro:   {} {}\n",
            results.kernel().unwrap_or_default(),
            Self::format_cmdline(results.cmdline()),
            results.cpu().unwrap_or_default(),
            results.arch().unwrap_or_default(),
            results.ram().unwrap_or_default(),
            results.swap().unwrap_or_default(),
            results.distro().unwrap_or_default(),
            results.distro_ver().unwrap_or_default(),
        );
        self.print_underline("Target information").await;
        self.styled(&message, None, "\n").await;
    }

    /// Print a summary for testing suites.
    pub async fn print_summary(&self, results: &[SuiteResults]) {
        let suites: Vec<&str> = results.iter().map(|r| r.suite().name()).collect();
        let runs: usize = results.iter().map(|r| r.tests_results().len()).sum();
        let passed: u32 = results.iter().map(SuiteResults::passed).sum();
        let failed: u32 = results.iter().map(SuiteResults::failed).sum();
        let skipped: u32 = results.iter().map(SuiteResults::skipped).sum();
        let broken: u32 = results.iter().map(SuiteResults::broken).sum();
        let warnings: u32 = results.iter().map(SuiteResults::warnings).sum();
        let exec_time: f64 = results.iter().map(SuiteResults::exec_time).sum();
        let message = format!(
            "Suite:   {}\nRuntime: {}\nRuns:    {runs}\n\nResults:\n    Passed:   {passed}\n    Failed:   {failed}\n    Broken:   {broken}\n    Skipped:  {skipped}\n    Warnings: {warnings}\n",
            suites.join(", "),
            Self::user_friendly_duration(exec_time),
        );
        self.styled(&message, None, "\n").await;
    }

    /// Handle `session_restore`.
    pub async fn session_restore(&self, restore: &str) {
        self.styled(&format!("Restore session: {restore}"), None, "\n")
            .await;
    }

    /// Handle `session_started`.
    pub async fn session_started(&self, num_suites: usize, tmpdir: &str) {
        *self.num_suites.lock().await = num_suites;
        let hostname = std::env::var("HOSTNAME").unwrap_or_else(|_| String::from("unknown"));
        let message =
            format!("Host information\n\tHostname:   {hostname}\n\tDirectory:  {tmpdir}\n");
        self.styled(&message, None, "\n").await;
    }

    /// Handle `session_stopped`.
    pub async fn session_stopped(&self) {
        self.styled("Session stopped", None, "\n").await;
    }

    /// Handle `sut_start`.
    pub async fn sut_start(&self, sut: &str) {
        self.styled(&format!("Connecting to SUT: {sut}\n"), None, "\n")
            .await;
    }

    /// Handle `sut_stop`.
    pub async fn sut_stop(&self, sut: &str) {
        self.styled(&format!("Disconnecting from SUT: {sut}"), None, "\n")
            .await;
    }

    /// Handle `sut_restart`.
    pub async fn sut_restart(&self, sut: &str) {
        self.styled(&format!("Restarting SUT: {sut}"), None, "\n")
            .await;
    }

    /// Handle `run_cmd_start`.
    pub async fn run_cmd_start(&self, cmd: &str) {
        self.styled(cmd, Some(Self::CYAN), "\n").await;
    }

    /// Handle `run_cmd_stdout`.
    pub async fn run_cmd_stdout(&self, data: &str) {
        self.styled(data, None, "").await;
    }

    /// Handle `run_cmd_stop`.
    pub async fn run_cmd_stop(&self, returncode: i32) {
        self.styled(&format!("\nExit code: {returncode}\n"), None, "\n")
            .await;
    }

    /// Handle `suite_started`.
    pub async fn suite_started(&self, suite: &Suite) {
        self.print_underline(&format!("Suite: {}", suite.name()))
            .await;
    }

    /// Handle `suite_completed`.
    pub async fn suite_completed(&self, results: &SuiteResults, exec_time: f64) {
        let message = format!(
            "\nExecution time: {}\n",
            Self::user_friendly_duration(exec_time)
        );
        self.styled(&message, None, "\n").await;
        if *self.num_suites.lock().await > 1 {
            self.print_summary(std::slice::from_ref(results)).await;
        }
    }

    /// Handle `suite_timeout`.
    pub async fn suite_timeout(&self, suite: &Suite, timeout: f64) {
        self.styled(
            &format!("Suite '{}' timed out after {timeout} seconds", suite.name()),
            Some(Self::RED),
            "\n",
        )
        .await;
    }

    /// Handle `session_warning`.
    pub async fn session_warning(&self, msg: &str) {
        self.styled(&format!("Warning: {msg}"), Some(Self::YELLOW), "\n")
            .await;
    }

    /// Handle `session_error`.
    pub async fn session_error(&self, error: &str) {
        self.styled(&format!("Error: {error}"), Some(Self::RED), "\n")
            .await;
    }

    /// Handle `session_completed`.
    pub async fn session_completed(&self, results: &[SuiteResults]) {
        if results.is_empty() {
            return;
        }
        self.styled("", None, "\n").await;
        self.print_target_info(&results[0]).await;
        self.print_section("TEST SUMMARY").await;
        self.print_summary(results).await;

        let mut broken = Vec::new();
        let mut failed = Vec::new();
        for suite_results in results {
            for test_results in suite_results.tests_results() {
                if test_results.failed() > 0 {
                    failed.push(test_results);
                }
                if test_results.broken() > 0 {
                    broken.push(test_results);
                }
            }
        }
        if !broken.is_empty() {
            self.styled("Broken:", Some(Self::RED), "\n").await;
            let names: Vec<String> = broken
                .iter()
                .map(|t| format!("    • {}", t.test().name()))
                .collect();
            self.styled(&names.join("\n"), None, "\n").await;
            self.styled("", None, "\n").await;
        }
        if !failed.is_empty() {
            self.styled("Failures:", Some(Self::RED), "\n").await;
            let names: Vec<String> = failed
                .iter()
                .map(|t| format!("    • {}", t.test().name()))
                .collect();
            self.styled(&names.join("\n"), None, "\n").await;
            self.styled("", None, "\n").await;
        }
    }

    /// Handle `session_dry_run_command`.
    pub async fn session_dry_run_command(&self, command: &str) {
        self.styled("Command:", Some(Self::CYAN), "\n").await;
        self.styled(&format!("    {command}\n"), None, "\n").await;
    }

    /// Handle `session_dry_run`.
    pub async fn session_dry_run(&self, suites: &[Suite]) {
        let mut total = 0;
        for suite in suites {
            let parallel: Vec<&str> = suite
                .tests()
                .iter()
                .filter(|t| t.parallelizable())
                .map(Test::name)
                .collect();
            let serial: Vec<&str> = suite
                .tests()
                .iter()
                .filter(|t| !t.parallelizable())
                .map(Test::name)
                .collect();
            total += suite.tests().len();

            self.print_underline(&format!("Suite: {}", suite.name()))
                .await;
            self.styled("Parallel tests:", Some(Self::CYAN), "\n").await;
            if parallel.is_empty() {
                self.styled("    (none)", None, "\n").await;
            } else {
                let names: Vec<String> = parallel.iter().map(|n| format!("    {n}")).collect();
                self.styled(&names.join("\n"), None, "\n").await;
            }
            self.styled("", None, "\n").await;
            self.styled("Serial tests:", Some(Self::CYAN), "\n").await;
            if serial.is_empty() {
                self.styled("    (none)", None, "\n").await;
            } else {
                let names: Vec<String> = serial.iter().map(|n| format!("    {n}")).collect();
                self.styled(&names.join("\n"), None, "\n").await;
            }
            self.styled("", None, "\n").await;
        }
        self.styled(&format!("Total tests: {total} (not executed)"), None, "\n")
            .await;
    }

    /// Handle `internal_error`.
    pub async fn internal_error(&self, func_name: &str, error: &str) {
        self.styled(
            &format!("\nUI error in function '{func_name}': {error}\n"),
            Some(Self::RED),
            "\n",
        )
        .await;
    }
}

#[derive(Default)]
struct SimpleState {
    sut_not_responding: bool,
    kernel_panic: bool,
    kernel_tainted: Option<String>,
    timed_out: bool,
}

/// Console UI without fancy output, mirroring `SimpleUserInterface`.
pub struct SimpleUi {
    console: ConsoleUi,
    state: tokio::sync::Mutex<SimpleState>,
}

impl SimpleUi {
    /// Create a simple UI writing to `printer`.
    pub fn new(no_colors: bool, printer: Arc<dyn Printer>) -> Self {
        Self {
            console: ConsoleUi::new(no_colors, printer),
            state: tokio::sync::Mutex::new(SimpleState::default()),
        }
    }

    /// Borrow the inner console UI.
    #[must_use]
    pub fn console(&self) -> &ConsoleUi {
        &self.console
    }

    /// Handle `sut_not_responding`.
    pub async fn sut_not_responding(&self) {
        self.state.lock().await.sut_not_responding = true;
        self.console
            .styled("SUT not responding", Some(ConsoleUi::RED), "\n")
            .await;
    }

    /// Handle `kernel_panic`.
    pub async fn kernel_panic(&self) {
        self.state.lock().await.kernel_panic = true;
        self.console
            .styled("kernel panic", Some(ConsoleUi::RED), "\n")
            .await;
    }

    /// Handle `kernel_tainted`.
    pub async fn kernel_tainted(&self, message: &str) {
        self.state.lock().await.kernel_tainted = Some(message.to_owned());
    }

    /// Handle `test_timed_out`.
    pub async fn test_timed_out(&self, timeout: i64) {
        self.state.lock().await.timed_out = true;
        self.console
            .styled("timed out", Some(ConsoleUi::RED), "\n")
            .await;
        let _ = timeout;
    }

    /// Handle `test_started`.
    pub async fn test_started(&self, test: &Test) {
        self.console
            .styled(&format!("{}: ", test.name()), Some(ConsoleUi::WHITE), "")
            .await;
    }

    /// Handle `test_completed`.
    pub async fn test_completed(&self, results: &TestResults) {
        let mut state = self.state.lock().await;
        if state.timed_out || state.sut_not_responding || state.kernel_panic {
            state.sut_not_responding = false;
            state.kernel_panic = false;
            state.timed_out = false;
            return;
        }
        let (msg, color) = ConsoleUi::result_color(results);
        let tainted = state.kernel_tainted.take();
        let exec_time = results.exec_time();
        drop(state);
        self.console.styled(msg, Some(color), "").await;
        if tainted.is_some() {
            self.console.styled(" | ", None, "").await;
            self.console
                .styled("tainted", Some(ConsoleUi::YELLOW), "")
                .await;
        }
        self.console
            .styled(
                &format!("  ({})", ConsoleUi::user_friendly_duration(exec_time)),
                None,
                "\n",
            )
            .await;
    }
}

/// Verbose console UI, mirroring `VerboseUserInterface`.
pub struct VerboseUi {
    console: ConsoleUi,
    timed_out: tokio::sync::Mutex<bool>,
}

impl VerboseUi {
    /// Create a verbose UI writing to `printer`.
    pub fn new(no_colors: bool, printer: Arc<dyn Printer>) -> Self {
        Self {
            console: ConsoleUi::new(no_colors, printer),
            timed_out: tokio::sync::Mutex::new(false),
        }
    }

    /// Borrow the inner console UI.
    #[must_use]
    pub fn console(&self) -> &ConsoleUi {
        &self.console
    }

    /// Handle `sut_stdout`.
    pub async fn sut_stdout(&self, data: &str) {
        self.console.styled(data, None, "").await;
    }

    /// Handle `kernel_tainted`.
    pub async fn kernel_tainted(&self, message: &str) {
        self.console
            .styled(
                &format!("Tainted kernel: {message}"),
                Some(ConsoleUi::YELLOW),
                "\n",
            )
            .await;
    }

    /// Handle `test_timed_out`.
    pub async fn test_timed_out(&self) {
        *self.timed_out.lock().await = true;
    }

    /// Handle `test_started`.
    pub async fn test_started(&self, test: &Test) {
        self.console.print_section(test.name()).await;
        self.console.styled("Executing: ", None, "").await;
        self.console
            .styled(&test.full_command(), None, "\n\n")
            .await;
    }

    /// Handle `test_completed`.
    pub async fn test_completed(&self, results: &TestResults) {
        if *self.timed_out.lock().await {
            self.console
                .styled("Test timed out", Some(ConsoleUi::RED), "\n")
                .await;
        }
        *self.timed_out.lock().await = false;

        let mut parts = Vec::new();
        if !results.stdout().contains("Summary:") {
            parts.extend([
                String::from("\nSummary:"),
                format!("passed    {}", results.passed()),
                format!("failed    {}", results.failed()),
                format!("broken    {}", results.broken()),
                format!("skipped   {}", results.skipped()),
                format!("warnings  {}", results.warnings()),
            ]);
        }
        parts.push(format!(
            "\nDuration: {}\n",
            ConsoleUi::user_friendly_duration(results.exec_time())
        ));
        self.console.styled(&parts.join("\n"), None, "\n").await;
    }

    /// Handle `test_stdout`.
    pub async fn test_stdout(&self, data: &str) {
        self.console.styled(data, None, "").await;
    }
}

#[derive(Default)]
struct ParallelState {
    sut_not_responding: bool,
    kernel_panic: bool,
    kernel_tainted: Option<String>,
    timed_out: bool,
    total: usize,
    done: usize,
}

/// Console UI for parallel execution, mirroring `ParallelUserInterface`.
pub struct ParallelUi {
    console: ConsoleUi,
    state: tokio::sync::Mutex<ParallelState>,
}

impl ParallelUi {
    /// Create a parallel UI writing to `printer`.
    pub fn new(no_colors: bool, printer: Arc<dyn Printer>) -> Self {
        Self {
            console: ConsoleUi::new(no_colors, printer),
            state: tokio::sync::Mutex::new(ParallelState::default()),
        }
    }

    /// Borrow the inner console UI.
    #[must_use]
    pub fn console(&self) -> &ConsoleUi {
        &self.console
    }

    /// Handle `sut_not_responding`.
    pub async fn sut_not_responding(&self) {
        self.state.lock().await.sut_not_responding = true;
    }

    /// Handle `kernel_panic`.
    pub async fn kernel_panic(&self) {
        self.state.lock().await.kernel_panic = true;
    }

    /// Handle `kernel_tainted`.
    pub async fn kernel_tainted(&self, message: &str) {
        self.state.lock().await.kernel_tainted = Some(message.to_owned());
    }

    /// Handle `test_timed_out`.
    pub async fn test_timed_out(&self) {
        self.state.lock().await.timed_out = true;
    }

    /// Handle `suite_started`, listing tests that run in parallel.
    pub async fn print_parallel(&self, suite: &Suite) {
        let parallel: Vec<String> = suite
            .tests()
            .iter()
            .filter(|t| t.parallelizable())
            .map(|t| format!("• {}", t.name()))
            .collect();
        if parallel.is_empty() {
            return;
        }
        self.state.lock().await.total += parallel.len();
        self.console
            .styled("Following tests will run in parallel:", None, "\n")
            .await;
        self.console
            .styled(&parallel.join("\n"), None, "\n\n")
            .await;
    }

    /// Handle `test_completed`.
    pub async fn test_completed(&self, results: &TestResults) {
        let mut state = self.state.lock().await;
        if results.test().parallelizable() {
            state.done += 1;
        }
        let prefix = if results.test().parallelizable() {
            format!(
                "{} ({}/{}): ",
                results.test().name(),
                state.done,
                state.total
            )
        } else {
            format!("{}: ", results.test().name())
        };
        let timed_out = state.timed_out;
        let not_responding = state.sut_not_responding;
        let panic = state.kernel_panic;
        let tainted = state.kernel_tainted.take();
        let exec_time = results.exec_time();
        let (msg, color) = ConsoleUi::result_color(results);
        state.sut_not_responding = false;
        state.kernel_panic = false;
        state.timed_out = false;
        drop(state);

        self.console.styled(&prefix, None, "").await;
        if timed_out {
            self.console
                .styled("timed out", Some(ConsoleUi::RED), "\n")
                .await;
        } else if not_responding {
            self.console
                .styled("SUT not responding", Some(ConsoleUi::RED), "\n")
                .await;
        } else if panic {
            self.console
                .styled("kernel panic", Some(ConsoleUi::RED), "\n")
                .await;
        } else {
            self.console.styled(&prefix, None, "").await;
            self.console.styled(msg, Some(color), "").await;
            if tainted.is_some() {
                self.console.styled(" | ", None, "").await;
                self.console
                    .styled("tainted", Some(ConsoleUi::YELLOW), "")
                    .await;
            }
            self.console
                .styled(
                    &format!("  ({})", ConsoleUi::user_friendly_duration(exec_time)),
                    None,
                    "\n",
                )
                .await;
        }
    }
}

/// Register the plain-string console handlers on `registry`.
///
/// Structured summaries (`session_completed`, `session_dry_run`,
/// `suite_started`, `suite_completed`, ...) need [`Suite`] data the
/// string-only registry cannot carry; render those with direct calls.
/// `session_started` expects `"<num_suites>\n<tmpdir>"`.
///
/// # Errors
///
/// Returns [`KirkError`](kirk_core::KirkError) when a registration fails.
pub async fn attach_console(
    ui: &Arc<ConsoleUi>,
    registry: &EventRegistry,
) -> Result<(), kirk_core::KirkError> {
    attach_session_events(ui, registry).await?;
    attach_command_events(ui, registry).await?;
    Ok(())
}

fn console_handler<F, Fut>(ui: &Arc<ConsoleUi>, call: F) -> kirk_events::Handler
where
    F: Fn(Arc<ConsoleUi>, String) -> Fut + Send + Sync + Clone + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    use kirk_events::{BoxFuture, EventArgs, HandlerResult};
    let ui = ui.clone();
    Arc::new(move |args: EventArgs| {
        let ui = ui.clone();
        let call = call.clone();
        let message = args.message.clone().unwrap_or_default();
        Box::pin(async move {
            call(ui, message).await;
            Ok(())
        }) as BoxFuture<HandlerResult>
    })
}

async fn attach_session_events(
    ui: &Arc<ConsoleUi>,
    registry: &EventRegistry,
) -> Result<(), kirk_core::KirkError> {
    registry
        .register(
            "session_restore",
            console_handler(ui, |ui, msg| async move {
                ui.session_restore(&msg).await;
            }),
            false,
        )
        .await?;
    registry
        .register(
            "session_started",
            console_handler(ui, |ui, msg| async move {
                let mut parts = msg.splitn(2, '\n');
                let count: usize = parts.next().unwrap_or("1").parse().unwrap_or(1);
                let tmpdir = parts.next().unwrap_or("");
                ui.session_started(count, tmpdir).await;
            }),
            false,
        )
        .await?;
    registry
        .register(
            "session_stopped",
            console_handler(ui, |ui, _| async move {
                ui.session_stopped().await;
            }),
            false,
        )
        .await?;
    registry
        .register(
            "sut_start",
            console_handler(ui, |ui, msg| async move {
                ui.sut_start(&msg).await;
            }),
            false,
        )
        .await?;
    registry
        .register(
            "sut_stop",
            console_handler(ui, |ui, msg| async move {
                ui.sut_stop(&msg).await;
            }),
            false,
        )
        .await?;
    registry
        .register(
            "sut_restart",
            console_handler(ui, |ui, msg| async move {
                ui.sut_restart(&msg).await;
            }),
            false,
        )
        .await?;
    registry
        .register(
            "session_warning",
            console_handler(ui, |ui, msg| async move {
                ui.session_warning(&msg).await;
            }),
            false,
        )
        .await?;
    registry
        .register(
            "session_error",
            console_handler(ui, |ui, msg| async move {
                ui.session_error(&msg).await;
            }),
            false,
        )
        .await?;
    registry
        .register(
            "session_dry_run_command",
            console_handler(ui, |ui, msg| async move {
                ui.session_dry_run_command(&msg).await;
            }),
            false,
        )
        .await?;
    Ok(())
}

async fn attach_command_events(
    ui: &Arc<ConsoleUi>,
    registry: &EventRegistry,
) -> Result<(), kirk_core::KirkError> {
    registry
        .register(
            "run_cmd_start",
            console_handler(ui, |ui, msg| async move {
                ui.run_cmd_start(&msg).await;
            }),
            false,
        )
        .await?;
    registry
        .register(
            "run_cmd_stdout",
            console_handler(ui, |ui, msg| async move {
                ui.run_cmd_stdout(&msg).await;
            }),
            false,
        )
        .await?;
    registry
        .register(
            "run_cmd_stop",
            console_handler(ui, |ui, msg| async move {
                ui.run_cmd_stop(msg.parse().unwrap_or_default()).await;
            }),
            false,
        )
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ui() -> (ConsoleUi, Arc<VecPrinter>) {
        let printer = Arc::new(VecPrinter::new());
        let ui = ConsoleUi::new(true, printer.clone());
        (ui, printer)
    }

    fn result(passed: u32, failed: u32, skipped: u32, broken: u32) -> TestResults {
        TestResults::new(Test::new("t", "echo").unwrap())
            .with_passed(passed)
            .with_failed(failed)
            .with_skipped(skipped)
            .with_broken(broken)
            .with_exec_time(0.1)
            .with_retcode(0)
            .with_stdout("")
    }

    #[test]
    fn friendly_duration_cases() {
        assert_eq!(ConsoleUi::user_friendly_duration(0.0), "0h 0m 0s");
        assert_eq!(ConsoleUi::user_friendly_duration(5.123), "5.123s");
        assert_eq!(ConsoleUi::user_friendly_duration(125.0), "2m 5s");
        assert_eq!(ConsoleUi::user_friendly_duration(3661.0), "1h 1m 1s");
    }

    #[test]
    fn format_cmdline_cases() {
        assert_eq!(ConsoleUi::format_cmdline(None), "");
        assert_eq!(ConsoleUi::format_cmdline(Some("")), "");
        assert_eq!(
            ConsoleUi::format_cmdline(Some("root=/dev/sda")),
            "root=/dev/sda"
        );
        let formatted = ConsoleUi::format_cmdline(Some("root=/dev/sda console=ttyS0"));
        assert!(formatted.contains("root=/dev/sda"));
        assert!(formatted.contains("console=ttyS0"));
    }

    #[test]
    fn result_color_cases() {
        assert_eq!(ConsoleUi::result_color(&result(1, 0, 0, 0)).0, "pass");
        assert_eq!(ConsoleUi::result_color(&result(0, 1, 0, 0)).0, "fail");
        assert_eq!(ConsoleUi::result_color(&result(0, 0, 1, 0)).0, "skip");
        assert_eq!(ConsoleUi::result_color(&result(0, 0, 0, 1)).0, "broken");
    }

    #[test]
    fn colors_disabled_when_requested() {
        let printer = Arc::new(VecPrinter::new());
        let colored = ConsoleUi::new(false, printer.clone());
        assert!(!colored.no_colors());
        let plain = ConsoleUi::new(true, printer);
        assert!(plain.no_colors());
    }

    #[tokio::test]
    async fn simple_not_responding_skips_result() {
        let printer = Arc::new(VecPrinter::new());
        let ui = SimpleUi::new(true, printer.clone());
        ui.sut_not_responding().await;
        ui.test_completed(&result(1, 0, 0, 0)).await;
        tokio::task::yield_now().await;
        let out = printer.contents();
        assert!(out.contains("SUT not responding"));
        assert!(!out.contains("pass"));
    }

    #[tokio::test]
    async fn simple_kernel_panic_skips_result() {
        let printer = Arc::new(VecPrinter::new());
        let ui = SimpleUi::new(true, printer.clone());
        ui.kernel_panic().await;
        ui.test_completed(&result(1, 0, 0, 0)).await;
        tokio::task::yield_now().await;
        assert!(printer.contents().contains("kernel panic"));
    }

    #[tokio::test]
    async fn simple_tainted_marks_result() {
        let printer = Arc::new(VecPrinter::new());
        let ui = SimpleUi::new(true, printer.clone());
        ui.kernel_tainted("proprietary module").await;
        ui.test_completed(&result(1, 0, 0, 0)).await;
        tokio::task::yield_now().await;
        let out = printer.contents();
        assert!(out.contains("tainted"));
        assert!(out.contains("pass"));
    }

    #[tokio::test]
    async fn simple_timed_out_skips_result() {
        let printer = Arc::new(VecPrinter::new());
        let ui = SimpleUi::new(true, printer.clone());
        ui.test_timed_out(30).await;
        ui.test_completed(&result(1, 0, 0, 0)).await;
        tokio::task::yield_now().await;
        assert!(printer.contents().contains("timed out"));
    }

    #[tokio::test]
    async fn verbose_handlers() {
        let printer = Arc::new(VecPrinter::new());
        let ui = VerboseUi::new(true, printer.clone());
        ui.sut_stdout("hello from sut").await;
        ui.kernel_tainted("proprietary module").await;
        let test = Test::new("t1", "echo").unwrap();
        ui.test_timed_out().await;
        ui.test_completed(&TestResults::new(test).with_exec_time(0.1).with_stdout(""))
            .await;
        tokio::task::yield_now().await;
        let out = printer.contents();
        assert!(out.contains("hello from sut"));
        assert!(out.contains("Tainted kernel"));
        assert!(out.contains("timed out"));
    }

    #[tokio::test]
    async fn parallel_counts_progress() {
        let printer = Arc::new(VecPrinter::new());
        let ui = ParallelUi::new(true, printer.clone());
        let suite = Suite::new(
            "s",
            vec![
                Test::new("p1", "echo").unwrap().with_parallelizable(true),
                Test::new("p2", "echo").unwrap().with_parallelizable(true),
                Test::new("s1", "echo").unwrap(),
            ],
        );
        ui.print_parallel(&suite).await;
        ui.test_completed(&TestResults::new(
            Test::new("p1", "echo").unwrap().with_parallelizable(true),
        ))
        .await;
        tokio::task::yield_now().await;
        let out = printer.contents();
        assert!(out.contains("Following tests will run in parallel:"));
        assert!(out.contains("p1 (1/2): "));
        assert!(out.contains("pass"));
    }

    #[tokio::test]
    async fn dry_run_splits_parallel_and_serial() {
        let (ui, printer) = ui();
        let suite = Suite::new(
            "mysuite",
            vec![
                Test::new("p1", "echo").unwrap().with_parallelizable(true),
                Test::new("s1", "echo").unwrap(),
            ],
        );
        ui.session_dry_run(std::slice::from_ref(&suite)).await;
        tokio::task::yield_now().await;
        let out = printer.contents();
        assert!(out.contains("Parallel tests:"));
        assert!(out.contains("Serial tests:"));
        assert!(out.contains("Total tests: 2 (not executed)"));
    }

    #[tokio::test]
    async fn completed_summary_lists_failures() {
        let (ui, printer) = ui();
        let test = Test::new("bad", "false").unwrap();
        let results = SuiteResults::new(Suite::new("s", vec![test.clone()]))
            .with_tests(vec![
                TestResults::new(test).with_failed(1).with_exec_time(1.0),
            ])
            .with_kernel("6.0")
            .with_distro("d")
            .with_distro_ver("1");
        ui.session_completed(std::slice::from_ref(&results)).await;
        tokio::task::yield_now().await;
        let out = printer.contents();
        assert!(out.contains("TEST SUMMARY"));
        assert!(out.contains("Failures:"));
        assert!(out.contains("bad"));
    }
}
