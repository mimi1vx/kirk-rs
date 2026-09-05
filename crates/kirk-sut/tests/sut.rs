//! Port of `kirk/libkirk/tests/test_sut.py` over an in-process fake channel.
//!
//! The fake mirrors the upstream `MockChannel` (first-substring-match
//! responses, always `Some`) plus per-command delays for the timeout test.
//! State lives behind shared handles so channel clones made by
//! [`GenericSut::setup_with_registry`] and the parallel gather observe the
//! same script.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use kirk_com::{CmdResult, ComChannel, IOBuffer, Registry};
use kirk_core::KirkError;
use kirk_core::data::Test;
use kirk_events::{BoxFuture, EventArgs, EventRegistry, HandlerResult};
use kirk_plugin::Plugin;
use kirk_sut::{
    FAULT_INJECTION_FILES, GenericSut, RUN_CMD_STDOUT_EVENT, RedirectSutStdout, RedirectTestStdout,
    SUT_STDOUT_EVENT, Sut, TEST_STDOUT_EVENT,
};

/// Scripted response: match commands containing `pattern`.
#[derive(Debug, Clone)]
struct Script {
    pattern: String,
    returncode: i32,
    stdout: String,
    delay_ms: u64,
}

/// Shared fake state; clones observe the same script and command log.
#[derive(Debug, Clone, Default)]
struct FakeState {
    scripts: Arc<std::sync::Mutex<Vec<Script>>>,
    commands: Arc<std::sync::Mutex<Vec<String>>>,
    active: Arc<AtomicBool>,
    communicate_calls: Arc<AtomicUsize>,
}

impl FakeState {
    fn set_response(&self, pattern: &str, returncode: i32, stdout: &str) {
        self.set_response_delayed(pattern, returncode, stdout, 0);
    }

    fn set_response_delayed(&self, pattern: &str, returncode: i32, stdout: &str, delay_ms: u64) {
        let mut scripts = self.scripts.lock().expect("fake script lock");
        // Mirror the upstream dict: re-scripting a pattern replaces it.
        scripts.retain(|script| script.pattern != pattern);
        scripts.push(Script {
            pattern: pattern.to_owned(),
            returncode,
            stdout: stdout.to_owned(),
            delay_ms,
        });
    }

    fn recorded(&self) -> Vec<String> {
        self.commands.lock().expect("fake command log lock").clone()
    }
}

struct FakeComChannel {
    name: String,
    parallel: bool,
    state: FakeState,
}

impl FakeComChannel {
    fn new(name: &str, parallel: bool, state: FakeState) -> Self {
        Self {
            name: name.to_owned(),
            parallel,
            state,
        }
    }
}

#[async_trait]
impl Plugin for FakeComChannel {
    fn name(&self) -> &str {
        &self.name
    }

    fn config_help(&self) -> HashMap<String, String> {
        HashMap::new()
    }

    fn setup(&mut self, _cfg: &HashMap<String, String>) -> Result<(), KirkError> {
        Ok(())
    }

    fn clone_box(&self, name: &str) -> Box<dyn Plugin> {
        Box::new(Self {
            name: name.to_owned(),
            parallel: self.parallel,
            state: self.state.clone(),
        })
    }
}

#[async_trait]
impl ComChannel for FakeComChannel {
    fn parallel_execution(&self) -> bool {
        self.parallel
    }

    async fn active(&self) -> bool {
        self.state.active.load(Ordering::SeqCst)
    }

    async fn communicate(&mut self, _iobuffer: Option<Arc<dyn IOBuffer>>) -> Result<(), KirkError> {
        self.state.communicate_calls.fetch_add(1, Ordering::SeqCst);
        self.state.active.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn stop(&mut self, _iobuffer: Option<Arc<dyn IOBuffer>>) -> Result<(), KirkError> {
        self.state.active.store(false, Ordering::SeqCst);
        Ok(())
    }

    async fn ping(&mut self) -> Result<f64, KirkError> {
        if self.active().await {
            Ok(0.001)
        } else {
            Err(KirkError::Communication(String::from("not active")))
        }
    }

    async fn run_command(
        &mut self,
        command: &str,
        _cwd: Option<&str>,
        _env: Option<&HashMap<String, String>>,
        _iobuffer: Option<Arc<dyn IOBuffer>>,
    ) -> Result<Option<CmdResult>, KirkError> {
        self.commands_push(command);
        let script = self.find_script(command);
        let delay_ms = script.as_ref().map_or(0, |found| found.delay_ms);
        if delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        }
        let (returncode, stdout) =
            script.map_or((0, String::new()), |found| (found.returncode, found.stdout));
        Ok(Some(CmdResult {
            command: command.to_owned(),
            returncode,
            stdout,
            exec_time: 0.001,
        }))
    }

    async fn fetch_file(&mut self, _target_path: &str) -> Result<Vec<u8>, KirkError> {
        Ok(Vec::new())
    }

    fn clone_channel_box(&self, new_name: &str) -> Box<dyn ComChannel> {
        Box::new(Self {
            name: new_name.to_owned(),
            parallel: self.parallel,
            state: self.state.clone(),
        })
    }
}

impl FakeComChannel {
    fn commands_push(&self, command: &str) {
        self.state
            .commands
            .lock()
            .expect("fake command log lock")
            .push(command.to_owned());
    }

    fn find_script(&self, command: &str) -> Option<Script> {
        self.state
            .scripts
            .lock()
            .expect("fake script lock")
            .iter()
            .find(|script| command.contains(&script.pattern))
            .cloned()
    }
}

/// SUT attached to a `shell` fake; returns the SUT plus shared fake state.
fn sut_with_shell(parallel: bool) -> (GenericSut, FakeState) {
    let state = FakeState::default();
    let mut registry = Registry::new();
    registry.register(Box::new(FakeComChannel::new(
        "shell",
        parallel,
        state.clone(),
    )));
    let mut sut = GenericSut::new();
    sut.setup_with_registry(&HashMap::new(), &registry)
        .expect("shell fake must attach");
    (sut, state)
}

/// Script the seven `get_info` probes with openSUSE-flavored output.
fn script_info(state: &FakeState, distro: &str, ram_kb: u32, swap_kb: u32) {
    state.set_response("os-release && echo \"$ID\"", 0, &format!("{distro}\n"));
    state.set_response("os-release && echo \"$VERSION_ID\"", 0, "15.5\n");
    state.set_response("uname -s -r -v", 0, "Linux 6.1.0\n");
    state.set_response("cat /proc/cmdline", 0, "root=/dev/sda\n");
    state.set_response("uname -m", 0, "x86_64\n");
    state.set_response("uname -p", 0, "x86_64\n");
    state.set_response(
        "cat /proc/meminfo",
        0,
        &format!("MemTotal:       {ram_kb} kB\nSwapTotal:       {swap_kb} kB\n"),
    );
}

fn sut_error_message(result: Result<(), KirkError>) -> String {
    result.expect_err("must fail").to_string()
}

#[test]
fn sut_name_is_default() {
    assert_eq!(GenericSut::new().name(), "default");
}

#[test]
fn config_help_describes_com() {
    let help = GenericSut::new().config_help();
    assert_eq!(
        help.get("com").map(String::as_str),
        Some("Communication channel name (default: shell)")
    );
}

#[test]
fn setup_defaults_to_shell() {
    let (sut, _) = sut_with_shell(false);
    let Ok(channel) = sut.channel() else {
        panic!("shell fake must attach");
    };
    assert_eq!(channel.name(), "shell");
}

#[test]
fn setup_missing_channel_errors() {
    let state = FakeState::default();
    let mut registry = Registry::new();
    registry.register(Box::new(FakeComChannel::new("shell", false, state)));
    let mut sut = GenericSut::new();
    let cfg = HashMap::from([(String::from("com"), String::from("ssh"))]);
    let err = sut
        .setup_with_registry(&cfg, &registry)
        .expect_err("unknown channel must fail");
    assert!(matches!(err, KirkError::Sut(_)));
    assert!(
        err.to_string()
            .contains("Can't find communication channel 'ssh'")
    );
}

#[test]
fn setup_without_channels_errors() {
    let registry = Registry::new();
    let mut sut = GenericSut::new();
    let err = sut
        .setup_with_registry(&HashMap::new(), &registry)
        .expect_err("empty registry must fail");
    assert!(
        err.to_string()
            .contains("No communication channels are provided")
    );
}

#[test]
fn setup_empty_com_name_errors() {
    let registry = Registry::new();
    let mut sut = GenericSut::new();
    let cfg = HashMap::from([(String::from("com"), String::new())]);
    let err = sut
        .setup_with_registry(&cfg, &registry)
        .expect_err("empty com must fail");
    assert!(
        err.to_string()
            .contains("Communication channel has not been defined")
    );
}

#[test]
fn channel_before_setup_errors() {
    let sut = GenericSut::new();
    let Err(err) = sut.channel() else {
        panic!("uninitialized SUT must fail");
    };
    assert!(err.to_string().contains("SUT is not initialized"));
}

#[tokio::test]
async fn start_stop_restart_lifecycle() {
    let (mut sut, state) = sut_with_shell(false);

    assert!(!sut.is_running().await.expect("running check"));
    sut.start(None).await.expect("start");
    assert!(sut.is_running().await.expect("running check"));

    // Starting twice communicates only once.
    sut.start(None).await.expect("second start");
    assert_eq!(state.communicate_calls.load(Ordering::SeqCst), 1);

    sut.stop(None).await.expect("stop");
    assert!(!sut.is_running().await.expect("running check"));

    // Stopping twice is a no-op.
    sut.stop(None).await.expect("second stop");

    sut.restart(None).await.expect("restart");
    assert!(sut.is_running().await.expect("running check"));
}

#[tokio::test]
async fn get_info_not_running_errors() {
    let (mut sut, _) = sut_with_shell(false);
    let err = sut.get_info().await.expect_err("stopped SUT must fail");
    assert!(err.to_string().contains("SUT is not running"));
}

#[tokio::test]
async fn get_info_reports_probes() {
    let (mut sut, state) = sut_with_shell(false);
    script_info(&state, "opensuse", 16_384, 8192);

    sut.start(None).await.expect("start");
    let info = sut.get_info().await.expect("info");

    assert_eq!(info.distro, "opensuse");
    assert_eq!(info.distro_ver, "15.5");
    assert_eq!(info.kernel, "Linux 6.1.0");
    assert_eq!(info.cmdline, "root=/dev/sda");
    assert_eq!(info.arch, "x86_64");
    assert_eq!(info.cpu, "x86_64");
    assert_eq!(info.ram, "16384 kB");
    assert_eq!(info.swap, "8192 kB");

    // Sequential probes run in upstream order with exact command strings.
    assert_eq!(
        state.recorded(),
        vec![
            ". /etc/os-release && echo \"$ID\"",
            ". /etc/os-release && echo \"$VERSION_ID\"",
            "uname -s -r -v",
            "cat /proc/cmdline",
            "uname -m",
            "uname -p",
            "cat /proc/meminfo",
        ]
    );
}

#[tokio::test]
async fn get_info_optimized_gathers_same_values() {
    let (mut sut, state) = sut_with_shell(true);
    script_info(&state, "fedora", 8192, 4096);
    sut.set_optimize(true);

    sut.start(None).await.expect("start");
    let info = sut.get_info().await.expect("info");

    assert_eq!(info.distro, "fedora");
    assert_eq!(info.distro_ver, "15.5");
    assert_eq!(info.ram, "8192 kB");
    assert_eq!(info.swap, "4096 kB");

    // All seven probes ran, association by index keeps values correct.
    let mut recorded = state.recorded();
    recorded.sort_unstable();
    let mut expected = vec![
        ". /etc/os-release && echo \"$ID\"",
        ". /etc/os-release && echo \"$VERSION_ID\"",
        "uname -s -r -v",
        "cat /proc/cmdline",
        "uname -m",
        "uname -p",
        "cat /proc/meminfo",
    ];
    expected.sort_unstable();
    assert_eq!(recorded, expected);
}

#[tokio::test]
async fn get_info_optimized_falls_back_without_parallel_channel() {
    let (mut sut, state) = sut_with_shell(false);
    script_info(&state, "fedora", 8192, 4096);
    sut.set_optimize(true);

    sut.start(None).await.expect("start");
    let info = sut.get_info().await.expect("info");

    assert_eq!(info.distro, "fedora");
    assert_eq!(info.ram, "8192 kB");
    assert_eq!(state.recorded().len(), 7);
}

#[tokio::test]
async fn run_cmd_unknown_on_timeout_and_failure() {
    let (mut sut, state) = sut_with_shell(false);
    state.set_response("uname -s -r -v", 0, "Linux 6.1.0\n");
    state.set_response_delayed("cat /proc/meminfo", 0, "MemTotal: 1 kB\n", 5_000);
    state.set_response("uname -m", 1, "boom\n");

    sut.start(None).await.expect("start");
    let info = sut.get_info().await.expect("info");

    assert_eq!(info.kernel, "Linux 6.1.0");
    assert_eq!(info.ram, "unknown");
    assert_eq!(info.swap, "unknown");
    assert_eq!(info.arch, "unknown");
}

#[tokio::test]
async fn get_tainted_info_not_running_errors() {
    let (mut sut, _) = sut_with_shell(false);
    let err = sut
        .get_tainted_info()
        .await
        .expect_err("stopped SUT must fail");
    assert!(err.to_string().contains("SUT is not running"));
}

#[tokio::test]
async fn get_tainted_info_clean_kernel() {
    let (mut sut, state) = sut_with_shell(false);
    state.set_response("cat /proc/sys/kernel/tainted", 0, "0\n");

    sut.start(None).await.expect("start");
    let info = sut.get_tainted_info().await.expect("taint");
    assert_eq!(info.code, 0);
    assert_eq!(info.messages, [] as [String; 0]);
}

#[tokio::test]
async fn get_tainted_info_tainted_kernel() {
    let (mut sut, state) = sut_with_shell(false);
    // Bits 0 and 1: proprietary module + force loaded.
    state.set_response("cat /proc/sys/kernel/tainted", 0, "3\n");

    sut.start(None).await.expect("start");
    let info = sut.get_tainted_info().await.expect("taint");
    assert_eq!(info.code, 3);
    assert_eq!(info.messages.len(), 2);
    assert!(
        info.messages
            .contains(&"proprietary module was loaded".to_owned())
    );
    assert!(
        info.messages
            .contains(&"module was force loaded".to_owned())
    );
}

#[tokio::test]
async fn get_tainted_info_reprobes_without_stale_cache() {
    let (mut sut, state) = sut_with_shell(false);
    state.set_response("cat /proc/sys/kernel/tainted", 0, "0\n");

    sut.start(None).await.expect("start");
    let first = sut.get_tainted_info().await.expect("taint");
    assert_eq!(first.code, 0);

    // Upstream only caches for in-flight callers; a later call re-probes.
    // Bit 9 set: "kernel issued warning".
    state.set_response("cat /proc/sys/kernel/tainted", 0, "512\n");
    let second = sut.get_tainted_info().await.expect("taint");
    assert_eq!(second.code, 512);
    assert_eq!(second.messages, vec!["kernel issued warning".to_owned()]);

    let taint_probes = state
        .recorded()
        .into_iter()
        .filter(|cmd| cmd.contains("tainted"))
        .count();
    assert_eq!(taint_probes, 2);
}

#[tokio::test]
async fn get_tainted_info_command_failure_errors() {
    let (mut sut, state) = sut_with_shell(false);
    state.set_response("cat /proc/sys/kernel/tainted", 1, "error");

    sut.start(None).await.expect("start");
    let err = sut
        .get_tainted_info()
        .await
        .expect_err("failed taint read must fail");
    assert!(
        err.to_string()
            .contains("Can't read tainted kernel information")
    );
}

#[tokio::test]
async fn get_tainted_info_non_digit_errors() {
    let (mut sut, state) = sut_with_shell(false);
    state.set_response("cat /proc/sys/kernel/tainted", 0, "Permission denied\n");

    sut.start(None).await.expect("start");
    let err = sut
        .get_tainted_info()
        .await
        .expect_err("non-digit taint must fail");
    assert!(err.to_string().contains("Permission denied"));
}

#[tokio::test]
async fn logged_as_root_reports_uid() {
    let (mut sut, state) = sut_with_shell(false);
    state.set_response("id -u", 0, "0\n");

    sut.start(None).await.expect("start");
    assert!(sut.logged_as_root().await.expect("root check"));

    state.set_response("id -u", 0, "1000\n");
    assert!(!sut.logged_as_root().await.expect("root check"));
}

#[tokio::test]
async fn logged_as_root_not_running_errors() {
    let (mut sut, _) = sut_with_shell(false);
    let err = sut
        .logged_as_root()
        .await
        .expect_err("stopped SUT must fail");
    assert!(err.to_string().contains("SUT is not running"));
}

#[tokio::test]
async fn logged_as_root_command_failure_errors() {
    let (mut sut, state) = sut_with_shell(false);
    state.set_response("id -u", 1, "error");

    sut.start(None).await.expect("start");
    let err = sut.logged_as_root().await.expect_err("failed id must fail");
    assert!(
        err.to_string()
            .contains("Can't determine if we are running as root")
    );
}

#[tokio::test]
async fn logged_as_root_invalid_output_errors() {
    let (mut sut, state) = sut_with_shell(false);
    state.set_response("id -u", 0, "not_a_number\n");

    sut.start(None).await.expect("start");
    let err = sut
        .logged_as_root()
        .await
        .expect_err("invalid id must fail");
    assert!(err.to_string().contains("'id -u' returned not_a_number"));
}

#[tokio::test]
async fn fault_injection_enabled_when_all_dirs_exist() {
    let (mut sut, state) = sut_with_shell(false);
    for file in FAULT_INJECTION_FILES {
        state.set_response(&format!("test -d /sys/kernel/debug/{file}"), 0, "");
    }

    sut.start(None).await.expect("start");
    assert!(sut.is_fault_injection_enabled().await.expect("fault check"));
}

#[tokio::test]
async fn fault_injection_disabled_when_dir_missing() {
    let (mut sut, state) = sut_with_shell(false);
    state.set_response("test -d", 1, "");

    sut.start(None).await.expect("start");
    assert!(!sut.is_fault_injection_enabled().await.expect("fault check"));
}

#[tokio::test]
async fn fault_injection_not_running_errors() {
    let (mut sut, _) = sut_with_shell(false);
    let err = sut
        .is_fault_injection_enabled()
        .await
        .expect_err("stopped SUT must fail");
    assert!(err.to_string().contains("SUT is not running"));
}

#[tokio::test]
async fn setup_fault_injection_writes_knobs() {
    let (mut sut, state) = sut_with_shell(false);
    state.set_response("echo", 0, "");

    sut.start(None).await.expect("start");
    sut.setup_fault_injection(50, 2).await.expect("fault setup");

    let mut expected = Vec::new();
    for file in FAULT_INJECTION_FILES {
        let base = format!("/sys/kernel/debug/{file}");
        expected.push(format!("echo 0 > {base}/space"));
        expected.push(format!("echo -1 > {base}/times"));
        expected.push(format!("echo 2 > {base}/interval"));
        expected.push(format!("echo 50 > {base}/probability"));
    }
    assert_eq!(state.recorded(), expected);
}

#[tokio::test]
async fn setup_fault_injection_zero_prob_resets() {
    let (mut sut, state) = sut_with_shell(false);
    state.set_response("echo", 0, "");

    sut.start(None).await.expect("start");
    sut.setup_fault_injection(0, 7).await.expect("fault reset");

    let mut expected = Vec::new();
    for file in FAULT_INJECTION_FILES {
        let base = format!("/sys/kernel/debug/{file}");
        expected.push(format!("echo 0 > {base}/space"));
        expected.push(format!("echo 1 > {base}/times"));
        expected.push(format!("echo 1 > {base}/interval"));
        expected.push(format!("echo 0 > {base}/probability"));
    }
    assert_eq!(state.recorded(), expected);
}

#[tokio::test]
async fn setup_fault_injection_not_running_errors() {
    let (mut sut, _) = sut_with_shell(false);
    let message = sut_error_message(sut.setup_fault_injection(50, 1).await);
    assert!(message.contains("SUT is not running"));
}

#[tokio::test]
async fn setup_fault_injection_write_failure_errors() {
    let (mut sut, state) = sut_with_shell(false);
    state.set_response("echo", 1, "read-only\n");

    sut.start(None).await.expect("start");
    let err = sut
        .setup_fault_injection(50, 1)
        .await
        .expect_err("failed write must fail");
    assert!(err.to_string().contains("Can't setup"));
}

/// Collect `event` payloads until `writes` complete, mirroring the upstream
/// poll loops around the event queue.
async fn collect_events(
    registry: &EventRegistry,
    event: &str,
    writes: impl Future<Output = ()>,
) -> Vec<String> {
    let received = Arc::new(std::sync::Mutex::new(Vec::new()));
    let store = Arc::clone(&received);
    let handler: Arc<dyn Fn(EventArgs) -> BoxFuture<HandlerResult> + Send + Sync> =
        Arc::new(move |args: EventArgs| {
            let store = Arc::clone(&store);
            Box::pin(async move {
                store
                    .lock()
                    .map_err(|_| String::from("event store poisoned"))?
                    .push(args.message.unwrap_or_default());
                Ok(())
            }) as BoxFuture<HandlerResult>
        });
    registry
        .register(event, handler, false)
        .await
        .expect("register handler");

    let runner = registry.clone();
    let pump = tokio::spawn(async move { runner.start().await });
    writes.await;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let done = received.lock().expect("event store lock").is_empty();
            if !done {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("event delivery");
    registry.stop();
    pump.await.expect("event pump");
    received.lock().expect("event store lock").clone()
}

#[tokio::test]
async fn redirect_sut_stdout_fires_sut_event() {
    let events = EventRegistry::new();
    let redirect = RedirectSutStdout::new("default", false, events.clone());
    assert_eq!(redirect.sut_name(), "default");
    assert!(!redirect.is_cmd());

    let payloads = collect_events(&events, SUT_STDOUT_EVENT, async {
        redirect.write("hello").await.expect("write");
    })
    .await;
    assert_eq!(payloads, vec!["hello".to_owned()]);
}

#[tokio::test]
async fn redirect_sut_stdout_cmd_fires_cmd_event() {
    let events = EventRegistry::new();
    let redirect = RedirectSutStdout::new("default", true, events.clone());
    assert!(redirect.is_cmd());

    let payloads = collect_events(&events, RUN_CMD_STDOUT_EVENT, async {
        redirect.write("cmd output").await.expect("write");
    })
    .await;
    assert_eq!(payloads, vec!["cmd output".to_owned()]);
}

#[tokio::test]
async fn redirect_test_stdout_accumulates_and_fires() {
    let events = EventRegistry::new();
    let test = Test::new("mytest", "echo").expect("test");
    let redirect = RedirectTestStdout::new(test, events.clone());
    assert_eq!(redirect.test().name(), "mytest");

    let runner = events.clone();
    let received = Arc::new(std::sync::Mutex::new(Vec::new()));
    let store = Arc::clone(&received);
    let handler: Arc<dyn Fn(EventArgs) -> BoxFuture<HandlerResult> + Send + Sync> =
        Arc::new(move |args: EventArgs| {
            let store = Arc::clone(&store);
            Box::pin(async move {
                store
                    .lock()
                    .map_err(|_| String::from("event store poisoned"))?
                    .push(args.message.unwrap_or_default());
                Ok(())
            }) as BoxFuture<HandlerResult>
        });
    events
        .register(TEST_STDOUT_EVENT, handler, false)
        .await
        .expect("register handler");
    let pump = tokio::spawn(async move { runner.start().await });

    redirect.write("output1").await.expect("write");
    redirect.write("output2").await.expect("write");

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let count = received.lock().expect("event store lock").len();
            if count >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("event delivery");
    events.stop();
    pump.await.expect("event pump");

    assert_eq!(redirect.stdout().expect("buffer"), "output1output2");
    assert_eq!(
        received.lock().expect("event store lock").clone(),
        vec!["output1".to_owned(), "output2".to_owned()]
    );
}
