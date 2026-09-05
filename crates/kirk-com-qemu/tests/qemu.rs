//! `QEMU` channel tests, mirroring `kirk/libkirk/tests/test_qemu.py`.
//!
//! Unit tests run without `KVM` (command shape, validation, retcode parsing,
//! expect-loop over an in-process fake serial). Live tests are `#[ignore]`d
//! and need `TEST_QEMU_IMAGE`/`TEST_QEMU_USERNAME`/`TEST_QEMU_PASSWORD`
//! (plus `TEST_QEMU_KERNEL`/`TEST_QEMU_BUSYBOX` for the `busybox` case).

use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use kirk_com::ComChannel;
use kirk_com_qemu::QemuChannel;
use kirk_com_qemu::QemuConfig;
use kirk_com_qemu::expect::ExpectState;
use kirk_com_qemu::expect::SerialIo;
use kirk_com_qemu::expect::WaitOptions;
use kirk_com_qemu::expect::parse_reply;
use kirk_com_qemu::expect::shell_quote;
use kirk_com_qemu::expect::split_options;
use kirk_com_qemu::expect::validate_env_key;
use kirk_com_qemu::expect::wait_for_message;
use kirk_core::KirkError;
use kirk_plugin::Plugin;

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_dir(name: &str) -> String {
    use std::sync::atomic::Ordering;
    let id = TEMP_COUNTER.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("kirk-qemu-{name}-{}-{id}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir.to_str().expect("UTF-8").to_string()
}

fn base_config(tmpdir: &str, serial: &str) -> HashMap<String, String> {
    HashMap::from([
        ("tmpdir".to_string(), tmpdir.to_string()),
        ("serial".to_string(), serial.to_string()),
    ])
}

struct FakeSerial {
    chunks: VecDeque<String>,
}

impl FakeSerial {
    fn new(chunks: &[&str]) -> Self {
        Self {
            chunks: chunks.iter().map(ToString::to_string).collect(),
        }
    }
}

#[async_trait::async_trait]
impl SerialIo for FakeSerial {
    async fn read_chunk(&mut self, _max: usize) -> std::io::Result<Option<String>> {
        Ok(self.chunks.pop_front())
    }
}

#[test]
fn rejects_bad_config_values() {
    // Bad tmpdir.
    let mut channel = QemuChannel::new();
    let err = channel
        .setup(&base_config("/no-such-kirk-dir-xyz", "isa"))
        .expect_err("bad tmpdir");
    assert!(matches!(err, KirkError::Communication(_)));

    // Bad serial.
    let tmpdir = temp_dir("cfg");
    let mut channel = QemuChannel::new();
    let err = channel
        .setup(&base_config(&tmpdir, "usb"))
        .expect_err("bad serial");
    assert!(matches!(err, KirkError::Communication(_)));

    // Missing image file.
    let mut map = base_config(&tmpdir, "isa");
    map.insert("image".to_string(), "/no-such-image-xyz".to_string());
    let err = channel.setup(&map).expect_err("missing image");
    assert!(matches!(err, KirkError::Communication(_)));
    std::fs::remove_dir_all(&tmpdir).ok();
}

#[test]
fn setup_defaults_and_transport() {
    let tmpdir = temp_dir("defaults");
    let mut channel = QemuChannel::new();
    channel
        .setup(&base_config(&tmpdir, "isa"))
        .expect("valid config");
    assert!(!channel.parallel_execution());
    assert_eq!(channel.name(), "qemu");
    assert!(!channel.config_help().is_empty());
    std::fs::remove_dir_all(&tmpdir).ok();
}

#[test]
fn command_shape_isa_vs_virtio() {
    let tmpdir = temp_dir("shape");
    let image = format!("{tmpdir}/disk.img");
    let kernel = format!("{tmpdir}/vmlinuz");
    std::fs::write(&image, b"fake").expect("write image");
    std::fs::write(&kernel, b"fake").expect("write kernel");

    let mut isa_map = base_config(&tmpdir, "isa");
    isa_map.insert("image".to_string(), image.clone());
    isa_map.insert("kernel".to_string(), kernel.clone());
    let isa = QemuConfig::from_map(&isa_map).expect("isa config");
    let (program, argv) = isa.build_argv(42).expect("isa argv");
    assert_eq!(program, "qemu-system-x86_64");
    // Spawned binary is `QEMU` itself: never `sh`.
    assert!(program.starts_with("qemu-system-"));
    assert!(argv.contains(&"-serial".to_string()));
    assert!(argv.contains(&"chardev:tty".to_string()));
    assert!(!argv.iter().any(|arg| arg.contains("virtio-serial")));
    assert!(argv.iter().any(|arg| arg.contains("console=ttyS0")));
    assert!(argv.iter().any(|arg| arg.ends_with("ttyS0-42.log")));
    assert!(argv.iter().any(|arg| arg.ends_with("transport-42")));

    let mut virtio_map = base_config(&tmpdir, "virtio");
    virtio_map.insert("image".to_string(), image.clone());
    virtio_map.insert("kernel".to_string(), kernel.clone());
    let virtio = QemuConfig::from_map(&virtio_map).expect("virtio config");
    let (_, argv) = virtio.build_argv(43).expect("virtio argv");
    assert!(argv.contains(&"virtio-serial".to_string()));
    assert!(argv.contains(&"virtconsole,chardev=tty".to_string()));
    assert!(argv.contains(&"virtserialport,chardev=transport".to_string()));
    assert!(!argv.iter().any(|arg| arg == "chardev:tty"));
    assert!(argv.iter().any(|arg| arg.contains("console=hvc0")));

    // Options are split into words, never shelled.
    let mut opts_map = base_config(&tmpdir, "isa");
    opts_map.insert("options".to_string(), "-cpu host -smp 4".to_string());
    let opts = QemuConfig::from_map(&opts_map).expect("options config");
    let (_, argv) = opts.build_argv(1).expect("options argv");
    assert!(argv.contains(&"-cpu".to_string()));
    assert!(argv.contains(&"host".to_string()));

    std::fs::remove_dir_all(&tmpdir).ok();
}

#[test]
fn retcode_and_quoting_helpers() {
    let (out, retcode) = parse_reply("\nhello\n0-deadbeef\n# ", "deadbeef").expect("parses");
    assert_eq!(out, "hello\n");
    assert_eq!(retcode, 0);
    parse_reply("\nno marker here\n# ", "deadbeef").expect_err("missing marker");
    assert_eq!(shell_quote("/tmp/a b"), "'/tmp/a b'");
    validate_env_key("PATH").expect("valid");
    validate_env_key("9LIVES").expect_err("invalid");
    assert_eq!(
        split_options("-m 2G -append 'a b'").expect("splits"),
        vec!["-m", "2G", "-append", "a b"]
    );
}

#[tokio::test]
async fn expect_loop_fake_serial() {
    // Leading `\n` mirrors the newline echo of a real serial.
    let mut io = FakeSerial::new(&["\necho hi\n", "0-xyz", "9\n# "]);
    let mut state = ExpectState::default();
    let cancel = AtomicBool::new(false);
    let options = WaitOptions {
        timeout: Duration::from_secs(5),
        panic_settle: Duration::from_millis(0),
    };
    let out = wait_for_message(
        &mut io,
        &mut state,
        "xyz9",
        options,
        &cancel,
        &|| true,
        None,
    )
    .await
    .expect("marker arrives");
    assert!(out.contains("0-xyz9"));
    let (parsed, retcode) = parse_reply(&out, "xyz9").expect("retcode parses");
    assert_eq!(retcode, 0);
    assert!(parsed.contains("echo hi"));
}

#[tokio::test]
async fn expect_loop_detects_panic() {
    let mut io = FakeSerial::new(&["boot\nKernel panic - not syncing\n"]);
    let mut state = ExpectState::default();
    let cancel = AtomicBool::new(false);
    let options = WaitOptions {
        timeout: Duration::from_secs(5),
        panic_settle: Duration::from_millis(0),
    };
    let err = wait_for_message(&mut io, &mut state, "#", options, &cancel, &|| true, None)
        .await
        .expect_err("panic raises");
    assert!(matches!(err, KirkError::KernelPanic(_)));
}

#[tokio::test]
async fn channel_rejects_empty_command_without_kvm() {
    let tmpdir = temp_dir("empty");
    let mut channel = QemuChannel::new();
    channel
        .setup(&base_config(&tmpdir, "isa"))
        .expect("valid config");
    let err = channel
        .run_command("", None, None, None)
        .await
        .expect_err("empty command");
    assert!(matches!(err, KirkError::Communication(_)));
    let err = channel.fetch_file("").await.expect_err("empty target path");
    assert!(matches!(err, KirkError::Communication(_)));
    let _: Arc<dyn kirk_com::IOBuffer> = Arc::new(Printer);
    std::fs::remove_dir_all(&tmpdir).ok();
}

struct Printer;

#[async_trait::async_trait]
impl kirk_com::IOBuffer for Printer {
    async fn write(&self, _data: &str) -> Result<(), KirkError> {
        Ok(())
    }
}

// --- Live tests (need KVM + guest image) --------------------------------

fn live_image_config(serial: &str) -> Option<(String, HashMap<String, String>)> {
    let image = std::env::var("TEST_QEMU_IMAGE").ok()?;
    let user = std::env::var("TEST_QEMU_USERNAME").ok()?;
    let password = std::env::var("TEST_QEMU_PASSWORD").ok()?;
    if image.is_empty() || user.is_empty() || password.is_empty() {
        return None;
    }
    let tmpdir = temp_dir("live");
    let map = HashMap::from([
        ("tmpdir".to_string(), tmpdir.clone()),
        ("image".to_string(), image),
        ("user".to_string(), user),
        ("password".to_string(), password),
        ("serial".to_string(), serial.to_string()),
    ]);
    Some((tmpdir, map))
}

async fn live_roundtrip(channel: &mut QemuChannel) {
    let iobuffer: Arc<dyn kirk_com::IOBuffer> = Arc::new(Printer);
    channel
        .communicate(Some(iobuffer.clone()))
        .await
        .expect("guest boots");
    assert!(channel.active().await);
    let result = channel
        .run_command("echo hello", None, None, Some(iobuffer.clone()))
        .await
        .expect("run works")
        .expect("result present");
    assert_eq!(result.returncode, 0);
    assert!(result.stdout.contains("hello"));
    channel.ping().await.expect("ping works");
    channel
        .run_command("echo fetch-me > /tmp/kirk-fetch.txt", None, None, None)
        .await
        .expect("stage file");
    let data = channel
        .fetch_file("/tmp/kirk-fetch.txt")
        .await
        .expect("fetch works");
    assert!(String::from_utf8_lossy(&data).contains("fetch-me"));
    channel.stop(Some(iobuffer)).await.expect("stops");
    assert!(!channel.active().await);
}

#[tokio::test]
#[ignore = "needs KVM and TEST_QEMU_IMAGE/USERNAME/PASSWORD"]
async fn live_isa() {
    let Some((tmpdir, map)) = live_image_config("isa") else {
        eprintln!("skipped: TEST_QEMU_IMAGE/USERNAME/PASSWORD not set");
        return;
    };
    let mut channel = QemuChannel::new();
    channel.setup(&map).expect("live config");
    live_roundtrip(&mut channel).await;
    std::fs::remove_dir_all(&tmpdir).ok();
}

#[tokio::test]
#[ignore = "needs KVM and TEST_QEMU_IMAGE/USERNAME/PASSWORD"]
async fn live_virtio() {
    let Some((tmpdir, map)) = live_image_config("virtio") else {
        eprintln!("skipped: TEST_QEMU_IMAGE/USERNAME/PASSWORD not set");
        return;
    };
    let mut channel = QemuChannel::new();
    channel.setup(&map).expect("live config");
    live_roundtrip(&mut channel).await;
    std::fs::remove_dir_all(&tmpdir).ok();
}

#[tokio::test]
#[ignore = "needs KVM and TEST_QEMU_KERNEL/TEST_QEMU_BUSYBOX"]
async fn live_busybox() {
    let kernel = std::env::var("TEST_QEMU_KERNEL").unwrap_or_default();
    let initrd = std::env::var("TEST_QEMU_BUSYBOX").unwrap_or_default();
    if kernel.is_empty() || initrd.is_empty() {
        eprintln!("skipped: TEST_QEMU_KERNEL/TEST_QEMU_BUSYBOX not set");
        return;
    }
    let tmpdir = temp_dir("busybox");
    let map = HashMap::from([
        ("tmpdir".to_string(), tmpdir.clone()),
        ("kernel".to_string(), kernel),
        ("initrd".to_string(), initrd),
        ("prompt".to_string(), "/ #".to_string()),
    ]);
    let mut channel = QemuChannel::new();
    channel.setup(&map).expect("busybox config");
    let iobuffer: Arc<dyn kirk_com::IOBuffer> = Arc::new(Printer);
    channel
        .communicate(Some(iobuffer.clone()))
        .await
        .expect("busybox boots");
    let result = channel
        .run_command("echo hello", None, None, None)
        .await
        .expect("run works")
        .expect("result present");
    assert_eq!(result.returncode, 0);
    channel.stop(Some(iobuffer)).await.expect("stops");
    std::fs::remove_dir_all(&tmpdir).ok();
}
