//! Encodes the capability invariant every `ComChannel` builtin must uphold:
//! advertising `parallel_execution() == true` without a working
//! `concurrent_handle()` silently serializes `--workers` execution instead
//! of erroring (see plans/parallel-executor-audit.md, findings A/B).

use kirk_com::ComChannel;
use kirk_com_ltx::LtxChannel;
use kirk_com_qemu::QemuChannel;
use kirk_com_shell::ShellChannel;
use kirk_com_ssh::SshChannel;

fn assert_capability_honest(name: &str, channel: &dyn ComChannel) {
    assert!(
        !channel.parallel_execution() || channel.concurrent_handle().is_some(),
        "{name}: parallel_execution() is true but concurrent_handle() is None \
         -- this channel claims to support --workers concurrency but silently \
         serializes every command",
    );
}

#[test]
fn shell_honors_parallel_execution_capability() {
    assert_capability_honest("shell", &ShellChannel::new());
}

#[test]
fn ssh_honors_parallel_execution_capability() {
    assert_capability_honest("ssh", &SshChannel::new("ssh"));
}

#[test]
fn qemu_honors_parallel_execution_capability() {
    assert_capability_honest("qemu", &QemuChannel::new());
}

#[test]
fn ltx_honors_parallel_execution_capability() {
    assert_capability_honest("ltx", &LtxChannel::new());
}
