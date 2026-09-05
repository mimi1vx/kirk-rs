//! `kirk` binary: parse, validate, print help, run the session.
//!
//! Exit codes mirror upstream: `0` ok, `1` session failure, `2` argument
//! error, `130` on interrupt. Only this binary layer uses `anyhow`.

use anyhow::Context;
use clap::Parser;
use kirk_cli::args::Args;
use kirk_cli::session::{builtin_plugins, run_session};
use kirk_cli::validate::{Validation, plugin_help, validate};

#[tokio::main]
async fn main() {
    std::process::exit(run().await);
}

/// Parse, validate, and run, returning the process exit code.
async fn run() -> i32 {
    let args = Args::parse();
    let (coms, suts) = builtin_plugins();
    match validate(&args, &coms, &suts) {
        Err(err) => {
            eprintln!("kirk: error: {err}");
            2
        }
        Ok(Validation::ComHelp) => {
            print!("{}", plugin_help("--com", &coms));
            0
        }
        Ok(Validation::SutHelp) => {
            print!("{}", plugin_help("--sut", &suts));
            0
        }
        Ok(Validation::Proceed) => match run_session(&args).await.context("session setup failed") {
            Ok(code) => code,
            Err(err) => {
                eprintln!("kirk: error: {err:#}");
                2
            }
        },
    }
}
