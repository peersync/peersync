#![feature(async_closure)]
#![feature(ip)]

mod app;
mod cleaner;
mod config;
mod dht;
mod docker;
mod logging;
mod merkle_tree;
mod multicast;
mod node;
mod parallel_download;
mod peer;
mod server;
mod system;
mod tracker;
mod util;

use std::{env, process::exit};

use clap::Parser;
use ctrlc;
use log::{error, info, LevelFilter};

#[derive(Parser)]
#[command(name = env!("CARGO_PKG_NAME"))]
#[command(version = env!("CARGO_PKG_VERSION"))]
#[command(about = env!("CARGO_PKG_DESCRIPTION"), long_about = None)]
struct Args {
    /// Location of configuration file
    #[arg(short, long, value_name = "/path/to/config.yaml")]
    config: String,

    /// Enable debug output
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let log_level = match args.verbose {
        0 => LevelFilter::Warn,
        1 => LevelFilter::Info,
        _ => LevelFilter::Debug,
    };

    if args.verbose >= 1 {
        env::set_var("RUST_BACKTRACE", "1");
    }

    if let Err(e) = logging::setup_logger(log_level) {
        eprintln!("Failed to initialize logger: {e}. ");
        exit(1);
    }

    info!("Started {} version {}. ", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));

    if let Err(e) = ctrlc::set_handler(move || {
        info!("Received SIGINT, shutting down. ");
        exit(0);
    }) {
        error!("Failed to set SIGINT handler: {e}. ");
        exit(1);
    }

    let config = match config::Config::new(&args.config) {
        Ok(config) => config,
        Err(e) => {
            error!("Failed to parse configuration file: {e}. ");
            exit(1);
        },
    };

    let app = match app::App::new(config) {
        Ok(app) => app,
        Err(e) => {
            eprintln!("Program failed: {e}. ");
            exit(1);
        },
    };

    if let Err(e) = app.start().await {
        eprintln!("Program failed: {e}. ");
        exit(1);
    }
}
