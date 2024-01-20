use std::io::stderr;

use anyhow::Result;
use chrono::Local;
use fern::colors::{Color, ColoredLevelConfig};
use log::{LevelFilter, Record};

pub fn setup_logger(log_level: LevelFilter) -> Result<()> {
    let colors = ColoredLevelConfig::new()
        .error(Color::Red)
        .warn(Color::Yellow)
        .info(Color::Green)
        .debug(Color::Magenta)
        .trace(Color::BrightBlue);

    let log_path = |record: &Record| {
        match (record.file(), record.line()) {
            (Some(path), Some(line_number)) => format!("{path}:{line_number}"),
            _ => format!("{}", record.target()),
        }
    };

    fern::Dispatch::new()
        .format(move |out, message, record| {
            out.finish(format_args!(
                "[{} {} {}] {}",
                Local::now().format("%Y-%m-%dT%H:%M:%S"),
                colors.color(record.level()),
                log_path(record),
                message,
            ))
        })
        .level_for(env!("CARGO_PKG_NAME"), log_level)
        .level(LevelFilter::Off)
        .chain(stderr())
        .apply()?;

    Ok(())
}
