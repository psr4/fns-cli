use std::fs::File;
use std::io;
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

pub fn init_logging(verbose: bool, log_file: Option<&str>) {
    let default_level = if verbose { "debug" } else { "info" };

    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_level));

    let registry = tracing_subscriber::registry().with(filter);

    match log_file {
        Some(path) => {
            let file = File::create(path)
                .unwrap_or_else(|e| panic!("Failed to create log file '{}': {}", path, e));

            let file_layer = fmt::layer().with_writer(file).with_ansi(false);

            let stderr_layer = fmt::layer().with_writer(io::stderr);

            registry.with(file_layer).with(stderr_layer).init();
        }
        None => {
            let fmt_layer = fmt::layer().with_writer(io::stderr);

            registry.with(fmt_layer).init();
        }
    }
}
