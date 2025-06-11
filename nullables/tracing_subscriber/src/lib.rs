use std::str::FromStr;
use tracing_subscriber::EnvFilter;

pub fn init_tracing() {
    init_tracing_with_default_log_level("info");
}

pub fn init_tracing_with_default_log_level(default_level: impl Into<String>) {
    let dirs = std::env::var(EnvFilter::DEFAULT_ENV).unwrap_or(default_level.into());
    let filter = EnvFilter::builder().parse_lossy(dirs);
    let value = std::env::var("NANO_LOG");
    let log_style: LogStyle = value
        .as_ref()
        .map(|i| i.as_str())
        .unwrap_or_default()
        .parse()
        .unwrap();

    init_tracing_subscriber(log_style, filter);
}

fn init_tracing_subscriber(log_style: LogStyle, filter: EnvFilter) {
    match log_style {
        LogStyle::Json => {
            tracing_subscriber::fmt::fmt()
                .json()
                .with_env_filter(filter)
                .init();
        }
        LogStyle::NoAnsi => {
            tracing_subscriber::fmt::fmt()
                .with_env_filter(filter)
                .with_ansi(false)
                .init();
        }
        LogStyle::Ansi => {
            tracing_subscriber::fmt::fmt()
                .with_env_filter(filter)
                .with_ansi(true)
                .init();
        }
    }
}

enum LogStyle {
    Ansi,
    NoAnsi,
    Json,
}

impl FromStr for LogStyle {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "json" => Ok(LogStyle::Json),
            "noansi" => Ok(LogStyle::NoAnsi),
            _ => Ok(LogStyle::Ansi),
        }
    }
}
