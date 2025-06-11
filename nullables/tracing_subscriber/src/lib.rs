use rsnano_nullable_env::Env;
use rsnano_output_tracker::{OutputListenerMt, OutputTrackerMt};
use std::{str::FromStr, sync::Arc};
use tracing_subscriber::EnvFilter;

#[derive(Default)]
pub struct TracingInitializer {
    is_nulled: bool,
    env: Env,
    init_listener: OutputListenerMt<()>,
}

impl TracingInitializer {
    pub fn new_null() -> Self {
        Self {
            is_nulled: true,
            env: Env::new_null(),
            init_listener: Default::default(),
        }
    }

    pub fn init(&self) {
        self.init_with_default_log_level("info");
    }

    pub fn init_with_default_log_level(&self, default_level: impl Into<String>) {
        self.init_listener.emit(());
        let dirs = self.log_dirs(default_level);
        let log_style = self.log_style();

        if !self.is_nulled {
            init_tracing_subscriber(log_style, dirs);
        }
    }

    fn log_dirs(&self, default_level: impl Into<String>) -> String {
        self.env
            .var(EnvFilter::DEFAULT_ENV)
            .unwrap_or(default_level.into())
    }

    fn log_style(&self) -> LogStyle {
        self.env
            .var("NANO_LOG")
            .as_ref()
            .map(|i| i.as_str())
            .unwrap_or_default()
            .parse()
            .unwrap()
    }

    pub fn track(&self) -> Arc<OutputTrackerMt<()>> {
        self.init_listener.track()
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

fn init_tracing_subscriber(log_style: LogStyle, dirs: String) {
    let filter = EnvFilter::builder().parse_lossy(dirs);

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn track_initializations() {
        let initializer = TracingInitializer::new_null();
        let init_tracker = initializer.track();
        initializer.init();
        assert_eq!(init_tracker.output().len(), 1);
    }
}
