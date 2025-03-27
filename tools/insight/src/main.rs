mod app;
mod bootstrap;
mod channels;
mod explorer;
mod frontier_scan;
mod gui;
mod ledger_stats;
mod message_collection;
mod message_rate_calculator;
mod message_recorder;
mod navigator;
mod node_callbacks;
mod node_runner;
mod nullable_runtime;
mod rate_calculator;

use eframe::egui;
use gui::MainView;
use tracing_subscriber::EnvFilter;

fn main() -> eframe::Result {
    let dirs = std::env::var(EnvFilter::DEFAULT_ENV).unwrap_or(String::from("info"));
    let filter = EnvFilter::builder().parse_lossy(dirs);
    tracing_subscriber::fmt::fmt()
        .with_env_filter(filter)
        .with_ansi(true)
        .init();

    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default().with_inner_size([1024.0, 768.0]),
        ..Default::default()
    };
    eframe::run_native(
        "RsNano Insight",
        options,
        Box::new(|_| Ok(Box::new(MainView::new()))),
    )
}
