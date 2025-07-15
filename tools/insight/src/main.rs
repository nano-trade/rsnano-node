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

use eframe::egui;
use gui::MainView;
use rsnano_nullable_tracing_subscriber::TracingInitializer;

fn main() -> eframe::Result {
    TracingInitializer::default().init();

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
