mod app;
mod confirmation_receiver;
mod confirmation_tracker;
mod domain;
mod frontiers_sync;
mod handshake;
mod high_prio_check;
mod setup;
mod wallets_factory;

use app::NanoSpamApp;

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> anyhow::Result<()> {
    NanoSpamApp::default().run(std::env::args()).await
}
