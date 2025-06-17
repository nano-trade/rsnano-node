mod account_map;
mod app;
mod block_factory;
mod block_publisher;
mod delayed_blocks;
mod handshake;

use app::NanoSpamApp;

#[tokio::main(flavor = "multi_thread", worker_threads = 3)]
async fn main() -> anyhow::Result<()> {
    NanoSpamApp::default().run().await
}
