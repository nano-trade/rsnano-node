mod account_map;
mod app;
mod block_factory;
mod delayed_blocks;
mod handshake;
mod rate_spec;

use app::NanoSpamApp;

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> anyhow::Result<()> {
    NanoSpamApp::default().run().await
}
