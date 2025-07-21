mod account_map;
mod account_map_factory;
mod app;
mod block_factory;
mod delayed_blocks;
mod frontiers_sync;
mod handshake;
mod node_config;
mod rate_spec;
mod wallets_factory;

use app::NanoSpamApp;

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> anyhow::Result<()> {
    NanoSpamApp::default().run().await
}
