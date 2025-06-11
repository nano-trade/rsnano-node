mod app;
mod handshake;

use app::NanoSpamApp;

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> anyhow::Result<()> {
    NanoSpamApp::default().run().await
}
