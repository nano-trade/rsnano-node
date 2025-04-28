use rsnano_core::{Block, BlockHash, Networks, PrivateKey, ProtocolInfo};
use rsnano_messages::NetworkFilter;
use rsnano_network::{Network, NetworkConfig, PeerConnector, TcpNetworkAdapter};
use rsnano_network_protocol::{
    HandshakeStats, InboundMessageQueue, LatestKeepalives, NanoDataReceiverFactory, SynCookies,
};
use rsnano_nullable_clock::SteadyClock;
use rsnano_stats::Stats;
use std::{
    net::SocketAddrV6,
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};
use tokio::task::spawn_blocking;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    setup_tracing();

    let node_addr: SocketAddrV6 = "[::1]:17075".parse()?;
    let node_id_key = PrivateKey::from(42);
    let protocol = ProtocolInfo::default_for(Networks::NanoTestNetwork);
    let genesis_hash = get_genesis_hash_from_env()?;

    // Unimportant details
    //--------------------------------------------------------------------------------

    let network = Arc::new(RwLock::new(Network::new(NetworkConfig::default_for(
        Networks::NanoTestNetwork,
    ))));

    let stats = Arc::new(Stats::default());
    let inbound_queue = Arc::new(InboundMessageQueue::new(1024, stats.clone()));
    let network_filter = Arc::new(NetworkFilter::default());
    let latest_keepalives = Arc::new(Mutex::new(LatestKeepalives::default()));
    let syn_cookies = Arc::new(SynCookies::default());
    let stats2 = Arc::new(HandshakeStats::default());

    let receiver_factory = Box::new(NanoDataReceiverFactory::new(
        &network,
        inbound_queue.clone(),
        network_filter,
        stats,
        stats2,
        syn_cookies,
        node_id_key,
        latest_keepalives,
        genesis_hash,
        protocol,
    ));

    network
        .write()
        .unwrap()
        .set_data_receiver_factory(receiver_factory);

    let clock = Arc::new(SteadyClock::default());
    let network_adapter = Arc::new(TcpNetworkAdapter::new(
        network.clone(),
        clock.clone(),
        tokio::runtime::Handle::current(),
    ));

    let connector = PeerConnector::new(
        Duration::from_secs(3),
        network_adapter,
        tokio::runtime::Handle::current(),
    );

    //--------------------------------------------------------------------------------

    connector.connect_to(node_addr)?;

    spawn_blocking(move || loop {
        inbound_queue.wait_for_messages();
        let batch = inbound_queue.next_batch(8);
        for (_, (message, _)) in batch {
            info!(?message, "received message");
        }
    })
    .await?;
    Ok(())
}

fn setup_tracing() {
    let dirs = std::env::var(EnvFilter::DEFAULT_ENV).unwrap_or(String::from("info"));
    let filter = EnvFilter::builder().parse_lossy(dirs);
    tracing_subscriber::fmt::fmt()
        .with_env_filter(filter)
        .with_ansi(true)
        .init();
}

fn get_genesis_hash_from_env() -> anyhow::Result<BlockHash> {
    let genesis_block_json =
        std::env::var("NANO_TEST_GENESIS_BLOCK").expect("Genesis block not set");
    let genesis_block: Block = serde_json::from_str(&genesis_block_json).unwrap();
    Ok(genesis_block.hash())
}
