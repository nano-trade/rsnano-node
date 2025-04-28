use rsnano_core::{BlockHash, Networks, PrivateKey, ProtocolInfo};
use rsnano_network::{ChannelDirection, Network, NetworkConfig, TcpNetworkAdapter};
use rsnano_network_protocol::{HandshakeProcess, HandshakeStats, SynCookies};
use rsnano_nullable_clock::SteadyClock;
use rsnano_nullable_tcp::TcpStream;
use std::{
    net::{SocketAddr, SocketAddrV6},
    sync::{Arc, RwLock},
};
use tokio::net::TcpSocket;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let node_addr: SocketAddrV6 = "[::1]:17075".parse()?;

    let clock = Arc::new(SteadyClock::default());
    let socket = TcpSocket::new_v6()?;
    let stream = socket.connect(SocketAddr::V6(node_addr)).await?;
    let stream = TcpStream::new(stream);
    println!("Connected!");

    let node_id_key = PrivateKey::from(42);
    let syn_cookies = Arc::new(SynCookies::default());
    let stats = Arc::new(HandshakeStats::default());
    let protocol = ProtocolInfo::default_for(Networks::NanoTestNetwork);
    let genesis_hash = BlockHash::decode_hex(
        std::env::var("NANO_TEST_GENESIS_PUB").expect("Genesis pub key not set"),
    )
    .unwrap();

    let mut network = Network::new(NetworkConfig::default_for(Networks::NanoTestNetwork));
    //network.set_data_receiver_factory(Box::new());

    let network = Arc::new(RwLock::new(network));

    let network_adapter = Arc::new(TcpNetworkAdapter::new(
        network.clone(),
        clock.clone(),
        tokio::runtime::Handle::current(),
    ));

    let channel = network_adapter.add(stream, ChannelDirection::Outbound)?;

    //let handshake_process =
    //    HandshakeProcess::new(genesis_hash, node_id_key, syn_cookies, stats, protocol);

    Ok(())
}
