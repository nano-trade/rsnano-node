use anyhow::bail;
use rsnano_core::{Block, BlockHash, Networks, PrivateKey, ProtocolInfo};
use rsnano_messages::{Keepalive, Message, MessageDeserializer, MessageSerializer};
use rsnano_network_protocol::{HandshakeProcess, SynCookies};
use rsnano_nullable_tcp::TcpStreamFactory;
use std::{
    net::{SocketAddr, SocketAddrV6},
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    time::sleep,
};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> anyhow::Result<()> {
    NanoSpamApp::default().run().await
}

#[derive(Default)]
struct NanoSpamApp {
    tcp_stream_factory: TcpStreamFactory,
}

impl NanoSpamApp {
    pub async fn run(&self) -> anyhow::Result<()> {
        setup_tracing();

        let peer_addr: SocketAddrV6 = "[::1]:17075".parse()?;
        let node_id_key = PrivateKey::from(42);
        let protocol = ProtocolInfo::default_for(Networks::NanoTestNetwork);
        let genesis_hash = get_genesis_hash_from_env()?;
        let mut tcp_stream = self.tcp_stream_factory.connect(peer_addr).await?;

        perform_handshake(protocol, genesis_hash, node_id_key, &mut tcp_stream).await?;

        let mut serializer = MessageSerializer::new(protocol);
        let mut deserializer = MessageDeserializer::new(protocol);

        let (mut read, mut write) = tokio::io::split(tcp_stream);
        tokio_scoped::scope(|scope| {
            scope.spawn(async {
                let mut recv_buffer = vec![0; 1024 * 4];
                loop {
                    let n = read.read(&mut recv_buffer).await.unwrap();
                    deserializer.push(&recv_buffer[..n]);
                    while let Some(msg) = deserializer.try_deserialize() {
                        let msg = msg.unwrap();
                        info!(message = ?msg.message, "received message");
                    }
                }
            });

            scope.spawn(async {
                loop {
                    println!("SENDING KEEPALIVE");
                    let buffer = serializer.serialize(&Message::Keepalive(Keepalive::default()));
                    write.write(&buffer).await.unwrap();
                    sleep(Duration::from_secs(1)).await;
                }
            });
        });
        Ok(())
    }
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

async fn perform_handshake(
    protocol: ProtocolInfo,
    genesis_hash: BlockHash,
    node_id_key: PrivateKey,
    tcp_stream: &mut rsnano_nullable_tcp::TcpStream,
) -> anyhow::Result<()> {
    let peer_addr = match tcp_stream.peer_addr()? {
        SocketAddr::V4(v4) => SocketAddrV6::new(v4.ip().to_ipv6_mapped(), v4.port(), 0, 0),
        SocketAddr::V6(v6) => v6,
    };
    let mut serializer = MessageSerializer::new(protocol);
    let mut deserializer = MessageDeserializer::new(protocol);

    let syn_cookies = Arc::new(SynCookies::default());
    let mut handshake = HandshakeProcess::new(genesis_hash, node_id_key, syn_cookies);

    let handshake_payload = handshake.initiate_handshake(peer_addr)?;
    let buffer = serializer.serialize(&Message::NodeIdHandshake(handshake_payload));
    tcp_stream.write_all(buffer).await?;

    let mut recv_buffer = vec![0; 1024];
    let response;
    loop {
        let size = tcp_stream.read(&mut recv_buffer).await?;
        deserializer.push(&recv_buffer[..size]);
        if let Some(msg) = deserializer.try_deserialize() {
            response = msg.unwrap().message;
            break;
        }
    }

    let Message::NodeIdHandshake(handshake_response) = response else {
        bail!("no handshake response received");
    };

    match handshake
        .process_handshake(&handshake_response, peer_addr)
        .unwrap()
    {
        (Some(_node_id), Some(response)) => {
            let buffer = serializer.serialize(&Message::NodeIdHandshake(response));
            tcp_stream.write(buffer).await?;
        }
        _ => unreachable!(),
    }
    Ok(())
}
