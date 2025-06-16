use std::{net::SocketAddrV6, time::Duration};

use anyhow::anyhow;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf},
    select,
    sync::{mpsc, oneshot},
    time::sleep,
};
use tracing::{debug, info};

use rsnano_core::{Block, BlockHash, Networks, PrivateKey, ProtocolInfo};
use rsnano_messages::{Message, MessageDeserializer, MessageSerializer, Publish};
use rsnano_nullable_env::Env;
use rsnano_nullable_tcp::{TcpStream, TcpStreamFactory};
use rsnano_nullable_tracing_subscriber::TracingInitializer;

use crate::{account_map::AccountMap, block_factory::BlockFactory, handshake::perform_handshake};

#[derive(Default)]
pub(crate) struct NanoSpamApp {
    pub tracing_init: TracingInitializer,
    pub tcp_stream_factory: TcpStreamFactory,
    pub env: Env,
}

impl NanoSpamApp {
    const MAX_BUFFERED_BLOCKS: usize = 1024 * 16;

    pub async fn run(&self) -> anyhow::Result<()> {
        self.tracing_init.init();

        let peer_addr: SocketAddrV6 = "[::1]:17075".parse()?;
        let node_id_key = PrivateKey::from(42);
        let protocol = ProtocolInfo::default_for(Networks::NanoTestNetwork);
        let genesis_hash = self.get_genesis_hash()?;
        let genesis_key = self.get_genesis_key()?;

        info!(?peer_addr, "Connecting to node...");
        let mut tcp_stream = self.tcp_stream_factory.connect(peer_addr).await?;

        info!("Performing handshake...");
        perform_handshake(protocol, genesis_hash, node_id_key, &mut tcp_stream).await?;

        info!("Starting spam...");
        let (read, write) = tokio::io::split(tcp_stream);
        let (tx_stop, rx_stop) = oneshot::channel::<()>();
        let (tx_block, rx_block) = mpsc::channel::<Block>(Self::MAX_BUFFERED_BLOCKS);
        tokio_scoped::scope(|scope| {
            scope.spawn(receive_messages(read, rx_stop, protocol));
            scope.spawn(create_blocks(genesis_key, genesis_hash, tx_block));
            scope.spawn(async {
                publish_blocks(rx_block, write, protocol).await;
                tx_stop.send(()).unwrap();
            });
        });
        Ok(())
    }

    fn get_genesis_hash(&self) -> anyhow::Result<BlockHash> {
        let json = self.get_env(Self::GENESIS_BLOCK_ENV)?;
        let genesis_block: Block = serde_json::from_str(&json)?;
        Ok(genesis_block.hash())
    }

    fn get_genesis_key(&self) -> anyhow::Result<PrivateKey> {
        let key_str = self.get_env(Self::GENESIS_PRV_KEY_ENV)?;
        PrivateKey::from_hex_str(&key_str)
    }

    fn get_env(&self, key: &str) -> anyhow::Result<String> {
        self.env
            .var(key)
            .map_err(|_| anyhow!("env var '{}' not set", key))
    }

    const GENESIS_BLOCK_ENV: &str = "NANO_TEST_GENESIS_BLOCK";
    const GENESIS_PRV_KEY_ENV: &str = "NANO_TEST_GENESIS_PRV";
}

async fn create_blocks(
    genesis_key: PrivateKey,
    genesis_hash: BlockHash,
    tx_block: mpsc::Sender<Block>,
) {
    let mut account_map = AccountMap::default();
    account_map.fill(10_000);
    let mut block_factory = BlockFactory::new(genesis_key, genesis_hash, account_map, 1);

    while let Some(block) = block_factory.create_next() {
        tx_block.send(block).await.unwrap();
    }
}

async fn publish_blocks(
    mut rx_block: mpsc::Receiver<Block>,
    mut write: WriteHalf<TcpStream>,
    protocol: ProtocolInfo,
) {
    while let Some(block) = rx_block.recv().await {
        let publish = Message::Publish(Publish::new_from_originator(block));
        let mut serializer = MessageSerializer::new(protocol);
        let buffer = serializer.serialize(&publish);
        write.write(&buffer).await.unwrap();
        sleep(Duration::from_millis(500)).await;
    }
}

async fn receive_messages(
    mut read: ReadHalf<TcpStream>,
    stop: oneshot::Receiver<()>,
    protocol: ProtocolInfo,
) {
    let mut recv_buffer = vec![0; 1024 * 4];
    let mut deserializer = MessageDeserializer::new(protocol);

    select! {
        _ = stop => {},
        _ = async {
            loop{
                let n = read.read(&mut recv_buffer).await.unwrap();
                deserializer.push(&recv_buffer[..n]);
                while let Some(msg) = deserializer.try_deserialize() {
                    let msg = msg.unwrap();
                    debug!(message = ?msg.message, "Received message");
                }
            }
        } => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn initialize_tracing() {
        let tracing_init = TracingInitializer::new_null();
        let init_tracker = tracing_init.track();
        let tcp_stream_factory = TcpStreamFactory::new_null();
        let env = Env::new_null();

        let app = NanoSpamApp {
            tracing_init,
            tcp_stream_factory,
            env,
        };

        let _ = app.run().await;

        assert_eq!(init_tracker.output().len(), 1);
    }
}
