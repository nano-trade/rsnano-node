use std::{
    net::SocketAddrV6,
    sync::Mutex,
    time::{Duration, Instant},
};

use anyhow::anyhow;
use tokio::{
    io::{AsyncReadExt, ReadHalf, WriteHalf},
    select,
    sync::mpsc,
    time::sleep,
};
use tracing::{debug, info, warn};

use rsnano_core::{Block, BlockHash, Networks, PrivateKey, ProtocolInfo};
use rsnano_messages::MessageDeserializer;
use rsnano_nullable_env::Env;
use rsnano_nullable_tcp::{TcpStream, TcpStreamFactory};
use rsnano_nullable_tracing_subscriber::TracingInitializer;

use crate::{
    account_map::AccountMap, block_factory::BlockFactory, block_publisher::BlockPublisher,
    delayed_blocks::DelayedBlocks, handshake::perform_handshake,
};
use rsnano_websocket_client::{
    NanoWebSocketClient, NanoWebSocketClientFactory, SubscribeArgs, TopicSub,
};
use rsnano_websocket_messages::{BlockConfirmed, Topic};
use tokio_util::sync::CancellationToken;

const SPAM_ACCOUNTS: usize = 20_000;
const MAX_BLOCKS: usize = 100_000;
const MAX_BUFFERED_BLOCKS: usize = 512;

#[derive(Default)]
pub(crate) struct NanoSpamApp {
    pub tracing_init: TracingInitializer,
    pub tcp_stream_factory: TcpStreamFactory,
    pub env: Env,
}

impl NanoSpamApp {
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

        let (tcp_read, tcp_write) = tokio::io::split(tcp_stream);
        let (tx_block, rx_block) = mpsc::channel::<Block>(MAX_BUFFERED_BLOCKS);
        let tx_block_clone = tx_block.clone();
        let delayed_blocks = Mutex::new(DelayedBlocks::new());
        let cancel_token = CancellationToken::new();

        info!("Connecting to websocket...");
        let mut ws_client = NanoWebSocketClientFactory::default()
            .connect("ws://[::1]:17078")
            .await
            .unwrap();

        ws_client
            .subscribe(SubscribeArgs {
                topic: TopicSub::Confirmation(Default::default()),
                ack: true,
                id: None,
            })
            .await
            .unwrap();

        // wait for ack
        ws_client.next().await.unwrap().unwrap();

        info!("Starting spam...");
        let started = Instant::now();
        tokio_scoped::scope(|scope| {
            scope.spawn(track_confirmations(ws_client, &delayed_blocks));
            scope.spawn(receive_messages(tcp_read, protocol, cancel_token.clone()));
            scope.spawn(create_blocks(
                genesis_key,
                genesis_hash,
                tx_block,
                &delayed_blocks,
            ));
            scope.spawn(republish_delayed_blocks(tx_block_clone, &delayed_blocks));
            scope.spawn(publish_blocks(
                rx_block,
                tcp_write,
                protocol,
                &delayed_blocks,
                cancel_token,
            ));
        });
        let duration_secs = started.elapsed().as_secs_f64();
        let cps = (MAX_BLOCKS as f64 / duration_secs) as i32;
        info!("Confirming all blocks took {duration_secs:.2}s");
        info!("Confirmation rate: {cps} cps");
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
    delayed_blocks: &Mutex<DelayedBlocks>,
) {
    let mut account_map = AccountMap::default();
    account_map.fill(SPAM_ACCOUNTS);
    let mut block_factory = BlockFactory::new(genesis_key, genesis_hash, account_map, MAX_BLOCKS);

    while let Some(block) = block_factory.create_next() {
        let mut cool_down = false;
        {
            let mut delayed = delayed_blocks.lock().unwrap();
            delayed.insert(block.clone());
            if delayed.len() > 1000 {
                cool_down = true;
            }
        }

        if cool_down {
            while delayed_blocks.lock().unwrap().len() > 1000 {
                sleep(Duration::from_millis(10)).await;
            }
        }

        tx_block.send(block).await.unwrap();
    }
    delayed_blocks.lock().unwrap().finished();
}

async fn publish_blocks(
    mut rx_block: mpsc::Receiver<Block>,
    tcp_write: WriteHalf<TcpStream>,
    protocol: ProtocolInfo,
    delayed_blocks: &Mutex<DelayedBlocks>,
    cancel_token: CancellationToken,
) {
    let mut publisher = BlockPublisher::new(protocol, tcp_write);
    while let Some(block) = rx_block.recv().await {
        let hash = block.hash();
        publisher.publish(block).await.unwrap();
        delayed_blocks
            .lock()
            .unwrap()
            .published(&hash, Instant::now());
    }
    cancel_token.cancel();
}

async fn republish_delayed_blocks(
    tx_block: mpsc::Sender<Block>,
    delayed_blocks: &Mutex<DelayedBlocks>,
) {
    loop {
        while let Some(block) = {
            let now = Instant::now();
            delayed_blocks.lock().unwrap().next(now)
        } {
            tx_block.send(block).await.unwrap();
        }

        if delayed_blocks.lock().unwrap().is_finished() {
            break;
        }

        sleep(Duration::from_millis(100)).await;
    }
}

async fn track_confirmations(
    mut ws_client: NanoWebSocketClient,
    delayed_blocks: &Mutex<DelayedBlocks>,
) {
    let mut total = 0;
    let mut confirmed = 0;
    let mut start = Instant::now();
    while {
        let guard = delayed_blocks.lock().unwrap();
        guard.len() > 0 || !guard.is_finished()
    } {
        let msg = ws_client.next().await.unwrap().unwrap();
        if msg.topic == Some(Topic::Confirmation) {
            let data: BlockConfirmed = serde_json::from_value(msg.message.unwrap()).unwrap();
            let known_block = delayed_blocks
                .lock()
                .unwrap()
                .confirmed(&BlockHash::decode_hex(data.hash).unwrap());
            if known_block {
                confirmed += 1;
                total += 1;
            }
            if confirmed > 0 && confirmed % 250 == 0 {
                let cps = (confirmed as f64 / start.elapsed().as_secs_f64()) as i32;
                info!("confirmed {confirmed} blocks ({total} total) with {cps} cps");
                confirmed = 0;
                start = Instant::now();
            }
        }
    }
}

async fn receive_messages(
    mut read: ReadHalf<TcpStream>,
    protocol: ProtocolInfo,
    cancel_token: CancellationToken,
) {
    let mut recv_buffer = vec![0; 1024 * 4];
    let mut deserializer = MessageDeserializer::new(protocol);

    select! {
        _ = cancel_token.cancelled() => {},
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
