use std::{
    net::SocketAddrV6,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
        mpsc::{Receiver, Sender},
    },
    thread::yield_now,
    time::{Duration, Instant},
};

use anyhow::anyhow;
use tokio::{
    io::{AsyncReadExt, ReadHalf, WriteHalf},
    select,
    sync::mpsc,
};
use tokio_util::sync::CancellationToken;
use tracing::info;

use rsnano_core::{Block, BlockHash, Networks, PrivateKey, ProtocolInfo};
use rsnano_network::bandwidth_limiter::RateLimiter;
use rsnano_nullable_env::Env;
use rsnano_nullable_tcp::{TcpStream, TcpStreamFactory};
use rsnano_nullable_tracing_subscriber::TracingInitializer;
use rsnano_websocket_client::{
    NanoWebSocketClient, NanoWebSocketClientFactory, SubscribeArgs, TopicSub,
};
use rsnano_websocket_messages::{BlockConfirmed, MessageEnvelope, Topic};

use crate::{
    account_map::AccountMap,
    block_factory::{BlockFactory, BlockResult},
    block_publisher::BlockPublisher,
    delayed_blocks::DelayedBlocks,
    handshake::perform_handshake,
};

const SPAM_ACCOUNTS: usize = 500_000;
const MAX_BLOCKS: usize = 15_000_000;
const MAX_BUFFERED_BLOCKS: usize = 1024;
const INITIAL_BPS: usize = 50;
const BPS_INCREASE_INTERVAL: Duration = Duration::from_secs(3);
const BPS_INCREASE: usize = 50;

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

        info!("Creating account keys...");
        let mut account_map = AccountMap::default();
        account_map.fill(SPAM_ACCOUNTS);
        let block_factory = Mutex::new(BlockFactory::new(
            genesis_key,
            genesis_hash,
            account_map,
            MAX_BLOCKS,
        ));

        info!(?peer_addr, "Connecting to node...");
        let mut tcp_stream = self.tcp_stream_factory.connect(peer_addr).await?;

        info!("Performing handshake...");
        perform_handshake(protocol, genesis_hash, node_id_key, &mut tcp_stream).await?;

        let (tcp_read, tcp_write) = tokio::io::split(tcp_stream);
        let (tx_block, rx_block) = mpsc::channel::<Block>(MAX_BUFFERED_BLOCKS);
        let tx_block_clone = tx_block.clone();
        let delayed_blocks = Mutex::new(DelayedBlocks::new());
        let cancel_tcp_recv = CancellationToken::new();
        let cancel_ws_recv = CancellationToken::new();
        let current_bps = AtomicUsize::new(INITIAL_BPS);

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

        let ws_queue_len = AtomicUsize::new(0);
        let (tx_ws_msg, rx_ws_msg) = std::sync::mpsc::channel::<(MessageEnvelope, Instant)>();
        let mut sum_conf_time = Duration::ZERO;

        info!("Starting with {} BPS", current_bps.load(Ordering::Relaxed));
        let started = Instant::now();
        std::thread::scope(|s| {
            s.spawn(|| {
                track_confirmations(
                    rx_ws_msg,
                    &delayed_blocks,
                    &block_factory,
                    &ws_queue_len,
                    &mut sum_conf_time,
                    &current_bps,
                )
            });
            s.spawn(|| create_blocks(&block_factory, tx_block, &delayed_blocks, &current_bps));

            tokio_scoped::scope(|scope| {
                scope.spawn(receive_websocket(
                    ws_client,
                    tx_ws_msg,
                    cancel_ws_recv.clone(),
                    &ws_queue_len,
                ));
                scope.spawn(receive_messages(
                    tcp_read,
                    protocol,
                    cancel_tcp_recv.clone(),
                ));
                scope.spawn(republish_delayed_blocks(
                    tx_block_clone,
                    &delayed_blocks,
                    cancel_ws_recv,
                ));
                scope.spawn(publish_blocks(
                    rx_block,
                    tcp_write,
                    protocol,
                    &delayed_blocks,
                    cancel_tcp_recv,
                ));
            });
        });
        let duration_secs = started.elapsed().as_secs_f64();
        let cps = (MAX_BLOCKS as f64 / duration_secs) as i32;
        info!("Confirming all blocks took {duration_secs:.2}s");
        info!("Confirmation rate: {cps} cps");
        let conf_time = sum_conf_time.as_millis() / MAX_BLOCKS as u128;
        info!("Average conf time: {conf_time} ms");
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

fn create_blocks(
    block_factory: &Mutex<BlockFactory>,
    tx_block: mpsc::Sender<Block>,
    delayed_blocks: &Mutex<DelayedBlocks>,
    current_bps: &AtomicUsize,
) {
    let mut bps_start = Instant::now();
    let mut limiter = RateLimiter::new(current_bps.load(Ordering::Relaxed));

    while let Some(result) = {
        let mut guard = block_factory.lock().unwrap();
        guard.create_next()
    } {
        let BlockResult::Block(block) = result else {
            yield_now();
            continue;
        };

        while !limiter.should_pass(1) {
            yield_now();
        }

        {
            let mut delayed = delayed_blocks.lock().unwrap();
            delayed.insert(block.clone());
        }

        tx_block.blocking_send(block).unwrap();
        if bps_start.elapsed() >= BPS_INCREASE_INTERVAL {
            let new_bps = current_bps.fetch_add(BPS_INCREASE, Ordering::Relaxed) + BPS_INCREASE;
            limiter.set_limit(new_bps);
            bps_start = Instant::now();
        }
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
    cancel_token: CancellationToken,
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

        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    cancel_token.cancel();
}

fn track_confirmations(
    rx_ws_msg: Receiver<(MessageEnvelope, Instant)>,
    delayed_blocks: &Mutex<DelayedBlocks>,
    block_factory: &Mutex<BlockFactory>,
    ws_queue_len: &AtomicUsize,
    sum_conf_time_total: &mut Duration,
    current_bps: &AtomicUsize,
) {
    let mut total = 0;
    let mut confirmed = 0;
    let mut start = Instant::now();
    let mut sum_conf_time = Duration::ZERO;
    while let Ok((msg, timestamp)) = rx_ws_msg.recv() {
        let len = ws_queue_len.fetch_sub(1, Ordering::Relaxed);
        if msg.topic == Some(Topic::Confirmation) {
            let data: BlockConfirmed = serde_json::from_value(msg.message.unwrap()).unwrap();
            let block_hash = BlockHash::decode_hex(data.hash).unwrap();
            let conf_time = delayed_blocks
                .lock()
                .unwrap()
                .confirmed(&block_hash, timestamp);
            if let Some(conf_time) = conf_time {
                confirmed += 1;
                total += 1;
                sum_conf_time += conf_time;
                *sum_conf_time_total += conf_time;
            }
            block_factory.lock().unwrap().confirm(block_hash);
            if confirmed > 0 && confirmed % 5000 == 0 {
                let cps = (confirmed as f64 / start.elapsed().as_secs_f64()) as i32;
                let avg_conf_time = sum_conf_time.as_millis() / confirmed;
                let bps = current_bps.load(Ordering::Relaxed);
                info!(
                    "Confirmed {confirmed} blocks ({total} total) | {bps} bps | {cps} cps | avg conf time: {avg_conf_time} ms | ws queue: {len}"
                );
                confirmed = 0;
                start = Instant::now();
                sum_conf_time = Duration::ZERO;
            }
        }
    }
}

async fn receive_websocket(
    mut ws_client: NanoWebSocketClient,
    tx_ws_msg: Sender<(MessageEnvelope, Instant)>,
    cancel_token: CancellationToken,
    ws_queue_len: &AtomicUsize,
) {
    loop {
        let res = select! {
            res = ws_client.next() =>  res,
            _ = cancel_token.cancelled() =>{ break;}
        };

        let msg = res.unwrap().unwrap();
        tx_ws_msg.send((msg, Instant::now())).unwrap();
        ws_queue_len.fetch_add(1, Ordering::Relaxed);
    }
    info!("receive websocket finished");
}

async fn receive_messages(
    mut read: ReadHalf<TcpStream>,
    _protocol: ProtocolInfo,
    cancel_token: CancellationToken,
) {
    let mut recv_buffer = vec![0; 1024 * 4];
    //let mut deserializer = MessageDeserializer::new(protocol);

    select! {
        _ = cancel_token.cancelled() => {},
        _ = async {
            loop{
                let _n = read.read(&mut recv_buffer).await.unwrap();
                //deserializer.push(&recv_buffer[..n]);
                //while let Some(msg) = deserializer.try_deserialize() {
                //    let msg = msg.unwrap();
                //    debug!(message = ?msg.message, "Received message");
                //}
            }
        } => {}
    }
    info!("receive TCP messages finished");
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
