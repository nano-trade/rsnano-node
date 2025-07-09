use std::{
    fs::remove_dir_all,
    hash::Hash,
    net::{Ipv6Addr, SocketAddrV6},
    process::{Command, Stdio},
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc::{Receiver, Sender},
        Mutex,
    },
    thread::yield_now,
    time::{Duration, Instant},
};

use tokio::{io::AsyncWriteExt, select, sync::mpsc, time::sleep};
use tokio_util::sync::CancellationToken;
use tracing::info;

use rsnano_core::{
    Amount, Block, BlockHash, JsonBlock, Networks, PrivateKey, ProtocolInfo, StateBlockArgs,
    WalletId, WorkNonce,
};
use rsnano_network::bandwidth_limiter::RateLimiter;
use rsnano_nullable_tcp::{TcpStream, TcpStreamFactory};
use rsnano_nullable_tracing_subscriber::TracingInitializer;
use rsnano_websocket_client::{
    NanoWebSocketClient, NanoWebSocketClientFactory, SubscribeArgs, TopicSub,
};
use rsnano_websocket_messages::{BlockConfirmed, MessageEnvelope, Topic};

use crate::{
    account_map::AccountMap,
    block_factory::{BlockFactory, BlockResult},
    delayed_blocks::DelayedBlocks,
    handshake::perform_handshake,
};
use clap::Parser;
use rsnano_messages::{Message, MessageSerializer, Publish};
use rsnano_rpc_client::NanoRpcClient;
use rsnano_rpc_messages::{ReceiveArgs, SendArgs, WalletAddArgs, WalletRepresentativeSetArgs};

const SPAM_ACCOUNTS: usize = 500_000;
const MAX_BLOCKS: usize = 15_000_000;
const MAX_BUFFERED_BLOCKS: usize = 1024;
const INITIAL_BPS: usize = 1;
const BPS_INCREASE_INTERVAL: Duration = Duration::from_secs(3);
const BPS_INCREASE: usize = 50;
const INITIAL_AMOUNT: Amount = Amount::nano(100_000_000);

const GENESIS_BLOCK: &str = r#"{
    "type": "open",
    "account": "nano_3nroioygg54nusrmyun4woimqex36sp3drnctdt5955uqu47fxbkrxk7n7ne",
    "source": "D315857CE70C54DE713F6E82E5613BB3A1266C15E28AD2F4338C7BBEC456F532",
    "representative": "nano_3nroioygg54nusrmyun4woimqex36sp3drnctdt5955uqu47fxbkrxk7n7ne",
    "signature": "3F6792C2DC623DF2E8643777160AB983B66B337E2478E13D2C3448126A8F4CD8DCCD19803C158A057FA44060AE0EFC09B1C311CB4FBF42F8D240610B38F56E08",
    "work": "70FEF01F7EC45DEC"
    }"#;

const GENESIS_PRV: &str = "49643F9B10CA1AA34F9AF8ED4AABD29F436104CCC375974B108534A48EAE3FE1";

const NODE_CONFIG: &str = r#"
[node]
    peering_port = PEERING_PORT
    allow_local_peers = true
    bandwidth_limit = 0
    enable_voting = true
    preconfigured_peers = PRECONF_PEERS
    preconfigured_representatives = ["nano_3e3j5tkog48pnny9dmfzj1r16pg8t1e76dz5tmac6iq689wyjfpiij4txtdo"]
    database_backend = "lmdb"

[node.lmdb]
    sync = "nosync_unsafe"

[node.bounded_backlog]
    enable = false

[node.websocket]
    enable = true
    address = "::1"
    port = WS_PORT

[rpc]
    enable = true
"#;

const RPC_CONFIG: &str = r#"
address = "::1"
enable_control = true
port = RPC_PORT
"#;

#[derive(Parser, Debug)]
struct Args {
    /// Attach to an already running node
    #[arg(long, default_value_t = false)]
    attach: bool,

    /// Number of principal representatives
    #[arg(long, default_value_t = 1)]
    prs: usize,

    /// Use C++ nano_node implementation
    #[arg(long, default_value_t = false)]
    cpp: bool,

    /// Only create the node config files and set up the wallets, then exit
    #[arg(long, default_value_t = false)]
    setup_only: bool,
}

#[derive(Default)]
pub(crate) struct NanoSpamApp {
    pub tracing_init: TracingInitializer,
    pub tcp_stream_factory: TcpStreamFactory,
}

fn peering_port(node_id: usize) -> u16 {
    17075 + (node_id as u16) * 10
}

fn rpc_port(node_id: usize) -> u16 {
    17076 + (node_id as u16) * 10
}

fn websocket_port(node_id: usize) -> u16 {
    17078 + (node_id as u16) * 10
}

impl NanoSpamApp {
    pub async fn run(&self) -> anyhow::Result<()> {
        self.tracing_init.init();
        let args = Args::parse();

        let node_id_key = PrivateKey::from(42);
        let protocol = ProtocolInfo::default_for(Networks::NanoTestNetwork);
        let genesis_hash = self.get_genesis_hash();
        let genesis_key = genesis_key();

        let mut data_dir = dirs::home_dir().unwrap();
        data_dir.push("NanoSpam");

        let genesis_rpc =
            NanoRpcClient::new(format!("http://[::1]:{}", rpc_port(0)).parse().unwrap());

        let initial_key = AccountMap::initial_spam_key();

        if !args.attach {
            if data_dir.exists() {
                info!("Deleting data from previous run: {data_dir:?}...");
                remove_dir_all(&data_dir).unwrap();
            }

            let pr_balance = (Amount::MAX - INITIAL_AMOUNT) / args.prs as u128;
            let mut genesis_wallet = WalletId::zero();

            for i in 0..args.prs {
                info!("********************************************************************************");
                info!("Setting up node PR{i}...");

                let mut node_dir = data_dir.clone();
                node_dir.push(format!("pr{i}"));

                info!("Creating directory {node_dir:?}");
                std::fs::create_dir_all(&node_dir).unwrap();

                let mut ledger_path = node_dir.clone();
                ledger_path.push("data.ldb");

                let mut node_config_path = node_dir.clone();
                node_config_path.push("config-node.toml");
                if !node_config_path.exists() {
                    info!("Creating node config file: {node_config_path:?}");
                    let node_config = NODE_CONFIG
                        .replace("PEERING_PORT", &peering_port(i).to_string())
                        .replace("WS_PORT", &websocket_port(i).to_string())
                        .replace("PRECONF_PEERS", &preconfigured_peers(args.prs, i));
                    std::fs::write(node_config_path, node_config).unwrap();
                }

                let mut rpc_config_path = node_dir.clone();
                rpc_config_path.push("config-rpc.toml");
                if !rpc_config_path.exists() {
                    info!("Creating rpc config file: {rpc_config_path:?}");
                    let rpc_config = RPC_CONFIG.replace("RPC_PORT", &rpc_port(i).to_string());
                    std::fs::write(rpc_config_path, rpc_config).unwrap();
                }

                let mut cmd = if args.cpp {
                    let mut cmd = Command::new("nano_node");
                    cmd.env("NANO_TEST_GENESIS_BLOCK", GENESIS_BLOCK)
                        .env("NANO_TEST_GENESIS_PRV ", GENESIS_PRV)
                        .env("NANO_TEST_EPOCH_1", "0")
                        .env("NANO_TEST_EPOCH_2", "0")
                        .env("NANO_TEST_EPOCH_2_RECV", "0")
                        .arg("--network")
                        .arg("test")
                        .arg("--data-path")
                        .arg(&node_dir)
                        .arg("--daemon");
                    cmd
                } else {
                    let mut cmd = Command::new("rsnano_node");
                    cmd.env("NANO_TEST_GENESIS_BLOCK", GENESIS_BLOCK)
                        .env("NANO_TEST_GENESIS_PRV ", GENESIS_PRV)
                        .arg("--network")
                        .arg("test")
                        .arg("--data-path")
                        .arg(&node_dir)
                        .arg("node")
                        .arg("run");
                    cmd
                };

                info!("Starting node: {cmd:?}");
                cmd.stdout(Stdio::null()).spawn().unwrap();

                // Set up wallet
                let rpc_client =
                    NanoRpcClient::new(format!("http://[::1]:{}", rpc_port(i)).parse().unwrap());
                info!("Waiting for RPC...");
                while rpc_client.version().await.is_err() {
                    sleep(Duration::from_millis(100)).await;
                }

                info!("Creating wallet...");
                let resp = rpc_client.wallet_create(None).await.unwrap();
                if i == 0 {
                    genesis_wallet = resp.wallet;
                }
                let pr_key = pr_key(i);
                rpc_client
                    .wallet_add(WalletAddArgs {
                        wallet: resp.wallet,
                        key: pr_key.raw_key(),
                        work: None,
                    })
                    .await
                    .unwrap();

                if i > 0 {
                    info!("Setting default representative...");
                    rpc_client
                        .wallet_representative_set(WalletRepresentativeSetArgs {
                            wallet: resp.wallet,
                            representative: pr_key.account(),
                            update_existing_accounts: Some(false.into()),
                        })
                        .await
                        .unwrap();

                    info!(
                        "Sending Ӿ{} to PR{i} wallet {} ...",
                        pr_balance.format_balance(0),
                        pr_key.account().encode_account()
                    );
                    let send_hash = genesis_rpc
                        .send(SendArgs {
                            wallet: genesis_wallet,
                            source: genesis_key.account(),
                            destination: pr_key.account(),
                            amount: pr_balance,
                            work: Some(WorkNonce::new(0)),
                            id: None,
                        })
                        .await
                        .unwrap()
                        .block;
                    wait_until_confirmed(&rpc_client, send_hash).await;

                    info!("Receiving...");
                    let recv_hash = rpc_client
                        .receive(ReceiveArgs {
                            wallet: resp.wallet,
                            account: pr_key.account(),
                            block: send_hash,
                            work: Some(WorkNonce::new(0)),
                        })
                        .await
                        .unwrap()
                        .block;
                    wait_until_confirmed(&rpc_client, recv_hash).await;
                    info!("DONE");
                    info!("********************************************************************************");
                }
            }

            info!("Sending initial spam amount...");
            // Send total spam amount
            let genesis_send = genesis_rpc
                .send(SendArgs {
                    wallet: genesis_wallet,
                    source: genesis_key.account(),
                    destination: initial_key.account(),
                    amount: INITIAL_AMOUNT,
                    work: Some(0.into()),
                    id: None,
                })
                .await
                .unwrap()
                .block;
            wait_until_confirmed(&genesis_rpc, genesis_send).await;
            info!("Receiving initial spam amount...");
            let block: Block = StateBlockArgs {
                key: &initial_key,
                previous: BlockHash::zero(),
                representative: initial_key.public_key(),
                balance: INITIAL_AMOUNT,
                link: genesis_send.into(),
                work: 0.into(),
            }
            .into();
            let recv = genesis_rpc.process(JsonBlock::from(block)).await.unwrap();
            wait_until_confirmed(&genesis_rpc, recv.hash).await;
        }

        if args.setup_only {
            return Ok(());
        }

        info!("Creating account keys...");
        let frontier = genesis_rpc
            .account_info(initial_key.account())
            .await
            .unwrap()
            .frontier;

        let mut account_map = AccountMap::default();
        account_map.fill(SPAM_ACCOUNTS, INITIAL_AMOUNT, frontier);

        let block_factory = Mutex::new(BlockFactory::new(account_map, MAX_BLOCKS));

        let mut tcp_streams = Vec::new();

        for i in 0..args.prs {
            let peer_addr = SocketAddrV6::new(Ipv6Addr::LOCALHOST, peering_port(i), 0, 0);
            info!(?peer_addr, "Connecting to node PR{i}...");
            let mut tcp_stream = self.tcp_stream_factory.connect(peer_addr).await?;
            info!("Performing handshake...");
            perform_handshake(protocol, genesis_hash, node_id_key.clone(), &mut tcp_stream).await?;
            tcp_streams.push(tcp_stream);
        }

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
                scope.spawn(republish_delayed_blocks(
                    tx_block_clone,
                    &delayed_blocks,
                    cancel_ws_recv,
                ));
                scope.spawn(publish_blocks(
                    rx_block,
                    tcp_streams,
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

    fn get_genesis_hash(&self) -> BlockHash {
        let genesis_block: Block = serde_json::from_str(GENESIS_BLOCK).unwrap();
        genesis_block.hash()
    }
}

fn preconfigured_peers(prs: usize, current_pr: usize) -> String {
    let mut result = String::new();
    result.push('[');
    for i in 0..prs {
        if i == current_pr {
            continue;
        }

        result.push_str(&format!("\"[::1]:{}\",", peering_port(i)));
    }
    result.push(']');
    result
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
    mut tcp_streams: Vec<TcpStream>,
    protocol: ProtocolInfo,
    delayed_blocks: &Mutex<DelayedBlocks>,
    cancel_token: CancellationToken,
) {
    let mut serializer = MessageSerializer::new(protocol);
    while let Some(block) = rx_block.recv().await {
        let hash = block.hash();
        let publish = Message::Publish(Publish::new_from_originator(block));
        let buffer = serializer.serialize(&publish);

        delayed_blocks
            .lock()
            .unwrap()
            .published(&hash, Instant::now());

        tokio_scoped::scope(|s| {
            for stream in &mut tcp_streams {
                s.spawn(async {
                    stream.write(buffer).await.unwrap();
                });
            }
        });
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

fn pr_key(node_id: usize) -> PrivateKey {
    if node_id == 0 {
        genesis_key()
    } else {
        PrivateKey::from(node_id as u64)
    }
}

fn genesis_key() -> PrivateKey {
    PrivateKey::from_hex_str(GENESIS_PRV).unwrap()
}

async fn wait_until_confirmed(rpc_client: &NanoRpcClient, hash: BlockHash) {
    info!("Waiting for confirmation...");
    loop {
        if let Ok(info) = rpc_client.block_info(hash).await {
            if info.confirmed.inner() {
                break;
            }
        }
        sleep(Duration::from_millis(100)).await;
    }
}
