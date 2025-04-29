use std::sync::{Arc, Mutex, RwLock, Weak};

use rsnano_core::{BlockHash, PrivateKey, ProtocolInfo};
use rsnano_messages::*;
use rsnano_network::{Channel, DataReceiver, DataReceiverFactory, Network};
use rsnano_stats::Stats;

use crate::{HandshakeProcess, HandshakeStats, LatestKeepalives, NanoDataReceiver, SynCookies};

pub struct NanoDataReceiverFactory {
    stats: Arc<Stats>,
    handshake_stats: Arc<HandshakeStats>,
    network: Weak<RwLock<Network>>,
    received: Arc<dyn Fn(Message, Arc<Channel>) + Send + Sync>,
    network_filter: Arc<NetworkFilter>,
    syn_cookies: Arc<SynCookies>,
    node_id: PrivateKey,
    latest_keepalives: Arc<Mutex<LatestKeepalives>>,
    genesis_hash: BlockHash,
    protocol: ProtocolInfo,
}

impl NanoDataReceiverFactory {
    pub fn new(
        network: &Arc<RwLock<Network>>,
        received: Arc<dyn Fn(Message, Arc<Channel>) + Send + Sync>,
        network_filter: Arc<NetworkFilter>,
        stats: Arc<Stats>,
        stats2: Arc<HandshakeStats>,
        syn_cookies: Arc<SynCookies>,
        node_id_key: PrivateKey,
        latest_keepalives: Arc<Mutex<LatestKeepalives>>,
        genesis_hash: BlockHash,
        protocol: ProtocolInfo,
    ) -> Self {
        Self {
            network: Arc::downgrade(network),
            received,
            syn_cookies: syn_cookies.clone(),
            node_id: node_id_key.clone(),
            stats: stats.clone(),
            handshake_stats: stats2,
            network_filter,
            latest_keepalives,
            genesis_hash,
            protocol,
        }
    }
}

impl DataReceiverFactory for NanoDataReceiverFactory {
    fn create_receiver_for(&self, channel: Arc<Channel>) -> Box<dyn DataReceiver + Send> {
        let handshake_process = HandshakeProcess::new(
            self.genesis_hash,
            self.node_id.clone(),
            self.syn_cookies.clone(),
        );

        let message_deserializer =
            MessageDeserializer::new(self.protocol, Some(self.network_filter.clone()));

        let mut receiver = NanoDataReceiver::new(
            channel,
            handshake_process,
            message_deserializer,
            self.received.clone(),
            self.latest_keepalives.clone(),
            self.stats.clone(),
            self.network.clone(),
            self.handshake_stats.clone(),
            self.protocol,
        );

        receiver.ensure_handshake();

        Box::new(receiver)
    }
}
