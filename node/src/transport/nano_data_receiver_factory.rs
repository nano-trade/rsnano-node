use std::sync::{Arc, Mutex, RwLock, Weak};

use rsnano_core::PrivateKey;
use rsnano_messages::*;
use rsnano_network::{Channel, DataReceiver, DataReceiverFactory, Network};
use rsnano_stats::Stats;

use rsnano_network_protocol::{
    HandshakeProcess, HandshakeStats, InboundMessageQueue, LatestKeepalives, SynCookies,
};

use super::nano_data_receiver::NanoDataReceiver;
use crate::config::NetworkParams;

pub(crate) struct NanoDataReceiverFactory {
    network_params: Arc<NetworkParams>,
    stats: Arc<Stats>,
    stats2: Arc<HandshakeStats>,
    network: Weak<RwLock<Network>>,
    inbound_queue: Arc<InboundMessageQueue>,
    network_filter: Arc<NetworkFilter>,
    syn_cookies: Arc<SynCookies>,
    node_id: PrivateKey,
    latest_keepalives: Arc<Mutex<LatestKeepalives>>,
}

impl NanoDataReceiverFactory {
    pub fn new(
        network: &Arc<RwLock<Network>>,
        inbound_queue: Arc<InboundMessageQueue>,
        network_filter: Arc<NetworkFilter>,
        network_params: Arc<NetworkParams>,
        stats: Arc<Stats>,
        stats2: Arc<HandshakeStats>,
        syn_cookies: Arc<SynCookies>,
        node_id_key: PrivateKey,
        latest_keepalives: Arc<Mutex<LatestKeepalives>>,
    ) -> Self {
        Self {
            network: Arc::downgrade(network),
            inbound_queue,
            syn_cookies: syn_cookies.clone(),
            node_id: node_id_key.clone(),
            network_params,
            stats: stats.clone(),
            stats2,
            network_filter,
            latest_keepalives,
        }
    }
}

impl DataReceiverFactory for NanoDataReceiverFactory {
    fn create_receiver_for(&self, channel: Arc<Channel>) -> Box<dyn DataReceiver + Send> {
        let handshake_process = HandshakeProcess::new(
            self.network_params.ledger.genesis_block.hash(),
            self.node_id.clone(),
            self.syn_cookies.clone(),
            self.stats2.clone(),
            self.network_params.network.protocol_info(),
        );

        let message_deserializer = MessageDeserializer::new(
            self.network_params.network.protocol_info(),
            Some(self.network_filter.clone()),
            self.network_params.network.work.clone(),
        );

        let mut receiver = NanoDataReceiver::new(
            channel,
            handshake_process,
            message_deserializer,
            self.inbound_queue.clone(),
            self.latest_keepalives.clone(),
            self.stats.clone(),
            self.network.clone(),
        );

        receiver.ensure_handshake();

        Box::new(receiver)
    }
}
