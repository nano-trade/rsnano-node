use std::{
    sync::{Arc, RwLock},
    time::{Instant, SystemTime},
};

use rsnano_core::{PrivateKey, Signature};
use rsnano_ledger::Ledger;
use rsnano_messages::{TelemetryData, TelemetryMaker};
use rsnano_network::{ChannelMode, Network};

use crate::{
    block_processing::UncheckedMap,
    config::{NetworkParams, NodeConfig},
};

use super::{MAJOR_VERSION, MINOR_VERSION, PATCH_VERSION, PRE_RELEASE_VERSION};

/// Creates the telemetry data for this node
pub(super) struct TelemetryFactory {
    pub ledger: Arc<Ledger>,
    pub network: Arc<RwLock<Network>>,
    pub node_id_key: PrivateKey,
    pub node_config: NodeConfig,
    pub network_params: NetworkParams,
    pub unchecked: Arc<UncheckedMap>,
    pub startup_time: Instant,
}

impl TelemetryFactory {
    pub fn get_telemetry(&self) -> TelemetryData {
        let peer_count = self
            .network
            .read()
            .unwrap()
            .count_by_mode(ChannelMode::Realtime) as u32;

        let mut telemetry_data = TelemetryData {
            node_id: self.node_id_key.public_key().into(),
            block_count: self.ledger.block_count(),
            cemented_count: self.ledger.cemented_count(),
            bandwidth_cap: self.node_config.bandwidth_limit as u64,
            protocol_version: self.network_params.network.protocol_version,
            uptime: self.startup_time.elapsed().as_secs(),
            unchecked_count: self.unchecked.len() as u64,
            genesis_block: self.network_params.ledger.genesis_block.hash(),
            peer_count,
            account_count: self.ledger.account_count(),
            major_version: MAJOR_VERSION,
            minor_version: MINOR_VERSION,
            patch_version: PATCH_VERSION,
            pre_release_version: PRE_RELEASE_VERSION,
            maker: TelemetryMaker::RsNano as u8,
            timestamp: SystemTime::now(),
            active_difficulty: self.network_params.work.threshold_base(),
            unknown_data: Vec::new(),
            signature: Signature::default(),
        };
        // Make sure this is the final operation!
        telemetry_data.sign(&self.node_id_key).unwrap();
        telemetry_data
    }
}
