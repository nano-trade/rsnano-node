use crate::config::NetworkConstants;
use once_cell::sync::Lazy;
use rsnano_core::Networks;
use rsnano_ledger::LedgerConstants;
use rsnano_work::WorkThresholds;

pub static DEV_NETWORK_PARAMS: Lazy<NetworkParams> =
    Lazy::new(|| NetworkParams::new(Networks::NanoDevNetwork));

#[derive(Clone)]
pub struct NetworkParams {
    pub work: WorkThresholds,
    pub network: NetworkConstants,
    pub ledger: LedgerConstants,
}

impl NetworkParams {
    pub fn new(network: Networks) -> Self {
        let work = WorkThresholds::default_for(network);
        let network_constants = NetworkConstants::new(work.clone(), network);
        Self {
            work: work.clone(),
            ledger: LedgerConstants::new(work.clone(), network),
            network: network_constants,
        }
    }
}
