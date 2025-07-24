use crate::consensus::ActiveElectionsContainer;
use rsnano_core::utils::{CancellationToken, Runnable};
use std::sync::{Arc, RwLock};
use tracing::info;

/// Creates votes for blocks within the AEC
pub(crate) struct AecVoter {
    aec: Arc<RwLock<ActiveElectionsContainer>>,
}

impl AecVoter {
    pub(crate) fn new(aec: Arc<RwLock<ActiveElectionsContainer>>) -> Self {
        Self { aec }
    }
}

impl Runnable for AecVoter {
    fn run(&mut self, cancel_token: &CancellationToken) {
        info!("tick");
    }
}
