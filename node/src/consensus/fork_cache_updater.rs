use std::sync::{Arc, RwLock};

use rsnano_ledger::BlockError;

use super::ForkCache;
use crate::block_processing::ProcessedResult;

pub(crate) struct ForkCacheUpdater {
    cache: Arc<RwLock<ForkCache>>,
}

impl ForkCacheUpdater {
    pub(crate) fn new(cache: Arc<RwLock<ForkCache>>) -> Self {
        Self { cache }
    }

    pub fn update(&self, results: &[ProcessedResult]) {
        for result in results {
            if result.status == Err(BlockError::Fork) {
                self.cache.write().unwrap().add(result.block.clone());
            }
        }
    }
}
