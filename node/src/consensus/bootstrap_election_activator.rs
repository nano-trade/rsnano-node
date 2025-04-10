use std::sync::{Arc, Mutex, RwLock};

use rsnano_core::BlockHash;
use rsnano_stats::{DetailType, StatType, Stats};

use super::{ActiveElectionsContainer, VoteCache};

/// Skip passive phase for blocks without cached votes to avoid bootstrap delays
pub(crate) struct BootstrapElectionActivator {
    pub active_elections: Arc<RwLock<ActiveElectionsContainer>>,
    pub vote_cache: Arc<Mutex<VoteCache>>,
    pub stats: Arc<Stats>,
}
impl BootstrapElectionActivator {
    pub(crate) fn election_started(&self, hash: BlockHash) {
        let in_cache = self.vote_cache.lock().unwrap().contains(&hash);
        if in_cache {
            // Probably not a bootstrap election
            return;
        }

        if self
            .active_elections
            .write()
            .unwrap()
            .transition_active_hash(&hash)
        {
            self.stats
                .inc(StatType::ActiveElections, DetailType::ActivateImmediately);
        }
    }
}
