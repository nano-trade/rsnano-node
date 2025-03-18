use std::sync::{Arc, Mutex};

use rsnano_core::BlockHash;
use rsnano_stats::{DetailType, StatType, Stats};

use super::{ActiveElections, VoteCache};

/// Skip passive phase for blocks without cached votes to avoid bootstrap delays
pub(crate) struct BootstrapElectionActivator {
    pub active_elections: Arc<ActiveElections>,
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

        if let Some(election) = self.active_elections.election_for_block(&hash) {
            election.lock().unwrap().transition_active();
            self.stats
                .inc(StatType::ActiveElections, DetailType::ActivateImmediately);
        }
    }
}
