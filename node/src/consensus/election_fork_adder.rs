use std::sync::{Arc, Mutex};

use rsnano_core::{Amount, Block, BlockHash};
use rsnano_ledger::{BlockStatus, RepWeightCache};
use rsnano_stats::{DetailType, StatType, Stats};
use tracing::debug;

use crate::block_processing::BlockContext;

use super::{ActiveElections, VoteCache};

/// Tries to add a fork block to its corresponding election
pub(crate) struct ElectionForkAdder {
    pub active_elections: Arc<ActiveElections>,
    pub vote_cache: Arc<Mutex<VoteCache>>,
    pub stats: Arc<Stats>,
    pub rep_weights: Arc<RepWeightCache>,
}

impl ElectionForkAdder {
    pub fn handle_processed_blocks(&self, batch: &[(BlockStatus, Arc<BlockContext>)]) {
        for (status, context) in batch {
            if *status == BlockStatus::Fork {
                self.handle_fork(&context.block);
            }
        }
    }

    pub fn handle_fork(&self, fork: &Block) {
        let fork_tally = self.get_cached_tally(&fork.hash());
        let added = self.active_elections.try_add_fork(fork, fork_tally);
        if added {
            self.stats
                .inc(StatType::Active, DetailType::ElectionBlockConflict);
            debug!("Block was added to an existing election: {}", fork.hash());
        }
    }

    fn get_cached_tally(&self, hash: &BlockHash) -> Amount {
        let votes = self.vote_cache.lock().unwrap().find(hash);
        let mut tally = Amount::zero();
        let weights = self.rep_weights.read();
        for vote in votes {
            tally += weights.get(&vote.voter).cloned().unwrap_or_default();
        }
        tally
    }
}
