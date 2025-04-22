use super::{ActiveElectionsContainer, ForkCache, VoteCache};
use rsnano_core::{Amount, Block, BlockHash, QualifiedRoot};
use rsnano_ledger::{BlockError, ProcessedResult, RepWeightCache};
use std::sync::{Arc, Mutex, RwLock};
use tracing::debug;

pub(crate) struct ForkProcessor {
    pub(crate) rep_weights: Arc<RepWeightCache>,
    pub(crate) fork_cache: Arc<RwLock<ForkCache>>,
    pub(crate) active_elections: Arc<RwLock<ActiveElectionsContainer>>,
    pub(crate) vote_cache: Arc<Mutex<VoteCache>>,
}

impl ForkProcessor {
    pub fn handle_forks(&self, batch: &[ProcessedResult]) {
        for result in batch {
            if result.status == Err(BlockError::Fork) {
                self.handle_fork(&result.block);
            }
        }
    }

    pub fn try_add_cached_forks(&self, root: &QualifiedRoot) {
        let fork_cache = self.fork_cache.read().unwrap();
        for fork in fork_cache.get_forks(&root) {
            self.handle_fork(fork);
        }
    }

    fn handle_fork(&self, fork: &Block) {
        let fork_tally = self.get_cached_tally(&fork.hash());

        let added = self
            .active_elections
            .write()
            .unwrap()
            .try_add_fork(fork, fork_tally);

        if added {
            debug!("Block was added to an existing election: {}", fork.hash());
        }
    }

    fn get_cached_tally(&self, hash: &BlockHash) -> Amount {
        let votes = self.vote_cache.lock().unwrap().find(hash);
        let mut tally = Amount::zero();
        let weights = self.rep_weights.read();
        for vote in votes {
            tally += weights.weight(&vote.voter);
        }
        tally
    }
}
