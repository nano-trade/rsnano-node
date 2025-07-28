use super::{last_votes::LastVotes, CpsLimiter, VoteGenerators};
use crate::consensus::{
    election::VoteType, election_schedulers::priority::bucket_count, ActiveElectionsContainer,
};
use rsnano_core::{
    utils::{CancellationToken, Runnable},
    BlockHash, Networks, Root,
};
use rsnano_nullable_clock::SteadyClock;
use std::sync::{Arc, RwLock};

/// Creates votes for blocks within the AEC
pub(crate) struct AecVoter {
    aec: Arc<RwLock<ActiveElectionsContainer>>,
    vote_generators: Arc<VoteGenerators>,
    clock: Arc<SteadyClock>,
    last_votes: LastVotes,
    cps_limiter: CpsLimiter,
    current_bucket: usize,
}

impl AecVoter {
    pub(crate) fn new(
        aec: Arc<RwLock<ActiveElectionsContainer>>,
        vote_generators: Arc<VoteGenerators>,
        clock: Arc<SteadyClock>,
        network: Networks,
        cps_limiter: CpsLimiter,
    ) -> Self {
        Self {
            aec,
            vote_generators,
            clock,
            last_votes: LastVotes::new(network),
            cps_limiter,
            current_bucket: bucket_count() - 1,
        }
    }

    fn flush(&self, queue: &mut Vec<(Root, BlockHash, VoteType)>) {
        // TODO: enqueue with one call
        for (root, hash, vote_type) in queue.drain(..) {
            self.vote_generators.generate_vote(&root, &hash, vote_type);
        }
    }
}

impl Runnable for AecVoter {
    fn run(&mut self, cancel_token: &CancellationToken) {
        let now = self.clock.now();
        let aec = self.aec.read().unwrap();
        let mut voted = true;
        let mut vote_queue = Vec::new();
        while voted {
            voted = false;
            loop {
                for election in aec.iter_bucket(self.current_bucket) {
                    let vote_type = election.vote_type();
                    let winner_hash = election.winner().hash();

                    if self.last_votes.can_vote(winner_hash, vote_type, now) {
                        if vote_type == VoteType::NonFinal && !self.cps_limiter.try_vote(now) {
                            self.flush(&mut vote_queue);
                            return;
                        }

                        vote_queue.push((election.qualified_root().root, winner_hash, vote_type));

                        self.last_votes.voted(winner_hash, vote_type, now);
                        voted = true;

                        // Vote for only one election per bucket
                        break;
                    }
                }
                if cancel_token.is_cancelled() {
                    return;
                }

                if self.current_bucket == 0 {
                    self.current_bucket = bucket_count() - 1;
                    break;
                } else {
                    self.current_bucket -= 1;
                }
            }
        }
        self.flush(&mut vote_queue);
    }
}
