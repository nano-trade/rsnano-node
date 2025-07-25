use super::{last_votes::LastVotes, CpsLimiter, VoteGenerators};
use crate::consensus::{election::VoteType, ActiveElectionsContainer};
use rsnano_core::{
    utils::{CancellationToken, Runnable},
    Networks,
};
use rsnano_nullable_clock::SteadyClock;
use std::{
    sync::{Arc, RwLock},
    thread::sleep,
    time::Duration,
};

/// Creates votes for blocks within the AEC
pub(crate) struct AecVoter {
    aec: Arc<RwLock<ActiveElectionsContainer>>,
    vote_generators: Arc<VoteGenerators>,
    clock: Arc<SteadyClock>,
    last_votes: LastVotes,
    cps_limiter: CpsLimiter,
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
        }
    }
}

impl Runnable for AecVoter {
    fn run(&mut self, cancel_token: &CancellationToken) {
        let now = self.clock.now();
        let aec = self.aec.read().unwrap();
        for election in aec.iter() {
            if cancel_token.is_cancelled() {
                return;
            }

            let vote_type = election.vote_type();
            let winner_hash = election.winner().hash();

            // TODO insert after voted!
            if self.last_votes.try_insert(winner_hash, vote_type, now) {
                self.vote_generators.generate_vote(
                    &election.qualified_root().root,
                    &winner_hash,
                    vote_type,
                );

                if vote_type == VoteType::NonFinal {
                    while !self.cps_limiter.try_vote(self.clock.now())
                        && !cancel_token.is_cancelled()
                    {
                        // TODO drop lock
                        sleep(Duration::from_millis(1));
                    }
                }
            }
        }
    }
}
