use std::sync::Arc;

use rsnano_core::{utils::BackpressureSender, BlockHash, MaybeSavedBlock, SavedBlock};
use rsnano_stats::{DetailType, Direction, StatType, Stats};

use crate::cementation::ConfirmingSet;

use super::{
    election::{ConfirmedElection, ElectionResult},
    ActiveElections, AecEvent,
};
use rsnano_nullable_clock::SteadyClock;

pub(crate) struct DependentElectionsConfirmer {
    pub(crate) stats: Arc<Stats>,
    pub(crate) confirming_set: Arc<ConfirmingSet>,
    pub(crate) active_elections: Arc<ActiveElections>,
    pub(crate) event_sender: BackpressureSender<AecEvent>,
    pub(crate) clock: Arc<SteadyClock>,
}

impl DependentElectionsConfirmer {
    /// Confirmed blocks might implicitly confirm dependent elections
    pub fn confirm_dependent_elections(&self, confirmed_blocks: &Vec<(SavedBlock, BlockHash)>) {
        let mut confirmed_blocks_with_election = Vec::with_capacity(confirmed_blocks.len());
        self.confirming_set.do_election_cache(|cache| {
            for (confirmed_block, _) in confirmed_blocks {
                let source_election = cache.get(&confirmed_block.hash()).cloned();
                confirmed_blocks_with_election.push((confirmed_block.clone(), source_election));
            }
        });

        for (_, election) in &confirmed_blocks_with_election {
            if let Some(election) = election {
                self.notify_block_confirmed(election.clone());
            }
        }

        let now = self.clock.now();
        let confirmed = self
            .active_elections
            .write()
            .unwrap()
            .confirm_dependent_elections(confirmed_blocks_with_election, now);

        for election in confirmed {
            self.stats
                .inc(StatType::ActiveElections, DetailType::Cemented);
            self.stats
                .inc(StatType::ActiveElectionsCemented, election.result.into());
            self.notify_block_confirmed(election);
        }
    }

    fn notify_block_confirmed(&self, election: ConfirmedElection) {
        let MaybeSavedBlock::Saved(block) = &election.winner else {
            return;
        };
        let block = block.clone();

        match election.result {
            ElectionResult::ActiveConfirmedQuorum => self.stats.inc_dir(
                StatType::ConfirmationObserver,
                DetailType::ActiveQuorum,
                Direction::Out,
            ),
            ElectionResult::ActiveConfirmationHeight => self.stats.inc_dir(
                StatType::ConfirmationObserver,
                DetailType::ActiveConfHeight,
                Direction::Out,
            ),
            ElectionResult::InactiveConfirmationHeight => self.stats.inc_dir(
                StatType::ConfirmationObserver,
                DetailType::InactiveConfHeight,
                Direction::Out,
            ),
        }

        self.event_sender
            .send(AecEvent::BlockConfirmed(block, election))
            .unwrap();
    }
}
