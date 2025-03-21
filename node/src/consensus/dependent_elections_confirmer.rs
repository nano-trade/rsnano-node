use std::sync::{mpsc::SyncSender, Arc};

use rsnano_core::{BlockHash, MaybeSavedBlock, SavedBlock};
use rsnano_stats::{DetailType, Direction, StatType, Stats};

use crate::cementation::ConfirmingSet;

use super::{ActiveElections, AecEvent, ConfirmedElection, ElectionResult};

pub(crate) struct DependentElectionsConfirmer {
    pub(crate) stats: Arc<Stats>,
    pub(crate) confirming_set: Arc<ConfirmingSet>,
    pub(crate) active_elections: Arc<ActiveElections>,
    pub(crate) event_sender: SyncSender<AecEvent>,
}

impl DependentElectionsConfirmer {
    /// Confirmed blocks might implicitly confirm dependent elections
    pub fn confirm_dependent_elections(&self, confirmed_blocks: &Vec<(SavedBlock, BlockHash)>) {
        let mut confirmed_blocks_with_election = Vec::with_capacity(confirmed_blocks.len());
        self.confirming_set.do_election_cache(|cache| {
            for (cemented_block, _) in confirmed_blocks {
                let source_election = cache.get(&cemented_block.hash()).cloned();
                confirmed_blocks_with_election.push((cemented_block.clone(), source_election));
            }
        });

        for (_, election) in &confirmed_blocks_with_election {
            if let Some(election) = election {
                self.notify_block_confirmed(election.clone());
            }
        }

        let confirmed = self
            .active_elections
            .confirm_dependent_elections(confirmed_blocks_with_election);

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
            .send(AecEvent::BlockCemented(block, election));
    }
}
