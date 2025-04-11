mod active_elections_container;
mod cooldown_controller;
mod recently_confirmed_cache;
mod root_container;
mod stats;
mod vote_router;

use std::{collections::HashMap, sync::Arc};

use rsnano_core::{
    utils::BlockPriority, Amount, Block, BlockHash, PublicKey, QualifiedRoot, SavedBlock, Vote,
    VoteCode, VoteSource,
};
use rsnano_network::Channel;

use super::election::{ConfirmedElection, Election};
pub use active_elections_container::*;
pub use cooldown_controller::AecCooldownReason;
use root_container::{Entry, RootContainer};

#[derive(Clone, Debug, PartialEq)]
pub struct ActiveElectionsConfig {
    /// Maximum number of simultaneous active elections (AEC size)
    pub max_elections: usize,
    /// Maximum cache size for recently_confirmed
    pub confirmation_cache: usize,
}

impl Default for ActiveElectionsConfig {
    fn default() -> Self {
        Self {
            max_elections: 5000,
            confirmation_cache: 65536,
        }
    }
}

pub enum AecEvent {
    ElectionStarted(BlockHash, QualifiedRoot),
    ElectionConfirmed(ConfirmedElection),

    /// Ended ether confirmed or unconfirmed
    ElectionEnded(Election, Option<BlockPriority>),

    BlockAddedToElection(BlockHash),
    BlockDiscarded(Block),
    BlockConfirmed(SavedBlock, ConfirmedElection),
    VoteCounted(PublicKey, VoteSource),
    /// old winner + new winner block
    WinnerChanged(BlockHash, Block),

    VoteProcessed(
        Arc<Vote>,
        Amount,
        VoteSource,
        Option<Arc<Channel>>,
        HashMap<BlockHash, VoteCode>,
    ),
    FinalPhaseStarted(BlockHash, QualifiedRoot),
    VacancyUpdated,
}

#[derive(PartialEq, Eq, Debug, Clone, Copy)]
pub enum AecInsertError {
    Stopped,
    Duplicate,

    /// This block or a fork got recently confirmed, so there is no need for a new election.
    RecentlyConfirmed,
}
