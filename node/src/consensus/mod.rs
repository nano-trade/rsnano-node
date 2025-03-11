mod active_elections;
mod active_elections_driver;
mod bootstrap_weights;
mod bucket;
mod bucketing;
mod confirmation_solicitor;
mod election;
pub mod election_schedulers;
mod election_status;
mod ordered_blocks;
mod rebroadcast;
mod recently_confirmed_cache;
mod rep_tiers;
mod vote_applier;
mod vote_broadcaster;
mod vote_cache;
mod vote_cache_processor;
mod vote_generation;
mod vote_processor;
mod vote_processor_queue;
mod vote_router;

use std::ops::Deref;

pub use active_elections::*;
pub(crate) use active_elections_driver::ActiveElectionsDriver;
pub(crate) use bootstrap_weights::*;
pub use bucket::*;
pub use bucketing::Bucketing;
pub use confirmation_solicitor::ConfirmationSolicitor;
pub use election::*;
pub use election_status::{ElectionStatus, ElectionStatusType};
pub(crate) use rebroadcast::*;
pub use recently_confirmed_cache::*;
pub use rep_tiers::*;
use rsnano_core::Amount;
pub use vote_applier::*;
pub use vote_broadcaster::*;
pub use vote_cache::{CacheEntry, TopEntry, VoteCache, VoteCacheConfig, VoterEntry};
pub(crate) use vote_cache_processor::*;
pub use vote_generation::*;
pub use vote_processor::*;
pub use vote_processor_queue::*;
pub use vote_router::*;

/// Used for ordering items by descending tally
#[derive(PartialEq, Eq)]
pub struct DescTallyKey(pub Amount);

impl DescTallyKey {
    pub fn amount(&self) -> Amount {
        self.0.clone()
    }
}

impl Deref for DescTallyKey {
    type Target = Amount;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Ord for DescTallyKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.0.cmp(&self.0)
    }
}

impl PartialOrd for DescTallyKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        other.0.partial_cmp(&self.0)
    }
}

impl From<Amount> for DescTallyKey {
    fn from(value: Amount) -> Self {
        Self(value)
    }
}
