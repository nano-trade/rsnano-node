#[macro_use]
extern crate anyhow;

#[macro_use]
extern crate strum_macros;

mod block_cementer;
mod block_insertion;
mod block_rollback;
mod dependent_blocks_finder;
mod generate_cache_flags;
mod ledger;
mod ledger_builder;
mod ledger_constants;
mod ledger_inserter;
mod ledger_sets;
mod rep_weight_cache;
mod rep_weights_updater;
mod representative_block_finder;
pub mod test_helpers;
mod vote_verifier;

#[cfg(test)]
mod ledger_tests;

pub(crate) use block_rollback::BlockRollbackPerformer;
pub use block_rollback::RollbackError;
pub use dependent_blocks_finder::*;
pub use generate_cache_flags::GenerateCacheFlags;
pub use ledger::*;
pub use ledger_builder::*;
pub use ledger_constants::{
    LedgerConstants, DEV_GENESIS_ACCOUNT, DEV_GENESIS_BLOCK, DEV_GENESIS_HASH, DEV_GENESIS_PUB_KEY,
};
pub use ledger_inserter::*;
pub use ledger_sets::*;
pub use rep_weight_cache::*;
pub use rep_weights_updater::*;
pub(crate) use representative_block_finder::RepresentativeBlockFinder;
use rsnano_stats::DetailType;
pub use rsnano_store_lmdb::{WriteGuard, WriteQueue, Writer};

#[derive(
    Copy, Clone, PartialEq, Eq, Debug, PartialOrd, Ord, EnumIter, EnumCount, Hash, IntoStaticStr,
)]
pub enum BlockSource {
    Unknown = 0,
    Live,
    LiveOriginator,
    Bootstrap,
    BootstrapLegacy,
    Unchecked,
    Local,
    Forced,
    Election,
}

impl From<BlockSource> for DetailType {
    fn from(value: BlockSource) -> Self {
        match value {
            BlockSource::Unknown => DetailType::Unknown,
            BlockSource::Live => DetailType::Live,
            BlockSource::LiveOriginator => DetailType::LiveOriginator,
            BlockSource::Bootstrap => DetailType::Bootstrap,
            BlockSource::BootstrapLegacy => DetailType::BootstrapLegacy,
            BlockSource::Unchecked => DetailType::Unchecked,
            BlockSource::Local => DetailType::Local,
            BlockSource::Forced => DetailType::Forced,
            BlockSource::Election => DetailType::Election,
        }
    }
}
