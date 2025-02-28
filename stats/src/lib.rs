mod stats_enums;
use rsnano_core::{BlockSubType, VoteSource};
pub use stats_enums::*;

impl From<VoteSource> for DetailType {
    fn from(value: VoteSource) -> Self {
        match value {
            VoteSource::Live => Self::Live,
            VoteSource::Rebroadcast => Self::Rebroadcast,
            VoteSource::Cache => Self::Cache,
        }
    }
}

impl From<BlockSubType> for DetailType {
    fn from(block_type: BlockSubType) -> Self {
        match block_type {
            BlockSubType::Send => DetailType::Send,
            BlockSubType::Receive => DetailType::Receive,
            BlockSubType::Open => DetailType::Open,
            BlockSubType::Change => DetailType::Change,
            BlockSubType::Epoch => DetailType::EpochBlock,
        }
    }
}
