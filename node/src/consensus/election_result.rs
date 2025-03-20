use std::time::{Duration, SystemTime};

use rsnano_core::{Amount, MaybeSavedBlock, SavedBlock};
use rsnano_stats::DetailType;

use super::VoteSummary;

/**
 * Tag for the type of the election status
 */
#[derive(PartialEq, Eq, Debug, Clone, Copy, FromPrimitive)]
pub enum ElectionResult {
    ActiveConfirmedQuorum = 1,
    ActiveConfirmationHeight = 2,
    InactiveConfirmationHeight = 3,
}

impl ElectionResult {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ActiveConfirmedQuorum => "active_quorum",
            Self::ActiveConfirmationHeight => "active_confirmation_height",
            Self::InactiveConfirmationHeight => "inactive",
        }
    }
}

impl From<ElectionResult> for DetailType {
    fn from(value: ElectionResult) -> Self {
        match value {
            ElectionResult::ActiveConfirmedQuorum => DetailType::ActiveConfirmedQuorum,
            ElectionResult::ActiveConfirmationHeight => DetailType::ActiveConfirmationHeight,
            ElectionResult::InactiveConfirmationHeight => DetailType::InactiveConfirmationHeight,
        }
    }
}

/// Information about an ended election
#[derive(Clone)]
pub struct EndedElection {
    pub winner: MaybeSavedBlock,
    pub tally: Amount,
    pub final_tally: Amount,
    pub confirmation_request_count: u32,
    pub block_count: u32,
    pub voter_count: u32,
    pub election_end: SystemTime,
    pub election_duration: Duration,
    pub result: ElectionResult,
    pub votes: Vec<VoteSummary>,
}

impl EndedElection {
    pub fn new(block: SavedBlock) -> Self {
        Self {
            winner: MaybeSavedBlock::Saved(block),
            election_end: SystemTime::now(),
            block_count: 1,
            result: ElectionResult::InactiveConfirmationHeight,
            tally: Amount::zero(),
            final_tally: Amount::zero(),
            voter_count: 0,
            confirmation_request_count: 0,
            election_duration: Duration::ZERO,
            votes: Vec::new(),
        }
    }
}
