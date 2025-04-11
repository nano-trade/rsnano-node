use rsnano_core::{BlockHash, Vote};
use std::{ops::Deref, sync::Arc};

/// A vote where only one given block hash is counted
pub struct FilteredVote {
    pub vote: Arc<Vote>,
    pub filter: BlockHash,
}

impl FilteredVote {
    pub(crate) fn new(vote: Arc<Vote>, filter: BlockHash) -> Self {
        Self { vote, filter }
    }

    pub fn filtered_blocks(&self) -> impl Iterator<Item = &BlockHash> {
        self.vote.hashes.iter().filter(|&h| {
            if self.filter.is_zero() {
                true
            } else {
                *h == self.filter
            }
        })
    }
}

impl Deref for FilteredVote {
    type Target = Vote;

    fn deref(&self) -> &Self::Target {
        &self.vote
    }
}

impl From<Arc<Vote>> for FilteredVote {
    fn from(value: Arc<Vote>) -> Self {
        Self::new(value, BlockHash::zero())
    }
}
