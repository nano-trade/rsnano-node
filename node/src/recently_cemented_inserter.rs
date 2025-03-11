use std::sync::{Arc, Mutex};

use bounded_vec_deque::BoundedVecDeque;
use rsnano_core::SavedBlock;
use rsnano_ledger::{ElectionStatus, Entry};

/// When a block gets cemented, this struct inserts that
/// block into the recently cemented cache
pub(crate) struct RecentlyCementedInserter {
    pub recently_cemented: Arc<Mutex<BoundedVecDeque<ElectionStatus>>>,
}

impl RecentlyCementedInserter {
    pub fn batch_confirmed(&self, confirmed: &Vec<(SavedBlock, Entry)>) {}
}
