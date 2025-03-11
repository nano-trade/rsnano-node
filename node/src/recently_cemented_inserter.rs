use std::sync::{Arc, Mutex};

use bounded_vec_deque::BoundedVecDeque;
use rsnano_ledger::ElectionStatus;

/// When a block gets cemented, this struct inserts that
/// block into the recently cemented cache
pub(crate) struct RecentlyCementedInserter {
    pub recently_cemented: Arc<Mutex<BoundedVecDeque<ElectionStatus>>>,
}

impl RecentlyCementedInserter {
    pub fn insert(&self, status: ElectionStatus) {
        self.recently_cemented.lock().unwrap().push_back(status);
    }
}
