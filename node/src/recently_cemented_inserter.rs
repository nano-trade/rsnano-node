use std::sync::{Arc, Mutex};

use bounded_vec_deque::BoundedVecDeque;

use crate::consensus::EndedElection;

/// When a block gets cemented, this struct inserts that
/// block into the recently cemented cache
pub(crate) struct RecentlyCementedInserter {
    pub recently_cemented: Arc<Mutex<BoundedVecDeque<EndedElection>>>,
}

impl RecentlyCementedInserter {
    pub fn insert(&self, status: EndedElection) {
        self.recently_cemented.lock().unwrap().push_back(status);
    }
}
