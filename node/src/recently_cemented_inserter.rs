use std::sync::{Arc, Mutex};

use bounded_vec_deque::BoundedVecDeque;

use crate::consensus::election::ConfirmedElection;

/// When a block gets confirmed, this struct inserts that
/// block into the recently cemented cache
pub(crate) struct RecentlyCementedInserter {
    pub recently_cemented: Arc<Mutex<BoundedVecDeque<ConfirmedElection>>>,
}

impl RecentlyCementedInserter {
    pub fn insert(&self, status: ConfirmedElection) {
        self.recently_cemented.lock().unwrap().push_back(status);
    }
}
