use std::sync::{Arc, Mutex};

use crate::utils::{CancellationToken, Runnable};

use super::VoteRouter;

pub(crate) struct VoteRouterCleanup {
    vote_router: Arc<Mutex<VoteRouter>>,
}

impl VoteRouterCleanup {
    pub(crate) fn new(vote_router: Arc<Mutex<VoteRouter>>) -> Self {
        Self { vote_router }
    }
}

impl Runnable for VoteRouterCleanup {
    fn run(&mut self, _: &CancellationToken) {
        self.vote_router.lock().unwrap().clean_up();
    }
}
