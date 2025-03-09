use std::sync::Arc;

use rsnano_stats::{DetailType, StatType, Stats};

use crate::utils::{CancellationToken, Runnable};

use super::ActiveElections;

/// Requests confirmations for active elections from peered representatives
pub(crate) struct ConfirmationRequester {
    pub active_elections: Arc<ActiveElections>,
    pub stats: Arc<Stats>,
}

impl Runnable for ConfirmationRequester {
    fn run(&mut self, _cancel_token: &CancellationToken) {
        self.stats.inc(StatType::Active, DetailType::Loop);
        self.active_elections.request_confirm();
    }
}
