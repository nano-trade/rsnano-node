use std::{sync::Arc, time::Duration};

use rsnano_core::QualifiedRoot;
use rsnano_nullable_clock::{SteadyClock, Timestamp};
use rsnano_stats::{DetailType, StatType, Stats};

use super::{
    bounded_hash_map::BoundedHashMap,
    election::{Election, ElectionBehavior},
    ConfirmationSolicitor,
};

pub(crate) struct ConfirmReqSender {
    stats: Arc<Stats>,
    last_requests: BoundedHashMap<QualifiedRoot, Timestamp>,
    clock: Arc<SteadyClock>,
}

impl ConfirmReqSender {
    pub(crate) fn new(stats: Arc<Stats>, clock: Arc<SteadyClock>) -> Self {
        Self {
            stats,
            clock,
            last_requests: BoundedHashMap::new(1024 * 32),
        }
    }

    pub fn send_confirm_req(&mut self, solicitor: &mut ConfirmationSolicitor, election: &Election) {
        if self.should_send_confirm_req(election) {
            if solicitor.add(election) {
                self.last_requests
                    .insert(election.qualified_root().clone(), self.clock.now());
                self.stats
                    .inc(StatType::Election, DetailType::ConfirmationRequest);
            }
        }
    }

    fn should_send_confirm_req(&self, election: &Election) -> bool {
        if let Some(last_req) = self.last_requests.get(&election.qualified_root()) {
            last_req.elapsed(self.clock.now()) >= Self::confirm_req_interval(election)
        } else {
            true
        }
    }

    /// Calculates time delay between broadcasting confirmation requests
    fn confirm_req_interval(election: &Election) -> Duration {
        match election.behavior() {
            ElectionBehavior::Priority | ElectionBehavior::Manual | ElectionBehavior::Hinted => {
                election.base_latency() * 5
            }
            ElectionBehavior::Optimistic => election.base_latency() * 2,
        }
    }
}
