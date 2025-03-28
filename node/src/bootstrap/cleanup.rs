use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use rsnano_nullable_clock::{SteadyClock, Timestamp};
use rsnano_stats::{DetailType, StatType, Stats};

use super::state::{BootstrapState, RunningQuery};

pub(super) struct BootstrapCleanup {
    clock: Arc<SteadyClock>,
    stats: Arc<Stats>,
    last_dependency_sync: Instant,
}

impl BootstrapCleanup {
    pub(super) fn new(clock: Arc<SteadyClock>, stats: Arc<Stats>) -> Self {
        Self {
            clock,
            stats,
            last_dependency_sync: Instant::now(),
        }
    }

    pub fn cleanup(&mut self, state: &mut BootstrapState) {
        let now = self.clock.now();
        self.stats.inc(StatType::Bootstrap, DetailType::LoopCleanup);
        state.scoring.decay();

        let decayed = state.candidate_accounts.decay_blocking(now);
        self.stats.add(
            StatType::BootstrapAccountSets,
            DetailType::BlockingDecayed,
            decayed as u64,
        );

        self.erase_timed_out_requests(state, now);
        self.reinsert_known_dependencies(state);
    }

    fn erase_timed_out_requests(&mut self, state: &mut BootstrapState, now: Timestamp) {
        let should_timeout = |query: &RunningQuery| query.response_cutoff < now;

        while let Some(front) = state.running_queries.front() {
            if !should_timeout(front) {
                break;
            }

            self.stats.inc(StatType::Bootstrap, DetailType::Timeout);
            self.stats
                .inc(StatType::BootstrapTimeout, front.query_type.into());
            state.running_queries.pop_front();
        }
    }

    fn reinsert_known_dependencies(&mut self, state: &mut BootstrapState) {
        if self.last_dependency_sync.elapsed() < Duration::from_secs(30) {
            return;
        }

        self.last_dependency_sync = Instant::now();
        self.stats
            .inc(StatType::Bootstrap, DetailType::SyncDependencies);

        let inserted = state.candidate_accounts.sync_dependencies();

        if inserted > 0 {
            self.stats.add(
                StatType::BootstrapAccountSets,
                DetailType::PriorityInsert,
                inserted as u64,
            );
            self.stats.add(
                StatType::BootstrapAccountSets,
                DetailType::DependencySynced,
                inserted as u64,
            );
        }
    }
}
