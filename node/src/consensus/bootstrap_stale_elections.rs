use super::{election::Election, ActiveElectionsContainer, AecTickerPlugin2};
use crate::bootstrap::Bootstrapper;
use rsnano_nullable_clock::{SteadyClock, Timestamp};
use rsnano_stats::{StatsCollection, StatsSource};
use std::{
    any::Any,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tracing::debug;

/// If an election isn't confirmed within "stale_threshold", then try to bootstrap
/// the election account, so that missing dependencies will be pulled
pub(crate) struct BootstrapStaleElections {
    bootstrapper: Arc<Bootstrapper>,
    clock: Arc<SteadyClock>,
    pub stats: Arc<StaleElectionsStats>,
    stale_threshold: Duration,
}

impl BootstrapStaleElections {
    pub const DEFAULT_STALE_THRESHOLD: Duration = Duration::from_secs(60);

    pub(crate) fn new(bootstrapper: Arc<Bootstrapper>, clock: Arc<SteadyClock>) -> Self {
        Self {
            bootstrapper,
            clock,
            stats: Arc::new(StaleElectionsStats::default()),
            stale_threshold: Self::DEFAULT_STALE_THRESHOLD,
        }
    }

    pub fn set_stale_threshold(&mut self, threshold: Duration) {
        self.stale_threshold = threshold;
    }

    #[allow(dead_code)]
    pub fn get_stale_threshold(&self) -> Duration {
        self.stale_threshold
    }

    fn is_stale(&mut self, now: Timestamp, election: &Election) -> bool {
        election.start().elapsed(now) >= self.stale_threshold
    }

    fn bootstrap_stale(&mut self, election: &Election) {
        debug!(
            account = %election.account().encode_account(),
            root = ?election.qualified_root(),
            "Bootstrapping account with stale election");

        self.bootstrapper
            .state()
            .candidate_accounts
            .priority_set_initial(&election.account());

        self.stats.bootstrap_stale.fetch_add(1, Ordering::Relaxed);
    }
}

impl AecTickerPlugin2 for BootstrapStaleElections {
    fn run(&mut self, aec: &mut ActiveElectionsContainer) {
        let now = self.clock.now();
        for election in aec.iter_round_robin() {
            if self.is_stale(now, election) {
                self.bootstrap_stale(election);
            }
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Default)]
pub(crate) struct StaleElectionsStats {
    pub bootstrap_stale: AtomicU64,
}

impl StatsSource for StaleElectionsStats {
    fn collect_stats(&self, result: &mut StatsCollection) {
        result.insert(
            "active_elections",
            "bootstrap_stale",
            self.bootstrap_stale.load(Ordering::Relaxed),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consensus::AecInsertRequest;
    use rsnano_core::{utils::BlockPriority, SavedBlock};
    use tracing_test::traced_test;

    #[test]
    fn process_empty() {
        let bootstrapper = Arc::new(Bootstrapper::new_null());
        let clock = Arc::new(SteadyClock::new_null());
        let mut plugin = BootstrapStaleElections::new(bootstrapper.clone(), clock);
        let mut aec = ActiveElectionsContainer::default();

        plugin.run(&mut aec);

        assert_eq!(bootstrapper.state().candidate_accounts.priority_len(), 0);
        assert_eq!(plugin.stats.bootstrap_stale.load(Ordering::Relaxed), 0);
    }

    #[test]
    #[traced_test]
    fn bootstrap_stale_election() {
        let bootstrapper = Arc::new(Bootstrapper::new_null());
        let clock = Arc::new(SteadyClock::new_null());
        let block = SavedBlock::new_test_instance();
        let prio = BlockPriority::new_test_instance();
        let account = block.account();
        let mut aec = ActiveElectionsContainer::default();
        aec.insert(
            AecInsertRequest::new_priority(block, prio),
            clock.now() - BootstrapStaleElections::DEFAULT_STALE_THRESHOLD,
        )
        .unwrap();

        let mut plugin = BootstrapStaleElections::new(bootstrapper.clone(), clock);
        plugin.run(&mut aec);

        assert!(bootstrapper
            .state()
            .candidate_accounts
            .prioritized(&account));
        assert_eq!(plugin.stats.bootstrap_stale.load(Ordering::Relaxed), 1);
        assert!(logs_contain("Bootstrapping account with stale election"))
    }

    #[test]
    fn stats() {
        let stats = StaleElectionsStats {
            bootstrap_stale: AtomicU64::new(123),
        };
        let mut collection = StatsCollection::default();
        stats.collect_stats(&mut collection);
        assert_eq!(collection.get("active_elections", "bootstrap_stale"), 123);
    }
}
