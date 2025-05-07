use super::{election::Election, AecTickerPlugin};
use crate::bootstrap::Bootstrapper;
use rsnano_nullable_clock::SteadyClock;
use rsnano_stats::{StatsCollection, StatsSource};
use std::{
    any::Any,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

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

    pub fn get_stale_threshold(&self) -> Duration {
        self.stale_threshold
    }
}

impl AecTickerPlugin for BootstrapStaleElections {
    fn process(&mut self, elections: &[Election]) {
        for election in elections {
            if election.start().elapsed(self.clock.now()) >= self.stale_threshold {
                self.bootstrapper
                    .state()
                    .candidate_accounts
                    .priority_set_initial(&election.account());

                self.stats.bootstrap_stale.fetch_add(1, Ordering::Relaxed);
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
    use crate::consensus::election::ElectionBehavior;
    use rsnano_core::SavedBlock;

    #[test]
    fn process_empty() {
        let bootstrapper = Arc::new(Bootstrapper::new_null());
        let clock = Arc::new(SteadyClock::new_null());
        let mut plugin = BootstrapStaleElections::new(bootstrapper.clone(), clock);

        plugin.process(&[]);

        assert_eq!(bootstrapper.state().candidate_accounts.priority_len(), 0);
        assert_eq!(plugin.stats.bootstrap_stale.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn bootstrap_stale_election() {
        let bootstrapper = Arc::new(Bootstrapper::new_null());
        let clock = Arc::new(SteadyClock::new_null());
        let block = SavedBlock::new_test_instance();
        let election = Election::new(
            block,
            ElectionBehavior::Manual,
            Duration::from_secs(1),
            clock.now() - BootstrapStaleElections::DEFAULT_STALE_THRESHOLD,
        );
        let mut plugin = BootstrapStaleElections::new(bootstrapper.clone(), clock);

        plugin.process(&[election.clone()]);

        assert!(bootstrapper
            .state()
            .candidate_accounts
            .prioritized(&election.account()));
        assert_eq!(plugin.stats.bootstrap_stale.load(Ordering::Relaxed), 1);
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
