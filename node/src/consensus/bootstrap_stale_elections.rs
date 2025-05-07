use super::{election::Election, AecTickerPlugin};
use crate::bootstrap::Bootstrapper;
use rsnano_nullable_clock::SteadyClock;
use std::{sync::Arc, time::Duration};

/// If an election isn't confirmed within STALE_THRESHOLD, then try to bootstrap
/// the election account, so that missing dependencies will be pulled
pub(crate) struct BootstrapStaleElections {
    bootstrapper: Arc<Bootstrapper>,
    clock: Arc<SteadyClock>,
}

impl BootstrapStaleElections {
    const STALE_THRESHOLD: Duration = Duration::from_secs(60);

    pub(crate) fn new(bootstrapper: Arc<Bootstrapper>, clock: Arc<SteadyClock>) -> Self {
        Self {
            bootstrapper,
            clock,
        }
    }
}

impl AecTickerPlugin for BootstrapStaleElections {
    fn process(&mut self, elections: &[Election]) {
        for election in elections {
            if election.start().elapsed(self.clock.now()) >= Self::STALE_THRESHOLD {
                self.bootstrapper
                    .state()
                    .candidate_accounts
                    .priority_set_initial(&election.account());
            }
        }
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
            clock.now() - Duration::from_secs(60),
        );
        let mut plugin = BootstrapStaleElections::new(bootstrapper.clone(), clock);

        plugin.process(&[election.clone()]);

        assert!(bootstrapper
            .state()
            .candidate_accounts
            .prioritized(&election.account()));
    }
}
