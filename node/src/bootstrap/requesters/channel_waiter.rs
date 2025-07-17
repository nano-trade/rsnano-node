use crate::bootstrap::{state::BootstrapState, BootstrapPromise, PollResult};
use rsnano_network::{token_bucket::TokenBucket, Channel, ChannelId, Network, TrafficType};
use rsnano_stats::StatsCollection;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex, RwLock,
};

/// Waits until a channel becomes available
#[derive(Clone)]
pub(super) struct ChannelWaiter {
    network: Arc<RwLock<Network>>,
    state: ChannelWaitState,
    limiter: Arc<Mutex<TokenBucket>>,
    max_requests: usize,
    stats: Arc<ChannelWaiterStats>,
}

#[derive(Clone)]
enum ChannelWaitState {
    Initial,
    WaitRunningQueries,
    WaitLimiter,
    WaitChannel,
}

impl ChannelWaiter {
    pub fn new(
        network: Arc<RwLock<Network>>,
        limiter: Arc<Mutex<TokenBucket>>,
        max_requests: usize,
    ) -> Self {
        Self {
            network,
            state: ChannelWaitState::Initial,
            limiter,
            max_requests,
            stats: Arc::new(Default::default()),
        }
    }

    pub fn stats(&self) -> Arc<ChannelWaiterStats> {
        self.stats.clone()
    }

    fn candidate_channels(network: &Network) -> Vec<ChannelId> {
        network
            .available_channels(TrafficType::BootstrapRequests)
            .map(|c| c.channel_id())
            .collect()
    }
}

impl BootstrapPromise<Arc<Channel>> for ChannelWaiter {
    fn poll(&mut self, boot_state: &mut BootstrapState) -> PollResult<Arc<Channel>> {
        match self.state {
            ChannelWaitState::Initial => {
                self.state = ChannelWaitState::WaitRunningQueries;
                return PollResult::Progress;
            }
            ChannelWaitState::WaitRunningQueries => {
                // Limit the number of in-flight requests
                if boot_state.running_queries.len() < self.max_requests {
                    self.state = ChannelWaitState::WaitLimiter;
                    return PollResult::Progress;
                }
                self.stats.queries_overfill.fetch_add(1, Ordering::Relaxed);
            }
            ChannelWaitState::WaitLimiter => {
                // Wait until more requests can be sent
                if self.limiter.lock().unwrap().try_consume(1) {
                    self.state = ChannelWaitState::WaitChannel;
                    return PollResult::Progress;
                }
                self.stats.rate_limit.fetch_add(1, Ordering::Relaxed);
            }
            ChannelWaitState::WaitChannel => {
                // Wait until a channel is available
                let network = self.network.read().unwrap();
                let channel_ids = Self::candidate_channels(&network);

                if let Some(channel_id) = boot_state.scoring.channel(channel_ids) {
                    if let Some(channel) = network.get(channel_id) {
                        self.state = ChannelWaitState::Initial;
                        return PollResult::Finished(channel.clone());
                    }
                }
                self.stats.no_candidate.fetch_add(1, Ordering::Relaxed);
            }
        }

        PollResult::Wait
    }
}

#[derive(Default)]
pub(crate) struct ChannelWaiterStats {
    pub queries_overfill: AtomicU64,
    pub rate_limit: AtomicU64,
    pub no_candidate: AtomicU64,
}

impl ChannelWaiterStats {
    pub fn collect_stats(&self, stat_name: &'static str, result: &mut StatsCollection) {
        result.insert(
            stat_name,
            "queries_overfill",
            self.queries_overfill.load(Ordering::Relaxed),
        );
        result.insert(
            stat_name,
            "rate_limit",
            self.rate_limit.load(Ordering::Relaxed),
        );
        result.insert(
            stat_name,
            "no_candidate",
            self.no_candidate.load(Ordering::Relaxed),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bootstrap::state::RunningQuery;

    #[test]
    fn initial_state() {
        let network = test_network();
        let limiter = Arc::new(Mutex::new(TokenBucket::new(TEST_RATE_LIMIT)));
        let waiter = ChannelWaiter::new(network, limiter, MAX_TEST_REQUESTS);
        assert!(matches!(waiter.state, ChannelWaitState::Initial));
    }

    #[test]
    fn happy_path_no_waiting() {
        let network = test_network();
        let channel = network.write().unwrap().add_test_channel();
        let limiter = Arc::new(Mutex::new(TokenBucket::new(TEST_RATE_LIMIT)));
        let mut waiter = ChannelWaiter::new(network, limiter, MAX_TEST_REQUESTS);
        let mut state = BootstrapState::default();

        let found = loop {
            match waiter.poll(&mut state) {
                PollResult::Progress => {}
                PollResult::Wait => {
                    panic!("Should never wait")
                }
                PollResult::Finished(c) => break c,
            }
        };

        assert_eq!(channel.channel_id(), found.channel_id());
    }

    #[test]
    fn wait_for_running_queries() {
        let network = test_network();
        let limiter = Arc::new(Mutex::new(TokenBucket::new(TEST_RATE_LIMIT)));
        let mut waiter = ChannelWaiter::new(network, limiter, 1);
        let mut state = BootstrapState::default();

        assert!(matches!(waiter.poll(&mut state), PollResult::Progress)); // initial

        state
            .running_queries
            .insert(RunningQuery::new_test_instance());

        assert!(matches!(waiter.poll(&mut state), PollResult::Wait));
        assert!(matches!(waiter.state, ChannelWaitState::WaitRunningQueries));

        assert!(matches!(waiter.poll(&mut state), PollResult::Wait));

        state.running_queries.clear();
        assert!(matches!(waiter.poll(&mut state), PollResult::Progress));
    }

    #[test]
    fn wait_for_limiter() {
        let network = test_network();
        let limiter = Arc::new(Mutex::new(TokenBucket::new(TEST_RATE_LIMIT)));
        limiter.lock().unwrap().try_consume(TEST_RATE_LIMIT);
        let mut waiter = ChannelWaiter::new(network, limiter.clone(), MAX_TEST_REQUESTS);
        let mut state = BootstrapState::default();

        assert!(matches!(waiter.poll(&mut state), PollResult::Progress)); // initial
        assert!(matches!(waiter.poll(&mut state), PollResult::Progress)); // running queries

        let result = waiter.poll(&mut state);
        assert!(matches!(result, PollResult::Wait));
        assert!(matches!(waiter.state, ChannelWaitState::WaitLimiter));

        let result = waiter.poll(&mut state);
        assert!(matches!(result, PollResult::Wait));

        limiter.lock().unwrap().reset();
        let result = waiter.poll(&mut state);
        assert!(matches!(result, PollResult::Progress));
        assert!(matches!(waiter.state, ChannelWaitState::WaitChannel));
    }

    #[test]
    fn wait_scoring() {
        let network = test_network();
        let limiter = Arc::new(Mutex::new(TokenBucket::new(TEST_RATE_LIMIT)));
        let mut waiter = ChannelWaiter::new(network, limiter, MAX_TEST_REQUESTS);
        let mut state = BootstrapState::default();

        assert!(matches!(waiter.poll(&mut state), PollResult::Progress)); // initial
        assert!(matches!(waiter.poll(&mut state), PollResult::Progress)); // running queries
        assert!(matches!(waiter.poll(&mut state), PollResult::Progress)); // limiter

        let result = waiter.poll(&mut state);
        assert!(matches!(result, PollResult::Wait));
        assert!(matches!(waiter.state, ChannelWaitState::WaitChannel));

        let result = waiter.poll(&mut state);
        assert!(matches!(result, PollResult::Wait));
    }

    const TEST_RATE_LIMIT: usize = 4;
    const MAX_TEST_REQUESTS: usize = 3;

    fn test_network() -> Arc<RwLock<Network>> {
        Arc::new(RwLock::new(Network::new_test_instance()))
    }
}
