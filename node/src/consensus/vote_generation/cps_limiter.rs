use crate::block_rate_calculator::CurrentBlockRates;
use rsnano_network::token_bucket::TokenBucket;
use rsnano_nullable_clock::Timestamp;
use std::{sync::Arc, time::Duration};

/// Limits the amount of created final votes with the goal of
/// reducing the network confirmation rate to a configured limit
#[derive(Clone)]
pub(crate) struct CpsLimiter {
    block_rates: Arc<CurrentBlockRates>,
    cps_limit: f64,
    current_rate: f64,
    vote_rate_limiter: TokenBucket,
    last_adjustment: Option<Timestamp>,
}

impl CpsLimiter {
    const RATE_ADJUSTMENT_INTERVAL: Duration = Duration::from_secs(30);

    pub(crate) fn new(block_rates: Arc<CurrentBlockRates>, cps_limit: usize) -> Self {
        Self {
            block_rates,
            cps_limit: cps_limit as f64,
            current_rate: cps_limit as f64,
            vote_rate_limiter: TokenBucket::new(cps_limit),
            last_adjustment: None,
        }
    }

    pub fn unlimited() -> Self {
        Self::new(Arc::new(CurrentBlockRates::default()), 0)
    }

    pub fn is_unlimited(&self) -> bool {
        self.cps_limit == 0f64
    }

    pub fn try_vote(&mut self, now: Timestamp) -> bool {
        if self.is_unlimited() {
            return true;
        }

        if self.should_adjust_vote_rate(now) {
            self.adjust_vote_rate(now);
        }

        self.vote_rate_limiter.try_consume(1, now)
    }

    fn should_adjust_vote_rate(&self, now: Timestamp) -> bool {
        match self.last_adjustment {
            Some(last) => last.elapsed(now) >= Self::RATE_ADJUSTMENT_INTERVAL,
            None => true,
        }
    }

    fn adjust_vote_rate(&mut self, now: Timestamp) {
        let cps = self.block_rates.cps() as f64;
        self.current_rate = calculate_new_limiter_rate(self.current_rate, cps, self.cps_limit);
        self.vote_rate_limiter.set_limit(self.current_rate as usize);
        self.last_adjustment = Some(now);
    }
}

fn calculate_new_limiter_rate(current_rate: f64, cps: f64, cps_limit: f64) -> f64 {
    // Limit vote creation to 100x of CPS limit. This is done to prevent the token bucket limit
    // to grow unbounded.
    let max = cps_limit * 100.0;

    // The lower limit is introduced, so that CPS rate recovers quicker after a sharp drop
    let min = (cps_limit * 0.5).max(1.0);

    // Adjustment factor for token buckent increase/decrease
    const ALPHA: f64 = 0.2;

    (current_rate * (1.0 + ALPHA * (1.0 - cps / cps_limit))).clamp(min, max)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ntest::{assert_false, assert_true};

    #[test]
    fn vote_when_below_cps_threshold() {
        let block_rates = Arc::new(CurrentBlockRates::default());
        const CPS_LIMIT: usize = 1000;
        let now = Timestamp::new_test_instance();
        let mut limiter = CpsLimiter::new(block_rates, CPS_LIMIT);
        assert_true!(limiter.try_vote(now));
        assert_true!(limiter.try_vote(now));
    }

    #[test]
    fn dont_vote_when_above_threshold() {
        let block_rates = Arc::new(CurrentBlockRates::default());
        const CPS_LIMIT: usize = 1;
        let now = Timestamp::new_test_instance();
        let mut limiter = CpsLimiter::new(block_rates.clone(), CPS_LIMIT);
        assert_true!(limiter.try_vote(now));

        block_rates.set_cps(1000);
        assert_false!(limiter.try_vote(now));
    }

    #[test]
    fn unlimited() {
        let mut limiter = CpsLimiter::unlimited();
        let now = Timestamp::new_test_instance();
        assert_true!(limiter.try_vote(now));
        assert_true!(limiter.try_vote(now));
        assert_true!(limiter.try_vote(now));
    }
}
