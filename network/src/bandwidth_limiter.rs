use rsnano_core::utils::ContainerInfo;

use crate::{token_bucket::TokenBucket, TrafficType};
use std::sync::Mutex;

pub struct RateLimiter {
    bucket: TokenBucket,
}

impl RateLimiter {
    pub fn new(limit: usize) -> Self {
        Self::with_burst_ratio(limit, 1.0)
    }

    pub fn with_burst_ratio(limit: usize, limit_burst_ratio: f64) -> Self {
        Self {
            bucket: TokenBucket::with_refill_rate(
                (limit as f64 * limit_burst_ratio) as usize,
                limit,
            ),
        }
    }

    pub fn should_pass(&mut self, message_size: usize) -> bool {
        self.bucket.try_consume(message_size)
    }

    pub fn set_limit(&mut self, new_limit: usize) {
        self.bucket.set_limit(new_limit)
    }

    pub fn reset(&mut self) {
        self.bucket.reset()
    }

    pub fn size(&self) -> usize {
        self.bucket.size()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct BandwidthLimiterConfig {
    pub generic_limit: usize,
    pub generic_burst_ratio: f64,

    pub bootstrap_limit: usize,
    pub bootstrap_burst_ratio: f64,
}

impl Default for BandwidthLimiterConfig {
    fn default() -> Self {
        Self {
            generic_limit: 10 * 1024 * 1024,
            generic_burst_ratio: 3_f64,
            bootstrap_limit: 5 * 1024 * 1024,
            bootstrap_burst_ratio: 1_f64,
        }
    }
}

pub struct BandwidthLimiter {
    limiter_generic: Mutex<TokenBucket>,
    limiter_bootstrap: Mutex<TokenBucket>,
}

impl BandwidthLimiter {
    pub fn new(config: BandwidthLimiterConfig) -> Self {
        Self {
            limiter_generic: Mutex::new(TokenBucket::with_burst_ratio(
                config.generic_limit,
                config.generic_burst_ratio,
            )),
            limiter_bootstrap: Mutex::new(TokenBucket::with_burst_ratio(
                config.bootstrap_limit,
                config.bootstrap_burst_ratio,
            )),
        }
    }

    /**
     * Check whether packet falls withing bandwidth limits and should be allowed
     * @return true if OK, false if needs to be dropped
     */
    pub fn should_pass(&self, buffer_size: usize, limit_type: TrafficType) -> bool {
        self.select_limiter(limit_type)
            .lock()
            .unwrap()
            .try_consume(buffer_size)
    }

    fn select_limiter(&self, limit_type: TrafficType) -> &Mutex<TokenBucket> {
        match limit_type {
            TrafficType::BootstrapServer => &self.limiter_bootstrap,
            _ => &self.limiter_generic,
        }
    }

    pub fn container_info(&self) -> ContainerInfo {
        let generic_size = self.limiter_generic.lock().unwrap().size();
        let bootstrap_size = self.limiter_bootstrap.lock().unwrap().size();
        [
            ("generic", generic_size, 0),
            ("bootstrap", bootstrap_size, 0),
        ]
        .into()
    }
}

impl Default for BandwidthLimiter {
    fn default() -> Self {
        Self::new(Default::default())
    }
}
