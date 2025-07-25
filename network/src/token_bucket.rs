use rsnano_nullable_clock::Timestamp;

/**
 * Token bucket based rate limiting. This is suitable for rate limiting ipc/api calls
 * and network traffic, while allowing short bursts.
 *
 * Tokens are refilled at N tokens per second and there's a bucket capacity to limit
 * bursts.
 *
 * A bucket has low overhead and can be instantiated for various purposes, such as one
 * bucket per session, or one for bandwidth limiting. A token can represent bytes,
 * messages, or the cost of API invocations.
 */
#[derive(Clone)]
pub struct TokenBucket {
    last_refill: Option<Timestamp>,
    current_size: usize,
    max_token_count: usize,

    /** The minimum observed bucket size, from which the largest burst can be derived */
    smallest_size: usize,
    refill_rate: usize,
}

const UNLIMITED: usize = 1_000_000_000;

impl TokenBucket {
    pub fn new(limit: usize) -> Self {
        Self::with_burst_ratio(limit, 1.0)
    }

    pub fn with_burst_ratio(limit: usize, limit_burst_ratio: f64) -> Self {
        Self::with_refill_rate((limit as f64 * limit_burst_ratio) as usize, limit)
    }

    /**
     * Set up a token bucket.
     * @param max_tokens Maximum number of tokens in this bucket, which limits bursts.
     * @param refill_rate Token refill rate, which limits the long term rate (tokens per seconds)
     */
    pub fn with_refill_rate(max_tokens: usize, refill_rate: usize) -> Self {
        let mut result = Self {
            last_refill: None,
            max_token_count: max_tokens,
            refill_rate,
            current_size: 0,
            smallest_size: 0,
        };

        result.reset_with(max_tokens, refill_rate);
        result
    }

    /**
     * Determine if an operation of cost \p tokens_required_a is possible, and deduct from the
     * bucket if that's the case.
     * The default cost is 1 token, but resource intensive operations may request
     * more tokens to be available.
     */
    pub fn try_consume(&mut self, tokens_required: usize, now: Timestamp) -> bool {
        debug_assert!(tokens_required <= UNLIMITED);
        self.refill(now);
        let possible = self.current_size >= tokens_required;
        if possible {
            self.current_size -= tokens_required;
        } else if tokens_required == UNLIMITED {
            self.current_size = 0;
        }

        // Keep track of smallest observed bucket size so burst size can be computed (for tests and stats)
        self.smallest_size = std::cmp::min(self.smallest_size, self.current_size);

        possible || self.refill_rate == UNLIMITED
    }

    pub fn set_limit(&mut self, new_limit: usize) {
        self.max_token_count = new_limit;
        self.refill_rate = new_limit;
    }

    pub fn reset(&mut self) {
        self.reset_with(self.max_token_count, self.refill_rate);
    }

    /** Update the max_token_count and/or refill_rate_a parameters */
    pub fn reset_with(&mut self, mut max_token_count: usize, mut refill_rate: usize) {
        // A token count of 0 indicates unlimited capacity. We use 1e9 as
        // a sentinel, allowing largest burst to still be computed.
        if max_token_count == 0 || refill_rate == 0 {
            refill_rate = UNLIMITED;
            max_token_count = UNLIMITED;
        }
        self.smallest_size = max_token_count;
        self.max_token_count = max_token_count;
        self.current_size = max_token_count;
        self.refill_rate = refill_rate;
        self.last_refill = None;
    }

    /** Returns the largest burst observed */
    #[allow(dead_code)]
    pub fn largest_burst(&self) -> usize {
        self.max_token_count - self.smallest_size
    }

    pub fn size(&self) -> usize {
        self.current_size
    }

    fn refill(&mut self, now: Timestamp) {
        let Some(last_refill) = self.last_refill else {
            self.last_refill = Some(now);
            return;
        };

        let tokens_to_add = (last_refill.elapsed(now).as_nanos() as f64 / 1e9_f64
            * self.refill_rate as f64) as usize;
        // Only update if there are any tokens to add
        if tokens_to_add > 0 {
            self.current_size =
                std::cmp::min(self.current_size + tokens_to_add, self.max_token_count);
            self.last_refill = Some(now);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn basic() {
        let mut bucket = TokenBucket::with_refill_rate(10, 10);
        let mut now = Timestamp::new_test_instance();

        // Initial burst
        assert_eq!(bucket.try_consume(10, now), true);
        assert_eq!(bucket.try_consume(10, now), false);

        // With a fill rate of 10 tokens/sec, await 1/3 sec and get 3 tokens
        now += Duration::from_millis(300);

        assert_eq!(bucket.try_consume(3, now), true);
        assert_eq!(bucket.try_consume(10, now), false);

        // Allow time for the bucket to completely refill and do a full burst
        now += Duration::from_secs(1);
        assert_eq!(bucket.try_consume(10, now), true);
        assert_eq!(bucket.largest_burst(), 10);
    }

    #[test]
    fn network() {
        // For the purpose of the test, one token represents 1MB instead of one byte.
        // Allow for 10 mb/s bursts (max bucket size), 5 mb/s long term rate
        let mut bucket = TokenBucket::with_refill_rate(10, 5);
        let mut now = Timestamp::new_test_instance();

        // Initial burst of 10 mb/s over two calls
        assert_eq!(bucket.try_consume(5, now), true);
        assert_eq!(bucket.largest_burst(), 5);
        assert_eq!(bucket.try_consume(5, now), true);
        assert_eq!(bucket.largest_burst(), 10);
        assert_eq!(bucket.try_consume(5, now), false);

        // After 200 ms, the 5 mb/s fillrate means we have 1 mb available
        now += Duration::from_millis(200);
        assert_eq!(bucket.try_consume(1, now), true);
        assert_eq!(bucket.try_consume(1, now), false);
    }

    #[test]
    fn reset() {
        let mut bucket = TokenBucket::with_refill_rate(0, 0);
        let mut now = Timestamp::new_test_instance();

        // consume lots of tokens, buckets should be unlimited
        assert!(bucket.try_consume(1000000, now));
        assert!(bucket.try_consume(1000000, now));

        // set bucket to be limited
        bucket.reset_with(1000, 1000);
        assert_eq!(bucket.try_consume(1001, now), false);
        assert_eq!(bucket.try_consume(1000, now), true);
        assert_eq!(bucket.try_consume(1000, now), false);
        now += Duration::from_millis(2);
        assert_eq!(bucket.try_consume(2, now), true);

        // reduce the limit
        bucket.reset_with(100, 100 * 1000);
        assert_eq!(bucket.try_consume(101, now), false);
        assert_eq!(bucket.try_consume(100, now), true);
        now += Duration::from_millis(1);
        assert_eq!(bucket.try_consume(100, now), true);

        // increase the limit
        bucket.reset_with(2000, 1);
        assert_eq!(bucket.try_consume(2001, now), false);
        assert_eq!(bucket.try_consume(2000, now), true);

        // back to unlimited
        bucket.reset_with(0, 0);
        assert_eq!(bucket.try_consume(1000000, now), true);
        assert_eq!(bucket.try_consume(1000000, now), true);
    }

    #[test]
    fn unlimited_rate() {
        let mut bucket = TokenBucket::with_refill_rate(0, 0);
        let now = Timestamp::new_test_instance();
        assert_eq!(bucket.try_consume(5, now), true);
        assert_eq!(bucket.largest_burst(), 5);
        assert_eq!(bucket.try_consume(1_000_000_000, now), true);
        assert_eq!(bucket.largest_burst(), 1_000_000_000);

        // With unlimited tokens, consuming always succeed
        assert_eq!(bucket.try_consume(1_000_000_000, now), true);
        assert_eq!(bucket.largest_burst(), 1_000_000_000);
    }

    #[test]
    fn busy_spin() {
        // Bucket should refill at a rate of 1 token per second
        let mut bucket = TokenBucket::with_refill_rate(1, 1);
        let mut now = Timestamp::new_test_instance();

        // Run a very tight loop for 5 seconds + a bit of wiggle room
        let mut counter = 0;
        let start = now;
        while now < start + Duration::from_millis(5500) {
            if bucket.try_consume(1, now) {
                counter += 1;
            }

            now += Duration::from_millis(250);
        }

        // Bucket starts fully refilled, therefore we see 1 additional request
        assert_eq!(counter, 6);
    }
}
