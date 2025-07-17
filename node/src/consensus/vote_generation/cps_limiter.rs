/// Limits the amount of created final votes with the goal of
/// reducing the network confirmation rate to a configured limit
#[derive(Default)]
pub(crate) struct CpsLimiter {
    call_count: usize,
}

impl CpsLimiter {
    pub fn try_vote(&mut self) -> bool {
        // TODO
        true
    }
    pub fn call_count(&self) -> usize {
        self.call_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ntest::assert_true;

    #[test]
    fn track_calls() {
        let mut limiter = CpsLimiter::default();
        assert_eq!(limiter.call_count(), 0);
        assert_true!(limiter.try_vote());
        assert_eq!(limiter.call_count(), 1);
        assert_true!(limiter.try_vote());
        assert_eq!(limiter.call_count(), 1);
    }
}
