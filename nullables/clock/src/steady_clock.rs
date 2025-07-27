use std::{
    collections::VecDeque,
    ops::{Add, AddAssign, Sub},
    sync::Mutex,
    time::{Duration, Instant},
};

pub struct SteadyClock {
    time_source: TimeSource,
}

impl SteadyClock {
    pub fn new_null() -> Self {
        let mut offsets = VecDeque::new();
        offsets.push_back(DEFAULT_STUB_DURATION);
        Self {
            time_source: TimeSource::Stub(Mutex::new(offsets)),
        }
    }

    pub fn new_null_with(now: Timestamp) -> Self {
        Self::new_null_with_offsets([Duration::from_nanos(now.0 as u64)])
    }

    pub fn new_null_with_offsets(offsets: impl IntoIterator<Item = Duration>) -> Self {
        let mut last = DEFAULT_STUB_DURATION;
        let mut nows = VecDeque::new();
        nows.push_back(last);
        for offset in offsets.into_iter() {
            let now = last + offset.as_nanos() as i128;
            nows.push_back(now);
            last = now;
        }
        Self {
            time_source: TimeSource::Stub(Mutex::new(nows)),
        }
    }

    pub fn now(&self) -> Timestamp {
        Timestamp(self.time_source.now())
    }

    pub fn advance(&self, step: Duration) {
        match &self.time_source {
            TimeSource::System(_) => panic!("Only a nulled clock can be advanced!"),
            TimeSource::Stub(mutex) => {
                let mut times = mutex.lock().unwrap();
                if times.len() != 1 {
                    panic!("Cannot advance because other configured responses exist!")
                }
                let val = times.pop_front().unwrap();
                times.push_back(val + step.as_nanos() as i128);
            }
        }
    }
}

impl Default for SteadyClock {
    fn default() -> Self {
        SteadyClock {
            time_source: TimeSource::System(Instant::now()),
        }
    }
}

enum TimeSource {
    System(Instant),
    Stub(Mutex<VecDeque<i128>>),
}

impl TimeSource {
    fn now(&self) -> i128 {
        match self {
            TimeSource::System(instant) => instant.elapsed().as_nanos() as i128,
            TimeSource::Stub(nows) => {
                let mut guard = nows.lock().unwrap();
                if guard.len() == 1 {
                    *guard.front().unwrap()
                } else {
                    guard.pop_front().unwrap()
                }
            }
        }
    }
}

const DEFAULT_STUB_DURATION: i128 = 1000 * 1000 * 1000 * 60 * 60 * 24 * 365;

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone, Copy, Default, Hash)]
pub struct Timestamp(i128);

impl Timestamp {
    pub const MAX: Self = Self(i128::MAX);

    pub const fn new(nanos: i128) -> Self {
        Self(nanos)
    }

    pub const fn new_test_instance() -> Self {
        Self(DEFAULT_STUB_DURATION)
    }

    pub fn elapsed(&self, now: Timestamp) -> Duration {
        Duration::from_nanos(now.0.checked_sub(self.0).unwrap_or_default() as u64)
    }

    pub fn checked_sub(&self, rhs: Duration) -> Option<Self> {
        self.0.checked_sub(rhs.as_nanos() as i128).map(Self)
    }

    pub fn millis(&self) -> i64 {
        (self.0 / 1_000_000) as i64
    }

    pub const DEFAULT_STUB_NOW: Timestamp = Timestamp(DEFAULT_STUB_DURATION);
}

impl Add<Duration> for Timestamp {
    type Output = Timestamp;

    fn add(self, rhs: Duration) -> Self::Output {
        Self(self.0.add(rhs.as_nanos() as i128))
    }
}

impl AddAssign<Duration> for Timestamp {
    fn add_assign(&mut self, rhs: Duration) {
        *self = *self + rhs
    }
}

impl Sub<Timestamp> for Timestamp {
    type Output = Duration;

    fn sub(self, rhs: Timestamp) -> Self::Output {
        Duration::from_nanos((self.0 - rhs.0) as u64)
    }
}

impl Sub<Duration> for Timestamp {
    type Output = Timestamp;

    fn sub(self, rhs: Duration) -> Self::Output {
        Self(self.0 - rhs.as_nanos() as i128)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;

    mod timestamp {
        use super::*;

        #[test]
        fn add_duration() {
            assert_eq!(
                Timestamp::new(1000000000) + Duration::from_millis(300),
                Timestamp::new(1300000000)
            );
        }

        #[test]
        fn sub() {
            assert_eq!(
                Timestamp::new(1000000000) - Timestamp::new(300000000),
                Duration::from_millis(700)
            );
        }
    }

    #[test]
    fn now() {
        let clock = SteadyClock::default();
        let now1 = clock.now();
        sleep(Duration::from_millis(1));
        let now2 = clock.now();
        assert!(now2 > now1);
    }

    mod nullability {
        use super::*;

        #[test]
        fn can_be_nulled() {
            let clock = SteadyClock::new_null();
            let now1 = clock.now();
            let now2 = clock.now();
            assert_eq!(now1, now2);

            clock.advance(Duration::from_secs(1));
            assert_eq!(clock.now(), now1 + Duration::from_secs(1));
        }

        #[test]
        fn configure_multiple_responses() {
            let clock = SteadyClock::new_null_with_offsets([
                Duration::from_secs(1),
                Duration::from_secs(10),
                Duration::from_secs(3),
            ]);
            let now1 = clock.now();
            let now2 = clock.now();
            let now3 = clock.now();
            let now4 = clock.now();
            let now5 = clock.now();
            let now6 = clock.now();
            assert_eq!(now2, now1 + Duration::from_secs(1));
            assert_eq!(now3, now2 + Duration::from_secs(10));
            assert_eq!(now4, now3 + Duration::from_secs(3));
            assert_eq!(now5, now4);
            assert_eq!(now6, now4);
        }

        #[test]
        #[should_panic]
        fn cannot_advance_when_multiple_responses_configured() {
            let clock = SteadyClock::new_null_with_offsets([
                Duration::from_secs(1),
                Duration::from_secs(10),
                Duration::from_secs(3),
            ]);

            clock.advance(Duration::from_secs(1));
        }
    }
}
