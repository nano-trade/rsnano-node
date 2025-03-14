use std::time::Duration;

use crate::utils::UnixTimestamp;

/// Combination of a unix timestamp + duration.
/// Duration field is specified in the 4 low-order bits of the timestamp.
/// This makes the timestamp have a minimum granularity of 16ms
/// The duration is specified as 2^(duration + 4) giving it a range of 16-524,288ms in power of two increments
#[derive(PartialEq, Eq, Debug, Clone, Copy)]
pub struct VoteTimestamp(u64);

impl VoteTimestamp {
    pub const FINAL: VoteTimestamp = VoteTimestamp(u64::MAX);
    pub const DURATION_MAX: u8 = 0x0F;
    pub const TIMESTAMP_MIN: UnixTimestamp = UnixTimestamp::new(0x0000_0000_0000_0010);
    const TIMESTAMP_MAX: u64 = 0xFFFF_FFFF_FFFF_FFF0;
    const TIMESTAMP_MASK: u64 = 0xFFFF_FFFF_FFFF_FFF0;

    pub const fn new(timestamp: u64, duration: u8) -> Self {
        debug_assert!(duration <= Self::DURATION_MAX);
        debug_assert!(timestamp != Self::TIMESTAMP_MAX || duration == Self::DURATION_MAX);
        let value = (timestamp & Self::TIMESTAMP_MASK) | (duration as u64);
        Self(value)
    }

    pub fn duration_bits(&self) -> u8 {
        let result = self.0 & !Self::TIMESTAMP_MASK;
        result as u8
    }

    /// Returns the timestamp of the vote (with the duration bits masked, set to zero)
    /// If it is a final vote, all the bits including duration bits are returned as they are, all FF
    pub fn unix_timestamp(&self) -> UnixTimestamp {
        if self.is_final() {
            UnixTimestamp::new(self.0)
        } else {
            UnixTimestamp::new(self.0 & Self::TIMESTAMP_MASK)
        }
    }

    pub fn is_final(&self) -> bool {
        *self == Self::FINAL
    }

    pub fn duration(&self) -> Duration {
        Duration::from_millis(1 << (self.duration_bits() + 4))
    }

    pub fn from_le_bytes(bytes: [u8; 8]) -> Self {
        Self(u64::from_le_bytes(bytes))
    }

    pub fn to_ne_bytes(&self) -> [u8; 8] {
        self.0.to_ne_bytes()
    }

    pub fn to_le_bytes(&self) -> [u8; 8] {
        self.0.to_le_bytes()
    }
}

impl From<u64> for VoteTimestamp {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<VoteTimestamp> for u64 {
    fn from(value: VoteTimestamp) -> Self {
        value.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_and_duration_masking() {
        let ts = VoteTimestamp::new(0x123f, 0xf);
        assert_eq!(ts.unix_timestamp(), UnixTimestamp::new(0x1230));
        assert_eq!(ts.duration().as_millis(), 524288);
        assert_eq!(ts.duration_bits(), 0xf);
    }
}
