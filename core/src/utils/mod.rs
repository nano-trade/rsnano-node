mod backperssure_channel;
mod cancellation_token;
mod container_info;
mod fair_queue;
mod peer;
mod stream;

pub use backperssure_channel::*;
pub use cancellation_token::CancellationToken;
use chrono::{DateTime, TimeZone, Utc};
pub use container_info::*;
pub use fair_queue::*;
pub use peer::*;
pub use stream::*;

use crate::Amount;
use std::{
    net::{Ipv6Addr, SocketAddrV6},
    ops::{Add, Mul},
    sync::{Arc, Condvar, Mutex},
    thread::available_parallelism,
    time::{Duration, SystemTime, SystemTimeError, UNIX_EPOCH},
};

pub trait Serialize {
    fn serialize(&self, stream: &mut dyn BufferWriter);
}

pub trait FixedSizeSerialize: Serialize {
    fn serialized_size() -> usize;
}

pub trait Deserialize {
    type Target;
    fn deserialize(stream: &mut dyn Stream) -> anyhow::Result<Self::Target>;
}

impl Serialize for u64 {
    fn serialize(&self, stream: &mut dyn BufferWriter) {
        stream.write_u64_be_safe(*self)
    }
}

impl FixedSizeSerialize for u64 {
    fn serialized_size() -> usize {
        std::mem::size_of::<u64>()
    }
}

impl Deserialize for u64 {
    type Target = Self;
    fn deserialize(stream: &mut dyn Stream) -> anyhow::Result<u64> {
        stream.read_u64_be()
    }
}

impl Serialize for [u8; 64] {
    fn serialize(&self, stream: &mut dyn BufferWriter) {
        stream.write_bytes_safe(self)
    }
}

impl FixedSizeSerialize for [u8; 64] {
    fn serialized_size() -> usize {
        64
    }
}

impl Deserialize for [u8; 64] {
    type Target = Self;

    fn deserialize(stream: &mut dyn Stream) -> anyhow::Result<Self::Target> {
        let mut buffer = [0; 64];
        stream.read_bytes(&mut buffer, 64)?;
        Ok(buffer)
    }
}

pub fn get_cpu_count() -> usize {
    // Try to read overridden value from environment variable
    let value = std::env::var("NANO_HARDWARE_CONCURRENCY")
        .unwrap_or_else(|_| "0".into())
        .parse::<usize>()
        .unwrap_or_default();

    if value > 0 {
        return value;
    }

    available_parallelism().unwrap().get()
}

pub type MemoryIntensiveInstrumentationCallback = extern "C" fn() -> bool;

pub static mut MEMORY_INTENSIVE_INSTRUMENTATION: Option<MemoryIntensiveInstrumentationCallback> =
    None;

extern "C" fn default_is_sanitizer_build_callback() -> bool {
    false
}
pub static mut IS_SANITIZER_BUILD: MemoryIntensiveInstrumentationCallback =
    default_is_sanitizer_build_callback;

pub fn memory_intensive_instrumentation() -> bool {
    match std::env::var("NANO_MEMORY_INTENSIVE") {
        Ok(val) => matches!(val.to_lowercase().as_str(), "1" | "true" | "on"),
        Err(_) => unsafe {
            match MEMORY_INTENSIVE_INSTRUMENTATION {
                Some(f) => f(),
                None => false,
            }
        },
    }
}

pub fn is_sanitizer_build() -> bool {
    unsafe { IS_SANITIZER_BUILD() }
}

pub fn milliseconds_since_epoch() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

pub fn system_time_as_seconds(time: SystemTime) -> u64 {
    time.duration_since(SystemTime::UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs()
}

/// Elapsed seconds since UNIX_EPOCH
#[derive(PartialEq, Eq, Clone, Copy, PartialOrd, Ord, Default)]
pub struct UnixTimestamp(u64);

impl UnixTimestamp {
    pub const ZERO: Self = Self(0);
    pub const MAX: Self = Self(u64::MAX);

    pub const fn new(seconds_since_epoch: u64) -> Self {
        Self(seconds_since_epoch)
    }

    pub const fn new_test_instance() -> Self {
        Self::new(1740000000)
    }

    pub fn now() -> Self {
        Self(Self::seconds_since_unix_epoch())
    }

    pub const fn as_u64(&self) -> u64 {
        self.0
    }

    fn seconds_since_unix_epoch() -> u64 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    pub fn to_be_bytes(&self) -> [u8; 8] {
        self.0.to_be_bytes()
    }

    pub fn from_be_bytes(bytes: [u8; 8]) -> Self {
        Self(u64::from_be_bytes(bytes))
    }

    pub fn add(&self, seconds: u64) -> Self {
        Self(self.0 + seconds)
    }

    pub fn utc(&self) -> DateTime<Utc> {
        Utc.timestamp_opt(self.0 as i64, 0)
            .latest()
            .unwrap_or_default()
    }
}

impl From<u64> for UnixTimestamp {
    fn from(value: u64) -> Self {
        Self::new(value)
    }
}

impl Add<Duration> for UnixTimestamp {
    type Output = UnixTimestamp;

    fn add(self, rhs: Duration) -> Self::Output {
        Self(self.0 + rhs.as_secs())
    }
}

impl Mul<u64> for UnixTimestamp {
    type Output = UnixTimestamp;

    fn mul(self, rhs: u64) -> Self::Output {
        UnixTimestamp::new(self.0 * rhs)
    }
}

impl TryFrom<SystemTime> for UnixTimestamp {
    type Error = SystemTimeError;

    fn try_from(value: SystemTime) -> Result<Self, Self::Error> {
        Ok(Self(value.duration_since(UNIX_EPOCH)?.as_secs()))
    }
}

impl std::fmt::Display for UnixTimestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl std::fmt::Debug for UnixTimestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.0, f)
    }
}

/// Elapsed milliseconds since UNIX_EPOCH
#[derive(PartialEq, Eq, Clone, Copy, PartialOrd, Ord, Default)]
pub struct UnixMillisTimestamp(u64);

impl UnixMillisTimestamp {
    pub const ZERO: Self = Self(0);
    pub const MAX: Self = Self(u64::MAX);

    pub const fn new(millis_since_epoch: u64) -> Self {
        Self(millis_since_epoch)
    }

    pub const fn new_test_instance() -> Self {
        Self::new(1740000000000)
    }

    pub fn now() -> Self {
        Self(milliseconds_since_epoch())
    }

    pub const fn as_u64(&self) -> u64 {
        self.0
    }

    pub fn to_be_bytes(&self) -> [u8; 8] {
        self.0.to_be_bytes()
    }

    pub fn from_be_bytes(bytes: [u8; 8]) -> Self {
        Self(u64::from_be_bytes(bytes))
    }

    pub fn checked_add(&self, duration: Duration) -> Option<Self> {
        self.0
            .checked_add(duration.as_millis() as u64)
            .map(|i| Self(i))
    }

    pub fn elapsed(&self, now: UnixMillisTimestamp) -> Duration {
        Duration::from_millis(now.0.checked_sub(self.0).unwrap_or(0))
    }
}

impl From<u64> for UnixMillisTimestamp {
    fn from(value: u64) -> Self {
        Self::new(value)
    }
}

impl Add<Duration> for UnixMillisTimestamp {
    type Output = UnixMillisTimestamp;

    fn add(self, rhs: Duration) -> Self::Output {
        Self(self.0 + rhs.as_secs())
    }
}

impl Mul<u64> for UnixMillisTimestamp {
    type Output = UnixMillisTimestamp;

    fn mul(self, rhs: u64) -> Self::Output {
        UnixMillisTimestamp::new(self.0 * rhs)
    }
}

impl std::fmt::Display for UnixMillisTimestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl std::fmt::Debug for UnixMillisTimestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.0, f)
    }
}

pub fn get_env_or_default<T>(variable_name: &str, default: T) -> T
where
    T: core::str::FromStr + Copy,
{
    std::env::var(variable_name)
        .map(|v| v.parse::<T>().unwrap_or(default))
        .unwrap_or(default)
}

pub fn get_env_or_default_string(variable_name: &str, default: impl Into<String>) -> String {
    std::env::var(variable_name).unwrap_or_else(|_| default.into())
}

pub fn get_env_bool(variable_name: impl AsRef<str>) -> Option<bool> {
    let variable_name = variable_name.as_ref();
    std::env::var(variable_name)
        .ok()
        .map(|val| match val.to_lowercase().as_ref() {
            "1" | "true" | "on" => true,
            "0" | "false" | "off" => false,
            _ => panic!("Invalid environment boolean value: {variable_name} = {val}"),
        })
}

pub fn parse_endpoint(s: &str) -> SocketAddrV6 {
    s.parse().unwrap()
}

pub const NULL_ENDPOINT: SocketAddrV6 = SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0, 0, 0);

pub const TEST_ENDPOINT_1: SocketAddrV6 =
    SocketAddrV6::new(Ipv6Addr::new(0, 0, 0, 0xffff, 0x10, 0, 0, 1), 1111, 0, 0);

pub const TEST_ENDPOINT_2: SocketAddrV6 =
    SocketAddrV6::new(Ipv6Addr::new(0, 0, 0, 0xffff, 0x10, 0, 0, 2), 2222, 0, 0);

pub const TEST_ENDPOINT_3: SocketAddrV6 =
    SocketAddrV6::new(Ipv6Addr::new(0, 0, 0, 0xffff, 0x10, 0, 0, 3), 3333, 0, 0);

pub fn new_test_timestamp() -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(1_000_000)
}

/// contains (done, Option<result>)
#[derive(Clone)]
pub struct OneShotNotification<T>(Arc<(Mutex<(bool, Option<T>)>, Condvar)>);

impl<T> OneShotNotification<T> {
    pub fn new() -> Self {
        Self(Arc::new((Mutex::new((false, None)), Condvar::new())))
    }

    pub fn notify(&self, t: T) {
        *self.0 .0.lock().unwrap() = (true, Some(t));
        self.0 .1.notify_one();
    }

    pub fn cancel(&self) {
        *self.0 .0.lock().unwrap() = (true, None);
        self.0 .1.notify_one();
    }

    pub fn wait(&self) -> Option<T> {
        let guard = self.0 .0.lock().unwrap();
        self.0 .1.wait_while(guard, |i| !i.0).unwrap().1.take()
    }
}

pub trait Runnable: Send {
    fn run(&mut self, cancel_token: &CancellationToken);
}

/// Lower timestamps have a higher priority
#[derive(PartialEq, Eq, Copy, Clone, Default)]
pub struct TimePriority(UnixTimestamp);

impl TimePriority {
    pub const ZERO: TimePriority = TimePriority::new(0);

    pub const fn new(timestamp: u64) -> Self {
        Self(UnixTimestamp::new(timestamp))
    }
}

impl Ord for TimePriority {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.0.cmp(&self.0)
    }
}

impl PartialOrd for TimePriority {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl std::fmt::Debug for TimePriority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl From<UnixTimestamp> for TimePriority {
    fn from(value: UnixTimestamp) -> Self {
        Self(value)
    }
}

impl From<TimePriority> for UnixTimestamp {
    fn from(value: TimePriority) -> Self {
        value.0
    }
}

#[derive(PartialEq, Eq, Copy, Clone, Default, PartialOrd, Ord)]
pub struct BlockPriority {
    pub balance: Amount,
    pub time: TimePriority,
}

impl BlockPriority {
    pub fn new(balance: Amount, time: TimePriority) -> Self {
        Self { balance, time }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_priority_order() {
        let a = BlockPriority::new(Amount::from(100), TimePriority::new(5));
        let b = BlockPriority::new(Amount::from(100), TimePriority::new(6));
        let c = BlockPriority::new(Amount::from(101), TimePriority::new(4));
        assert!(a > b);
        assert!(c > a);
    }
}
