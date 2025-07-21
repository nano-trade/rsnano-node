use anyhow::{anyhow, Context};
use std::{str::FromStr, time::Duration};

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct RateSpec {
    pub initial_bps: usize,
    pub increment: usize,
    pub interval: Duration,
}

impl RateSpec {
    pub(crate) fn new(bps: usize) -> Self {
        Self {
            initial_bps: bps,
            increment: 0,
            interval: Duration::ZERO,
        }
    }

    pub(crate) fn with_increment(bps: usize, increment: usize, interval: Duration) -> Self {
        Self {
            initial_bps: bps,
            increment,
            interval,
        }
    }

    fn parse_bps(input: &str) -> anyhow::Result<usize> {
        input.parse().context("invalid bps")
    }

    fn parse_increment_and_interval(input: &str) -> anyhow::Result<(usize, Duration)> {
        if input.is_empty() {
            Ok((0, Duration::ZERO))
        } else {
            if let Some((incr_str, interval_str)) = input.split_once('@') {
                let increment = Self::parse_increment(incr_str)?;
                let interval = Self::parse_interval(interval_str)?;
                Ok((increment, interval))
            } else {
                let increment = Self::parse_increment(input)?;
                Ok((increment, Duration::from_secs(1)))
            }
        }
    }

    fn parse_increment(input: &str) -> anyhow::Result<usize> {
        input.parse().context("invalid increment")
    }

    fn parse_interval(input: &str) -> anyhow::Result<Duration> {
        if input.is_empty() {
            return Ok(Duration::from_secs(1));
        }

        if let Some(secs) = Self::try_parse_suffix("s", input) {
            return Ok(Duration::from_secs(secs?));
        }

        if let Some(mins) = Self::try_parse_suffix("min", input) {
            return Ok(Duration::from_secs(60 * mins?));
        }

        Err(anyhow!("invalid interval"))
    }

    fn try_parse_suffix(suffix: &str, input: &str) -> Option<anyhow::Result<u64>> {
        if input.ends_with(suffix) {
            Some(
                input[..input.len() - suffix.len()]
                    .parse::<u64>()
                    .context("invalid interval"),
            )
        } else {
            None
        }
    }
}

impl FromStr for RateSpec {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some((bps_str, incr_str)) = s.split_once('+') {
            let bps = Self::parse_bps(bps_str)?;
            let (increment, interval) = Self::parse_increment_and_interval(incr_str)?;
            Ok(Self::with_increment(bps, increment, interval))
        } else {
            Self::parse_bps(s).map(Self::new)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn construction() {
        assert_eq!(
            RateSpec::new(1000),
            RateSpec {
                initial_bps: 1000,
                increment: 0,
                interval: Duration::ZERO
            }
        );

        assert_eq!(
            RateSpec::with_increment(1000, 500, Duration::from_secs(1)),
            RateSpec {
                initial_bps: 1000,
                increment: 500,
                interval: Duration::from_secs(1)
            }
        );
    }

    #[test]
    fn parse_succeeds() {
        assert_parse("1000", RateSpec::new(1000));
        assert_parse("1000+", RateSpec::new(1000));
        assert_parse(
            "1000+50",
            RateSpec::with_increment(1000, 50, Duration::from_secs(1)),
        );
        assert_parse(
            "1000+50@",
            RateSpec::with_increment(1000, 50, Duration::from_secs(1)),
        );
        assert_parse(
            "1000+50@3s",
            RateSpec::with_increment(1000, 50, Duration::from_secs(3)),
        );
        assert_parse(
            "1000+50@3min",
            RateSpec::with_increment(1000, 50, Duration::from_secs(60 * 3)),
        );
    }

    #[test]
    fn parse_fails() {
        assert_parse_fails("abc", "invalid bps");
        assert_parse_fails("1000+abc", "invalid increment");
        assert_parse_fails("1000+10@abc", "invalid interval");
        assert_parse_fails("1000+10@100x", "invalid interval");
        assert_parse_fails("1000+@1s", "invalid increment");
    }

    fn assert_parse(input: &str, expected: RateSpec) {
        assert_eq!(
            input.parse::<RateSpec>().unwrap(),
            expected,
            "input string: {}",
            input
        );
    }

    fn assert_parse_fails(input: &str, error: &str) {
        assert_eq!(input.parse::<RateSpec>().unwrap_err().to_string(), error);
    }
}
