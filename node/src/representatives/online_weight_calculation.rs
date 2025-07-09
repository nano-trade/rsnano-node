use super::{OnlineReps, OnlineWeightSampler};
use rsnano_core::utils::{CancellationToken, Runnable};
use rsnano_nullable_clock::SteadyClock;
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tracing::info;

pub struct OnlineWeightCalculation {
    sampler: OnlineWeightSampler,
    online_reps: Arc<Mutex<OnlineReps>>,
    clock: Arc<SteadyClock>,
    first_run: bool,
    last_sample: Instant,
}

impl OnlineWeightCalculation {
    pub fn new(
        sampler: OnlineWeightSampler,
        online_reps: Arc<Mutex<OnlineReps>>,
        clock: Arc<SteadyClock>,
    ) -> Self {
        Self {
            sampler,
            online_reps,
            clock,
            first_run: true,
            last_sample: Instant::now(),
        }
    }

    fn calculate_trended_weight(&mut self) {
        let result = self.sampler.calculate_trend();
        info!(
            "Trended weight updated: {}, samples: {}",
            result.trended.format_balance(0),
            result.sample_count
        );
        self.online_reps.lock().unwrap().set_trended(result.trended);
    }
}

impl Runnable for OnlineWeightCalculation {
    fn run(&mut self, _: &CancellationToken) {
        if self.first_run {
            // Don't sample online weight on first run, because it is always 0
            self.sampler.sanitize();
            self.last_sample = Instant::now();
            self.calculate_trended_weight();
            self.first_run = false;
        } else {
            {
                let mut online = self.online_reps.lock().unwrap();
                online.trim(self.clock.now());
                online.calculate_online_weight();
            }
            if self.last_sample.elapsed() > Duration::from_secs(60) {
                let online_weight = self.online_reps.lock().unwrap().online_weight();
                self.sampler.add_sample(online_weight);
                self.calculate_trended_weight();
                self.last_sample = Instant::now();
            }
        }
    }
}
