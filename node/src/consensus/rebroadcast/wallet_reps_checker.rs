use crate::wallets::WalletRepresentatives;
use rsnano_core::utils::{CancellationToken, Runnable};
use std::sync::{Arc, Mutex};

pub(crate) struct WalletRepsChecker {
    wallet_reps: Arc<Mutex<WalletRepresentatives>>,
    consumers: Vec<Box<dyn WalletRepsConsumer + Send + Sync>>,
}

impl WalletRepsChecker {
    pub(crate) fn new(wallet_reps: Arc<Mutex<WalletRepresentatives>>) -> Self {
        Self {
            wallet_reps,
            consumers: Vec::new(),
        }
    }

    pub fn add_consumer(&mut self, consumer: impl WalletRepsConsumer + Send + Sync + 'static) {
        self.consumers.push(Box::new(consumer));
    }
}

impl Runnable for WalletRepsChecker {
    fn run(&mut self, _cancel_token: &CancellationToken) {
        let reps = self.wallet_reps.lock().unwrap().clone();
        for consumer in &self.consumers {
            consumer.update_wallet_reps(&reps);
        }
    }
}

pub(crate) trait WalletRepsConsumer {
    fn update_wallet_reps(&self, reps: &WalletRepresentatives);
}

impl<T> WalletRepsConsumer for Arc<T>
where
    T: WalletRepsConsumer,
{
    fn update_wallet_reps(&self, reps: &WalletRepresentatives) {
        self.as_ref().update_wallet_reps(reps);
    }
}
