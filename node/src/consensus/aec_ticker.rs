use std::sync::{Arc, RwLock};

use rsnano_core::utils::{CancellationToken, Runnable};
use rsnano_nullable_clock::SteadyClock;

use super::{election::Election, ActiveElectionsContainer};

/// Every 300ms tries to transitions election state and send votes + blocks
pub struct AecTicker {
    active_elections: Arc<RwLock<ActiveElectionsContainer>>,
    clock: Arc<SteadyClock>,
    plugins: Vec<Box<dyn AecTickerPlugin>>,
}

impl AecTicker {
    pub fn new(
        active_elections: Arc<RwLock<ActiveElectionsContainer>>,
        clock: Arc<SteadyClock>,
    ) -> Self {
        Self {
            active_elections,
            clock,
            plugins: Vec::new(),
        }
    }

    pub fn new_null() -> Self {
        Self {
            active_elections: Arc::new(RwLock::new(ActiveElectionsContainer::default())),
            clock: Arc::new(SteadyClock::new_null()),
            plugins: Vec::new(),
        }
    }

    pub fn add_plugin(&mut self, plugin: impl AecTickerPlugin + 'static) {
        self.plugins.push(Box::new(plugin));
    }
}

impl Runnable for AecTicker {
    fn run(&mut self, _cancel_token: &CancellationToken) {
        let elections = self
            .active_elections
            .write()
            .unwrap()
            .transition_time(self.clock.now());

        for plugin in &mut self.plugins {
            plugin.process(&elections);
        }
    }
}

pub trait AecTickerPlugin: Send {
    fn process(&mut self, elections: &[Election]);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consensus::AecInsertRequest;
    use rsnano_core::SavedBlock;
    use rsnano_nullable_clock::Timestamp;
    use std::sync::Mutex;

    #[test]
    fn call_plugins() {
        let mut ticker = AecTicker::new_null();
        let plugin = StubPlugin::default();
        let called = plugin.0.clone();
        ticker.add_plugin(plugin);

        let block = SavedBlock::new_test_instance_with_key(1);
        let now = Timestamp::new_test_instance();

        ticker
            .active_elections
            .write()
            .unwrap()
            .insert(AecInsertRequest::new_manual(block.clone()), now)
            .unwrap();

        ticker.run(&CancellationToken::new_null());

        assert_eq!(called.lock().unwrap().len(), 1);
    }

    #[derive(Default)]
    struct StubPlugin(Arc<Mutex<Vec<Election>>>);

    impl AecTickerPlugin for StubPlugin {
        fn process(&mut self, elections: &[Election]) {
            *self.0.lock().unwrap() = elections.to_vec();
        }
    }
}
