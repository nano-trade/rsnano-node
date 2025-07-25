use std::{
    any::{Any, TypeId},
    sync::{Arc, RwLock},
};

use rsnano_core::utils::{CancellationToken, Runnable};
use rsnano_nullable_clock::SteadyClock;

use super::{election::Election, ActiveElectionsContainer};

/// Every 300ms tries to transitions election state and send votes + blocks
pub struct AecTicker {
    active_elections: Arc<RwLock<ActiveElectionsContainer>>,
    clock: Arc<SteadyClock>,
    plugins: Vec<Box<dyn AecTickerPlugin>>,
    plugins2: Vec<Box<dyn AecTickerPlugin2>>,
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
            plugins2: Vec::new(),
        }
    }

    pub fn new_null() -> Self {
        Self {
            active_elections: Arc::new(RwLock::new(ActiveElectionsContainer::default())),
            clock: Arc::new(SteadyClock::new_null()),
            plugins: Vec::new(),
            plugins2: Vec::new(),
        }
    }

    pub fn add_plugin(&mut self, plugin: impl AecTickerPlugin + 'static) {
        self.plugins.push(Box::new(plugin));
    }

    pub fn add_plugin2(&mut self, plugin: impl AecTickerPlugin2 + 'static) {
        self.plugins2.push(Box::new(plugin));
    }

    pub fn get_plugin2<T>(&self) -> Option<&T>
    where
        T: AecTickerPlugin2 + 'static,
    {
        let Some(p) = self
            .plugins2
            .iter()
            .find(|p| (***p).type_id() == TypeId::of::<T>())
        else {
            return None;
        };

        (*p).as_any().downcast_ref::<T>()
    }

    pub fn get_plugin<T>(&self) -> Option<&T>
    where
        T: AecTickerPlugin + 'static,
    {
        let Some(p) = self
            .plugins
            .iter()
            .find(|p| (***p).type_id() == TypeId::of::<T>())
        else {
            return None;
        };

        (*p).as_any().downcast_ref::<T>()
    }
}

impl Runnable for AecTicker {
    fn run(&mut self, _cancel_token: &CancellationToken) {
        let elections = {
            let mut aec = self.active_elections.write().unwrap();
            let elections = aec.transition_time(self.clock.now());

            for plugin in &mut self.plugins2 {
                plugin.run(&mut aec);
            }

            elections
        };

        for plugin in &mut self.plugins {
            plugin.process(&elections);
        }
    }
}

pub trait AecTickerPlugin: Send + 'static {
    fn process(&mut self, elections: &[Election]);
    fn type_id(&self) -> TypeId {
        TypeId::of::<Self>()
    }
    fn as_any(&self) -> &dyn Any;
}

pub trait AecTickerPlugin2: Send + 'static {
    fn run(&mut self, aec: &mut ActiveElectionsContainer);
    fn type_id(&self) -> TypeId {
        TypeId::of::<Self>()
    }
    fn as_any(&self) -> &dyn Any;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consensus::AecInsertRequest;
    use rsnano_core::{utils::BlockPriority, SavedBlock};
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
            .insert(
                AecInsertRequest::new_manual(block.clone(), BlockPriority::new_test_instance()),
                now,
            )
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

        fn as_any(&self) -> &dyn Any {
            self
        }
    }
}
