use super::ElectionSchedulers;
use crate::ledger_event_processor::LedgerEventProcessorPlugin;
use rsnano_ledger::LedgerEvent;
use std::sync::Arc;

pub(crate) struct ElectionSchedulersPlugin {
    schedulers: Arc<ElectionSchedulers>,
}

impl ElectionSchedulersPlugin {
    pub(crate) fn new(schedulers: Arc<ElectionSchedulers>) -> Self {
        Self { schedulers }
    }
}

impl LedgerEventProcessorPlugin for ElectionSchedulersPlugin {
    fn process(&mut self, event: &LedgerEvent) {
        match event {
            LedgerEvent::BlocksProcessed(results) => {
                self.schedulers
                    .activate_accounts_with_fresh_blocks(&results);
            }
            LedgerEvent::BlocksConfirmed(confirmed) => {
                self.schedulers
                    .activate_successors(confirmed.iter().map(|(b, _)| b));
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsnano_core::{BlockHash, SavedBlock};

    #[test]
    fn when_blocks_confirmed_should_activate_elections_for_sucessors() {
        let schedulers = Arc::new(ElectionSchedulers::new_null());
        let mut processor = ElectionSchedulersPlugin::new(schedulers.clone());
        let activation_tracker = schedulers.track_activate_successors();

        let block = SavedBlock::new_test_instance();
        let confirmed_blocks = vec![(block.clone(), BlockHash::from(123))];
        processor.process(&LedgerEvent::BlocksConfirmed(confirmed_blocks));

        let output = activation_tracker.output();
        assert_eq!(output, [block]);
    }
}
