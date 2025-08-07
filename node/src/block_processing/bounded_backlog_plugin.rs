use std::sync::Arc;

use super::{BoundedBacklog, LedgerEvent};
use crate::ledger_event_processor::LedgerEventProcessorPlugin;

pub(crate) struct BoundedBacklogPlugin {
    bounded_backlog: Arc<BoundedBacklog>,
}

impl BoundedBacklogPlugin {
    pub(crate) fn new(bounded_backlog: Arc<BoundedBacklog>) -> Self {
        Self { bounded_backlog }
    }
}

impl LedgerEventProcessorPlugin for BoundedBacklogPlugin {
    fn process(&mut self, event: &LedgerEvent) {
        match event {
            LedgerEvent::BlocksProcessed(results) => {
                self.bounded_backlog.insert_processed(&results);
            }
            LedgerEvent::BlocksConfirmed(confirmed) => {
                self.bounded_backlog.remove(&confirmed);
            }
            LedgerEvent::BlocksRolledBack(rolled_back) => {
                // Unblock rolled back accounts as the dependency is no longer valid
                self.bounded_backlog.erase_hashes(rolled_back.hashes());
            }
            _ => {}
        }
    }
}
