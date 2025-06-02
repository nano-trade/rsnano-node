use std::sync::Arc;

use rsnano_ledger::Ledger;

use super::RollbackRequest;

pub(crate) struct BlockBatchRollback {
    pub ledger: Arc<Ledger>,
}

impl BlockBatchRollback {
    pub(crate) fn roll_back(&self, request: RollbackRequest) {
        let mut results = self
            .ledger
            .roll_back_batch(&request.targets, request.max_rollbacks);

        let mut processed_hashes = Vec::new();
        for result in results.drain(..) {
            if !result.rolled_back.is_empty() {
                for h in &result.rolled_back {
                    processed_hashes.push(h.hash());
                }
            } else {
                processed_hashes.push(result.target_hash);
            }
        }

        *request.result.rolled_back.lock().unwrap() = Some(processed_hashes);
        request.result.done.notify_all();
    }
}
