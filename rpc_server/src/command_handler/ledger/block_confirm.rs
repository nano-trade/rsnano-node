use std::time::SystemTime;

use rsnano_core::MaybeSavedBlock;
use rsnano_ledger::{AnySet, ConfirmedSet};
use rsnano_node::consensus::{ElectionStatus, ElectionStatusType};
use rsnano_rpc_messages::{HashRpcMessage, StartedResponse};

use crate::command_handler::RpcCommandHandler;

impl RpcCommandHandler {
    pub(crate) fn block_confirm(&self, args: HashRpcMessage) -> anyhow::Result<StartedResponse> {
        let any = self.node.ledger.any();
        let block = self.load_block_any(&any, &args.hash)?;
        if !any.confirmed().block_exists_or_pruned(&args.hash) {
            // Start new confirmation for unconfirmed (or not being confirmed) block
            if !self.node.confirming_set.contains(&args.hash) {
                self.node.election_schedulers.manual.push(block, None);
            }
        } else {
            // Add record in confirmation history for confirmed block
            let mut status = ElectionStatus::default();
            status.winner = Some(MaybeSavedBlock::Saved(block));
            status.election_end = SystemTime::now();
            status.block_count = 1;
            status.election_status_type = ElectionStatusType::ActiveConfirmationHeight;
            self.node.active.insert_recently_cemented(status);
        }
        Ok(StartedResponse::new(true))
    }
}
