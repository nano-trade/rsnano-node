use crate::command_handler::RpcCommandHandler;
use rsnano_rpc_messages::{CountResponse, HashRpcMessage};

impl RpcCommandHandler {
    pub(crate) fn block_rollback(&self, args: HashRpcMessage) -> CountResponse {
        let result = self.node.ledger.rollback(&args.hash);
        let rolled_back = result.unwrap_or_default();
        CountResponse::new(rolled_back as u64)
    }
}
