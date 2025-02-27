use crate::command_handler::RpcCommandHandler;
use anyhow::anyhow;
use rsnano_core::{BlockType, SavedBlock};
use rsnano_ledger::AnySet2;
use rsnano_rpc_messages::{
    unwrap_bool_or_false, BlockInfoArgs, BlockInfoResponse, BlockSubTypeDto,
};

impl RpcCommandHandler {
    pub(crate) fn block_info(&self, args: BlockInfoArgs) -> anyhow::Result<BlockInfoResponse> {
        let include_linked_account = unwrap_bool_or_false(args.include_linked_account);
        let any = self.node.ledger.any2();
        let block = any
            .detailed_block(&args.hash)
            .ok_or_else(|| anyhow!(Self::BLOCK_NOT_FOUND))?;

        let linked_account = if include_linked_account {
            match any.linked_account(&block.block) {
                Some(a) => Some(a.encode_account()),
                None => Some("0".to_owned()),
            }
        } else {
            None
        };

        Ok(BlockInfoResponse {
            block_account: block.block.account(),
            amount: block.amount,
            balance: block.block.balance(),
            height: block.block.height().into(),
            local_timestamp: block.block.timestamp().as_u64().into(),
            successor: block.block.successor().unwrap_or_default(),
            confirmed: block.confirmed.into(),
            contents: block.block.json_representation(),
            subtype: Self::subtype_for(&block.block),
            source_account: None,
            receive_hash: None,
            receivable: None,
            linked_account,
        })
    }

    fn subtype_for(block: &SavedBlock) -> Option<BlockSubTypeDto> {
        if block.block_type() == BlockType::State {
            Some(block.subtype().into())
        } else {
            None
        }
    }
}
