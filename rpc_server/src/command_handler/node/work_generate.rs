use super::difficulty_ledger;
use crate::command_handler::RpcCommandHandler;
use anyhow::bail;
use rsnano_core::{Block, BlockType, DifficultyV1};
use rsnano_ledger::{AnySet, LedgerSet};
use rsnano_rpc_messages::{WorkGenerateArgs, WorkGenerateDto};

impl RpcCommandHandler {
    pub(crate) fn work_generate(&self, args: WorkGenerateArgs) -> anyhow::Result<WorkGenerateDto> {
        let default_difficulty = self.node.ledger.constants.work.threshold_base();
        let mut difficulty = args
            .difficulty
            .unwrap_or_else(|| default_difficulty.into())
            .inner();

        let max_difficulty = DifficultyV1::from_multiplier(
            self.node.config.max_work_generate_multiplier,
            default_difficulty,
        );

        // Validate difficulty
        if difficulty > max_difficulty
            || difficulty
                < self
                    .node
                    .network_params
                    .work
                    .threshold_entry(BlockType::State)
        {
            bail!("Difficulty out of range");
        }

        // Retrieving optional block
        if let Some(json_block) = args.block {
            let block: Block = json_block.into();
            if args.hash != block.root().into() {
                bail!("Block root mismatch");
            }
            // Recalculate difficulty if not provided
            if args.difficulty.is_none() && args.multiplier.is_none() {
                let any = self.node.ledger.any();
                difficulty = difficulty_ledger(self.node.clone(), &any, &block);
            }

            // If optional block difficulty is higher than requested difficulty, send error
            if self.node.network_params.work.difficulty_block(&block) >= difficulty {
                bail!("Provided work is already enough for given difficulty");
            }
        }

        let use_peers = args.use_peers.unwrap_or_default().inner();

        let work = if !use_peers {
            if self.node.work.work_generation_enabled() {
                self.node
                    .distributed_work
                    .make_blocking(args.hash.into(), difficulty, None)
            } else {
                bail!("Local work generation is disabled");
            }
        } else {
            let _account = if let Some(_account) = args.account {
                // Fetch account from block if not given
                let any = self.node.ledger.any();
                if any.block_exists(&args.hash) {
                    any.block_account(&args.hash)
                } else {
                    None
                }
            } else {
                None
            };

            // TODO implement
            bail!("Distributed work generation isn't implemented yet");
        };

        let Some(work) = work else {
            bail!("Work generation cancelled")
        };

        let result_difficulty = self
            .node
            .network_params
            .work
            .difficulty(&args.hash.into(), work);
        let result_multiplier = DifficultyV1::to_multiplier(result_difficulty, default_difficulty);

        Ok(WorkGenerateDto {
            hash: args.hash,
            work,
            difficulty: result_difficulty.into(),
            multiplier: Some(result_multiplier.into()),
        })
    }
}
