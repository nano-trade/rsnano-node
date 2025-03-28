use rsnano_core::{AccountInfo, BlockHash, SavedBlock};
use rsnano_store_lmdb::LmdbWriteTransaction;

use crate::{AnySet, BorrowingAnySet, Ledger, LedgerSet};

use super::{
    instructions_executor::RollbackInstructionsExecutor, planner_factory::RollbackPlannerFactory,
    rollback_planner::RollbackStep,
};

pub(crate) struct BlockRollbackPerformer<'a> {
    ledger: &'a Ledger,
    pub txn: &'a mut LmdbWriteTransaction,
    pub rolled_back: Vec<SavedBlock>,
}

impl<'a> BlockRollbackPerformer<'a> {
    pub(crate) fn new(ledger: &'a Ledger, txn: &'a mut LmdbWriteTransaction) -> Self {
        Self {
            ledger,
            txn,
            rolled_back: Vec::new(),
        }
    }

    pub(crate) fn roll_back(&mut self, block_hash: &BlockHash) -> Result<(), RollbackError> {
        self.roll_back_block_and_successors(block_hash)?;
        Ok(())
    }

    fn any(&self) -> BorrowingAnySet {
        BorrowingAnySet {
            constants: &self.ledger.constants,
            store: &self.ledger.store,
            tx: self.txn,
        }
    }

    fn roll_back_head_block(&mut self, head_block: SavedBlock) -> Result<(), RollbackError> {
        let any = self.any();
        let planner =
            RollbackPlannerFactory::new(self.ledger, &any, &head_block).create_planner()?;
        let step = planner.roll_back_head_block()?;
        self.execute(step, head_block)?;
        Ok(())
    }

    fn execute(&mut self, step: RollbackStep, head_block: SavedBlock) -> Result<(), RollbackError> {
        match step {
            RollbackStep::RollBackBlock(instructions) => {
                RollbackInstructionsExecutor::new(self.ledger, self.txn, &instructions).execute();
                self.rolled_back.push(head_block);
                Ok(())
            }
            RollbackStep::RequestDependencyRollback(dependency_hash) => {
                self.roll_back_block_and_successors(&dependency_hash)
            }
        }
    }

    fn roll_back_block_and_successors(
        &mut self,
        block_hash: &BlockHash,
    ) -> Result<(), RollbackError> {
        let block = self.load_block(block_hash)?;
        while self.any().block_exists(block_hash) {
            let head_block = self.load_account_head(&block)?;
            self.roll_back_head_block(head_block)?;
        }
        Ok(())
    }

    fn load_account_head(&self, block: &SavedBlock) -> Result<SavedBlock, RollbackError> {
        let account_info = self.get_account_info(block);
        self.load_block(&account_info.head)
    }

    fn get_account_info(&self, block: &SavedBlock) -> AccountInfo {
        self.any()
            .get_account(&block.account())
            .expect("account not found")
    }

    fn load_block(&self, block_hash: &BlockHash) -> Result<SavedBlock, RollbackError> {
        self.any()
            .get_block(block_hash)
            .ok_or(RollbackError::BlockNotFound)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum RollbackError {
    /// The block to roll back wasn't found
    BlockNotFound,

    /// A confirmed block must not be rolled back!
    BlockConfirmed,

    PreviousBlockMissing,
    RepresentativeBlockMissing,
}
