use super::rollback_planner::RollbackPlanner;
use crate::{AnySet, ConfirmedSet, Ledger};
use rsnano_core::{
    utils::UnixTimestamp, Account, AccountInfo, Block, BlockHash, ConfirmationHeightInfo,
    PendingInfo, PendingKey, PublicKey, SavedBlock,
};

pub(crate) struct RollbackPlannerFactory<'a> {
    ledger: &'a Ledger,
    any: &'a dyn AnySet,
    head_block: &'a SavedBlock,
}

impl<'a> RollbackPlannerFactory<'a> {
    pub(crate) fn new(ledger: &'a Ledger, any: &'a dyn AnySet, head_block: &'a SavedBlock) -> Self {
        Self {
            ledger,
            any,
            head_block,
        }
    }

    pub(crate) fn create_planner(&self) -> anyhow::Result<RollbackPlanner<'a>> {
        let account = self.get_account(self.head_block)?;
        let planner = RollbackPlanner {
            epochs: &self.ledger.constants.epochs,
            head_block: self.head_block.clone(),
            account,
            current_account_info: self.load_account(&account),
            previous_representative: self.get_previous_representative()?,
            previous: self.load_previous_block()?,
            linked_account: self.load_linked_account(),
            pending_receive: self.load_pending_receive(),
            latest_block_for_destination: self.latest_block_for_destination(),
            confirmation_height: self.account_confirmation_height(),
            now: UnixTimestamp::now(),
        };
        Ok(planner)
    }

    fn latest_block_for_destination(&self) -> Option<BlockHash> {
        self.any
            .account_head(&self.head_block.destination_or_link())
    }

    fn load_pending_receive(&self) -> Option<PendingInfo> {
        self.any.get_pending(&PendingKey::new(
            self.head_block.destination_or_link(),
            self.head_block.hash(),
        ))
    }

    fn load_linked_account(&self) -> Account {
        self.any
            .block_account(&self.head_block.source_or_link())
            .unwrap_or_default()
    }

    fn load_previous_block(&self) -> anyhow::Result<Option<SavedBlock>> {
        let previous = self.head_block.previous();
        Ok(if previous.is_zero() {
            None
        } else {
            Some(self.load_block(&previous)?)
        })
    }

    fn account_confirmation_height(&self) -> ConfirmationHeightInfo {
        self.any
            .confirmed()
            .get_conf_info(&self.head_block.account())
            .unwrap_or_default()
    }

    fn get_account(&self, block: &Block) -> anyhow::Result<Account> {
        self.any
            .block_account(&block.hash())
            .ok_or_else(|| anyhow!("account not found"))
    }

    fn load_account(&self, account: &Account) -> AccountInfo {
        self.any.get_account(account).unwrap_or_default()
    }

    fn load_block(&self, block_hash: &BlockHash) -> anyhow::Result<SavedBlock> {
        self.any
            .get_block(block_hash)
            .ok_or_else(|| anyhow!("block not found"))
    }

    fn get_previous_representative(&self) -> anyhow::Result<Option<PublicKey>> {
        let previous = self.head_block.previous();
        let rep_block_hash = if !previous.is_zero() {
            self.any.representative_block_hash(&previous)
        } else {
            BlockHash::zero()
        };

        let previous_rep = if !rep_block_hash.is_zero() {
            let rep_block = self.load_block(&rep_block_hash)?;
            Some(rep_block.representative_field().unwrap_or_default())
        } else {
            None
        };
        Ok(previous_rep)
    }
}
