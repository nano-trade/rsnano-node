use super::BlockValidator;
use crate::BlockError;
use rsnano_core::Block;

impl<'a> BlockValidator<'a> {
    pub(crate) fn ensure_block_is_not_for_burn_account(&self) -> Result<(), BlockError> {
        if self.account.is_zero() {
            Err(BlockError::OpenedBurnAccount)
        } else {
            Ok(())
        }
    }

    pub(crate) fn ensure_no_double_account_open(&self) -> Result<(), BlockError> {
        if self.account_exists() && self.block.is_open() {
            Err(BlockError::Fork)
        } else {
            Ok(())
        }
    }

    pub(crate) fn ensure_open_block_has_link(&self) -> Result<(), BlockError> {
        if let Block::State(state) = self.block {
            if self.block.is_open() && state.link().is_zero() {
                return Err(BlockError::GapSource);
            }
        }
        Ok(())
    }
}
