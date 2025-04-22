use super::BlockValidator;
use crate::BlockError;
use rsnano_core::PublicKey;

impl<'a> BlockValidator<'a> {
    pub(crate) fn ensure_block_does_not_exist_yet(&self) -> Result<(), BlockError> {
        if self.block_exists {
            Err(BlockError::Old)
        } else {
            Ok(())
        }
    }

    pub(crate) fn ensure_valid_signature(&self) -> Result<(), BlockError> {
        let result = if self.is_epoch_block() {
            self.epochs.validate_epoch_signature(self.block)
        } else {
            let pub_key: PublicKey = self.account.into();
            pub_key.verify(self.block.hash().as_bytes(), self.block.signature())
        };
        result.map_err(|_| BlockError::BadSignature)
    }

    pub(crate) fn ensure_account_exists_for_none_open_block(&self) -> Result<(), BlockError> {
        if !self.block.is_open() && self.is_new_account() {
            Err(BlockError::GapPrevious)
        } else {
            Ok(())
        }
    }

    pub(crate) fn ensure_previous_block_is_correct(&self) -> Result<(), BlockError> {
        self.ensure_previous_block_exists()?;
        self.ensure_previous_block_is_account_head()
    }

    fn ensure_previous_block_exists(&self) -> Result<(), BlockError> {
        if self.account_exists() && self.previous_block.is_none() {
            return Err(BlockError::GapPrevious);
        }

        if self.is_new_account() && !self.block.previous().is_zero() {
            return Err(BlockError::GapPrevious);
        }

        Ok(())
    }

    fn ensure_previous_block_is_account_head(&self) -> Result<(), BlockError> {
        if let Some(info) = &self.old_account_info {
            if self.block.previous() != info.head {
                return Err(BlockError::Fork);
            }
        }

        Ok(())
    }

    pub(crate) fn ensure_sufficient_work(&self) -> Result<(), BlockError> {
        if !self.work.is_valid_pow(self.block, &self.block_details()) {
            Err(BlockError::InsufficientWork)
        } else {
            Ok(())
        }
    }

    pub(crate) fn ensure_valid_predecessor(&self) -> Result<(), BlockError> {
        if self.block.previous().is_zero() {
            return Ok(());
        }

        let previous = self
            .previous_block
            .as_ref()
            .ok_or(BlockError::GapPrevious)?;

        if !self.block.valid_predecessor(previous.block_type()) {
            Err(BlockError::BlockPosition)
        } else {
            Ok(())
        }
    }
}
